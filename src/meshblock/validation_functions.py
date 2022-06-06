import geopandas as gpd
import logging
import pandas as pd
import sys
from collections import defaultdict
from copy import deepcopy
from itertools import chain
from operator import attrgetter, itemgetter
from pathlib import Path
from shapely.geometry import Point
from shapely.ops import polygonize, unary_union

filepath = Path(__file__).resolve()
sys.path.insert(1, str(Path(__file__).resolve().parents[1]))
import helpers

# Set logger.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)


class Validator:
    """Handles the execution of validation functions against the crn dataset."""

    def __init__(self, crn: gpd.GeoDataFrame, source: str, dst: Path, layer: str) -> None:
        """
        Initializes variables for validation functions.

        :param gpd.GeoDataFrame crn: GeoDataFrame containing LineStrings.
        :param str source: abbreviation for the source province / territory.
        :param Path dst: output GeoPackage path.
        :param str layer: output GeoPackage layer name.
        """

        self.crn = crn.copy(deep=True)
        self.source = source
        self.dst = dst
        self.layer = layer
        self.src_restore = Path(filepath.parents[2] / "data/nrn_bo_restore.gpkg")
        self.errors = defaultdict(list)
        self.meshblock_ = None
        self._meshblock_input = None
        self.meshblock_progress = {k: 0 for k in ("Valid", "Invalid", "Excluded")}
        self.id = "segment_id"
        self.bo_id = "ngd_uid"
        self._crn_bos_nodes = None
        self._crn_roads_nodes_lookup = dict()
        self._crn_bos_nodes_lookup = dict()
        self._deadends = set()

        # BO integration flag.
        self._integrated = None

        # Define thresholds.
        self._bo_road_prox = 5

        # Standardize data.
        self.crn = helpers.standardize(self.crn)

        # Snap nodes of integrated arcs to CRN roads.
        self.crn = helpers.snap_nodes(self.crn)

        # Separate crn BOs and roads.
        self.crn_roads = self.crn.loc[(self.crn["segment_type"] == 1) |
                                      (self.crn["segment_type"].isna())].copy(deep=True)
        self.crn_bos = self.crn.loc[self.crn["segment_type"] == 3].copy(deep=True)

        # Load source restoration data.
        logger.info(f"Loading source restoration data: {self.src_restore}|layer={self.layer}.")
        self.df_restore = gpd.read_file(self.src_restore, layer=self.layer)
        logger.info("Successfully loaded source restoration data.")

        logger.info("Configuring validations.")

        # Define validation.
        # Note: List validations in order if execution order matters.
        self.validations = {
            100: {"func": self.connectivity,
                  "desc": "All BOs must have nodal connections to other arcs."},
            101: {"func": self.connectivity_crn_proximity,
                  "desc": "Unintegrated BO node is <= 5 meters from a CRN road (entire arc)."},
            102: {"func": self.connectivity_bo_missing,
                  "desc": "Untouchable BO identifier is missing."},
            200: {"func": self.meshblock,
                  "desc": "Generate meshblock from LineStrings."},
            201: {"func": self.meshblock_representation_deadend,
                  "desc": "All non-deadend arcs (excluding ferries) must form a meshblock polygon."},
            202: {"func": self.meshblock_representation_non_deadend,
                  "desc": "All deadend arcs (excluding ferries) must be completely within 1 meshblock polygon."}
        }

    def __call__(self) -> None:
        """Orchestrates the execution of validation functions and compiles the resulting errors."""

        try:

            # Iterate validations.
            for code, params in self.validations.items():
                func, description = itemgetter("func", "desc")(params)

                logger.info(f"Applying validation E{code}: \"{func.__name__}\".")

                # Execute validation and store non-empty results.
                results = func()
                if len(results["values"]):
                    self.errors[f"E{code} - {description}"] = deepcopy(results)

            # Export data.
            helpers.export(self.crn, dst=self.dst, name=self.layer)

            # Populate progress tracker with total meshblock input, excluded, and flagged record counts.
            self.meshblock_progress["Valid"] = len(self._meshblock_input) - self.meshblock_progress["Invalid"]
            self.meshblock_progress["Excluded"] = len(self.crn) - len(self._meshblock_input)

        except (KeyError, SyntaxError, ValueError) as e:
            logger.exception("Unable to apply validation.")
            logger.exception(e)
            sys.exit(1)

    def connectivity(self) -> dict:
        """
        Validation: All BOs must have nodal connections to other arcs.
        Note: This method exists to generate the dependant variables for various connectivity validations and is not
        intended to produce error logs itself.

        :return dict: placeholder dict based on standard validations. For this method, its contents will be unpopulated.
        """

        errors = {"values": list(), "query": None}

        # Extract nodes.
        self.crn_roads["nodes"] = self.crn_roads["geometry"].map(
            lambda g: tuple(set(itemgetter(0, -1)(attrgetter("coords")(g)))))
        self.crn_bos["nodes"] = self.crn_bos["geometry"].map(
            lambda g: tuple(set(itemgetter(0, -1)(attrgetter("coords")(g)))))

        # Explode nodes collections and group identifiers based on shared nodes.
        crn_roads_nodes_exp = self.crn_roads["nodes"].explode()
        self._crn_roads_nodes_lookup = dict(helpers.groupby_to_list(
            pd.DataFrame({"node": crn_roads_nodes_exp.values, self.id: crn_roads_nodes_exp.index}),
            group_field="node", list_field=self.id).map(tuple))

        crn_bos_nodes_exp = self.crn_bos["nodes"].explode()
        self._crn_bos_nodes_lookup = dict(helpers.groupby_to_list(
            pd.DataFrame({"node": crn_bos_nodes_exp.values, self.id: crn_bos_nodes_exp.index}),
            group_field="node", list_field=self.id).map(tuple))

        # Explode BO node collections to allow for individual node validation.
        self._crn_bos_nodes = self.crn_bos["nodes"].explode().copy(deep=True)

        # Flag BO nodes connected to an crn road node.
        self._integrated = self._crn_bos_nodes.map(lambda node: node in self._crn_roads_nodes_lookup)

        return errors

    def connectivity_bo_missing(self) -> dict:
        """
        Validates: Untouchable BO identifier is missing.

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # Compile untouchable BO identifiers.
        untouchable_ids = set(self.df_restore.loc[self.df_restore["boundary"] == 1, self.bo_id])

        # Compile missing untouchable BO identifiers.
        missing_ids = untouchable_ids - set(self.crn[self.bo_id])

        # Compile error logs.
        if len(missing_ids):
            errors["values"] = missing_ids
            errors["query"] = f"\"{self.bo_id}\" in {*missing_ids,}".replace(",)", ")")

        return errors

    def connectivity_crn_proximity(self) -> dict:
        """
        Validates: Unintegrated BO node is <= 5 meters from a CRN road (entire arc).

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # Compile unintegrated BO nodes.
        unintegrated_bo_nodes = pd.Series(tuple(set(self._crn_bos_nodes.loc[~self._integrated])))

        # Generate simplified node buffers with distance tolerance.
        node_buffers = unintegrated_bo_nodes.map(lambda node: Point(node).buffer(self._bo_road_prox, resolution=5))

        # Query crn roads which intersect each node buffer.
        node_intersects = node_buffers.map(
            lambda buffer: set(self.crn_roads.sindex.query(buffer, predicate="intersects")))

        # Filter unintegrated bo nodes to those with buffers with one or more intersecting crn roads.
        unintegrated_bo_nodes = unintegrated_bo_nodes.loc[node_intersects.map(len) >= 1]
        if len(unintegrated_bo_nodes):

            # Compile identifiers of arcs for resulting BO nodes.
            vals = set(chain.from_iterable(unintegrated_bo_nodes.map(self._crn_bos_nodes_lookup).values))

            # Compile error logs.
            errors["values"] = vals
            errors["query"] = f"\"{self.id}\" in {*vals,}".replace(",)", ")")

        return errors

    def meshblock(self) -> dict:
        """
        Validates: Generate meshblock from LineStrings.
        Note: This method exists to generate the dependant variables for various meshblock validations and is not
        intended to produce error logs itself.

        :return dict: placeholder dict based on standard validations. For this method, its contents will be unpopulated.
        """

        errors = {"values": list(), "query": None}

        # Compile indexes of deadend arcs.
        nodes = self.crn["geometry"].map(lambda g: itemgetter(0, -1)(attrgetter("coords")(g))).explode()
        self._deadends = set(nodes.loc[~nodes.duplicated(keep=False)].index)

        # Configure meshblock input (all non-deadend and non-ferry arcs).
        self._meshblock_input = self.crn.loc[(~self.crn.index.isin(self._deadends)) &
                                             (self.crn["segment_type"] != 2)].copy(deep=True)

        # Generate meshblock.
        self.meshblock_ = gpd.GeoDataFrame(
            geometry=list(polygonize(unary_union(self._meshblock_input["geometry"].to_list()))),
            crs=self._meshblock_input.crs)

        return errors

    def meshblock_representation_deadend(self) -> dict:
        """
        All deadend arcs (excluding ferries) must be completely within 1 meshblock polygon.

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # Query meshblock polygons which contain each deadend arc.
        within = self.crn.loc[(self.crn.index.isin(self._deadends)) & (self.crn["segment_type"] != 2), "geometry"]\
            .map(lambda g: set(self.meshblock_.sindex.query(g, predicate="within")))

        # Flag arcs which are not completely within one polygon.
        flag = within.map(len) != 1

        # Compile error logs.
        if sum(flag):
            vals = set(within.loc[flag].index)
            errors["values"] = vals
            errors["query"] = f"\"{self.id}\" in {*vals,}".replace(",)", ")")

            # Update invalid count for progress tracker.
            self.meshblock_progress["Invalid"] += sum(flag)

        return errors

    def meshblock_representation_non_deadend(self) -> dict:
        """
        Validates: All non-deadend arcs (excluding ferries) must form a meshblock polygon.

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # Extract boundary LineStrings from meshblock Polygons.
        meshblock_boundaries = self.meshblock_.boundary

        # Query meshblock polygons which cover each arc.
        covered_by = self.crn_bos.loc[self.crn_bos["bo_new"] != 1, "geometry"].map(
            lambda g: set(meshblock_boundaries.sindex.query(g, predicate="covered_by")))

        # Flag arcs which do not form a polygon.
        flag = covered_by.map(len) == 0

        # Compile error logs.
        if sum(flag):
            vals = set(covered_by.loc[flag].index)
            errors["values"] = vals
            errors["query"] = f"\"{self.id}\" in {*vals,}".replace(",)", ")")

            # Update invalid count for progress tracker.
            self.meshblock_progress["Invalid"] += sum(flag)

        return errors
