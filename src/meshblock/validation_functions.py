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
    """Handles the execution of validation functions against the nrn dataset."""

    def __init__(self, nrn: gpd.GeoDataFrame, source: str, dst: Path, layer: str) -> None:
        """
        Initializes variables for validation functions.

        :param gpd.GeoDataFrame nrn: GeoDataFrame containing LineStrings.
        :param str source: abbreviation for the source province / territory.
        :param Path dst: output GeoPackage path.
        :param str layer: output GeoPackage layer name.
        """

        self.nrn = nrn.copy(deep=True)
        self.source = source
        self.dst = dst
        self.layer = layer
        self.untouchable_bos = Path(filepath.parents[2] / "data/interim/untouchable_bos.zip")
        self.errors = defaultdict(list)
        self.meshblock_ = None
        self._meshblock_input = None
        self.meshblock_progress = dict()
        self.id = "segment_id"
        self.bo_id = "ngd_uid"
        self._export = False
        self._nrn_bos_nodes = None
        self._nrn_roads_nodes_lookup = dict()
        self._nrn_bos_nodes_lookup = dict()

        # BO integration flag.
        self._integrated = None

        # Define thresholds.
        self._bo_nrn_prox = 5
        self._snap_prox = 0.1
        self._snap_clearance = 10

        # Resolve added BOs and export updated dataset, if required.
        flag_resolve1 = (self.nrn[self.bo_id].isna() | self.nrn[self.bo_id].isin({-1, 0, 1})) & \
                        (self.nrn["segment_type"] == 3)
        if sum(flag_resolve1):
            self.nrn.loc[flag_resolve1, "bo_new"] = 1
            self._export = True
        flag_resolve2 = (self.nrn["bo_new"] == 1) & (self.nrn["segment_type"] != 3)
        if sum(flag_resolve2):
            self.nrn.loc[flag_resolve2, "segment_type"] = 3
            self._export = True

        # Drop non-LineString geometries.
        invalid_geoms = ~self.nrn.geom_type.isin({"LineString", "MultiLineString"})
        if sum(invalid_geoms):
            self.nrn = self.nrn.loc[~invalid_geoms].copy(deep=True)
            self._export = True

        # Explode MultiLineStrings.
        if "MultiLineString" in set(self.nrn.geom_type):
            self.nrn = self.nrn.explode()

        # Resolve identifiers.
        self.nrn, _export = helpers.update_ids(self.nrn, identifier=self.id, index=True)
        if _export:
            self._export = True

        # Snap nodes of integrated arcs to NRN roads.
        self.nrn, _export = helpers.snap_nodes(self.nrn)
        if _export:
            self._export = True

        # Separate nrn BOs and roads.
        self.nrn_roads = self.nrn.loc[(self.nrn["segment_type"] == 1) |
                                      (self.nrn["segment_type"].isna())].copy(deep=True)
        self.nrn_bos = self.nrn.loc[self.nrn["segment_type"] == 3].copy(deep=True)

        logger.info("Configuring validations.")

        # Define validation.
        # Note: List validations in order if execution order matters.
        self.validations = {
            100: {"func": self.connectivity,
                  "desc": "All BOs must have nodal connections to other arcs."},
            101: {"func": self.connectivity_nrn_proximity,
                  "desc": "Unintegrated BO node is <= 5 meters from an NRN road (entire arc)."},
            102: {"func": self.connectivity_bo_missing,
                  "desc": "Untouchable BO identifier is missing."},
            200: {"func": self.meshblock,
                  "desc": "Generate meshblock from LineStrings."},
            201: {"func": self.meshblock_representation,
                  "desc": "All non-deadend arcs (excluding ferries) must form a meshblock polygon."}
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

            # Export data, if required.
            if self._export:
                helpers.export(self.nrn, dst=self.dst, name=self.layer)

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
        self.nrn_roads["nodes"] = self.nrn_roads["geometry"].map(
            lambda g: tuple(set(itemgetter(0, -1)(attrgetter("coords")(g)))))
        self.nrn_bos["nodes"] = self.nrn_bos["geometry"].map(
            lambda g: tuple(set(itemgetter(0, -1)(attrgetter("coords")(g)))))

        # Explode nodes collections and group identifiers based on shared nodes.
        nrn_roads_nodes_exp = self.nrn_roads["nodes"].explode()
        self._nrn_roads_nodes_lookup = dict(helpers.groupby_to_list(
            pd.DataFrame({"node": nrn_roads_nodes_exp.values, self.id: nrn_roads_nodes_exp.index}),
            group_field="node", list_field=self.id).map(tuple))

        nrn_bos_nodes_exp = self.nrn_bos["nodes"].explode()
        self._nrn_bos_nodes_lookup = dict(helpers.groupby_to_list(
            pd.DataFrame({"node": nrn_bos_nodes_exp.values, self.id: nrn_bos_nodes_exp.index}),
            group_field="node", list_field=self.id).map(tuple))

        # Explode BO node collections to allow for individual node validation.
        self._nrn_bos_nodes = self.nrn_bos["nodes"].explode().copy(deep=True)

        # Flag BO nodes connected to an nrn road node.
        self._integrated = self._nrn_bos_nodes.map(lambda node: node in self._nrn_roads_nodes_lookup)

        return errors

    def connectivity_bo_missing(self) -> dict:
        """
        Validates: Untouchable BO identifier is missing.

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # Load untouchable BO identifiers.
        untouchable_df = pd.read_csv(self.untouchable_bos)
        untouchable_ids = set(untouchable_df.loc[untouchable_df["pr"] == self.source, self.bo_id])

        # Compile missing untouchable BO identifiers.
        missing_ids = untouchable_ids - set(self.nrn[self.bo_id])

        # Compile error logs.
        if len(missing_ids):
            errors["values"] = missing_ids
            errors["query"] = f"\"{self.bo_id}\" in {*missing_ids,}"

        return errors

    def connectivity_nrn_proximity(self) -> dict:
        """
        Validates: Unintegrated BO node is <= 5 meters from an NRN road (entire arc).
        Enforces snapping of BO nodes to NRN nodes / edges within a given tolerance.

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # Compile unintegrated BO nodes.
        unintegrated_bo_nodes = pd.Series(tuple(set(self._nrn_bos_nodes.loc[~self._integrated])))

        # Generate simplified node buffers with distance tolerance.
        node_buffers = unintegrated_bo_nodes.map(lambda node: Point(node).buffer(self._bo_nrn_prox, resolution=5))

        # Query nrn roads which intersect each node buffer.
        node_intersects = node_buffers.map(
            lambda buffer: set(self.nrn_roads.sindex.query(buffer, predicate="intersects")))

        # Filter unintegrated bo nodes to those with buffers with one or more intersecting nrn roads.
        unintegrated_bo_nodes = unintegrated_bo_nodes.loc[node_intersects.map(len) >= 1]
        if len(unintegrated_bo_nodes):

            # Compile identifiers of arcs for resulting BO nodes.
            vals = set(chain.from_iterable(unintegrated_bo_nodes.map(self._nrn_bos_nodes_lookup).values))

            # Compile error logs.
            errors["values"] = vals
            errors["query"] = f"\"{self.id}\" in {*vals,}"

        return errors

    def meshblock(self) -> dict:
        """
        Validates: Generate meshblock from LineStrings.
        Note: This method exists to generate the dependant variables for various meshblock validations and is not
        intended to produce error logs itself.

        :return dict: placeholder dict based on standard validations. For this method, its contents will be unpopulated.
        """

        errors = {"values": list(), "query": None}

        # Configure meshblock input (all non-deadend and non-ferry arcs).
        nodes = self.nrn["geometry"].map(lambda g: itemgetter(0, -1)(attrgetter("coords")(g))).explode()
        deadends = set(nodes.loc[~nodes.duplicated(keep=False)].index)
        self._meshblock_input = self.nrn.loc[(~self.nrn.index.isin(deadends)) &
                                             (self.nrn["segment_type"] != 2)].copy(deep=True)

        # Generate meshblock.
        self.meshblock_ = gpd.GeoDataFrame(
            geometry=list(polygonize(unary_union(self._meshblock_input["geometry"].to_list()))),
            crs=self._meshblock_input.crs)

        return errors

    def meshblock_representation(self) -> dict:
        """
        Validates: All non-deadend arcs (excluding ferries) must form a meshblock polygon.

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # Extract boundary LineStrings from meshblock Polygons.
        meshblock_boundaries = self.meshblock_.boundary

        # Query meshblock polygons which cover each segment.
        covered_by = self.nrn_bos.loc[self.nrn_bos["bo_new"] != 1, "geometry"].map(
            lambda g: set(meshblock_boundaries.sindex.query(g, predicate="covered_by")))

        # Flag segments which do not form a polygon.
        flag = covered_by.map(len) == 0

        # Compile error logs.
        if sum(flag):
            vals = set(covered_by.loc[flag].index)
            errors["values"] = vals
            errors["query"] = f"\"{self.id}\" in {*vals,}"

        # Populate progress tracker with total meshblock input, excluded, and flagged record counts.
        self.meshblock_progress["Valid"] = len(self._meshblock_input) - sum(flag)
        self.meshblock_progress["Invalid"] = sum(flag)
        self.meshblock_progress["Excluded"] = len(self.nrn) - len(self._meshblock_input)

        return errors
