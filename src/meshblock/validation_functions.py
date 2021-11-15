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

    def __init__(self, nrn: gpd.GeoDataFrame, dst: Path, layer: str) -> None:
        """
        Initializes variables for validation functions.

        :param gpd.GeoDataFrame nrn: GeoDataFrame containing LineStrings.
        :param Path dst: output GeoPackage path.
        :param str layer: output GeoPackage layer name.
        """

        self.nrn = nrn.copy(deep=True)
        self.dst = dst
        self.layer = layer
        self.errors = defaultdict(list)
        self.meshblock_progress = dict()
        self.id = "segment_id"
        self._export = False
        self._nrn_bos_nodes = None
        self._nrn_roads_nodes_lookup = dict()
        self._nrn_bos_nodes_lookup = dict()

        # BO integration flag.
        self._integrated = None

        # Resolve identifiers.
        self.nrn, self._export = helpers.update_ids(self.nrn, identifier=self.id, index=True)

        # Separate nrn BOs and roads.
        self.nrn_roads = self.nrn.loc[(self.nrn["segment_type"] == 1) |
                                      (self.nrn["segment_type"].isna())].copy(deep=True)
        self.nrn_bos = self.nrn.loc[self.nrn["segment_type"] == 3].copy(deep=True)

        # Configure meshblock variables (all non-ferry and non-ignore arcs).
        self.meshblock_ = None
        if "ignore_201" in self.nrn.columns:
            self.meshblock_input = self.nrn.loc[(self.nrn["segment_type"] != 2) &
                                                (self.nrn["ignore_201"] != 1)].copy(deep=True)
        else:
            self.meshblock_input = self.nrn.loc[self.nrn["segment_type"] != 2].copy(deep=True)

            # Add ignore attribute.
            self.nrn["ignore_201"] = 0
            self._export = True

        logger.info("Configuring validations.")

        # Define validation.
        # Note: List validations in order if execution order matters.
        self.validations = {
            100: {"func": self.connectivity,
                  "desc": "All BOs must have nodal connections to other arcs."},
            101: {"func": self.connectivity_nrn_proximity,
                  "desc": "Unintegrated BO node is <= 5 meters from an NRN road (entire arc)."},
            200: {"func": self.meshblock,
                  "desc": "Generate meshblock from LineStrings."},
            201: {"func": self.meshblock_representation,
                  "desc": "All non-deadend arcs (excluding ferries) must form a single meshblock polygon on both left "
                          "and right sides, or just one side for boundary arcs."}
        }

        # Define validation thresholds.
        self._bo_nrn_proximity = 5

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

    def connectivity_nrn_proximity(self) -> dict:
        """
        Validates: Unintegrated BO node is <= 5 meters from an NRN road (entire arc).

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # Compile unintegrated BO nodes.
        unintegrated_bo_nodes = pd.Series(tuple(set(self._nrn_bos_nodes.loc[~self._integrated])))

        # Generate simplified node buffers with distance tolerance.
        node_buffers = unintegrated_bo_nodes.map(lambda node: Point(node).buffer(self._bo_nrn_proximity, resolution=5))

        # Query nrn roads which intersect each node buffer.
        node_intersects = node_buffers.map(
            lambda buffer: set(self.nrn_roads.sindex.query(buffer, predicate="intersects")))

        # Filter unintegrated bo nodes to those with buffers with one or more intersecting nrn roads.
        unintegrated_bo_nodes = unintegrated_bo_nodes.loc[node_intersects.map(len) >= 1]
        if len(unintegrated_bo_nodes):

            # Compile identifiers of arcs for resulting BO nodes.
            vals = tuple(chain.from_iterable(unintegrated_bo_nodes.map(self._nrn_bos_nodes_lookup).values))

            # Compile error logs.
            errors["values"] = set(vals)
            errors["query"] = f"\"{self.id}\" in {*set(vals),}"

        return errors

    def meshblock(self) -> dict:
        """
        Validates: Generate meshblock from LineStrings.
        Note: This method exists to generate the dependant variables for various meshblock validations and is not
        intended to produce error logs itself.

        :return dict: placeholder dict based on standard validations. For this method, its contents will be unpopulated.
        """

        errors = {"values": list(), "query": None}

        # Generate meshblock.
        self.meshblock_ = gpd.GeoDataFrame(
            geometry=list(polygonize(unary_union(self.meshblock_input["geometry"].to_list()))),
            crs=self.meshblock_input.crs)

        return errors

    def meshblock_representation(self) -> dict:
        """
        Validates: All non-deadend arcs (excluding ferries) must form a single meshblock polygon on both left and right
        sides, or just one side for boundary arcs.

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # Query meshblock polygons which cover each segment.
        covered_by = self.meshblock_input["geometry"].map(
            lambda g: set(self.meshblock_.sindex.query(g, predicate="covered_by")))

        # Flag segments which do not have 2 covering polygons.
        flag = covered_by.map(len) != 2

        # Invert flag for boundary arcs which have 1 covering polygon.
        invert_flag_idxs = set((covered_by.loc[flag & (self.meshblock_input["boundary"] == 1)].map(len) == 1).index)
        if len(invert_flag_idxs):
            flag.loc[flag.index.isin(invert_flag_idxs)] = False

        # Compile error logs.
        if sum(flag):
            vals = set(covered_by.loc[flag].index)
            errors["values"] = vals
            errors["query"] = f"\"{self.id}\" in {*vals,}"

        # Populate progress tracker with total meshblock input, ignored, and flagged record counts.
        self.meshblock_progress["Valid"] = len(self.meshblock_input) - sum(flag)
        self.meshblock_progress["Invalid"] = sum(flag)
        self.meshblock_progress["Ignored"] = len(self.nrn) - len(self.meshblock_input)

        return errors

    def execute(self) -> None:
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
