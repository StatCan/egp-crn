import geopandas as gpd
import logging
import pandas as pd
import string
import sys
import uuid
from collections import defaultdict
from copy import deepcopy
from itertools import chain
from operator import attrgetter, itemgetter
from pathlib import Path
from shapely.geometry import Point

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

    def __init__(self, nrn: gpd.GeoDataFrame, ngd: gpd.GeoDataFrame, dst: Path, layer: str) -> None:
        """
        Initializes variables for validation functions.

        :param gpd.GeoDataFrame nrn: GeoDataFrame containing LineStrings.
        :param gpd.GeoDataFrame ngd: GeoDataFrame containing LineStrings.
        :param Path dst: output GeoPackage path.
        :param str layer: output GeoPackage layer name.
        """

        self.nrn = nrn.copy(deep=True)
        self.ngd = ngd.copy(deep=True)
        self.dst = dst
        self.layer = layer
        self.errors = defaultdict(list)
        self.integration_progress = dict()
        self.id = "segment_id"
        self._export = False
        self._nrn_bos_nodes = None
        self._nrn_roads_nodes_lookup = dict()
        self._nrn_bos_nodes_lookup = dict()
        self._ngd_nodes_lookup = dict()

        # BO integration flag.
        self._integrated = None

        # Resolve identifiers.
        self._update_ids()

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
            101: {"func": self.connectivity_integrated,
                  "desc": "BO node connects to NRN node."},
            102: {"func": self.connectivity_unintegrated,
                  "desc": "BO node does not connect to NRN node."},
            103: {"func": self.connectivity_nrn_proximity,
                  "desc": "Unintegrated BO node is <= 5 meters from an NRN road (entire arc)."},
            201: {"func": self.representation_existence,
                  "desc": "All BO ngd_uids must exist."},
            202: {"func": self.representation_duplication,
                  "desc": "Duplicated BO ngd_uids must be contiguous (gaps bridged by NRN roads are acceptable)."}
        }

        # Define validation thresholds.
        self._bo_nrn_proximity = 5

    def _update_ids(self, index: bool = True) -> None:
        """
        Updates identifiers if they are not unique 32 digit hexadecimal strings.

        :param bool index: assigns the identifier column as GeoDataFrame index, default = True.
        """

        logger.info(f"Resolving nrn identifiers for: \"{self.id}\".")

        try:

            self.nrn[self.id] = self.nrn[self.id].astype(str)

            # Flag invalid identifiers.
            hexdigits = set(string.hexdigits)
            flag_non_hex = (self.nrn[self.id].map(len) != 32) | \
                           (self.nrn[self.id].map(lambda val: not set(val).issubset(hexdigits)))
            flag_dups = (self.nrn[self.id].duplicated(keep=False)) & (self.nrn[self.id] != "None")
            flag_invalid = flag_non_hex | flag_dups

            # Resolve invalid identifiers.
            if sum(flag_invalid):
                logger.warning(f"Resolving {sum(flag_invalid)} invalid identifiers for: \"{self.id}\".")

                # Overwrite identifiers.
                self.nrn.loc[flag_invalid, self.id] = [uuid.uuid4().hex for _ in range(sum(flag_invalid))]

                # Trigger export requirement for class.
                self._export = True

            # Assign index.
            if index:
                self.nrn.index = self.nrn[self.id]

        except ValueError as e:
            logger.exception(f"Unable to validate segment identifiers for \"{self.id}\".")
            logger.exception(e)
            sys.exit(1)

    def connectivity(self) -> dict:
        """
        Validation: All BOs must have nodal connections to other arcs.
        Note: The output of each connectivity validation feeds into the next. Therefore, this method exists to generate
        the dependant variables for various connectivity validations and is not intended to produce error logs itself.

        :return dict: placeholder dict based on standard validations. For this method, its contents will be unpopulated.
        """

        errors = {"values": list(), "query": None}

        # Extract nodes.
        self.nrn_roads["nodes"] = self.nrn_roads["geometry"].map(
            lambda g: tuple(set(itemgetter(0, -1)(attrgetter("coords")(g)))))
        self.nrn_bos["nodes"] = self.nrn_bos["geometry"].map(
            lambda g: tuple(set(itemgetter(0, -1)(attrgetter("coords")(g)))))
        self.ngd["nodes"] = self.ngd["geometry"].map(
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

        ngd_nodes_exp = self.ngd["nodes"].explode()
        self._ngd_nodes_lookup = dict(helpers.groupby_to_list(
            pd.DataFrame({"node": ngd_nodes_exp.values, self.id: ngd_nodes_exp.index}),
            group_field="node", list_field=self.id).map(tuple))

        # Explode BO node collections to allow for individual node validation.
        self._nrn_bos_nodes = self.nrn_bos["nodes"].explode().copy(deep=True)

        # Populate progress tracker with total BO node count.
        self.integration_progress["Total"] = len(self._nrn_bos_nodes)

        return errors

    def connectivity_integrated(self) -> dict:
        """
        Validates: BO node connects to NRN node.

        :return dict: placeholder dict based on standard validations. For this method, its contents will be unpopulated.
        """

        errors = {"values": list(), "query": None}

        # Flag BO nodes connected to an nrn road node.
        self._integrated = self._nrn_bos_nodes.map(lambda node: node in self._nrn_roads_nodes_lookup)

        # Populate progress tracker with BO node count.
        self.integration_progress["Integrated"] = sum(self._integrated)

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

            # Populate progress tracker with BO node count.
            self.integration_progress[f"Unintegrated (all) - within NRN proximity ({self._bo_nrn_proximity} m)"] =\
                len(vals)

        return errors

    def connectivity_unintegrated(self) -> dict:
        """
        Validates: BO node does not connect to NRN node.
        A) BO node connects only to NGD road node.
        B) BO node connects only to another BO node.
        C) BO node connects only to NGD road node and another BO node.
        D) BO is isolated (i.e. connected only to itself).

        :return dict: placeholder dict based on standard validations. For this method, its contents will be unpopulated.
        """

        errors = {"values": list(), "query": None}

        # A) Flag BO nodes that connect only to NGD road nodes.
        flag_ngd_connection = (~self._integrated) & (self._nrn_bos_nodes.map(
            lambda node: (node in self._ngd_nodes_lookup) & (len(itemgetter(node)(self._nrn_bos_nodes_lookup)) == 1)))

        # Populate progress tracker with BO node count.
        self.integration_progress["Unintegrated (BO-to-NGD)"] = sum(flag_ngd_connection)

        # B) Flag BO nodes that connect only to another BO node.
        flag_bo_connection = (~self._integrated) & (self._nrn_bos_nodes.map(
            lambda node: (node not in self._ngd_nodes_lookup) &
                         (len(itemgetter(node)(self._nrn_bos_nodes_lookup)) > 1)))

        # Populate progress tracker with BO node count.
        self.integration_progress["Unintegrated (BO-to-BO)"] = sum(flag_bo_connection)

        # C) Flag BO nodes that connect only to NGD road nodes and another BO node.
        flag_ngd_and_bo_connection = (~self._integrated) & (self._nrn_bos_nodes.map(
            lambda node: (node in self._ngd_nodes_lookup) & (len(itemgetter(node)(self._nrn_bos_nodes_lookup)) > 1)))

        # Populate progress tracker with BO node count.
        self.integration_progress["Unintegrated (BO-to-BO\\NGD)"] =\
            sum(flag_ngd_and_bo_connection)

        # D) Flag BO nodes that are isolated (i.e. connected only to itself).
        flag_no_connection = (~self._integrated) & (self._nrn_bos_nodes.map(
            lambda node: (node not in self._ngd_nodes_lookup) &
                         (len(itemgetter(node)(self._nrn_bos_nodes_lookup)) == 1)))

        # Populate progress tracker with BO node count.
        self.integration_progress["Unintegrated (BO-to-None)"] = sum(flag_no_connection)

        return errors

    def representation_duplication(self) -> dict:
        """
        Validates: Duplicated BO ngd_uids must be contiguous (gaps bridged by NRN roads are acceptable).

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # TODO

        return errors

    def representation_existence(self) -> dict:
        """
        Validates: All BO ngd_uids must exist.

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # TODO

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
