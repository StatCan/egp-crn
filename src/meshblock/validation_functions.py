import geopandas as gpd
import logging
import pandas as pd
import sys
from collections import defaultdict
from copy import deepcopy
from itertools import chain, tee
from operator import attrgetter, itemgetter
from pathlib import Path
from shapely.geometry import LineString, MultiLineString, Point
from shapely.ops import nearest_points, polygonize, unary_union
from typing import Dict

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
        self.untouchable_bos = Path(filepath.parents[2] / "data/interim/untouchable_bos.csv")
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
        self._snap_prox_min = 0.01
        self._snap_prox_max = 10

        # Resolve added BOs and export updated dataset, if required.
        flag_resolve = (self.nrn[self.bo_id].isna() | self.nrn[self.bo_id].isin({-1, 0, 1})) & \
                       (self.nrn["segment_type"] == 3)
        if sum(flag_resolve):
            if "bo_new" not in self.nrn.columns:
                self.nrn["bo_new"] = 0
            self.nrn.loc[flag_resolve, "bo_new"] = 1
            self._export = True

        # Drop non-LineString geometries.
        invalid_geoms = ~self.nrn.geom_type.isin({"LineString", "MultiLineString"})
        if sum(invalid_geoms):
            self.nrn = self.nrn.loc[~invalid_geoms].copy(deep=True)
            self._export = True

        # Explode MultiLineStrings and resolve identifiers.
        if "MultiLineString" in set(self.nrn.geom_type):
            self.nrn = self.nrn.explode()
        self.nrn, self._export = helpers.update_ids(self.nrn, identifier=self.id, index=True)

        # Snap nodes of integrated arcs to NRN roads.
        self._snap_arcs()

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

    def _snap_arcs(self) -> None:
        """
        1) Snaps non-NRN road nodes to NRN road nodes if they are <= the minimum snapping distance.
        2) Snaps non-NRN road nodes to NRN road edges if they are both:
            a) <= the minimum snapping distance from an NRN road edge
            b) > the maximum snapping distance from an NRN road node
        """

        # Compile nodes.
        nrn_roads_nodes = set(self.nrn.loc[self.nrn["segment_type"] == 1, "geometry"].map(
            lambda g: tuple(set(itemgetter(0, -1)(attrgetter("coords")(g))))).explode())
        non_nrn_roads_nodes = self.nrn.loc[
            (~self.nrn["segment_type"].isin({1, 2})) & (self.nrn["boundary"] != 1), "geometry"].map(
            lambda g: tuple(set(itemgetter(0, -1)(attrgetter("coords")(g))))).explode()

        # Compile snappable nodes (non-nrn roads nodes not connected to an nrn road node).
        snap_nodes = non_nrn_roads_nodes.loc[~non_nrn_roads_nodes.isin(nrn_roads_nodes)].copy(deep=True)
        if len(snap_nodes):

            # Compile nrn roads edges (full arcs).
            nrn_roads_edges = self.nrn.loc[self.nrn["segment_type"] == 1, "geometry"].copy(deep=True)

            # Compile nrn road nodes as Points.
            nrn_roads_nodes = gpd.GeoSeries(map(Point, set(nrn_roads_nodes)), crs=self.nrn.crs)

            # Generate simplified node buffers using distance tolerance.
            snap_node_buffers_min = snap_nodes.map(lambda pt: Point(pt).buffer(self._snap_prox_min, resolution=5))
            snap_node_buffers_max = snap_nodes.map(lambda pt: Point(pt).buffer(self._snap_prox_max, resolution=5))

            # Query nrn road nodes and edges which intersect each node buffer.
            # Construct DataFrame containing results.
            snap_features = pd.DataFrame({
                "from_node": snap_nodes,
                "to_node": snap_node_buffers_min.map(
                    lambda buffer: set(nrn_roads_nodes.sindex.query(buffer, predicate="intersects"))),
                "to_node_max": snap_node_buffers_max.map(
                    lambda buffer: set(nrn_roads_nodes.sindex.query(buffer, predicate="intersects"))),
                "to_edge": snap_node_buffers_min.map(
                    lambda buffer: set(nrn_roads_edges.sindex.query(buffer, predicate="intersects")))
            })

            # Snapping type: NRN road node.
            # Filter snappable nodes to those with buffers intersecting >= 1 nrn road node.
            snap_nodes = snap_features.loc[snap_features["to_node"].map(len) >= 1].copy(deep=True)
            if len(snap_nodes):

                # Compile target nrn road nodes as a lookup dict.
                to_node_idxs = set(chain.from_iterable(snap_nodes["to_node"]))
                to_nodes = nrn_roads_nodes.loc[nrn_roads_nodes.index.isin(to_node_idxs)]
                to_node_lookup = dict(zip(to_nodes.index, to_nodes.map(
                    lambda pt: itemgetter(0)(attrgetter("coords")(pt)))))

                # Replace snappable target node sets with node tuple of first result.
                snap_nodes["to_node"] = snap_nodes["to_node"].map(
                    lambda idxs: itemgetter(tuple(idxs)[0])(to_node_lookup))

                # Create node snapping lookup and update required arcs.
                snap_nodes_lookup = dict(zip(snap_nodes["from_node"], snap_nodes["to_node"]))
                snap_arc_ids = set(snap_nodes.index)
                self.nrn.loc[self.nrn[self.id].isin(snap_arc_ids), "geometry"] =\
                    self.nrn.loc[self.nrn[self.id].isin(snap_arc_ids), "geometry"].map(
                        lambda g: self._update_nodes(g, node_map=snap_nodes_lookup))

                # Log modifications and trigger export.
                logger.warning(f"Snapped {len(snap_nodes)} non-NRN nodes to NRN nodes based on {self._snap_prox_min} m "
                               f"threshold.")
                self._export = True

            # Snapping type: NRN road edge.
            # Filter snappable nodes to those with buffers intersecting 0 nrn road nodes within the maximum threshold
            # and >= 1 nrn road edge within the minimum threshold.
            snap_nodes = snap_features.loc[(snap_features["to_node_max"].map(len) == 0) &
                                           (snap_features["to_edge"].map(len) >= 1)].copy(deep=True)
            if len(snap_nodes):

                # Compile target nrn road edge identifiers and geometries as lookup dicts.
                to_edge_idx_id_lookup = dict(zip(range(len(nrn_roads_edges)), nrn_roads_edges.index))
                to_edge_idx_geom_lookup = dict(zip(range(len(nrn_roads_edges)), nrn_roads_edges))

                # Compile identifiers and geometries for first result of snappable target edge sets.
                snap_nodes["to_edge_id"] = snap_nodes["to_edge"].map(
                    lambda idxs: itemgetter(tuple(idxs)[0])(to_edge_idx_id_lookup))
                snap_nodes["to_edge_geom"] = snap_nodes["to_edge"].map(
                    lambda idxs: itemgetter(tuple(idxs)[0])(to_edge_idx_geom_lookup))

                # Configure nearest snap point on each target edge.
                snap_nodes["to_edge_pt"] = snap_nodes[["from_node", "to_edge_geom"]].apply(
                    lambda row: attrgetter("coords")(nearest_points(Point(row[0]), row[1])[-1])[0], axis=1)

                # Create node snapping lookup and update required arcs.
                snap_nodes_lookup = dict(zip(snap_nodes["from_node"], snap_nodes["to_edge_pt"]))
                snap_arc_ids = set(snap_nodes.index)
                self.nrn.loc[self.nrn[self.id].isin(snap_arc_ids), "geometry"] =\
                    self.nrn.loc[self.nrn[self.id].isin(snap_arc_ids), "geometry"].map(
                        lambda g: self._update_nodes(g, node_map=snap_nodes_lookup))

                # Split nrn arcs at snapping points (split points).

                # Create arc split points lookup and update required arcs.
                split_arc_ids = set(snap_nodes["to_edge_id"])
                split_arcs = helpers.groupby_to_list(snap_nodes, group_field="to_edge_id", list_field="to_edge_pt")
                split_id_pts_lookup = dict(zip(split_arcs.index, split_arcs.map(tuple).values))
                split_id_geom_lookup = dict(self.nrn.loc[self.nrn[self.id].isin(split_arc_ids), "geometry"])

                self.nrn.loc[self.nrn[self.id].isin(split_arc_ids), "geometry"] =\
                    self.nrn.loc[self.nrn[self.id].isin(split_arc_ids), self.id].map(
                        lambda val: self._split_arc(itemgetter(val)(split_id_geom_lookup),
                                                    split_pts=itemgetter(val)(split_id_pts_lookup)))

                # Explode MultiLineStrings and resolve identifiers.
                self.nrn.reset_index(drop=True, inplace=True)
                self.nrn = self.nrn.explode()
                self.nrn, export = helpers.update_ids(self.nrn, identifier=self.id, index=True)
                if export:
                    self._export = True

    @staticmethod
    def _split_arc(arc: LineString, split_pts: tuple) -> MultiLineString:
        """
        Splits a coordinate sequence into multiple sequences based on one or more 'split' points.

        :param LineString arc: LineString to be split.
        :param tuple split_pts: coordinate sequence to use for splitting the LineString.
        :return MultiLineString: MultiLineString.
        """

        # Unpack arc coordinates.
        coords = tuple(attrgetter("coords")(arc))

        # Configure distances of each coordinate and node along the arc.
        # Sort results using a nested tuple containing the point values and flag for node-status.
        seq = sorted(tuple(zip(map(lambda pt: arc.project(Point(pt)), (*coords, *split_pts)),
                               (*coords, *split_pts),
                               (*(0,) * len(coords), *(1,) * len(split_pts)))), key=itemgetter(0))

        # Resolve loop arcs by moving first result to end of sequence.
        if coords[0] == coords[-1]:
            seq = [*seq[1:], seq[0]]

        # Construct segmentation index ranges based on arc node and splitter node positions.
        seq_unzip = tuple(zip(*seq))
        start, end = tee([0, *(idx for idx, val in enumerate(seq_unzip[-1]) if val == 1), len(seq_unzip[-1]) - 1])
        next(end, None)

        # Iterate segmentation index ranges and constuct LineStrings from corresponding coordinates.
        arcs = MultiLineString([LineString(seq_unzip[1][start: end + 1]) for start, end in tuple(zip(start, end))])

        return arcs

    @staticmethod
    def _update_nodes(g: LineString, node_map: Dict[tuple, tuple]) -> LineString:
        """
        Updates one or both nodes in the LineString.

        :param LineString g: LineString to be updated.
        :param Dict[tuple, tuple] node_map: mapping of from and to nodes.
        :return LineString: updated LineString.
        """

        # Compile coordinates.
        coords = list(attrgetter("coords")(g))

        # Conditionally update nodes.
        for idx in (0, -1):
            try:
                coords[idx] = itemgetter(coords[idx])(node_map)
            except KeyError:
                pass

        return LineString(coords)

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
        untouchable_ids = set(pd.read_csv(self.untouchable_bos)[self.bo_id])

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
        covered_by = self.nrn_bos["geometry"].map(
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
