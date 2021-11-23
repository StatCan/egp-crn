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

        # Configure meshblock variables (all non-ferry and non-ignore arcs).
        self.meshblock_ = None
        self._meshblock_input = None

        # Add ignore attribute.
        if "ignore_201" not in self.nrn.columns:
            self.nrn["ignore_201"] = 0
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
            200: {"func": self.meshblock,
                  "desc": "Generate meshblock from LineStrings."},
            201: {"func": self.meshblock_representation,
                  "desc": "All non-deadend arcs (excluding ferries) must form a single meshblock polygon on both left "
                          "and right sides, or just one side for boundary arcs."}
        }

        # Define validation thresholds.
        self._bo_nrn_prox = 5
        self._bo_nrn_prox_snap = 0.01

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
        Enforces snapping of BO nodes to NRN nodes / edges within a given tolerance.

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        snap_nodes_lookup = dict()
        snap_bo_ids_total = set()

        def _cut_arc(arc: LineString, nodes: tuple) -> MultiLineString:
            """
            Cuts a coordinate sequence into multiple sequences based on one or more 'cut' points.

            :param LineString arc: LineString to be cut.
            :param tuple nodes: coordinate sequence to use for cutting.
            :return MultiLineString: MultiLineString.
            """

            # Unpack arc coordinates.
            coords = tuple(attrgetter("coords")(arc))

            # Configure distances of each coordinate and node along the arc.
            # Sort results using a nested tuple containing the point values and flag for node-status.
            seq = sorted(tuple(zip(map(lambda pt: arc.project(Point(pt)), (*coords, *nodes)),
                                   (*coords, *nodes),
                                   (*(0,)*len(coords), *(1,)*len(nodes)))), key=itemgetter(0))

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

        def _update_nodes(g: LineString) -> LineString:
            """
            Updates one or both nodes in the LineString.

            :param LineString g: LineString to be updated.
            :return LineString: updated LineString.
            """

            # Compile coordinates.
            coords = list(attrgetter("coords")(g))

            # Conditionally update nodes.
            for idx in (0, -1):
                try:
                    coords[idx] = itemgetter(coords[idx])(snap_nodes_lookup)
                except KeyError:
                    pass

            return LineString(coords)

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

            # Automatically snap unintegrated bo nodes <= 1 m from a) an NRN node or b) an NRN edge.

            # Compile all nrn nodes.
            nrn_nodes = gpd.GeoSeries(map(Point, set(self.nrn_roads["nodes"].explode())), crs=self.nrn.crs)

            # Generate simplified node buffers with distance tolerance.
            bo_node_buffers = unintegrated_bo_nodes.map(
                lambda node: Point(node).buffer(self._bo_nrn_prox_snap, resolution=5))

            # Query nrn nodes and roads which intersect each node buffer.
            # Construct DataFrame containing results.
            snap_features = pd.DataFrame({
                "from_node": unintegrated_bo_nodes,
                "to_node": bo_node_buffers.map(
                    lambda buffer: set(nrn_nodes.sindex.query(buffer, predicate="intersects"))),
                "to_arc": bo_node_buffers.map(
                    lambda buffer: set(self.nrn_roads.sindex.query(buffer, predicate="intersects")))
            })

            # Snapping instance a) NRN node.
            # Identify snap nodes by filtering to those with buffers with one or more intersecting nrn nodes.
            snap_nodes = snap_features.loc[snap_features["to_node"].map(len) >= 1].copy(deep=True)
            if len(snap_nodes):

                # Replace "to_node" set with actual node tuple of first result.
                to_node_idxs = set(chain.from_iterable(snap_nodes["to_node"]))
                nrn_nodes = nrn_nodes.loc[nrn_nodes.index.isin(to_node_idxs)]
                nrn_nodes_lookup = dict(zip(nrn_nodes.index, nrn_nodes.map(
                    lambda pt: itemgetter(0)(attrgetter("coords")(pt)))))
                snap_nodes["to_node"] = snap_nodes["to_node"].map(
                    lambda idxs: itemgetter(tuple(idxs)[0])(nrn_nodes_lookup))

                # Create node snapping lookup and update required bo arcs.
                snap_nodes_lookup = dict(zip(snap_nodes["from_node"], snap_nodes["to_node"]))
                snap_bo_ids = set(chain.from_iterable(snap_nodes["from_node"].map(self._nrn_bos_nodes_lookup).values))
                self.nrn.loc[self.nrn[self.id].isin(snap_bo_ids), "geometry"] = \
                    self.nrn.loc[self.nrn[self.id].isin(snap_bo_ids), "geometry"].map(_update_nodes)

                # Store updated bo ids.
                snap_bo_ids_total.update(snap_bo_ids)

            # Snapping instance b) NRN edge.
            # Identify snap nodes by filtering to those with buffers with one or more intersecting nrn arcs.
            snap_nodes = snap_features.loc[(snap_features["to_node"].map(len) == 0) &
                                           (snap_features["to_arc"].map(len) >= 1)].copy(deep=True)
            if len(snap_nodes):

                # Filter results to those nodes not containing nrn arcs between the 2 proximity thresholds.
                snap_nodes["to_arc_max"] = snap_nodes.index.map(lambda idx: itemgetter(idx)(node_intersects)).values
                snap_nodes = snap_nodes.loc[snap_nodes["to_arc"] == snap_nodes["to_arc_max"], ["from_node", "to_arc"]]
                if len(snap_nodes):

                    # Compile identifier and geometry of first result of "to_arc".
                    nrn_idx_geom_lookup = dict(zip(range(len(self.nrn_roads)), self.nrn_roads["geometry"]))
                    nrn_idx_id_lookup = dict(zip(range(len(self.nrn_roads)), self.nrn_roads[self.id]))
                    snap_nodes["to_geom"] = snap_nodes["to_arc"].map(
                        lambda idxs: itemgetter(tuple(idxs)[0])(nrn_idx_geom_lookup))
                    snap_nodes["to_id"] = snap_nodes["to_arc"].map(
                        lambda idxs: itemgetter(tuple(idxs)[0])(nrn_idx_id_lookup))

                    # Configure nearest point on nrn arc.
                    snap_nodes["to_pt"] = snap_nodes[["from_node", "to_geom"]].apply(
                        lambda row: attrgetter("coords")(nearest_points(Point(row[0]), row[1])[-1])[0], axis=1)

                    # Create node snapping lookup and update required bo arcs.
                    snap_nodes_lookup = dict(zip(snap_nodes["from_node"], snap_nodes["to_pt"]))
                    snap_bo_ids = set(chain.from_iterable(snap_nodes["from_node"].map(
                        self._nrn_bos_nodes_lookup).values))
                    self.nrn.loc[self.nrn[self.id].isin(snap_bo_ids), "geometry"] = \
                        self.nrn.loc[self.nrn[self.id].isin(snap_bo_ids), "geometry"].map(_update_nodes)

                    # Store updated bo ids.
                    snap_bo_ids_total.update(snap_bo_ids)

                    # Split nrn arcs at snap points.

                    # Group nrn snap points by identifier. Compile snap points and arc in single DataFrame.
                    snap_arcs = helpers.groupby_to_list(snap_nodes, group_field="to_id", list_field="to_pt")
                    nrn_id_geom_lookup = dict(zip(snap_nodes["to_id"], snap_nodes["to_geom"]))
                    snap_arcs = pd.DataFrame({
                        "pts": snap_arcs.map(tuple).values,
                        "arc": snap_arcs.index.map(lambda val: itemgetter(val)(nrn_id_geom_lookup)).values
                    }, index=snap_arcs.index)

                    # Configure updated coordinate sequences for arcs.
                    snap_arcs["coords"] = snap_arcs["arc"].map(lambda g: tuple(attrgetter("coords")(g)))
                    snap_arcs["new_arc"] = snap_arcs[["arc", "pts"]].apply(lambda row: _cut_arc(*row), axis=1)

                    # Update required NRN arcs.
                    snap_arcs_lookup = dict(zip(snap_arcs.index, snap_arcs["new_arc"]))
                    self.nrn.loc[self.nrn[self.id].isin(snap_arcs_lookup), "geometry"] =\
                        self.nrn.loc[self.nrn[self.id].isin(snap_arcs_lookup), self.id].map(snap_arcs_lookup)

                    # Explode MultiLineStrings and _update_ids.
                    self.nrn.reset_index(drop=True, inplace=True)
                    self.nrn = self.nrn.explode()

                    # Resolve identifiers.
                    self.nrn, _ = helpers.update_ids(self.nrn, identifier=self.id, index=True)

            # Update error log values.
            vals = vals - snap_bo_ids_total

            # Compile error logs.
            errors["values"] = vals
            errors["query"] = f"\"{self.id}\" in {*vals,}"

        # Log modifications count and trigger export.
        if len(snap_bo_ids_total):
            logger.warning(f"Snapped {len(snap_bo_ids_total)} BO node(s) based on tolerance of "
                           f"{self._bo_nrn_prox_snap} m.")
            self._export = True

        return errors

    def meshblock(self) -> dict:
        """
        Validates: Generate meshblock from LineStrings.
        Note: This method exists to generate the dependant variables for various meshblock validations and is not
        intended to produce error logs itself.

        :return dict: placeholder dict based on standard validations. For this method, its contents will be unpopulated.
        """

        errors = {"values": list(), "query": None}

        # Configure meshblock input (all non-ferry and non-ignore arcs).
        self.meshblock_input = self.nrn.loc[(self.nrn["segment_type"] != 2) &
                                            (self.nrn["ignore_201"] != 1)].copy(deep=True)

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
