import geopandas as gpd
import logging
import math
import pandas as pd
import sys
from collections import defaultdict
from copy import deepcopy
from functools import reduce
from itertools import chain, compress, tee
from operator import attrgetter, itemgetter
from pathlib import Path
from shapely.geometry import LineString, MultiLineString, MultiPoint, Point
from shapely.ops import split
from typing import List, Tuple, Union

sys.path.insert(1, str(Path(__file__).resolve().parents[1]))
import helpers


# Set logger.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)


def ordered_pairs(coords: Tuple[tuple, ...]) -> List[Tuple[tuple, tuple]]:
    """
    Creates an ordered sequence of adjacent coordinate pairs, sorted.

    :param Tuple[tuple, ...] coords: tuple of coordinate tuples.
    :return List[Tuple[tuple, tuple]]: ordered sequence of coordinate pair tuples.
    """

    coords_1, coords_2 = tee(coords)
    next(coords_2, None)

    return sorted(zip(coords_1, coords_2))


def split_line(line1: LineString, line2: LineString) -> Union[LineString, MultiLineString]:
    """
    Splits a LineString based on a crossing LineString.

    :param LineString line1: LineString to be split.
    :param LineString line2: LineString to be used as the splitter.
    :return Union[LineString, MultiLineString]: segmented version of the original LineString.
    """

    try:

        return MultiLineString(split(line1, line2))

    except TypeError:
        return line1


class Validator:
    """Handles the execution of validation functions against the segment dataset."""

    def __init__(self, segment: gpd.GeoDataFrame, dst: Path, layer: str) -> None:
        """
        Initializes variables for validation functions.

        :param gpd.GeoDataFrame segment: GeoDataFrame containing LineStrings.
        :param Path dst: output GeoPackage path.
        :param str layer: output GeoPackage layer name.
        """

        self.dst = dst
        self.layer = layer
        self.errors = defaultdict(list)
        self.id = "segment_id"
        self.crs = f"EPSG:{segment.crs.to_epsg()}"
        self._export = False
        self._segment = self.layer.startswith("segment_")

        # Drop non-LineString geometries.
        invalid_geoms = ~segment.geom_type.isin({"LineString", "MultiLineString"})
        if sum(invalid_geoms):
            segment = segment.loc[~invalid_geoms].copy(deep=True)
            self._export = True

        # Create original and standardized dataframes.
        self.segment_original = segment.copy(deep=True)
        self.segment = None
        self._standardize_df()

        logger.info("Configuring validations.")

        # Define validation.
        # Note: List validations in order if execution order matters.
        self.validations = {
            303: {"func": self.connectivity_segmentation,
                  "desc": "Arcs must not cross (i.e. must be segmented at each intersection)."},
            101: {"func": self.construction_singlepart,
                  "desc": "Arcs must be single part (i.e. \"LineString\")."},
            102: {"func": self.construction_min_length,
                  "desc": "Arcs must be >= 3 meters in length."},
            103: {"func": self.construction_simple,
                  "desc": "Arcs must be simple (i.e. must not self-overlap, self-cross, nor touch their interior)."},
            104: {"func": self.construction_cluster_tolerance,
                  "desc": "Arcs must have >= 0.01 meters distance between adjacent vertices (cluster tolerance)."},
            201: {"func": self.duplication_duplicated,
                  "desc": "Arcs must not be duplicated."},
            202: {"func": self.duplication_overlap,
                  "desc": "Arcs must not overlap (i.e. contain duplicated adjacent vertices)."},
            301: {"func": self.connectivity_node_intersection,
                  "desc": "Arcs must only connect at endpoints (nodes)."},
            302: {"func": self.connectivity_min_distance,
                  "desc": "Arcs must be >= 5 meters from each other, excluding connected arcs (i.e. no dangles)."}
        }

        # Define validation thresholds.
        self._min_len = 3
        self._min_dist = 5
        self._min_cluster_dist = 0.01

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
                helpers.export(self.segment_original, dst=self.dst, name=self.layer)

        except (KeyError, SyntaxError, ValueError) as e:
            logger.exception("Unable to apply validation.")
            logger.exception(e)
            sys.exit(1)

    def _standardize_df(self) -> None:
        """
        Applies the following standardizations to the original dataframe:
        1) explodes multi-part geometries.
        2) resolves invalid identifiers.

        Then creates a copy of the original dataframe with the following standardizations:
        1) exclude non-road segments (segment_type=1).

        Then generates computationally intensive, reusable geometry attributes.
        """

        logger.info("Standardizing DataFrame.")

        # Apply standardizations to original dataframe.
        self.segment_original = helpers.explode_geometry(self.segment_original, index=self.id)
        self.segment_original, _export = helpers.update_ids(self.segment_original, identifier=self.id, index=True)
        if _export:
            self._export = True

        # Create copy of original dataframe.
        self.segment = self.segment_original.loc[self.segment_original["segment_type"].astype(int) == 1].copy(deep=True)

        logger.info("Generating reusable geometry attributes.")

        # Generate computationally intensive geometry attributes as new columns.
        self.segment["pts_tuple"] = self.segment["geometry"].map(attrgetter("coords")).map(tuple)
        self.segment["pt_start"] = self.segment["pts_tuple"].map(itemgetter(0))
        self.segment["pt_end"] = self.segment["pts_tuple"].map(itemgetter(-1))
        self.segment["pts_ordered_pairs"] = self.segment["pts_tuple"].map(ordered_pairs)

        # Generate computationally intensive lookups.
        pts = self.segment["pts_tuple"].explode()
        pts_df = pd.DataFrame({"pt": pts.values, self.id: pts.index})
        self.pts_id_lookup = helpers.groupby_to_list(pts_df, "pt", self.id).map(set).to_dict()
        self.idx_id_lookup = dict(zip(range(len(self.segment)), self.segment.index))

    def connectivity_min_distance(self) -> dict:
        """
        Validation: Arcs must be >= 5 meters from each other, excluding connected arcs (i.e. no dangles).

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # Compile all non-duplicated nodes (dead ends) as a DataFrame.
        pts = self.segment["pt_start"].append(self.segment["pt_end"])
        deadends = pts.loc[~pts.duplicated(keep=False)]
        deadends = pd.DataFrame({"pt": deadends.values, self.id: deadends.index})

        # Generate simplified node buffers with distance tolerance.
        deadends["buffer"] = deadends["pt"].map(lambda pt: Point(pt).buffer(self._min_dist, resolution=5))

        # Query segments which intersect each dead end buffer.
        deadends["intersects"] = deadends["buffer"].map(
            lambda buffer: set(self.segment.sindex.query(buffer, predicate="intersects")))

        # Flag dead ends which have buffers with one or more intersecting segments.
        deadends = deadends.loc[deadends["intersects"].map(len) > 1]
        if len(deadends):

            # Aggregate deadends to their source features.
            # Note: source features will exist twice if both nodes are deadends; these results will be aggregated.
            deadends_agg = helpers.groupby_to_list(deadends, self.id, "intersects")\
                .map(chain.from_iterable).map(set).to_dict()
            deadends["intersects"] = deadends[self.id].map(deadends_agg)
            deadends.drop_duplicates(subset=self.id, inplace=True)

            # Compile identifiers corresponding to each 'intersects' index.
            deadends["intersects"] = deadends["intersects"].map(lambda idxs: set(itemgetter(*idxs)(self.idx_id_lookup)))

            # Compile identifiers containing either of the source geometry nodes.
            deadends["connected"] = deadends[self.id].map(
                lambda identifier: set(chain.from_iterable(
                    itemgetter(node)(self.pts_id_lookup) for node in itemgetter(0, -1)(
                        itemgetter(identifier)(self.segment["pts_tuple"]))
                )))

            # Subtract identifiers of connected features from buffer-intersecting features.
            deadends["disconnected"] = deadends["intersects"] - deadends["connected"]

            # Filter to those results with disconnected segments.
            flag = deadends["disconnected"].map(len) > 0
            if sum(flag):

                # Remove duplicated results.
                deadends = deadends.loc[flag]
                deadends["ids"] = deadends[[self.id, "disconnected"]].apply(
                    lambda row: tuple({row[0], *row[1]}), axis=1)
                deadends.drop_duplicates(subset="ids", keep="first", inplace=True)

                # Compile error logs.
                errors["values"] = deadends["ids"].map(
                    lambda ids: f"Disconnected features are too close: {*ids,}").to_list()
                vals = set(chain.from_iterable(deadends["ids"]))
                errors["query"] = f"\"{self.id}\" in {*vals,}"

        return errors

    def connectivity_node_intersection(self) -> dict:
        """
        Validates: Arcs must only connect at endpoints (nodes).

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # Compile nodes.
        nodes = set(self.segment["pt_start"].append(self.segment["pt_end"]))

        # Compile interior vertices (non-nodes).
        # Note: only segments with > 2 vertices are used.
        non_nodes = set(self.segment.loc[self.segment["pts_tuple"].map(len) > 2, "pts_tuple"]
                        .map(lambda pts: set(pts[1:-1])).map(tuple).explode())

        # Compile invalid vertices.
        invalid_pts = nodes.intersection(non_nodes)

        # Filter invalid vertices to those with multiple connected features.
        invalid_pts = set(compress(invalid_pts,
                                   map(lambda pt: len(itemgetter(pt)(self.pts_id_lookup)) > 1, invalid_pts)))
        if len(invalid_pts):

            # Filter segments to those with an invalid vertex.
            invalid_ids = set(chain.from_iterable(map(lambda pt: itemgetter(pt)(self.pts_id_lookup), invalid_pts)))
            segment = self.segment.loc[self.segment.index.isin(invalid_ids)]

            # Flag invalid segments where the invalid vertex is a non-node.
            flag = segment["pts_tuple"].map(lambda pts: len(set(pts[1:-1]).intersection(invalid_pts))) > 0
            if sum(flag):

                # Compile error logs.
                vals = set(segment.loc[flag].index)
                errors["values"] = vals
                errors["query"] = f"\"{self.id}\" in {*vals,}"

        return errors

    def connectivity_segmentation(self) -> dict:
        """
        Validates: Arcs must not cross (i.e. must be segmented at each intersection).
        If source layer is segment_{source}: This validation will automatically segment the required geometries.
        If source layer is nrn_bo_{source}: This validation will not automatically segment the required geometries.

        Note: due to coordinate rounding, perpetual segmentation errors may exist post-segmentation if they contain a
        vertex within the rounding tolerance of the point of segmentation. These perpetual errors will be logged rather
        than automatically resolved.

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # Query segments which cross each segment.
        crosses = self.segment["geometry"].map(lambda g: set(self.segment.sindex.query(g, predicate="crosses")))

        # Flag segments which have one or more crossing segments.
        flag = crosses.map(len) > 0
        if sum(flag):

            if not self._segment:

                # Compile error logs.
                vals = set(self.segment.loc[flag].index)
                errors["values"] = vals
                errors["query"] = f"\"{self.id}\" in {*vals,}"

            else:

                # Segment original geometries.

                # Assign 'crosses' results to original dataframe.
                self.segment_original["splitter"] = [{} for _ in range(len(self.segment_original))]
                self.segment_original.loc[self.segment_original.index.isin(crosses.index), "splitter"] = crosses
                flag = self.segment_original["splitter"].map(len) > 0

                # Compile splitter geometries.
                self.segment_original.loc[flag, "splitter"] = self.segment_original.loc[flag, "splitter"]\
                    .map(lambda indexes: itemgetter(*indexes)(self.segment_original["geometry"]))\
                    .map(lambda geoms: geoms if isinstance(geoms, tuple) else (geoms,))

                # Add original geometry to beginning of splitter tuple.
                self.segment_original.loc[flag, "splitter"] = self.segment_original.loc[flag, ["geometry", "splitter"]]\
                    .apply(lambda row: (row[0], *row[1]), axis=1)

                # Iteratively split geometries using each splitter geometry.
                self.segment_original.loc[flag, "geometry"] = self.segment_original.loc[flag, "splitter"].map(
                    lambda geoms: reduce(split_line, geoms))

                # Standardize original dataframe and regenerate re-projected copy.
                self._standardize_df()

                # Re-apply validation on original, now-segmented, dataframe and log results.

                # Flag records which were segmented.
                filter_flag = self.segment_original["splitter"].map(len) > 0
                self.segment_original.drop(columns=["splitter"], inplace=True)

                # Query segments (flagged) which cross each segment (all).
                crosses = self.segment_original.loc[filter_flag, "geometry"].map(
                    lambda g: set(self.segment_original.sindex.query(g, predicate="crosses")))

                # Flag segments which have one or more crossing segments.
                crosses = crosses.loc[crosses.map(len) > 0]
                flag = self.segment_original.index.isin(crosses.index)
                if sum(flag):

                    # Compile error logs.
                    vals = set(self.segment_original.loc[flag].index)
                    errors["values"] = vals
                    errors["query"] = f"\"{self.id}\" in {*vals,}"

        return errors

    def construction_cluster_tolerance(self) -> dict:
        """
        Validates: Arcs must have >= 1x10-2 (0.01) meters distance between adjacent vertices (cluster tolerance).

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # Filter segments to those with > 2 vertices.
        segment = self.segment.loc[self.segment["pts_tuple"].map(len) > 2]
        if len(segment):

            # Explode segment coordinate pairs and calculate distances.
            coord_pairs = segment["pts_ordered_pairs"].explode()
            coord_dist = coord_pairs.map(lambda pair: math.dist(*pair))

            # Flag pairs with distances that are too small.
            flag = coord_dist < self._min_cluster_dist
            if sum(flag):

                # Export invalid pairs as MultiPoint geometries.
                pts = coord_pairs.loc[flag].map(MultiPoint)
                pts_df = gpd.GeoDataFrame({self.id: pts.index.values}, geometry=[*pts], crs=self.crs)

                logger.info(f"Writing to file: {self.dst.name}|layer={self.layer}_v104")
                pts_df.to_file(str(self.dst), driver="GPKG", layer=f"{self.layer}_v104")

                # Compile error logs.
                vals = set(coord_pairs.loc[flag].index)
                errors["values"] = vals
                errors["query"] = f"\"{self.id}\" in {*vals,}"

        return errors

    def construction_min_length(self) -> dict:
        """
        Validates: Arcs must be >= 3 meters in length, except structures (e.g. Bridges).

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # Flag arcs which are too short.
        flag = self.segment.length < self._min_len
        if sum(flag):
            
            # Flag isolated structures (structures not connected to another structure).
            
            # Compile structures.
            structures = self.segment.loc[~self.segment["structure_type"].isin({"Unknown", "None"})]
            
            # Compile duplicated structure nodes.
            structure_nodes = pd.Series(structures["pt_start"].append(structures["pt_end"]))
            structure_nodes_dups = set(structure_nodes.loc[structure_nodes.duplicated(keep=False)])
            
            # Flag isolated structures.
            isolated_structure_index = set(structures.loc[~((structures["pt_start"].isin(structure_nodes_dups)) |
                                                            (structures["pt_end"].isin(structure_nodes_dups)))].index)
            isolated_structure_flag = self.segment.index.isin(isolated_structure_index)
            
            # Modify flag to exclude isolated structures.
            flag = (flag & (~isolated_structure_flag))
            if sum(flag):

                # Compile error logs.
                vals = set(self.segment.loc[flag].index)
                errors["values"] = vals
                errors["query"] = f"\"{self.id}\" in {*vals,}"

        return errors

    def construction_simple(self) -> dict:
        """
        Validates: Arcs must be simple (i.e. must not self-overlap, self-cross, nor touch their interior).

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # Flag complex (non-simple) geometries.
        flag = ~self.segment.is_simple
        if sum(flag):

            # Compile error logs.
            vals = set(self.segment.loc[flag].index)
            errors["values"] = vals
            errors["query"] = f"\"{self.id}\" in {*vals,}"

        return errors

    def construction_singlepart(self) -> dict:
        """
        Validates: Arcs must be single part (i.e. 'LineString').

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # Flag non-LineStrings.
        flag = self.segment.geom_type != "LineString"
        if sum(flag):

            # Compile error logs.
            vals = set(self.segment.loc[flag].index)
            errors["values"] = vals
            errors["query"] = f"\"{self.id}\" in {*vals,}"

        return errors

    def duplication_duplicated(self) -> dict:
        """
        Validates: Arcs must not be duplicated.

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # Filter segments to those with duplicated lengths.
        segment = self.segment.loc[self.segment.length.duplicated(keep=False)]
        if len(segment):

            # Filter segments to those with duplicated nodes.
            segment = segment.loc[segment[["pt_start", "pt_end"]].agg(set, axis=1).map(tuple).duplicated(keep=False)]

            # Flag duplicated geometries.
            dups = segment.loc[segment["geometry"].map(
                lambda g1: segment["geometry"].map(lambda g2: g1.equals(g2)).sum() > 1)]
            if len(dups):

                # Compile error logs.
                vals = set(dups.index)
                errors["values"] = vals
                errors["query"] = f"\"{self.id}\" in {*vals,}"

        return errors

    def duplication_overlap(self) -> dict:
        """
        Validates: Arcs must not overlap (i.e. contain duplicated adjacent vertices).

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # Query segments which overlap each segment.
        overlaps = self.segment["geometry"].map(lambda g: set(self.segment.sindex.query(g, predicate="overlaps")))

        # Flag segments which have one or more overlapping segments.
        flag = overlaps.map(len) > 0

        # Compile error logs.
        if sum(flag):
            vals = set(overlaps.loc[flag].index)
            errors["values"] = vals
            errors["query"] = f"\"{self.id}\" in {*vals,}"

        return errors
