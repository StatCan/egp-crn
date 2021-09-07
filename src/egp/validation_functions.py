import geopandas as gpd
import logging
import networkx as nx
import numpy as np
import pandas as pd
import shapely.ops
import string
import sys
from collections import Counter, defaultdict
from datetime import datetime
from itertools import chain, combinations, compress, groupby, product, tee
from operator import attrgetter, itemgetter
from pathlib import Path
from scipy.spatial import cKDTree
from scipy.spatial.distance import euclidean
from shapely.geometry import Point
from typing import Dict, List, Tuple, Union

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


class Validator:
    """Handles the execution of validation functions against the segment dataset."""

    def __init__(self, segment: gpd.GeoDataFrame) -> None:
        """
        Initializes variables for validation functions.

        :param gpd.GeoDataFrame segment: GeoDataFrame containing LineStrings.
        """

        self.segment = segment.copy(deep=True)
        self.errors = defaultdict(list)

        logger.info("Standardizing segments.")

        # Performs the following data standardizations:
        # 1) exclude non-road segments (segment_type=1).
        # 2) re-projects to a meter-based projection (EPSG:3348).
        # 3) explodes multi-part geometries.
        # 4) flattens coordinates to 2-dimensions.
        # 5) rounds coordinates to 7 decimal places.
        self.segment = self.segment.loc[self.segment.segment_type == 1]
        self.segment = self.segment.to_crs("EPSG:3348")
        self.segment = helpers.explode_geometry(self.segment)
        self.segment = helpers.flatten_coordinates(self.segment)
        self.segment = helpers.round_coordinates(self.segment, precision=7)

        logger.info("Configuring validations.")

        # Define validation.
        # Note: List validations in order if execution order matters.
        self.validations = {
            101: self.construction_singlepart,
            102: self.construction_min_length,
            103: self.construction_self_overlap,
            104: self.construction_self_cross,
            105: self.construction_cluster_tolerance,
            201: self.duplication_duplicated,
            202: self.duplication_overlap,
            301: self.connectivity_orphans,
            302: self.connectivity_node_intersection,
            303: self.connectivity_min_distance,
            304: self.connectivity_segmentation
        }

        logger.info("Generating reusable geometry attributes.")

        # Store computationally intensive geometry attributes as new dataframe columns.
        self.segment["pt_start"] = self.segment["geometry"].map(lambda g: g.coords[0])
        self.segment["pt_end"] = self.segment["geometry"].map(lambda g: g.coords[-1])
        self.segment["pts_set"] = self.segment["geometry"].map(lambda g: set(g.coords))
        self.segment["pts_ordered_pairs"] = self.segment["geometry"].map(lambda g: ordered_pairs(g.coords))

    def connectivity_min_distance(self) -> list:
        """
        Validation: Arcs must be >= 5 meters from each other, excluding connected arcs (i.e. no dangles).

        :return list: lists of error messages.
        """

        errors = list()

        # TODO

        return errors

    def connectivity_node_intersection(self) -> list:
        """
        Validates: Arcs must only connect at endpoints (nodes).

        :return list: lists of error messages.
        """

        errors = list()

        # TODO

        return errors

    def connectivity_orphans(self) -> list:
        """
        Validates: Arcs must connect to at least one other arc.

        :return list: lists of error messages.
        """

        errors = list()

        # TODO

        return errors

    def connectivity_segmentation(self) -> list:
        """
        Validates: Arcs must not cross (i.e. must be segmented at each intersection).

        :return list: lists of error messages.
        """

        errors = list()

        # TODO

        return errors

    def construction_cluster_tolerance(self) -> list:
        """
        Validates: Arcs must have >= 1x10-2 (0.01) meters distance between adjacent vertices (cluster tolerance).

        :return list: lists of error messages.
        """

        errors = list()

        # TODO

        return errors

    def construction_min_length(self) -> list:
        """
        Validates: Arcs must be >= 5 meters in length.

        :return list: lists of error messages.
        """

        errors = list()

        # TODO

        return errors

    def construction_self_cross(self) -> list:
        """
        Validates: Arcs must not cross themselves (i.e. must be “simple”).

        :return list: lists of error messages.
        """

        errors = list()

        # TODO

        return errors

    def construction_self_overlap(self) -> list:
        """
        Validates: Arcs must not contain repeated adjacent vertices (i.e. self-overlap).

        :return list: lists of error messages.
        """

        errors = list()

        # TODO

        return errors

    def construction_singlepart(self) -> list:
        """
        Validates: Arcs must be single part (i.e. 'LineString').

        :return list: lists of error messages.
        """

        errors = list()

        # TODO

        return errors

    def duplication_duplicated(self) -> list:
        """
        Validates: Arcs must not be duplicated.

        :return list: lists of error messages.
        """

        errors = list()

        # TODO

        return errors

    def duplication_overlap(self) -> list:
        """
        Validates: Arcs must not overlap (i.e. contain duplicated adjacent vertices).

        :return list: lists of error messages.
        """

        errors = list()

        # TODO

        return errors

    def execute(self) -> None:
        """Orchestrates the execution of validation functions and compiles the resulting errors."""

        try:

            # Iterate validations.
            for code, func in self.validations.items():

                logger.info(f"Applying validation: \"{func.__name__}\".")

                # Execute validation and store non-empty results.
                results = func()
                if len(results):
                    self.errors[f"E{code}"] = results

        except (KeyError, SyntaxError, ValueError) as e:
            logger.exception("Unable to apply validation.")
            logger.exception(e)
            sys.exit(1)
