import geopandas as gpd
import logging
import networkx as nx
import numpy as np
import pandas as pd
import shapely.ops
import string
import sys
from collections import Counter, defaultdict
from copy import deepcopy
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
        self.id = "segment_id"

        logger.info("Validating segment identifiers.")
        # Performs the following identifier validations:
        # 1) identifiers must be 32 digit hexadecimal strings.
        # 2) identifiers must be unique (nulls excluded from this validation).
        self.segment[self.id] = self.segment[self.id].astype(str)

        try:

            hexdigits = set(string.hexdigits)
            flag_non_hex = (self.segment[self.id].map(len) != 32) | \
                           (self.segment[self.id].map(lambda val: not set(val).issubset(hexdigits)))
            flag_dups = (self.segment[self.id].duplicated(keep=False)) & (self.segment[self.id] != "None")

            # Log invalid segment identifiers.
            if sum(flag_non_hex) or sum(flag_dups):
                logger.exception(f"Invalid identifiers detected for \"{self.id}\".")
                if sum(flag_non_hex):
                    values = "\n".join(self.segment.loc[flag_non_hex, self.id].drop_duplicates(keep="first").values)
                    logger.exception(f"Validation: identifiers must be 32 digit hexadecimal strings.\n{values}")
                if sum(flag_dups):
                    values = "\n".join(self.segment.loc[flag_dups, self.id].drop_duplicates(keep="first").values)
                    logger.exception(f"Validation: identifiers must be unique.\n{values}")
                sys.exit(1)

        except ValueError as e:
            logger.exception(f"Unable to validate segment identifiers for \"{self.id}\".")
            logger.exception(e)
            sys.exit(1)

        logger.info("Standardizing segments.")

        # Performs the following data standardizations:
        # 1) exclude non-road segments (segment_type=1).
        # 2) re-projects to a meter-based projection (EPSG:3348).
        # 3) explodes multi-part geometries.
        # 4) flattens coordinates to 2-dimensions.
        # 5) rounds coordinates to 7 decimal places.
        self.segment = self.segment.loc[self.segment.segment_type.astype(int) == 1]
        self.segment = self.segment.to_crs("EPSG:3348")
        self.segment = helpers.explode_geometry(self.segment)
        self.segment = helpers.flatten_coordinates(self.segment)
        self.segment = helpers.round_coordinates(self.segment, precision=7)

        logger.info("Configuring validations.")

        # Define validation.
        # Note: List validations in order if execution order matters.
        self.validations = {
            101: {"func": self.construction_singlepart,
                  "desc": "Arcs must be single part (i.e. \"LineString\")."},
            102: {"func": self.construction_min_length,
                  "desc": "Arcs must be >= 5 meters in length."},
            103: {"func": self.construction_self_overlap,
                  "desc": "Arcs must not contain repeated adjacent vertices (i.e. self-overlap)."},
            104: {"func": self.construction_self_cross,
                  "desc": "Arcs must not cross themselves (i.e. must be \"simple\")."},
            105: {"func": self.construction_cluster_tolerance,
                  "desc": "Arcs must have >= 0.01 meters distance between adjacent vertices (cluster tolerance)."},
            201: {"func": self.duplication_duplicated,
                  "desc": "Arcs must not be duplicated."},
            202: {"func": self.duplication_overlap,
                  "desc": "Arcs must not overlap (i.e. contain duplicated adjacent vertices)."},
            301: {"func": self.connectivity_orphans,
                  "desc": "Arcs must connect to at least one other arc."},
            302: {"func": self.connectivity_node_intersection,
                  "desc": "Arcs must only connect at endpoints (nodes)."},
            303: {"func": self.connectivity_min_distance,
                  "desc": "Arcs must be >= 5 meters from each other, excluding connected arcs (i.e. no dangles)."},
            304: {"func": self.connectivity_segmentation,
                  "desc": "Arcs must not cross (i.e. must be segmented at each intersection)."}
        }

        logger.info("Generating reusable geometry attributes.")

        # Store computationally intensive geometry attributes as new dataframe columns.
        self.segment["pt_start"] = self.segment["geometry"].map(lambda g: g.coords[0])
        self.segment["pt_end"] = self.segment["geometry"].map(lambda g: g.coords[-1])
        self.segment["pts_set"] = self.segment["geometry"].map(lambda g: set(g.coords))
        self.segment["pts_ordered_pairs"] = self.segment["geometry"].map(lambda g: ordered_pairs(g.coords))

    def connectivity_min_distance(self) -> dict:
        """
        Validation: Arcs must be >= 5 meters from each other, excluding connected arcs (i.e. no dangles).

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # TODO

        return errors

    def connectivity_node_intersection(self) -> dict:
        """
        Validates: Arcs must only connect at endpoints (nodes).

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # TODO

        return errors

    def connectivity_orphans(self) -> dict:
        """
        Validates: Arcs must connect to at least one other arc.

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # TODO

        return errors

    def connectivity_segmentation(self) -> dict:
        """
        Validates: Arcs must not cross (i.e. must be segmented at each intersection).

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # TODO

        return errors

    def construction_cluster_tolerance(self) -> dict:
        """
        Validates: Arcs must have >= 1x10-2 (0.01) meters distance between adjacent vertices (cluster tolerance).

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # TODO

        return errors

    def construction_min_length(self) -> dict:
        """
        Validates: Arcs must be >= 5 meters in length.

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # TODO

        return errors

    def construction_self_cross(self) -> dict:
        """
        Validates: Arcs must not cross themselves (i.e. must be “simple”).

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # TODO

        return errors

    def construction_self_overlap(self) -> dict:
        """
        Validates: Arcs must not contain repeated adjacent vertices (i.e. self-overlap).

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # TODO

        return errors

    def construction_singlepart(self) -> dict:
        """
        Validates: Arcs must be single part (i.e. 'LineString').

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # Flag non-LineStrings.
        flag = self.segment.geom_type != "LineString"

        # Compile error logs.
        if sum(flag):
            vals = self.segment.loc[flag, self.id].values
            vals_quotes = map(lambda val: f"'{val}'", vals)
            errors["values"] = vals
            errors["query"] = f"\"{self.id}\" in ({','.join(vals_quotes)})"

        return errors

    def duplication_duplicated(self) -> dict:
        """
        Validates: Arcs must not be duplicated.

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # TODO

        return errors

    def duplication_overlap(self) -> dict:
        """
        Validates: Arcs must not overlap (i.e. contain duplicated adjacent vertices).

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

                logger.info(f"Applying validation: \"{func.__name__}\".")

                # Execute validation and store non-empty results.
                results = func()
                if len(results["values"]):
                    self.errors[f"E{code} - {description}"] = deepcopy(results)

        except (KeyError, SyntaxError, ValueError) as e:
            logger.exception("Unable to apply validation.")
            logger.exception(e)
            sys.exit(1)
