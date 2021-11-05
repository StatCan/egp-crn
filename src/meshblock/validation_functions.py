import geopandas as gpd
import logging
import pandas as pd
import string
import sys
import uuid
from collections import defaultdict
from copy import deepcopy
from functools import reduce
from itertools import chain, compress
from operator import attrgetter, itemgetter
from pathlib import Path
from scipy.spatial.distance import euclidean
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

        # Resolve identifiers.
        self._update_ids()

        logger.info("Configuring validations.")

        # Define validation.
        # Note: List validations in order if execution order matters.
        self.validations = {
            100: {"func": self.connectivity,
                  "desc": "All BOs must have nodal connections to other arcs."},
            101: {"func": self.connectivity_integrated,
                  "desc": "BO node connects to NRN node."},
            102: {"func": self.connectivity_unintegrated_no_connection,
                  "desc": "BO node does not have any connections."},
            103: {"func": self.connectivity_unintegrated_bo_connection,
                  "desc": "BO node only connects to another BO node."},
            201: {"func": self.representation_existence,
                  "desc": "All BO ngd_uids must exist."},
            202: {"func": self.representation_duplication,
                  "desc": "Duplicated BO ngd_uids must be contiguous (gaps bridged by NRN roads are acceptable)."}
        }

        # Define validation thresholds.
        self._bo_ngd_proximity = 5

    def _update_ids(self, index: bool = True) -> None:
        """
        Updates identifiers if they are not unique 32 digit hexadecimal strings.

        :param bool index: assigns the identifier column as GeoDataFrame index, default = True.
        """

        logger.info(f"Resolving nrn identifiers for: \"{self.id}\".")

        try:

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

    def connectivity(self) -> None:
        """
        Validation: All BOs must have nodal connections to other arcs.
        Note: The output of each connectivity validation feeds into the next. Therefore, this method exists to generate
        the dependant variables for various connectivity validations and is not intended to produce error logs itself.
        """

        # TODO

    def connectivity_integrated(self) -> dict:
        """
        Validates: BO node connects to NRN node.

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # TODO

        return errors

    def connectivity_unintegrated_bo_connection(self) -> dict:
        """
        Validates: BO node only connects to another BO node.

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # TODO

        return errors

    def connectivity_unintegrated_no_connection(self) -> dict:
        """
        Validates: BO node does not have any connections.

        :return dict: dict containing error messages and, optionally, a query to identify erroneous records.
        """

        errors = {"values": list(), "query": None}

        # TODO

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
