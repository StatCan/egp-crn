import bisect
import geopandas as gpd
import logging
import numpy as np
import os
import pandas as pd
import re
import shapely
import sys
from collections import OrderedDict
from operator import itemgetter
from shapely.geometry import LineString, Point

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import helpers


# Suppress pandas chained assignment warning.
pd.options.mode.chained_assignment = None


# Set logger.
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)


class Segmentor:
    """Converts address points into segmented addrange attributes, joining the results to the roadseg source dataset."""

    def __init__(self, addresses, roadseg, address_fields, address_join_field, roadseg_join_field):

        logger.info("Configuring address attributes.")

        self.addresses = gpd.GeoDataFrame(columns=["street", "number", "suffix"], geometry=addresses["geometry"])
        self.addresses["join"] = addresses[address_join_field].copy(deep=True)
        self.roadseg = roadseg.rename(columns={roadseg_join_field: "join"}).copy(deep=True)

        # Populate dataframe with required address source attributes.
        for attribute, data in address_fields.items():

            # Apply regex substitution to field.
            if isinstance(data, dict):
                field, regex_sub = itemgetter("field", "regex_sub")(data)
                self.addresses[attribute] = addresses[field].map(
                    lambda val: re.sub(**regex_sub, string=val, flags=re.I)).copy(deep=True)

            else:
                self.addresses[attribute] = addresses[data].copy(deep=True)

        logger.info("Validating address records.")
        try:

            # Convert address numbers to integer.
            self.addresses["number"] = self.addresses["number"].map(int)

        except ValueError:

            # Flag invalid address numbers.
            invalid = self.addresses[~self.addresses["number"].map(lambda val: str(val).isdigit)]
            message = "\n".join(map(str, invalid[invalid.columns.difference(["geometry"])].itertuples(index=True)))
            logger.exception(f"Invalid address number for the following record(s):\n{message}")
            sys.exit(1)

        logger.info("Filtering unit-level addresses.")

        # Drop unit-level addresses, keeping only first instance.
        self.addresses.drop_duplicates(subset=["street", "number", "suffix"], keep="first", inplace=True)

    def __call__(self):
        """Executes the address segmentation methods."""

        self.configure_roadseg_linkages()
        self.configure_address_parity()
        self.configure_addrange_attributes()

        return self.roadseg.copy(deep=True)

    def configure_addrange_attributes(self):
        """Configures addrange attributes, where possible."""

        # TODO

    def configure_address_parity(self):
        """Computes roadseg parity and groups linked addresses."""

        # TODO

    def configure_roadseg_linkages(self):
        """Associates each address with a roadseg record."""

        logger.info("Linking addresses to roadseg records.")

        def get_nearest_linkage(pt, roadseg_indexes):
            """Returns the roadseg index associated with the nearest roadseg geometry to the given address point."""

            # Get roadseg geometries.
            roadseg_geometries = tuple(map(lambda index: self.roadseg["geometry"].iloc[index], roadseg_indexes))

            # Get roadseg distances from address point.
            roadseg_distances = tuple(map(lambda road: pt.distance(road), roadseg_geometries))

            # Get the roadseg index associated with the smallest distance.
            roadseg_index = roadseg_indexes[roadseg_distances.index(min(roadseg_distances))]

            return roadseg_index

        # Link addresses on join fields.
        self.addresses["roadseg_index"] = self.addresses["join"].map(
            lambda val: tuple(set(self.roadseg[self.roadseg["join"] == val].index)))

        # Filter multi-linkage addresses to roadseg linkage with nearest geometric distance.
        flag_multi = self.addresses["roadseg_index"].map(len) > 1
        self.addresses.loc[flag_multi, "roadseg_index"] = self.addresses[flag_multi][["geometry", "roadseg_index"]]\
            .apply(lambda row: get_nearest_linkage(*row), axis=1)

        # Compile linked roadseg geometry for each address.
        self.addresses["roadseg_geometry"] = self.addresses.merge(
            self.roadseg["geometry"], how="left", left_on="roadseg_index", right_index=True)["geometry_y"]
