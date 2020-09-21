import geopandas as gpd
import logging
import numpy as np
import os
import pandas as pd
import re
import shapely
import sys
from bisect import bisect
from collections import OrderedDict
from operator import itemgetter
from shapely.geometry import Point

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
        """Validates and formats input data."""

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

        logger.info("Configuring addrange attributes.")

        def get_digdirfg(sequence):
            """Returns the digdirfg attribute for the given sequence of address numbers."""

            # Return digitizing direction for single addresses.
            if len(sequence) == 1:
                return "Not Applicable"

            # Derive digitizing direction from sequence sorting direction.
            if sequence == sorted(sequence):
                return "Same Direction"
            else:
                return "Opposite Direction"

        def get_hnumstr(sequence):
            """Returns the hnumstr attribute for the given sequence of address numbers."""

            # Validate structure for single addresses.
            if len(sequence) == 1:
                return "Even" if (sequence[0] % 2 == 0) else "Odd"

            # Configure sequence sort status.
            if sequence == sorted(sequence) or sequence == sorted(sequence, reverse=True):

                # Configure sequence parities.
                parities = tuple(map(lambda number: number % 2 == 0, sequence))

                # Validate structure for sorted address ranges.
                if all(parities):
                    return "Even"
                elif not any(parities):
                    return "Odd"
                else:
                    return "Mixed"

            # Return structure for unsorted address ranges.
            else:
                return "Irregular"

        def get_number_sequence(numbers, suffixes, distances):
            """Returns the filtered number sequence for the given addresses."""

            # Reduce addresses at a duplicated intersection distance to only the first instance.
            if len(distances) == len(set(distances)):
                sequence = numbers
            else:
                sequence = pd.DataFrame({"number": numbers, "suffix": suffixes, "distance": distances}).drop_duplicates(
                    subset="distance", keep="first")["number"].to_list()

            # Remove duplicated addresses.
            sequence = list(OrderedDict.fromkeys(sequence))

            return sequence

        def sort_addresses(numbers, suffixes, distances):
            """
            Sorts the addresses successively by:
            1) distance - the distance of the intersection point along the road segment.
            2) number
            3) suffix
            Taking into account the directionality of the addresses relative to the road segment.
            """

            # Create individual address tuples from separated address components.
            addresses = tuple(zip(numbers, suffixes, distances))

            # Apply initial sorting, by distance, to identify address directionality.
            addresses_sorted = sorted(addresses, key=itemgetter(2))
            directionality = -1 if addresses_sorted[0][0] > addresses_sorted[-1][0] else 1

            # Sort addresses - same direction.
            if directionality == 1:
                return tuple(sorted(addresses, key=itemgetter(2, 1, 0)))

            # Sort addresses - opposite direction.
            else:
                return tuple(sorted(sorted(sorted(
                    addresses, key=itemgetter(1), reverse=True),
                    key=itemgetter(0), reverse=True),
                    key=itemgetter(2)))

        # Split address dataframe on parity.
        addresses_l = self.addresses[self.addresses["parity"] == "l"].copy(deep=True)
        addresses_r = self.addresses[self.addresses["parity"] == "r"].copy(deep=True)

        # Create dataframes from grouped addresses.
        cols = ("number", "suffix", "distance")
        addresses_l = pd.DataFrame({col: helpers.groupby_to_list(addresses_l, "roadseg_index", col) for col in cols})
        addresses_r = pd.DataFrame({col: helpers.groupby_to_list(addresses_r, "roadseg_index", col) for col in cols})

        # Sort addresses.
        addresses_l = addresses_l.apply(lambda row: sort_addresses(*row), axis=1)
        addresses_r = addresses_r.apply(lambda row: sort_addresses(*row), axis=1)

        # Configure addrange attributes.
        addrange = pd.DataFrame(index=map(int, {*addresses_l.index, *addresses_r.index}))

        # Configure addrange attributes - hnumf, hnuml.
        logger.info("Configuring addrange attributes: hnumf, hnuml.")

        addrange.loc[addresses_l.index, "l_hnumf"] = addresses_l.map(lambda addresses: addresses[0][0])
        addrange.loc[addresses_l.index, "l_hnuml"] = addresses_l.map(lambda addresses: addresses[-1][0])
        addrange.loc[addresses_r.index, "r_hnumf"] = addresses_r.map(lambda addresses: addresses[0][0])
        addrange.loc[addresses_r.index, "r_hnuml"] = addresses_r.map(lambda addresses: addresses[-1][0])

        # Configuring addrange attributes - hnumsuff, hnumsufl.
        logger.info("Configuring addrange attributes: hnumsuff, hnumsufl.")

        addrange.loc[addresses_l.index, "l_hnumsuff"] = addresses_l.map(lambda addresses: addresses[0][1])
        addrange.loc[addresses_l.index, "l_hnumsufl"] = addresses_l.map(lambda addresses: addresses[-1][1])
        addrange.loc[addresses_r.index, "r_hnumsuff"] = addresses_r.map(lambda addresses: addresses[0][1])
        addrange.loc[addresses_r.index, "r_hnumsufl"] = addresses_r.map(lambda addresses: addresses[-1][1])

        # Configuring addrange attributes - hnumtypf, hnumtypl.
        logger.info("Configuring addrange attributes: hnumtypf, hnumtypl.")

        addrange.loc[addresses_l.index, "l_hnumtypf"] = addresses_l.map(lambda addresses: "Actual Located")
        addrange.loc[addresses_l.index, "l_hnumtypl"] = addresses_l.map(lambda addresses: "Actual Located")
        addrange.loc[addresses_r.index, "r_hnumtypf"] = addresses_r.map(lambda addresses: "Actual Located")
        addrange.loc[addresses_r.index, "r_hnumtypl"] = addresses_r.map(lambda addresses: "Actual Located")

        # Get address number sequence.
        logger.info("Configuring address number sequence.")

        addresses_l["sequence"] = addresses_l.map(lambda row: get_number_sequence(*row))
        addresses_r["sequence"] = addresses_r.map(lambda row: get_number_sequence(*row))

        # Configure addrange attributes - hnumstr.
        logger.info("Configuring addrange attributes: hnumstr.")

        addrange.loc[addresses_l.index, "l_hnumstr"] = addresses_l["sequence"].map(get_hnumstr)
        addrange.loc[addresses_r.index, "r_hnumstr"] = addresses_r["sequence"].map(get_hnumstr)

        # Configure addrange attributes - digdirfg.
        logger.info("Configuring addrange attributes: digdirfg.")

        addrange.loc[addresses_l.index, "l_digdirfg"] = addresses_l["sequence"].map(get_digdirfg)
        addrange.loc[addresses_r.index, "r_digdirfg"] = addresses_r["sequence"].map(get_digdirfg)

        # Merge addrange attributes with roadseg.
        logger.info("Merging addrange attributes with roadseg.")

        self.roadseg = self.roadseg.merge(addrange, how="left", left_index=True, right_index=True)

    def configure_address_parity(self):
        """Computes roadseg parity and groups linked addresses."""

        logger.info("Configuring address parity.")

        def get_parity(pt, vector):
            """
            Determines the parity (left or right side) of an address point relative to a road vector.

            Parity is derived from the determinant of the vectors formed by the road segment and the address-to-roadseg
            vectors. A positive determinant indicates 'left' parity and negative determinant indicates 'right' parity.
            """

            det = (vector[1][0] - vector[0][0]) * (pt.y - vector[0][1]) - \
                  (vector[1][1] - vector[0][1]) * (pt.x - vector[0][0])
            sign = np.sign(det)

            return "l" if sign == 1 else "r"

        def get_road_vector(pt, segment):
            """
            Returns the following:
            a) the distance of the address intersection along the roadseg segment.
            b) the vector comprised of the roadseg segment coordinates immediately before and after the address
            intersection point.
            """

            # Calculate the distance along the road segment of all roadseg points and the intersection point.
            node_distance = (*map(lambda coord: segment.project(Point(coord)), segment.coords[:-1]), segment.length)
            intersection_distance = segment.project(pt)

            # Compute the index of the intersection point within the roadseg points.
            intersection_index = bisect(node_distance, intersection_distance)

            # Conditionally compile the roadseg points, as a vector, immediately bounding the intersection point.
            # Intersection matches a pre-existing roadseg point.
            if intersection_distance in node_distance:

                # Intersection matches the first roadseg point.
                if intersection_index == 1:
                    vector = itemgetter(intersection_index - 1, intersection_index)(segment.coords)

                # Intersection matches the last roadseg point.
                elif intersection_index == len(node_distance):
                    vector = itemgetter(intersection_index - 2, intersection_index - 1)(segment.coords)

                # Intersection matches an interior roadseg point.
                else:
                    vector = itemgetter(intersection_index - 2, intersection_index)(segment.coords)

            # Intersection matches no pre-existing roadseg point.
            else:
                vector = itemgetter(intersection_index - 1, intersection_index)(segment.coords)

            return intersection_distance, vector

        # Get intersection point between each address and linked roadseg segment.
        self.addresses["intersection"] = self.addresses[["geometry", "roadseg_geometry"]].apply(
            lambda row: shapely.ops.nearest_points(*row), axis=1)

        # Get the distance and road segment vector which bounds the intersection point.
        results = self.addresses[["intersection", "roadseg_geometry"]].apply(lambda row: get_road_vector(*row), axis=1)
        self.addresses["distance"] = results.map(itemgetter(0))
        self.addresses["road_vector"] = results.map(itemgetter(1))

        # Get address parity.
        self.addresses["parity"] = self.addresses[["intersection", "road_vector"]].apply(
            lambda row: get_parity(*row), axis=1)

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
