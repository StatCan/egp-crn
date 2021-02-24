import geopandas as gpd
import logging
import numpy as np
import pandas as pd
import re
import shapely
import sys
from bisect import bisect
from collections import OrderedDict
from operator import itemgetter
from pathlib import Path
from shapely.geometry import LineString, Point
from typing import List, Tuple, Union

sys.path.insert(1, str(Path(__file__).resolve().parents[1]))
import helpers


logger = logging.getLogger()


class Segmentor:
    """Converts address points into segmented addrange attributes, joining the results to the roadseg source dataset."""

    def __init__(self, source: str, addresses: gpd.GeoDataFrame, roadseg: gpd.GeoDataFrame, address_fields: dict,
                 address_join_field: dict, roadseg_join_field: dict) -> None:
        """
        Validates and formats input data for use by the address segmentation class.

        :param str source: abbreviation for the source province / territory.
        :param gpd.GeoDataFrame addresses: GeoDataFrame of address points.
        :param gpd.GeoDataFrame roadseg: GeoDataFrame of NRN roadseg.
        :param dict address_fields: yaml-constructed definition of addressing fields.
        :param dict address_join_field: yaml-constructed definition of address join field.
        :param dict roadseg_join_field: yaml-constructed definition of roadseg join field.
        """

        self.source = source.lower()
        self.export_gpkg = Path(__file__).resolve().parents[2] / f"data/interim/{self.source}_addresses_review.gpkg"

        logger.info("Configuring address attributes.")

        self.addresses = gpd.GeoDataFrame(columns=["street", "number", "suffix"], geometry=addresses["geometry"],
                                          crs=addresses.crs)
        self.roadseg = roadseg.copy(deep=True)

        # Configure and populate required address source attributes.
        for attribute, data in address_fields.items():
            if data:

                # Apply regex substitution to field.
                if isinstance(data, dict):
                    field, regex_sub = itemgetter("field", "regex_sub")(data)
                    self.addresses[attribute] = addresses[field].map(
                        lambda val: re.sub(**regex_sub, string=val, flags=re.I)).copy(deep=True)

                else:
                    self.addresses[attribute] = addresses[data].copy(deep=True)

        # Configure and populate address join attribute - optionally apply concatenation to input fields.
        if isinstance(address_join_field, dict):
            fields, separator = itemgetter("fields", "separator")(address_join_field)
            self.addresses["join"] = addresses[fields].apply(
                lambda row: separator.join([str(val) for val in row if val]), axis=1).copy(deep=True)
        else:
            self.addresses["join"] = addresses[address_join_field].copy(deep=True)

        # Configure and populate roadseg join attribute - optionally apply concatenation to input fields.
        if isinstance(roadseg_join_field, dict):
            fields, separator = itemgetter("fields", "separator")(roadseg_join_field)
            self.roadseg["join"] = roadseg[fields].apply(
                lambda row: separator.join([str(val) for val in row if val]), axis=1).copy(deep=True)
        else:
            self.roadseg["join"] = roadseg[roadseg_join_field].copy(deep=True)

        logger.info("Validating address records.")

        try:

            # Convert address numbers to integer.
            self.addresses["number"] = self.addresses["number"].map(int)

        except ValueError:

            # Flag invalid address numbers.
            invalid = self.addresses.loc[~self.addresses["number"].map(lambda val: str(val).isdigit)]
            message = "\n".join(map(str, invalid[invalid.columns.difference(["geometry"])].itertuples(index=True)))
            logger.exception(f"Invalid address number for the following record(s):\n{message}")
            sys.exit(1)

        logger.info("Filtering unit-level addresses.")

        # Drop unit-level addresses, keeping only first instance.
        self.addresses.drop_duplicates(subset=["street", "number", "suffix"], keep="first", inplace=True)

        logger.info("Input data is ready for segmentation.")

    def __call__(self) -> gpd.GeoDataFrame:
        """
        Executes the address segmentation methods.

        :return gpd.GeoDataFrame: GeoDataFrame of NRN roadseg with NRN addrange attribution.
        """

        logger.info("Segmentation initiated.")

        self.configure_roadseg_linkages()
        self.configure_address_parity()
        self.configure_addrange_attributes()

        logger.info("Segmentation completed.")

        return self.roadseg.copy(deep=True)

    def configure_addrange_attributes(self) -> None:
        """Configures and assigns addrange attributes to NRN roadseg, where possible."""

        logger.info("Configuring addrange attributes.")

        def get_digdirfg(sequence: List[int]) -> str:
            """
            Configures the addrange digdirfg value for the given sequence of address numbers.

            :param List[int] sequence: sequence of address numbers.
            :return str: addrange digdirfg value.
            """

            sequence = list(sequence)

            # Return digitizing direction for single addresses.
            if len(sequence) == 1:
                return "Not Applicable"

            # Derive digitizing direction from sequence sorting direction.
            if sequence == sorted(sequence):
                return "Same Direction"
            else:
                return "Opposite Direction"

        def get_hnumstr(sequence: List[int]) -> str:
            """
            Configures the addrange hnumstr value for the given sequence of address numbers.

            :param List[int] sequence: sequence of address numbers.
            :return str: addrange hnumstr value.
            """

            sequence = list(sequence)

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

        def get_number_sequence(addresses: Tuple[Tuple[int, ...], Tuple[Union[int, str], ...], Tuple[float, ...]]) -> \
                List[int]:
            """
            Configures the filtered number sequence for the given addresses.

            :param Tuple[Tuple[int, ...], Tuple[Union[int, str], ...], Tuple[float, ...]] addresses: nested lists of
                address numbers, address suffixes, and address distances along the associated NRN roadseg LineString,
                respectively.
            :return List[int]: sequence of address numbers with duplicated distances dropped.
            """

            # Separate address components.
            numbers, suffixes, distances = tuple(zip(*addresses))

            # Reduce addresses at a duplicated intersection distance to only the first instance.
            if len(distances) == len(set(distances)):
                sequence = numbers
            else:
                sequence = pd.DataFrame({"number": numbers, "suffix": suffixes, "distance": distances}).drop_duplicates(
                    subset="distance", keep="first")["number"].to_list()

            # Remove duplicated addresses.
            sequence = list(OrderedDict.fromkeys(sequence))

            return sequence

        def sort_addresses(numbers: List[int], suffixes: List[Union[int, str]], distances: List[float]) \
                -> Tuple[Tuple[int, ...], Tuple[Union[int, str], ...], Tuple[float, ...]]:
            """
            Sorts addresses successively by its components:
            1) distance - the distance of the address along the associated NRN roadseg LineString.
            2) address number
            3) address suffix
            Sorting accounts for the directionality of the address sequence.

            :param List[int] numbers: sequence of address numbers.
            :param List[Union[int, str]] suffixes: sequence of address suffixes.
            :param List[float] distances: sequence of address distances along the associated NRN roadseg LineString.
            :return Tuple[Tuple[int, ...], Tuple[Union[int, str], ...], Tuple[float, ...]]: nested lists of address
                numbers, address suffixes, and address distance along the associated NRN roadseg LineString,
                respectively, sorted.
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
        addresses_l = self.addresses.loc[self.addresses["parity"] == "l"].copy(deep=True)
        addresses_r = self.addresses.loc[self.addresses["parity"] == "r"].copy(deep=True)

        # Create dataframes from grouped addresses.
        cols = ("number", "suffix", "distance")
        addresses_l = pd.DataFrame({col: helpers.groupby_to_list(addresses_l, "roadseg_index", col) for col in cols})
        addresses_r = pd.DataFrame({col: helpers.groupby_to_list(addresses_r, "roadseg_index", col) for col in cols})

        # Sort addresses.
        addresses_l = addresses_l.apply(lambda row: sort_addresses(*row), axis=1)
        addresses_r = addresses_r.apply(lambda row: sort_addresses(*row), axis=1)

        # Configure addrange attributes.
        addrange = pd.DataFrame(index=list(map(int, {*addresses_l.index, *addresses_r.index})))

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

        addrange.loc[addresses_l.index, "l_hnumtypf"] = "Actual Located"
        addrange.loc[addresses_l.index, "l_hnumtypl"] = "Actual Located"
        addrange.loc[addresses_r.index, "r_hnumtypf"] = "Actual Located"
        addrange.loc[addresses_r.index, "r_hnumtypl"] = "Actual Located"

        # Get address number sequence.
        logger.info("Configuring address number sequence.")

        address_sequence_l = addresses_l.map(get_number_sequence)
        address_sequence_r = addresses_r.map(get_number_sequence)

        # Configure addrange attributes - hnumstr.
        logger.info("Configuring addrange attributes: hnumstr.")

        addrange.loc[addresses_l.index, "l_hnumstr"] = address_sequence_l.map(get_hnumstr)
        addrange.loc[addresses_r.index, "r_hnumstr"] = address_sequence_r.map(get_hnumstr)

        # Configure addrange attributes - digdirfg.
        logger.info("Configuring addrange attributes: digdirfg.")

        addrange.loc[addresses_l.index, "l_digdirfg"] = address_sequence_l.map(get_digdirfg)
        addrange.loc[addresses_r.index, "r_digdirfg"] = address_sequence_r.map(get_digdirfg)

        # Merge addrange attributes with roadseg.
        logger.info("Merging addrange attributes with roadseg.")

        self.roadseg = self.roadseg[self.roadseg.columns.difference(addrange.columns)].merge(
            addrange, how="left", left_index=True, right_index=True)

    def configure_address_parity(self) -> None:
        """Configures each address point's parity and distance along the associated NRN roadseg LineString."""

        logger.info("Configuring address parity.")

        def get_parity(pt: Point, vector: Tuple[tuple, tuple]) -> str:
            """
            Determines the parity (left or right side) of an address point relative to a road vector. Parity is derived
            from the sign of the determinant of the following vectors:
            1) road segment
            2) direct connection between the address point and road segment.
            A positive determinant indicates 'left' parity and negative determinant indicates 'right' parity.

            :param shapely.geometry.Point pt: address point.
            :param Tuple[tuple, tuple] vector: nested tuple of 2 pairs of coordinates, derived from an NRN roadseg
                LineString.
            :return str: address parity.
            """

            det = (vector[1][0] - vector[0][0]) * (pt.y - vector[0][1]) - \
                  (vector[1][1] - vector[0][1]) * (pt.x - vector[0][0])
            sign = np.sign(det)

            return "l" if sign == 1 else "r"

        def get_road_vector(pt: Point, segment: LineString) -> Tuple[float, Tuple[tuple, tuple]]:
            """
            Computes the following:
            1) the distance of the address intersection point along the NRN roadseg LineString.
            2) the vector comprised of the NRN roadseg LineString coordinates immediately before and after the address
            intersection point.

            :param shapely.geometry.Point pt: address-NRN roadseg intersection point.
            :param shapely.geometry.LineString segment: NRN roadseg LineString.
            :return Tuple[float, Tuple[tuple, tuple]]: nested tuple comprised of the address point distance along the
                NRN roadseg LineString and the vector comprised of the NRN roadseg LineString coordinates immediately
                before and after the address intersection point, respectively.
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
            lambda row: itemgetter(-1)(shapely.ops.nearest_points(*row)), axis=1)

        # Get the distance and road segment vector which bounds the intersection point.
        results = self.addresses[["intersection", "roadseg_geometry"]].apply(lambda row: get_road_vector(*row), axis=1)
        self.addresses["distance"] = results.map(itemgetter(0))
        self.addresses["road_vector"] = results.map(itemgetter(1))

        # Get address parity.
        self.addresses["parity"] = self.addresses[["geometry", "road_vector"]].apply(
            lambda row: get_parity(*row), axis=1)

        # Export address-roadseg connections for review.
        layer = "address_roadseg_connections"
        logger.info(f"Exporting address-roadseg connections for review: {self.export_gpkg}, layer={layer}.")

        # Generate connection LineStrings as new GeoDataFrame.
        connections = gpd.GeoDataFrame(
            self.addresses[["street", "number", "suffix"]],
            geometry=self.addresses[["geometry", "intersection"]].apply(
                lambda row: LineString([itemgetter(0, 1)(itemgetter(0)(pt.coords)) for pt in row]), axis=1),
            crs=self.addresses.crs)

        # Export connections to Geopackage.
        connections.to_file(self.export_gpkg, driver="GPKG", layer=layer)

    def configure_roadseg_linkages(self) -> None:
        """Associates each address point with an NRN roadseg record."""

        logger.info("Linking addresses to roadseg records.")

        # Configure roadseg geometry lookup dictionary.
        roadseg_geom_lookup = self.roadseg["geometry"].to_dict()

        def get_nearest_linkage(pt: Point, roadseg_indexes: Tuple[int, ...]) -> int:
            """
            Resolves many-to-one linkages between an address point and NRN roadseg by keeping the closest (lowest
            geometric distance) of the NRN roadseg linkages to the address point.

            :param shapely.geometry.Point pt: address point.
            :param Tuple[int, ...] roadseg_indexes: NRN roadseg indexes linked to the address point.
            :return int: roadseg_indexes value of the closest NRN roadseg geometry to the address point (from the subset
                of indexes in roadseg_indexes).
            """

            # Get roadseg geometries.
            roadseg_geometries = itemgetter(*roadseg_indexes)(roadseg_geom_lookup)

            # Get roadseg distances from address point.
            roadseg_distances = tuple(map(lambda road: pt.distance(road), roadseg_geometries))

            # Get the roadseg index associated with the smallest distance.
            roadseg_index = roadseg_indexes[roadseg_distances.index(min(roadseg_distances))]

            return roadseg_index

        # Link addresses on join fields.
        self.addresses["addresses_index"] = self.addresses.index
        self.roadseg["roadseg_index"] = self.roadseg.index

        merge = self.addresses.merge(self.roadseg[["roadseg_index", "join"]], how="left", on="join")
        self.addresses["roadseg_index"] = helpers.groupby_to_list(merge, "addresses_index", "roadseg_index")

        self.addresses.drop(columns=["addresses_index"], inplace=True)
        self.roadseg.drop(columns=["roadseg_index"], inplace=True)

        # Export non-linked addresses for review.
        non_linked_flag = self.addresses["roadseg_index"].map(itemgetter(0)).isna()

        if sum(non_linked_flag):

            layer = "non_linked_addresses"
            logger.info(f"Exporting {sum(non_linked_flag)} non-linked addresses for review: {self.export_gpkg}, "
                        f"layer={layer}.")

            # Export addresses to GeoPackage.
            self.addresses.loc[non_linked_flag, ["street", "number", "suffix", "geometry"]].to_file(
                self.export_gpkg, driver="GPKG", layer=layer)

            # Discard non-linked addresses.
            self.addresses.drop(self.addresses.loc[non_linked_flag].index, axis=0, inplace=True)

        # Convert linkages to integer tuples, if possible.
        def as_int(val: Union[int, str]) -> Union[int, str]:
            """
            Converts the given value to integer, failed conversion returns the original value.

            :param Union[int, str] val: value to be converted.
            :return Union[int, str]: value converted to integer, or unaltered.
            """

            try:
                return int(val)
            except ValueError:
                return val

        self.addresses["roadseg_index"] = self.addresses["roadseg_index"].map(
            lambda vals: tuple(set(map(as_int, vals))))

        # Filter multi-linkage addresses to roadseg linkage with nearest geometric distance.
        flag_multi = self.addresses["roadseg_index"].map(len) > 1

        if sum(flag_multi):
            logger.info(f"Resolving many-to-one address-roadseg linkages for {sum(flag_multi)} address records.")

            # Resolve many-to-one linkages.
            self.addresses.loc[flag_multi, "roadseg_index"] = self.addresses.loc[
                flag_multi, ["geometry", "roadseg_index"]].apply(lambda row: get_nearest_linkage(*row), axis=1)

        # Unpack first roadseg linkage for single-linkage addresses.
        self.addresses.loc[~flag_multi, "roadseg_index"] = self.addresses.loc[
            ~flag_multi, "roadseg_index"].map(itemgetter(0))

        # Compile linked roadseg geometry for each address.
        self.addresses["roadseg_geometry"] = self.addresses.merge(
            self.roadseg["geometry"], how="left", left_on="roadseg_index", right_index=True)["geometry_y"]
