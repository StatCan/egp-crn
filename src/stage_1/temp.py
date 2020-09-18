import bisect
import numpy as np
import os
import pandas as pd
import shapely
import sys
from collections import OrderedDict
from operator import itemgetter
from shapely.geometry import LineString, Point

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import helpers


# TODO: add to configure_address_parity

# Get line connecting each address and road segment.
addresses["connecting_vector"] = addresses[["geometry", "roadseg_geometry"]].apply(
    lambda row: LineString(shapely.ops.nearest_points(*row)), axis=1)

# Get line comprised of road segment points before and after line connection.
addresses["distances"] = addresses["roadseg_geometry"].map(
    lambda g: tuple(map(lambda coord: g.project(Point(coord)), g.coords)))
addresses.loc[addresses["distances"].map(lambda vals: vals[0] == vals[-1]), "distances"] = \
addresses[addresses["distances"].map(lambda vals: vals[0] == vals[-1])][["distances", "roadseg_geometry"]].apply(
    lambda row: (*row[0][:-1], row[1].length), axis=1)
addresses["distance_connection"] = addresses[["roadseg_geometry", "connecting_vector"]].apply(
    lambda row: row[0].project(Point(row[1].coords[-1])), axis=1)
addresses["distance_connection_index"] = addresses[["distances", "distance_connection"]].apply(
    lambda row: bisect.bisect(row[0], row[1]), axis=1)
addresses["road_vector"] = None
flag = addresses[["distance_connection", "distances"]].apply(lambda row: row[0] in row[1], axis=1)
flag_end = (flag) & (addresses["distance_connection_index"] == addresses["distances"].map(len))
flag_start = (flag) & (addresses["distance_connection_index"] == 1)
flag_middle = (flag) & (~flag_end) & (~flag_start)
addresses.loc[flag_end, "road_vector"] = addresses[flag_end][["roadseg_geometry", "distance_connection_index"]].apply(
    lambda row: itemgetter(row[1] - 2, row[1] - 1)(row[0].coords), axis=1)
addresses.loc[flag_middle, "road_vector"] = addresses[flag_middle][
    ["roadseg_geometry", "distance_connection_index"]].apply(lambda row: itemgetter(row[1] - 2, row[1])(row[0].coords),
                                                             axis=1)
addresses.loc[flag_start, "road_vector"] = addresses[flag_start][
    ["roadseg_geometry", "distance_connection_index"]].apply(lambda row: itemgetter(row[1] - 1, row[1])(row[0].coords),
                                                             axis=1)
addresses.loc[~flag, "road_vector"] = addresses[~flag][["roadseg_geometry", "distance_connection_index"]].apply(
    lambda row: itemgetter(row[1] - 1, row[1])(row[0].coords), axis=1)
addresses.drop(columns=["distances", "distance_connection", "distance_connection_index"], inplace=True)


# Calculate the determinant of the vectors and use the sign as the parity.
def get_parity(address_pt, road_vector):
    """Determines the parity (left or right side) of an address point relative to a road vector."""

    pt1, pt2 = road_vector
    sign = np.sign((pt2[0] - pt1[0]) * (address_pt.y - pt1[1]) - (pt2[1] - pt1[1]) * (address_pt.x - pt1[0]))

    return "l" if sign == 1 else "r"


addresses["parity"] = addresses[["geometry", "road_vector"]].apply(lambda row: get_parity(*row), axis=1)

# Configure first and last addresses for each road segment.

# Calculate address distances along road segments.
addresses["distance"] = addresses[["roadseg_geometry", "connecting_vector"]].apply(
    lambda row: row[0].project(Point(row[1].coords[-1])), axis=1)

# Separate addresses by parity.
addresses_l = addresses[addresses["parity"] == "l"].copy(deep=True)
addresses_r = addresses[addresses["parity"] == "r"].copy(deep=True)

# TODO: add to configure_addrange_attributes

# Compile grouped attributes.
addresses_l_grouped = pd.DataFrame(
    {col: helpers.groupby_to_list(addresses_l, "roadseg_index", col) for col in ("number", "suffix", "distance")})
addresses_r_grouped = pd.DataFrame(
    {col: helpers.groupby_to_list(addresses_r, "roadseg_index", col) for col in ("number", "suffix", "distance")})

# Sorted addresses by distance.
addresses_l_sorted = addresses_l_grouped[["number", "suffix", "distance"]].apply(lambda row: tuple(zip(*row)),
                                                                                 axis=1).map(
    lambda vals: sorted(vals, key=itemgetter(2)))
addresses_r_sorted = addresses_r_grouped[["number", "suffix", "distance"]].apply(lambda row: tuple(zip(*row)),
                                                                                 axis=1).map(
    lambda vals: sorted(vals, key=itemgetter(2)))

# Identify address directionality and re-sort in normal or reversed order.
flag_l_opposite = addresses_l_sorted.map(lambda vals: vals[0][0] > vals[-1][0])
flag_r_opposite = addresses_r_sorted.map(lambda vals: vals[0][0] > vals[-1][0])
addresses_l_sorted.loc[flag_l_opposite] = addresses_l_sorted[flag_l_opposite].map(
    lambda vals: sorted(sorted(sorted(vals, key=itemgetter(1), reverse=True), key=itemgetter(0), reverse=True),
                        key=itemgetter(2)))
addresses_r_sorted.loc[flag_r_opposite] = addresses_r_sorted[flag_r_opposite].map(
    lambda vals: sorted(sorted(sorted(vals, key=itemgetter(1), reverse=True), key=itemgetter(0), reverse=True),
                        key=itemgetter(2)))

flag_l_same = addresses_l_sorted.map(lambda vals: vals[0][0] <= vals[-1][0])
flag_r_same = addresses_r_sorted.map(lambda vals: vals[0][0] <= vals[-1][0])
addresses_l_sorted.loc[flag_l_same] = addresses_l_sorted[flag_l_same].map(
    lambda vals: sorted(vals, key=itemgetter(2, 1, 0)))
addresses_r_sorted.loc[flag_r_same] = addresses_r_sorted[flag_r_same].map(
    lambda vals: sorted(vals, key=itemgetter(2, 1, 0)))

# Configure address attributes - hnumf, hnuml.
l_hnumf = addresses_l_sorted.map(lambda vals: vals[0][0])
l_hnuml = addresses_l_sorted.map(lambda vals: vals[-1][0])
r_hnumf = addresses_r_sorted.map(lambda vals: vals[0][0])
r_hnuml = addresses_r_sorted.map(lambda vals: vals[-1][0])

# Configure address attributes - hnumsuff, hnumsufl.
l_hnumsuff = addresses_l_sorted.map(lambda vals: vals[0][1])
l_hnumsufl = addresses_l_sorted.map(lambda vals: vals[-1][1])
r_hnumsuff = addresses_r_sorted.map(lambda vals: vals[0][1])
r_hnumsufl = addresses_r_sorted.map(lambda vals: vals[-1][1])

# Configure address attributes - hnumtypf, hnumtypl.
l_hnumtypf = addresses_l_sorted.map(lambda vals: "Actual Located")
l_hnumtypl = addresses_l_sorted.map(lambda vals: "Actual Located")
r_hnumtypf = addresses_r_sorted.map(lambda vals: "Actual Located")
r_hnumtypl = addresses_r_sorted.map(lambda vals: "Actual Located")

# Configure address attributes - hnumstr.
# Keep only the first address for addresses at the same distance along a road segment.
addresses_l_sequence = addresses_l_sorted.map(
    lambda vals: itemgetter(0)(tuple(zip(*vals))) if len(vals) == len(set(itemgetter(2)(tuple(zip(*vals))))) else
    pd.DataFrame(vals).drop_duplicates(subset=2, keep="first")[0].tolist())
addresses_r_sequence = addresses_r_sorted.map(
    lambda vals: itemgetter(0)(tuple(zip(*vals))) if len(vals) == len(set(itemgetter(2)(tuple(zip(*vals))))) else
    pd.DataFrame(vals).drop_duplicates(subset=2, keep="first")[0].tolist())


def get_structure(sequence):
    """Determines the address range structure (hnumstr)."""

    # Convert sequence to integers.
    seq = [int(val) for val in sequence]

    # Handle single address ranges.
    if len(seq) == 1:
        return "Even" if (seq[0] % 2 == 0) else "Odd"

    # Remove duplicated addresses.
    seq = list(OrderedDict.fromkeys(seq))

    # Check if addresses are sorted.
    if seq == sorted(seq) or seq == sorted(seq, reverse=True):

        # Check sequence parity.
        parities = tuple(map(lambda val: val % 2 == 0, seq))

        if all(parities):
            return "Even"
        elif not any(parities):
            return "Odd"
        else:
            return "Mixed"

    else:
        return "Irregular"


l_hnumstr = addresses_l_sequence.map(get_structure)
r_hnumstr = addresses_r_sequence.map(get_structure)


# Configure address attributes - digdirfg.
def get_digitizing_direction(sequence):
    """Determines the address digitizing direction (digdirfg)"""

    # Convert sequence to integers.
    seq = [int(val) for val in sequence]

    # Handle single address ranges.
    if len(seq) == 1:
        return "Not Applicable"

    # Remove duplicated addresses.
    seq = list(OrderedDict.fromkeys(seq))

    # Check sorting direction.
    if seq == sorted(seq):
        return "Same Direction"
    else:
        return "Opposite Direction"


l_digdirfg = pd.Series(["Not Applicable"] * len(addresses_l_sequence), index=addresses_l_sequence.index)
r_digdirfg = pd.Series(["Not Applicable"] * len(addresses_r_sequence), index=addresses_r_sequence.index)
l_digdirfg.loc[l_hnumstr.isin({"Even", "Odd", "Mixed"})] = addresses_l_sequence[
    l_hnumstr.isin({"Even", "Odd", "Mixed"})].map(get_digitizing_direction)
r_digdirfg.loc[r_hnumstr.isin({"Even", "Odd", "Mixed"})] = addresses_r_sequence[
    r_hnumstr.isin({"Even", "Odd", "Mixed"})].map(get_digitizing_direction)

# Merge all address attributes.
address_attributes = pd.DataFrame({
    "l_digdirfg": l_digdirfg,
    "l_hnumf": l_hnumf,
    "l_hnuml": l_hnuml,
    "l_hnumsuff": l_hnumsuff,
    "l_hnumsufl": l_hnumsufl,
    "l_hnumtypf": l_hnumtypf,
    "l_hnumtypl": l_hnumtypl,
    "l_hnumstr": l_hnumstr,
    "r_digdirfg": r_digdirfg,
    "r_hnumf": r_hnumf,
    "r_hnuml": r_hnuml,
    "r_hnumsuff": r_hnumsuff,
    "r_hnumsufl": r_hnumsufl,
    "r_hnumtypf": r_hnumtypf,
    "r_hnumtypl": r_hnumtypl,
    "r_hnumstr": r_hnumstr
})
address_attributes.index = map(int, address_attributes.index)

# Join address attributes to roadseg.
roadseg = roadseg.merge(address_attributes, how="left", left_index=True, right_index=True)
