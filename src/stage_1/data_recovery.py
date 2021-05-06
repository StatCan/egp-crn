import geopandas as gpd
import logging
import pandas as pd
import sys
import uuid
from itertools import tee
from operator import attrgetter, itemgetter
from pathlib import Path
from typing import List, Tuple

filepath = Path(__file__).resolve()
sys.path.insert(1, str(filepath.parents[1]))
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

# Compile data.
df = gpd.read_file("C:/Users/jesse/Downloads/nrn_old/sk.gpkg", layer="roadseg")
df.columns = map(str.lower, df.columns)
df["uuid"] = [uuid.uuid4().hex for _ in range(len(df))]

df_old = gpd.read_file("C:/Users/jesse/Downloads/data_recovery/sk_modified.gpkg", layer="sk_modified")
df_old.columns = map(str.lower, df_old.columns)
df_old["uuid"] = [uuid.uuid4().hex for _ in range(len(df_old))]

# Extract coordinates from geometries.
series_coords = df["geometry"].map(attrgetter("coords")).map(tuple)
series_coords_old = df_old["geometry"].map(attrgetter("coords")).map(tuple)

# Create ordered coordinate pairs, sorted.
coord_pairs_full = series_coords.map(ordered_pairs).map(tuple)
coord_pairs = coord_pairs_full.explode()
coord_pairs_old_full = series_coords_old.map(ordered_pairs).map(tuple)
coord_pairs_old = coord_pairs_old_full.explode()

# Create lookup dictionary for old coordinate pairs to their uuids.
coord_pairs_old_lookup = helpers.groupby_to_list(pd.DataFrame({"pair": coord_pairs_old, "uuid": coord_pairs_old.index}),
                                                 "pair", "uuid").to_dict()

# Fetch associated old uuid for each coordinate pair.
coord_pairs_old_lookup_s = set(coord_pairs_old_lookup)
linkage_flag = coord_pairs.isin(coord_pairs_old_lookup_s)
coord_linkage = coord_pairs.loc[linkage_flag].map(lambda pair: itemgetter(pair)(coord_pairs_old_lookup)).explode()

# Group old uuids for each linkage.
coord_linkage = helpers.groupby_to_list(
    pd.DataFrame({"uuid": coord_linkage.index, "uuid_old": coord_linkage}).reset_index(drop=True), "uuid", "uuid_old")
coord_linkage = coord_linkage.map(set).map(tuple)

# Classify linkages into 'exact' and 'partial'.

# Create lookup dictionaries for full coordinate pairs from their uuids.
coord_pairs_lookup = coord_pairs_full.to_dict()
coord_pairs_old_lookup = coord_pairs_old_full.to_dict()

# Define match function for coordinate pairs.
def match_pairs(source: str, linkages: Tuple[str, ...]) -> Tuple[str, ...]:
    """
    Returns a tuple of uuids which have an exact matching sequence of coordinate pairs to that of the source uuid.

    :param str source: source uuid.
    :param Tuple[str, ...] linkages: tuple of linked uuids.
    :return Tuple[str, ...]: tuple of linked uuids.
    """

    # Fetch coordinate pairs associated with the source uuid.
    source_pairs = itemgetter(source)(coord_pairs_lookup)

    # Iterate linked uuids.
    exact_matches = list()
    for linkage in linkages:

        # Fetch coordinate pairs associated with the linked uuid and compare to source.
        if source_pairs == itemgetter(linkage)(coord_pairs_old_lookup):
            exact_matches.append(linkage)

    return tuple(exact_matches)


# Compile match parameters as a Series.
match_params = pd.DataFrame({"source": coord_linkage.index, "linkages": coord_linkage}).apply(
    lambda row: [*row], axis=1)

# Classify linked uuids.
match_results = match_params.map(lambda params: match_pairs(*params))
linkages = pd.DataFrame({"exact": match_results,
                         "partial": pd.Series(coord_linkage.map(set) - match_results.map(set)).map(tuple)},
                        index=coord_linkage.index)

# Merge match results with original DataFrame.
df = df.merge(linkages, how="left", left_index=True, right_index=True)

# Set non-matches to empty tuples.
flag = ~df.index.isin(match_results.index)
df.loc[flag, "exact"] = df.loc[flag, "exact"].map(lambda val: ())
df.loc[flag, "partial"] = df.loc[flag, "partial"].map(lambda val: ())
