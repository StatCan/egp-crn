import fiona
import geopandas as gpd
import numpy as np
import os
import pandas as pd
import sys
import uuid
from operator import itemgetter
from shapely.geometry import Point

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import helpers


# Load GeoPackage.
dframes = helpers.load_gpkg("../../data/interim/pe.gpkg")

# Concatenate ferryseg and roadseg dataframes, if possible.
if "ferryseg" in dframes:
    df = gpd.GeoDataFrame(pd.concat(itemgetter("ferryseg", "roadseg")(dframes), ignore_index=False, sort=False))
else:
    df = dframes["roadseg"].copy(deep=True)

# Compile uuid groups for all endpoints.

# Construct a uuid series aligned to the series of endpoints.
pts_uuid = df["uuid"].values.repeat(2)

# Construct x- and y-coordinate series aligned to the series of points.
pts_x, pts_y = np.concatenate([np.array(itemgetter(0, -1)(geom.coords)) for geom in df["geometry"]]).T

# Join the uuids, x-, and y-coordinates.
pts_df = pd.DataFrame({"x": pts_x, "y": pts_y, "uuid": pts_uuid})

# Group uuids according to x- and y-coordinates.
uuids_grouped = pts_df.groupby(["x", "y"])["uuid"].apply(list)

# junctype: NatProvTer.
# Process: Load boundaries, query indexes (points) not within boundaries, store indexes, drop results from dataframe.
boundary = gpd.read_file("../../data/interim/boundaries.geojson", crs=dframes["roadseg"].crs)
natprovter = uuids_grouped[~np.vectorize(
    lambda coords: Point(coords).within(boundary["geometry"][0]))(uuids_grouped.index)].index.values
uuids_grouped.drop(natprovter, inplace=True)

# junctype: Ferry.
# Process: If ferryseg exists, query indexes (points) where the uuid group contains a ferryseg uuid (via set
# subtraction), store indexes, drop results from dataframe.
if "ferryseg" in dframes:

    ferryseg_uuids = set(dframes["ferryseg"]["uuid"].values)

    ferry = uuids_grouped[uuids_grouped.map(
        lambda uuids: len(set(uuids) - ferryseg_uuids) < len(set(uuids)))].index.values
    uuids_grouped.drop(ferry, inplace=True)

# junctype: Dead End.
# Process: Query indexes (points) with a uuid group of only 1 uuid, store indexes, no need to drop results.
deadend = uuids_grouped[uuids_grouped.map(len) == 1].index.values

# junctype: Intersection.
# Process: Query indexes (points) with a uuid group of >= 3 unique uuids, store indexes, no need to drop results.
intersection = uuids_grouped[uuids_grouped.map(lambda uuids: len(set(uuids)) >= 3)].index.values