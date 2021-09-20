import datetime
import geopandas as gpd
import logging
import numpy as np
import pandas as pd
import re
import sys
import time
from operator import attrgetter, itemgetter
from osgeo import ogr
from shapely.geometry import LineString
from shapely.wkt import loads
from typing import Any, List, Union


# Set logger.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)


# Enable ogr exceptions.
ogr.UseExceptions()


class Timer:
    """Tracks stage runtime."""

    def __init__(self) -> None:
        """Initializes the Timer class."""

        self.start_time = None

    def __enter__(self) -> None:
        """Starts the timer."""

        logger.info("Started.")
        self.start_time = time.time()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """
        Computes and returns the elapsed time.

        :param Any exc_type: required parameter for __exit__.
        :param Any exc_val: required parameter for __exit__.
        :param Any exc_tb: required parameter for __exit__.
        """

        total_seconds = time.time() - self.start_time
        delta = datetime.timedelta(seconds=total_seconds)
        logger.info(f"Finished. Time elapsed: {delta}.")


def explode_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Explodes MultiLineStrings and MultiPoints to LineStrings and Points, respectively.

    :param gpd.GeoDataFrame gdf: GeoDataFrame.
    :return gpd.GeoDataFrame: GeoDataFrame containing only single-part geometries.
    """

    logger.info("Standardizing segments: exploding multi-type geometries.")

    multi_types = {"MultiLineString", "MultiPoint"}
    if len(set(gdf.geom_type.unique()).intersection(multi_types)):

        # Separate multi- and single-type records.
        multi = gdf.loc[gdf.geom_type.isin(multi_types)]
        single = gdf.loc[~gdf.index.isin(multi.index)]

        # Explode multi-type geometries.
        multi_exploded = multi.explode().reset_index(drop=True)

        # Merge all records.
        merged = gpd.GeoDataFrame(pd.concat([single, multi_exploded], ignore_index=True), crs=gdf.crs)
        return merged.copy(deep=True)

    else:
        return gdf.copy(deep=True)


def flatten_coordinates(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Flattens the GeoDataFrame geometry coordinates to 2-dimensions.

    :param gpd.GeoDataFrame gdf: GeoDataFrame.
    :return gpd.GeoDataFrame: GeoDataFrame with 2-dimensional coordinates.
    """

    logger.info("Standardizing segments: flattening coordinates to 2-dimensions.")

    try:

        # Flatten coordinates.
        if len(gdf.geom_type.unique()) > 1:
            raise TypeError("Multiple geometry types detected for dataframe.")

        else:

            # Flag records with coordinates not already flattened to 2-dimensions.
            flag = gdf["geometry"].map(lambda g: any(map(lambda pt: len(pt) > 2, attrgetter("coords")(g))))

            # Flatten coordinates for flagged records.
            gdf.loc[flag, "geometry"] = gdf.loc[flag, "geometry"].map(
                lambda g: LineString(itemgetter(0, 1)(pt) for pt in attrgetter("coords")(g)))

    except TypeError as e:
        logger.exception(e)
        sys.exit(1)

    return gdf


def groupby_to_list(df: Union[gpd.GeoDataFrame, pd.DataFrame], group_field: Union[List[str], str], list_field: str) -> \
        pd.Series:
    """
    Faster alternative to :func:`~pd.groupby.apply/agg(list)`.
    Groups records by one or more fields and compiles an output field into a list for each group.

    :param Union[gpd.GeoDataFrame, pd.DataFrame] df: (Geo)DataFrame.
    :param Union[List[str], str] group_field: field or list of fields by which the (Geo)DataFrame records will be
        grouped.
    :param str list_field: (Geo)DataFrame field to output, based on the record groupings.
    :return pd.Series: Series of grouped values.
    """

    if isinstance(group_field, list):
        for field in group_field:
            if df[field].dtype.name != "geometry":
                df[field] = df[field].astype("U")
        transpose = df.sort_values(group_field)[[*group_field, list_field]].values.T
        keys, vals = np.column_stack(transpose[:-1]), transpose[-1]
        keys_unique, keys_indexes = np.unique(keys.astype("U") if isinstance(keys, np.object) else keys,
                                              axis=0, return_index=True)

    else:
        keys, vals = df.sort_values(group_field)[[group_field, list_field]].values.T
        keys_unique, keys_indexes = np.unique(keys, return_index=True)

    vals_arrays = np.split(vals, keys_indexes[1:])

    return pd.Series([list(vals_array) for vals_array in vals_arrays], index=keys_unique).copy(deep=True)


def round_coordinates(gdf: gpd.GeoDataFrame, precision: int = 7) -> gpd.GeoDataFrame:
    """
    Rounds the GeoDataFrame geometry coordinates to a specific decimal precision.

    :param gpd.GeoDataFrame gdf: GeoDataFrame.
    :param int precision: decimal precision to round the GeoDataFrame geometry coordinates to.
    :return gpd.GeoDataFrame: GeoDataFrame with modified decimal precision.
    """

    logger.info(f"Standardizing segments: rounding coordinates to decimal precision: {precision}.")

    try:

        # Flag records with coordinates not already rounded to specified precision.
        flag = gdf["geometry"].map(
            lambda g: max(map(lambda pt: max(len(str(pt[0]).split(".")[-1]),
                                             len(str(pt[1]).split(".")[-1])), attrgetter("coords")(g)))) > precision

        # Round coordinates for flagged records.
        gdf.loc[flag, "geometry"] = gdf.loc[flag, "geometry"].map(
            lambda g: loads(re.sub(r"\d*\.\d+", lambda m: f"{float(m.group(0)):.{precision}f}", g.wkt)))

        return gdf

    except (TypeError, ValueError) as e:
        logger.exception("Unable to round coordinates for GeoDataFrame.")
        logger.exception(e)
        sys.exit(1)
