import datetime
import geopandas as gpd
import logging
import numpy as np
import pandas as pd
import sqlite3
import string
import sys
import time
import uuid
from operator import attrgetter
from osgeo import ogr, osr
from pathlib import Path
from tqdm import tqdm
from typing import Any, List, Tuple, Union


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


def explode_geometry(gdf: gpd.GeoDataFrame, index: str) -> gpd.GeoDataFrame:
    """
    Explodes MultiLineStrings and MultiPoints to LineStrings and Points, respectively.

    :param gpd.GeoDataFrame gdf: GeoDataFrame.
    :param str index: index column name.
    :return gpd.GeoDataFrame: GeoDataFrame containing only single-part geometries.
    """
    
    # Reset index, conditionally drop.
    gdf.reset_index(drop=(index in gdf.columns), inplace=True)
    
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


def export(df: gpd.GeoDataFrame, dst: Path, name: str) -> None:
    """
    Exports a GeoDataFrame to a GeoPackage.

    :param gpd.GeoDataFrame df: GeoDataFrame containing LineStrings.
    :param Path dst: output GeoPackage path.
    :param str name: output GeoPackage layer name.
    """

    try:

        # Open GeoPackage.
        driver = ogr.GetDriverByName("GPKG")
        gpkg = driver.Open(str(dst), update=1)

        # Configure spatial reference system.
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(df.crs.to_epsg())

        # Create GeoPackage layer.
        geom_type = attrgetter(f"wkb{df.geom_type.iloc[0]}")(ogr)
        layer = gpkg.CreateLayer(name=name, srs=srs, geom_type=geom_type, options=["OVERWRITE=YES"])

        # Set field definitions.
        ogr_field_map = {"b": ogr.OFSTBoolean, "i": ogr.OFTInteger, "O": ogr.OFTString}
        for field_name, field_dtype in df.dtypes.to_dict().items():
            if field_name != "geometry":
                field_defn = ogr.FieldDefn(field_name, ogr_field_map[field_dtype.kind])
                layer.CreateField(field_defn)

        # Write layer.
        layer.StartTransaction()

        for feat in tqdm(df.itertuples(index=False), total=len(df),
                         desc=f"Writing to file: {gpkg.GetName()}|layer={name}",
                         bar_format="{desc}: |{bar}| {percentage:3.0f}% {r_bar}"):

            # Instantiate feature.
            feature = ogr.Feature(layer.GetLayerDefn())

            # Compile feature properties.
            properties = feat._asdict()

            # Set feature geometry.
            geom = ogr.CreateGeometryFromWkb(properties.pop("geometry").wkb)
            feature.SetGeometry(geom)

            # Iterate and set feature properties (attributes).
            for field_index, prop in enumerate(properties.items()):
                feature.SetField(field_index, prop[-1])

            # Create feature.
            layer.CreateFeature(feature)

            # Clear pointer for next iteration.
            feature = None

        layer.CommitTransaction()

    except (KeyError, ValueError, sqlite3.Error) as e:
        logger.exception(f"Error raised when writing output: {dst}|layer={name}.")
        logger.exception(e)
        sys.exit(1)


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


def update_ids(gdf: gpd.GeoDataFrame, identifier: str, index: bool = True) -> Tuple[gpd.GeoDataFrame, bool]:
    """
    Updates identifiers if they are not unique 32 digit hexadecimal strings.

    :param gpd.GeoDataFrame gdf: GeoDataFrame.
    :param str identifier: identifier column.
    :param bool index: assigns the identifier column as GeoDataFrame index, default = True.
    :return Tuple[gpd.GeoDataFrame, bool]: updated GeoDataFrame and flag indicating if records have been modified.
    """

    logger.info(f"Resolving segment identifiers for: \"{identifier}\".")

    try:

        export_flag = False

        # Cast identifier to str and float columns to int.
        gdf[identifier] = gdf[identifier].astype(str)
        for col in gdf.columns:
            if gdf[col].dtype.kind == "f":
                gdf.loc[gdf[col].isna(), col] = -1
                gdf[col] = gdf[col].astype(int)

                # Trigger export requirement for class.
                export_flag = True

        # Flag invalid identifiers.
        hexdigits = set(string.hexdigits)
        flag_non_hex = (gdf[identifier].map(len) != 32) | \
                       (gdf[identifier].map(lambda val: not set(val).issubset(hexdigits)))
        flag_dups = (gdf[identifier].duplicated(keep=False)) & (gdf[identifier] != "None")
        flag_invalid = flag_non_hex | flag_dups

        # Resolve invalid identifiers.
        if sum(flag_invalid):
            logger.warning(f"Resolving {sum(flag_invalid)} invalid identifiers for: \"{identifier}\".")

            # Overwrite identifiers.
            gdf.loc[flag_invalid, identifier] = [uuid.uuid4().hex for _ in range(sum(flag_invalid))]

            # Trigger export requirement for class.
            export_flag = True

        # Assign index.
        if index:
            gdf.index = gdf[identifier]

        return gdf.copy(deep=True), export_flag

    except ValueError as e:
        logger.exception(f"Unable to validate segment identifiers for \"{identifier}\".")
        logger.exception(e)
        sys.exit(1)
