import logging
import os
import pandas as pd
import sys
from osgeo import osr
from shapely.geometry import LineString

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import helpers


logger = logging.getLogger()


def identify_duplicate_lines(df):
    """Identifies the uuids of duplicate line geometries."""

    # Filter geometries to those with duplicate lengths.
    df_same_len = df[df["geometry"].length.duplicated(keep=False)]

    # Identify duplicate geometries.
    mask = df_same_len["geometry"].map(lambda geom1: df_same_len["geometry"].map(lambda geom2:
                                                                                 geom1.equals(geom2)).sum() > 1)

    # Compile uuids of flagged records.
    flag_uuids = df_same_len[mask].index.values
    errors = pd.Series(df.index.isin(flag_uuids), index=df.index)

    return errors


def identify_duplicate_points(df):
    """Identifies the uuids of duplicate point geometries."""

    # Retrieve coordinates as tuples.
    if df.geom_type[0] == "MultiPoint":
        coords = df["geometry"].map(lambda geom: geom[0].coords[0])
    else:
        coords = df["geometry"].map(lambda geom: geom.coords[0])

    # Identify duplicate geometries.
    mask = coords.duplicated(keep=False)

    # Compile uuids of flagged records.
    flag_uuids = df[mask].index.values
    errors = pd.Series(df.index.isin(flag_uuids), index=df.index)

    return errors


def validate_min_length(df):
    """Validates the minimum feature length of a GeoDataFrame of LineStrings."""

    # Filter records to 0.0002 degrees length (approximately 22.2 meters).
    # Purely intended to reduce processing.
    df_sub = df[df.length <= 0.0002]

    # Transform records to a meter-based crs: EPSG:3348.

    # Define transformation.
    prj_source, prj_target = osr.SpatialReference(), osr.SpatialReference()
    prj_source.ImportFromEPSG(4617)
    prj_target.ImportFromEPSG(3348)
    prj_transformer = osr.CoordinateTransformation(prj_source, prj_target)

    # Transform records, keeping only the length property.
    df_prj = df_sub["geometry"].map(lambda geom: LineString(prj_transformer.TransformPoints(geom.coords)).length)

    # Validation: ensure line segments are >= 2 meters in length.
    flag_uuids = df_prj[df_prj < 2].index.values
    errors = pd.Series(df.index.isin(flag_uuids), index=df.index)

    return errors
