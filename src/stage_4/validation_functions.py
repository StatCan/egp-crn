import calendar
import fiona
import geopandas as gpd
import logging
import networkx as nx
import numpy as np
import os
import pandas as pd
import shapely.ops
import string
import sys
from datetime import datetime
from itertools import chain
from operator import itemgetter

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import helpers


logger = logging.getLogger()


def strip_whitespace(df):
    """Strips leading and trailing whitespace from the given value for each dataframe column."""

    mods = list()

    # Compile valid columns, excluding geometry.
    df_valid = df.select_dtypes(include="object")
    if "geometry" in df_valid.columns:
        df_valid.drop("geometry", axis=1, inplace=True)

    # Iterate columns, applying modification.
    for col in df_valid:
        if df[col].map(lambda val: val != val.strip()).any():
            df[col] = df[col].map(str.strip)
            mods.append(col)

    # Configure modification message.
    if len(mods):
        mods = "Fields stripped of whitespace: {}.".format(", ".join(map("\"{}\"".format, mods)))

    return df, mods


def title_route_text(df, default):
    """
    Sets to title case all route name attributes:
        rtename1en, rtename2en, rtename3en, rtename4en,
        rtename1fr, rtename2fr, rtename3fr, rtename4fr.
    Parameter default should be a dictionary with a key for each of the required fields.
    """

    # Set text-based route fields to title case, except default value.
    cols = ["rtename1en", "rtename2en", "rtename3en", "rtename4en",
            "rtename1fr", "rtename2fr", "rtename3fr", "rtename4fr"]

    mods = df[cols].copy(deep=True)

    for col in cols:
        orig_series = df[col].copy(deep=True)
        df[col] = df[col].map(lambda route: route if route == default[col] else route.title())

        # Store modifications flag for column.
        mods[col] = pd.Series(orig_series != df[col])

    # Compile uuids of modified records.
    mods = mods[mods.any(axis=1)].index.values

    return df, mods


def validate_dates(df, default):
    """
    Applies a set of validations to credate and revdate fields.
    Parameter default is assumed to be identical for credate and revdate fields.
    """

    errors = {i: list() for i in range(1, 6+1)}
    mods = list()

    # Get current date.
    today_str = datetime.today().strftime("%Y%m%d")
    today = {"year": int(today_str[:4]), "month": int(today_str[4:6]), "day": int(today_str[6:8])}

    # Define functions.
    def modify_date(uuid, date):
        """
        1) Replaces non-numeric or non-standard length values with today's date to avoid ValueErrors in validations.
        2) Appends 01 to date for missing month and day values.
        Appends uuid to mods series if modification is actually applied.
        """

        # Set non-numeric or non-standard length value to today's date.
        try:
            int(date)
        except ValueError:
            mods.append(uuid)
            return today_str

        if len(date) not in (4, 6, 8):
            mods.append(uuid)
            return today_str

        # Append 01 to incomplete date values.
        flag = False

        while len(date) in (4, 6):
            date += "01"
            flag = True

        if flag:
            mods.append(uuid)

        return date

    def validate_day(date):
        """Validate the day value in a date."""

        year, month, day = map(int, [date[:4], date[4:6], date[6:8]])

        if not 1 <= day <= calendar.mdays[month]:
            if not all([day == 29, month == 2, calendar.isleap(year)]):
                return 1

        return 0

    def validate_date_vs_today(date):
        """Validate the date relative to today's date."""

        year, month, day = map(int, [date[:4], date[4:6], date[6:8]])

        if year == today["year"]:
            if not all([month <= today["month"], day <= today["day"]]):
                return 1

        return 0

    # Iterate credate and revdate, applying validations.
    for col in ("credate", "revdate"):

        # Subset to non-default values.
        df_sub = df[df[col] != default]

        if len(df_sub):

            # Validation 1: length must be 4, 6, or 8.
            results = df_sub[col].map(lambda date: 1 if len(date) not in (4, 6, 8) else 0)
            errors[1].extend(results[results == 1].index.values)

            # Modification: default to 01 for missing month and day values.
            df_sub["uuid_temp"] = df.index
            df_sub[col] = df_sub[["uuid_temp", col]].apply(lambda row: modify_date(*row), axis=1)
            df_sub.drop("uuid_temp", axis=1, inplace=True)
            df[col] = df_sub[col]

            # Validation 2: valid date - year.
            results = df_sub[col].map(lambda date: 1 if not (1960 <= int(date[:4]) <= today["year"]) else 0)
            errors[2].extend(results[results == 1].index.values)

            # Validation 3: valid date - month.
            results = df_sub[col].map(lambda date: 1 if (int(date[4:6]) not in range(1, 12+1)) else 0)
            errors[3].extend(results[results == 1].index.values)

            # Validation 4: valid date - day.
            results = df_sub[col].map(lambda date: validate_day(date))
            errors[4].extend(results[results == 1].index.values)

            # Validation 5: ensure date <= today.
            results = df_sub[col].map(lambda date: validate_date_vs_today(date))
            errors[5].extend(results[results == 1].index.values)

    # Validation 6: ensure credate <= revdate.
    df_sub = df[(df["credate"] != default) & (df["revdate"] != default)]
    if len(df_sub):
        results = df_sub[["credate", "revdate"]].apply(lambda row: 1 if not int(row[0]) <= int(row[1]) else 0, axis=1)
        errors[6].extend(results[results == 1].index.values)

    return df[["credate", "revdate"]], errors, mods


def validate_exitnbr_conflict(df, default):
    """
    Applies a set of validations to exitnbr field.
    Parameter default should refer to exitnbr.
    """

    errors = list()

    # Iterate multi-segment road elements (via nid field) and where exitnbr is not the default value.
    query = (df["nid"].duplicated(keep=False)) & (df["nid"] != default) & (df["exitnbr"] != default)
    for nid in df[query]["nid"].unique():

        # Compile exitnbr values, excluding the default value.
        vals = df[(df["nid"] == nid) & (df["exitnbr"] != default)]["exitnbr"].unique()

        # Validation: ensure road element has <= 1 unique exitnbr, excluding the default value.
        if len(vals) > 1:

            # Compile error properties.
            errors.append("nid: \"{}\", exitnbr values: {}".format(nid, ", ".join(map("\"{}\"".format, vals))))

    return errors


def validate_exitnbr_roadclass(df, default):
    """
    Applies a set of validations to exitnbr and roadclass fields.
    Parameter default should refer to exitnbr.
    """

    # Subset dataframe to non-default values.
    df_subset = df[df["exitnbr"] != default]

    # Validation: ensure roadclass == "Ramp" or "Service Lane" when exitnbr is not the default value.
    # Compile uuids of flagged records.
    errors = df_subset[~df_subset["roadclass"].isin(["Ramp", "Service Lane"])].index.values

    return errors


def validate_ids(name, df, default):
    """
    Applies a set of validations to all id fields.
    Sets all id fields to lowercase.
    Parameter default should be a dictionary with a key for each of the required fields.
    """

    mods = list()
    errors = {1: list(), 2: list(), 3: list(), 4: list()}
    dtypes = helpers.compile_dtypes()

    # Iterate fields ending with "id".
    for field in [fld for fld in df.columns if fld.endswith("id") and fld != "uuid" and dtypes[name][fld] == "str"]:

        # Subset dataframe to non-default values.
        df_subset = df[df[field] != default[field]]

        # Modification: set ids to lowercase.
        if df_subset[field].map(lambda val: val != val.lower()).any():
            df_subset[field] = df_subset[field].map(str.lower)
            df.loc[df_subset.index, field] = df_subset[field]
            mods.append(field)

        # Validation 1: ensure ids are 32 digits.
        # Compile uuids of flagged records.
        flag_uuids = df_subset[df_subset[field].map(lambda val: len(val) != 32)].index.values
        for val in flag_uuids:
            errors[1].append("uuid: {}, based on attribute field: {}.".format(val, field))

        # Validation 2: ensure ids are hexadecimal.
        # Compile uuids of flagged records.
        flag_uuids = df_subset[df_subset[field].map(
            lambda val: not all(map(lambda c: c in string.hexdigits, set(val))))].index.values
        for val in flag_uuids:
            errors[2].append("uuid: {}, based on attribute field: {}.".format(val, field))

    # Configure modification message.
    if len(mods):
        mods = "Fields set to lowercase: {}.".format(", ".join(map("\"{}\"".format, mods)))

    # Iterate unique id fields.
    unique_fields = {
        "addrange": ["nid"],
        "altnamlink": ["nid"],
        "blkpassage": ["nid"],
        "ferryseg": ["ferrysegid"],
        "junction": ["nid"],
        "roadseg": ["roadsegid"],
        "strplaname": ["nid"],
        "tollpoint": ["nid"]
    }

    for field in unique_fields[name]:

        # Validation 3: ensure certain id fields are unique.
        # Compile uuids of flagged records.
        flag_uuids = df[df[field].duplicated(keep=False)].index.values
        for val in flag_uuids:
            errors[3].append("uuid: {}, based on attribute field: {}.".format(val, field))

        # Validation 4: ensure unique id fields are not the default field value.
        # Compile uuids of flagged records.
        flag_uuids = df[df[field] == default[field]].index.values
        for val in flag_uuids:
            errors[4].append("uuid: {}, based on attribute field: {}.".format(val, field))

    return df, errors, mods


def validate_nbrlanes(df, default):
    """Applies a set of validations to nbrlanes field."""

    # Subset dataframe to non-default values.
    df_subset = df[df["nbrlanes"] != default]

    # Validation: ensure 1 <= nbrlanes <= 8.
    flags = df_subset["nbrlanes"].map(lambda nbrlanes: not 1 <= int(nbrlanes) <= 8)

    # Compile uuids of flagged records.
    errors = df_subset[flags].index.values

    return errors


def validate_pavement(df):
    """Applies a set of validations to pavstatus, pavsurf, and unpavsurf fields."""

    errors = dict()

    # Apply validations and compile uuids of flagged records.

    # Validation: when pavstatus == "Paved", ensure pavsurf != "None" and unpavsurf == "None".
    errors[1] = df[(df["pavstatus"] == "Paved") & (df["pavsurf"] == "None")].index.values
    errors[2] = df[(df["pavstatus"] == "Paved") & (df["unpavsurf"] != "None")].index.values

    # Validation: when pavstatus == "Unpaved", ensure pavsurf == "None" and unpavsurf != "None".
    errors[3] = df[(df["pavstatus"] == "Unpaved") & (df["pavsurf"] != "None")].index.values
    errors[4] = df[(df["pavstatus"] == "Unpaved") & (df["unpavsurf"] == "None")].index.values

    return errors


def validate_roadclass_rtnumber1(df, default):
    """
    Applies a set of validations to roadclass and rtnumber1 fields.
    Parameter default should refer to rtnumber1.
    """

    # Apply validations and compile uuids of flagged records.

    # Validation: ensure rtnumber1 is not the default value when roadclass == "Freeway" or "Expressway / Highway".
    errors = df[df["roadclass"].isin(["Freeway", "Expressway / Highway"]) &
                df["rtnumber1"].map(lambda rtnumber1: rtnumber1 == default)].index.values

    return errors


def validate_roadclass_self_intersection(df, default):
    """
    Applies a set of validations to roadclass and structtype fields.
    Parameter default should refer to nid.
    """

    errors = dict()

    # Validation: for self-intersecting road segments, ensure structtype != "None".
    segments_single, errors[1] = validate_roadclass_structtype(df, default)

    # Validation: ensure roadclass is in ("Expressway / Highway", "Freeway", "Ramp", "Rapid Transit") for all road
    #             elements which a) self-intersect and b) touch another road segment where roadclass is in the
    #             aforementioned set.

    flag_nids = list()
    valid = ["Expressway / Highway", "Freeway", "Ramp", "Rapid Transit"]

    # Compile coords of road segments where roadclass is in the validation list.
    valid_coords = list(set(chain(
        *[itemgetter(0, -1)(geom.coords) for geom in df[df["roadclass"].isin(valid)]['geometry'].values])))

    # Single-segment road elements:

    if not segments_single.empty:

        # Compile nids of road segments with coords in the validation coords list.
        flag_intersect = np.vectorize(lambda geom: geom.coords[0] in valid_coords)(segments_single["geometry"].values)
        flag_nids.extend(segments_single[flag_intersect]["nid"].values)

    # Multi-segment road elements:

    # Compile multi-segment road elements (via non-unique nids).
    # Filter to nids with invalid roadclass (intended to reduce spatial processing).
    segments_multi = df[(df["nid"].duplicated(keep=False)) & (~df["roadclass"].isin(valid)) & (df["nid"] != default)]

    if not segments_multi.empty:

        logger.info("Validating multi-segment road elements.")

        # Compile nids of road segments with coords in the validation coords list.
        intersect_func = lambda geom: any(coord in valid_coords for coord in itemgetter(0, -1)(geom.coords))
        flag_intersect = np.vectorize(intersect_func)(segments_multi["geometry"].values)

        # Iterate flagged elements to identify self-intersections.
        for nid in segments_multi[flag_intersect]["nid"].unique():

            # Dissolve road segments.
            element = shapely.ops.linemerge(df[df["nid"] == nid]["geometry"].values)

            # Identify self-intersections.
            if element.is_ring or not element.is_simple:

                # Store nid.
                flag_nids.append(nid)

    # Compile uuids of road segments with flagged nid and invalid roadclass.
    errors[2] = df[(df["nid"].isin(flag_nids)) & (~df["roadclass"].isin(valid))].index.values
    return errors


def validate_roadclass_structtype(df, default):
    """
    Applies a set of validations to roadclass and structtype fields.
    Parameter default should refer to nid.
    """

    flag_segments = pd.DataFrame()
    errors = list()

    # Identify self-intersections formed by single-segment road elements (i.e. where nid is unique).

    # Compile single-segment road elements (via unique nids).
    segments = df[(~df["nid"].duplicated(keep=False)) & (df["nid"] != default)]

    if not segments.empty:

        logger.info("Validating single-segment road elements.")

        # Identify self-intersections (start coord == end coord).
        flag_self_intersect = np.vectorize(lambda geom: geom.coords[0] == geom.coords[-1])(segments["geometry"].values)
        flag_segments = segments[flag_self_intersect]

        # Validation: for self-intersecting road segments, ensure structtype != "None".
        errors = flag_segments[flag_segments["structtype"] == "None"].index.values

    return flag_segments, errors


def validate_route_contiguity(ferryseg, roadseg, default):
    """
    Applies a set of validations to route attributes (rows represent field groups):
        rtename1en, rtename2en, rtename3en, rtename4en,
        rtename1fr, rtename2fr, rtename3fr, rtename4fr,
        rtnumber1, rtnumber2, rtnumber3, rtnumber4, rtnumber5.
    Parameter default should be a dictionary with a key for each of the required fields.
    """

    errors = list()

    # Concatenate ferryseg and roadseg.
    df = gpd.GeoDataFrame(pd.concat([ferryseg, roadseg], ignore_index=True, sort=False))

    # Validation: ensure route has contiguous geometry.
    for field_group in [["rtename1en", "rtename2en", "rtename3en", "rtename4en"],
                        ["rtename1fr", "rtename2fr", "rtename3fr", "rtename4fr"],
                        ["rtnumber1", "rtnumber2", "rtnumber3", "rtnumber4", "rtnumber5"]]:

        logger.info("Validating routes in field group: {}.".format(", ".join(map("\"{}\"".format, field_group))))

        # Compile route names.
        route_names = [df[col].unique() for col in field_group]
        # Remove default values.
        route_names = [names[np.where(names != default[field_group[index]])] for index, names in enumerate(route_names)]
        # Concatenate arrays.
        route_names = np.concatenate(route_names, axis=None)
        # Remove duplicates.
        route_names = np.unique(route_names)
        # Sort route names.
        route_names = sorted(route_names)

        # Iterate route names.
        for route_name in route_names:

            logger.info("Validating route: \"{}\".".format(route_name))

            # Subset dataframe to those records with route name in at least one field.
            route_df = df.iloc[list(np.where(df[field_group] == route_name)[0])]

            # Load dataframe as networkx graph.
            route_graph = helpers.gdf_to_nx(route_df, keep_attributes=False)

            # Validate contiguity (networkx connectivity).
            if not nx.is_connected(route_graph):

                # Identify deadends (locations of discontiguity).
                deadends = [coords for coords, degree in route_graph.degree() if degree == 1]
                deadends = "\n".join(["{}, {}".format(*deadend) for deadend in deadends])

                # Compile error properties.
                errors.append("Route name: \"{}\", based on attribute fields: {}."
                              "\nEndpoints:\n{}.".format(route_name, ", ".join(field_group), deadends))

    return errors


def validate_speed(df, default):
    """Applies a set of validations to speed field."""

    errors = dict()

    # Subset dataframe to non-default values.
    df_subset = df[df["speed"] != default]

    # Validation: ensure 5 <= speed <= 120.
    flags = df_subset["speed"].map(lambda speed: not 5 <= int(speed) <= 120)

    # Compile uuids of flagged records.
    errors[1] = df_subset[flags].index.values

    # Validation 2: ensure speed is a multiple of 5.
    flags = df_subset["speed"].map(lambda speed: int(speed) % 5 != 0)

    # Compile uuids of flagged records.
    errors[2] = df_subset[flags].index.values

    return errors
