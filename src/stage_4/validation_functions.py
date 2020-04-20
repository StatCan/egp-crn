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


# Compile default field values and dtypes.
defaults_all = helpers.compile_default_values()
dtypes_all = helpers.compile_dtypes()


def strip_whitespace(df):
    """Strips leading and trailing whitespace from the given value for each dataframe column."""

    mod_flag = False

    # Compile valid columns, excluding geometry.
    df_valid = df.select_dtypes(include="object")
    if "geometry" in df_valid.columns:
        df_valid.drop("geometry", axis=1, inplace=True)

    # Iterate columns.
    for col in df_valid:

        # Apply modification, if required.
        col_mod = df[df[col].map(lambda val: val != val.strip())][col]
        if len(col_mod):
            df.loc[col_mod.index, col] = col_mod.map(str.strip)
            mod_flag = True

            # Log modifications.
            logger.warning("Modified {} record(s) in column {}."
                           "\nModification details: Field values stripped of leading and trailing whitespace."
                           .format(len(col_mod), col))

    if mod_flag:
        return {"errors": None, "modified_dframes": df.copy(deep=True)}
    else:
        return {"errors": None}


def title_route_text(df):
    """
    Sets to title case all route name attributes:
        rtename1en, rtename2en, rtename3en, rtename4en,
        rtename1fr, rtename2fr, rtename3fr, rtename4fr.
    """

    mod_flag = False

    # Identify columns to iterate.
    cols = [col for col in ("rtename1en", "rtename2en", "rtename3en", "rtename4en",
                            "rtename1fr", "rtename2fr", "rtename3fr", "rtename4fr") if col in df.columns]

    # Iterate validation columns.
    for col in cols:

        # Apply modification, if required.
        col_mod = df[df[col].map(lambda route: route != defaults_all["roadseg"][col] and not route.istitle())][col]
        if len(col_mod):
            df.loc[col_mod.index, col] = col_mod.map(str.title)
            mod_flag = True

            # Log modifications.
            logger.warning("Modified {} record(s) in column {}."
                           "\nModification details: Field values set to title case.".format(len(col_mod), col))

    if mod_flag:
        return {"errors": None, "modified_dframes": df.copy(deep=True)}
    else:
        return {"errors": None}


def validate_dates(df):
    """Applies a set of validations to credate and revdate fields."""

    errors = {i: list() for i in range(1, 7+1)}
    mod_flag = False
    defaults = helpers.compile_default_values()["roadseg"]

    # Get current date.
    today_str = datetime.today().strftime("%Y%m%d")
    today = {"year": int(today_str[:4]), "month": int(today_str[4:6]), "day": int(today_str[6:8])}

    # Define functions.
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
        df_sub = df[df[col] != defaults[col]]

        if len(df_sub):

            # Validation 1: date content must be numeric.
            results = df_sub[col].map(lambda date: 1 if not date.isnumeric() else 0)
            errors[1].extend(results[results == 1].index.values)

            # Validation 2: length must be 4, 6, or 8.
            results = df_sub[col].map(lambda date: 1 if len(date) not in (4, 6, 8) else 0)
            errors[2].extend(results[results == 1].index.values)

            # Modification 1: set non-numeric and non-standard length dates to today's date.
            # Apply modification, if required.
            col_mod = df_sub[df_sub[col].map(lambda date: not date.isnumeric() or len(date) not in (4, 6, 8))][col]
            if len(col_mod):
                df_sub.loc[col_mod.index, col] = today_str
                df.loc[col_mod.index, col] = today_str
                mod_flag = True

                # Log modifications.
                logger.warning("Modified {} record(s) in column {}."
                               "\nModification details: Date set to today's date.".format(len(col_mod), col))

            # Modification 2: set missing month and day values to 01.
            # Apply modification, if required.
            col_mod = df_sub[df_sub[col].map(lambda date: len(date) in (4, 6))][col]
            if len(col_mod):
                append_vals = {4: "0101", 6: "01"}
                df_sub.loc[col_mod.index, col] = col_mod.map(lambda date: date + append_vals[len(date)])
                df.loc[col_mod.index, col] = col_mod.map(lambda date: date + append_vals[len(date)])
                mod_flag = True

                # Log modifications.
                logger.warning("Modified {} record(s) in column {}."
                               "\nModification details: Date suffixed with \"01\" for missing month and / or day values"
                               .format(len(col_mod), col))

            # Validation 3: valid date - year.
            results = df_sub[col].map(lambda date: 1 if not (1960 <= int(date[:4]) <= today["year"]) else 0)
            errors[3].extend(results[results == 1].index.values)

            # Validation 4: valid date - month.
            results = df_sub[col].map(lambda date: 1 if (int(date[4:6]) not in range(1, 12+1)) else 0)
            errors[4].extend(results[results == 1].index.values)

            # Validation 5: valid date - day.
            results = df_sub[col].map(lambda date: validate_day(date))
            errors[5].extend(results[results == 1].index.values)

            # Validation 6: ensure date <= today.
            results = df_sub[col].map(lambda date: validate_date_vs_today(date))
            errors[6].extend(results[results == 1].index.values)

    # Validation 7: ensure credate <= revdate.
    df_sub = df[(df["credate"] != defaults["credate"]) & (df["revdate"] != defaults["revdate"])]
    if len(df_sub):
        results = df_sub[["credate", "revdate"]].apply(lambda row: 1 if not int(row[0]) <= int(row[1]) else 0, axis=1)
        errors[7].extend(results[results == 1].index.values)

    if mod_flag:
        return {"errors": errors, "modified_dframes": df.copy(deep=True)}
    else:
        return {"errors": errors}


def validate_exitnbr_conflict(df):
    """Applies a set of validations to exitnbr field."""

    errors = list()
    default = defaults_all["roadseg"]["exitnbr"]

    # Iterate multi-segment road elements (via nid field) and where exitnbr is not the default value.
    query = (df["nid"].duplicated(keep=False)) & (df["nid"] != default) & (df["exitnbr"] != default)
    nid_count = len(df[query]["nid"].unique())
    for index, nid in enumerate(df[query]["nid"].unique()):

        logger.info("Validating road element (nid {} of {}): \"{}\"".format(index + 1, nid_count, nid))

        # Compile exitnbr values, excluding the default value.
        vals = df[(df["nid"] == nid) & (df["exitnbr"] != default)]["exitnbr"].unique()

        # Validation: ensure road element has <= 1 unique exitnbr, excluding the default value.
        if len(vals) > 1:

            # Compile error properties.
            errors.append("nid: \"{}\", exitnbr values: {}".format(nid, ", ".join(map("\"{}\"".format, vals))))

    return {"errors": errors}


def validate_exitnbr_roadclass(df):
    """Applies a set of validations to exitnbr and roadclass fields."""

    # Subset dataframe to non-default values.
    df_subset = df[df["exitnbr"] != defaults_all["roadseg"]["exitnbr"]]

    # Validation: ensure roadclass == "Ramp" or "Service Lane" when exitnbr is not the default value.
    # Compile uuids of flagged records.
    errors = df_subset[~df_subset["roadclass"].isin(["Ramp", "Service Lane"])].index.values

    return {"errors": errors}


def validate_ids(df):
    """
    Applies a set of validations to all id fields.
    Sets all id fields to lowercase.
    """

    errors = {1: list(), 2: list(), 3: list(), 4: list()}
    mod_flag = False

    # Compile fields ending with "id".
    id_fields = [fld for fld in df.columns if fld.endswith("id") and fld != "uuid"]

    # Identify dataframe name to configure dtypes and default values.
    dtypes = dtypes_all["roadseg"]
    defaults = defaults_all["roadseg"]
    for table in defaults_all:
        if all([fld in defaults_all[table] for fld in id_fields]):
            dtypes = dtypes_all[table]
            defaults = defaults_all[table]
            break

    # Iterate str id fields.
    for field in [fld for fld in df.columns if fld.endswith("id") and fld != "uuid" and dtypes[fld] == "str"]:

        # Subset dataframe to non-default values.
        df_sub = df[df[field] != defaults[field]]

        if len(df_sub):

            # Modification: set ids to lowercase.
            # Apply modification, if required.
            col_mod = df_sub[df_sub[field].map(lambda val: val != val.lower())][field]
            if len(col_mod):
                df_sub.loc[col_mod.index, field] = col_mod.map(str.lower)
                df.loc[col_mod.index, field] = col_mod.map(str.lower)
                mod_flag = True

                # Log modifications.
                logger.warning("Modified {} record(s) in column {}."
                               "\nModification details: Field values set to lower case.".format(len(col_mod), field))

            # Validation 1: ensure ids are 32 digits.
            # Compile uuids of flagged records.
            flag_uuids = df_sub[df_sub[field].map(lambda val: len(val) != 32)].index.values
            for val in flag_uuids:
                errors[1].append("uuid: {}, based on attribute field: {}.".format(val, field))

            # Validation 2: ensure ids are hexadecimal.
            # Compile uuids of flagged records.
            flag_uuids = df_sub[df_sub[field].map(
                lambda val: not all(map(lambda c: c in string.hexdigits, set(val))))].index.values
            for val in flag_uuids:
                errors[2].append("uuid: {}, based on attribute field: {}.".format(val, field))

    # Iterate unique id fields.
    unique_fields = ["ferrysegid", "roadsegid"]
    for field in [fld for fld in unique_fields if fld in df.columns]:

        # Validation 3: ensure unique id fields are unique.
        # Compile uuids of flagged records.
        flag_uuids = df[df[field].duplicated(keep=False)].index.values
        for val in flag_uuids:
            errors[3].append("uuid: {}, based on attribute field: {}.".format(val, field))

        # Validation 4: ensure unique id fields are not the default field value.
        # Compile uuids of flagged records.
        flag_uuids = df[df[field] == defaults[field]].index.values
        for val in flag_uuids:
            errors[4].append("uuid: {}, based on attribute field: {}.".format(val, field))

    if mod_flag:
        return {"errors": errors, "modified_dframes": df.copy(deep=True)}
    else:
        return {"errors": errors}


def validate_nbrlanes(df):
    """Applies a set of validations to nbrlanes field."""

    # Subset dataframe to non-default values.
    df_subset = df[df["nbrlanes"] != defaults_all["roadseg"]["nbrlanes"]]

    # Validation: ensure 1 <= nbrlanes <= 8.
    flags = df_subset["nbrlanes"].map(lambda nbrlanes: not 1 <= int(nbrlanes) <= 8)

    # Compile uuids of flagged records.
    errors = df_subset[flags].index.values

    return {"errors": errors}


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

    return {"errors": errors}


def validate_roadclass_rtnumber1(df):
    """Applies a set of validations to roadclass and rtnumber1 fields."""

    # Apply validations and compile uuids of flagged records.

    # Validation: ensure rtnumber1 is not the default value when roadclass == "Freeway" or "Expressway / Highway".
    errors = df[df["roadclass"].isin(["Freeway", "Expressway / Highway"]) &
                df["rtnumber1"].map(lambda rtnumber1: rtnumber1 == defaults_all["roadseg"]["rtnumber1"])].index.values

    return {"errors": errors}


def validate_roadclass_self_intersection(df):
    """Applies a set of validations to roadclass and structtype fields."""

    default = defaults_all["roadseg"]["nid"]

    # Validation: ensure roadclass is in ("Expressway / Highway", "Freeway", "Ramp", "Rapid Transit") for all road
    #             elements which a) self-intersect and b) touch another road segment where roadclass is in this set.

    flag_nids = list()
    valid = ["Expressway / Highway", "Freeway", "Ramp", "Rapid Transit"]

    # Compile coords of road segments where roadclass is in the validation list.
    valid_coords = list(set(chain(
        *[itemgetter(0, -1)(geom.coords) for geom in df[df["roadclass"].isin(valid)]['geometry'].values])))

    # Single-segment road elements:

    # Retrieve single-segment self-intersections.
    # Function call intended to avoid duplicating logic in this current function.
    segments_single = validate_roadclass_structtype(df, return_segments_only=True)

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
        nid_count = len(segments_multi[flag_intersect]["nid"].unique())
        for index, nid in enumerate(segments_multi[flag_intersect]["nid"].unique()):

            logger.info("Validating road element (nid {} of {}): \"{}\"".format(index + 1, nid_count, nid))

            # Dissolve road segments.
            element = shapely.ops.linemerge(df[df["nid"] == nid]["geometry"].values)

            # Identify self-intersections.
            if element.is_ring or not element.is_simple:

                # Store nid.
                flag_nids.append(nid)

    # Compile uuids of road segments with flagged nid and invalid roadclass.
    errors = df[(df["nid"].isin(flag_nids)) & (~df["roadclass"].isin(valid))].index.values
    return {"errors": errors}


def validate_roadclass_structtype(df, return_segments_only=False):
    """Applies a set of validations to roadclass and structtype fields."""

    flag_segments = pd.DataFrame()
    errors = list()
    default = defaults_all["roadseg"]["nid"]

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

    if return_segments_only:
        return flag_segments
    else:
        return {"errors": errors}


def validate_route_contiguity(roadseg, ferryseg):
    """
    Applies a set of validations to route attributes (rows represent field groups):
        rtename1en, rtename2en, rtename3en, rtename4en,
        rtename1fr, rtename2fr, rtename3fr, rtename4fr,
        rtnumber1, rtnumber2, rtnumber3, rtnumber4, rtnumber5.
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
        route_names = [names[np.where(names != defaults_all["roadseg"][field_group[index]])]
                       for index, names in enumerate(route_names)]
        # Concatenate arrays.
        route_names = np.concatenate(route_names, axis=None)
        # Remove duplicates.
        route_names = np.unique(route_names)
        # Sort route names.
        route_names = sorted(route_names)

        # Iterate route names.
        route_count = len(route_names)
        for index, route_name in enumerate(route_names):

            logger.info("Validating route {} of {}: \"{}\".".format(index + 1, route_count, route_name))

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

    return {"errors": errors}


def validate_speed(df):
    """Applies a set of validations to speed field."""

    errors = dict()

    # Subset dataframe to non-default values.
    df_subset = df[df["speed"] != defaults_all["roadseg"]["speed"]]

    # Validation: ensure 5 <= speed <= 120.
    flags = df_subset["speed"].map(lambda speed: not 5 <= int(speed) <= 120)

    # Compile uuids of flagged records.
    errors[1] = df_subset[flags].index.values

    # Validation 2: ensure speed is a multiple of 5.
    flags = df_subset["speed"].map(lambda speed: int(speed) % 5 != 0)

    # Compile uuids of flagged records.
    errors[2] = df_subset[flags].index.values

    return {"errors": errors}
