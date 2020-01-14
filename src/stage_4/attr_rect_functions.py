import calendar
import logging
import networkx as nx
import numpy as np
import os
import pandas as pd
import shapely.ops
import sys
from datetime import datetime
from itertools import chain
from operator import itemgetter

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import helpers


logger = logging.getLogger()


def strip_whitespace(val):
    """Strips leading and trailing whitespace from the given value."""

    return val.strip()


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

    orig_df = df[cols]

    for col in cols:
        orig_series = df[col]
        df[col] = df[col].map(lambda route: route if route == default[col] else route.title())

        # Store modifications flag for column.
        orig_df[col] = pd.Series(orig_series != df[col])

    # Configure final modification flags.
    mod_flags = orig_df.any(axis=1)

    return df, mod_flags


def validate_dates(credate, revdate, default):
    """
    Applies a set of validations to credate and revdate fields.
    Parameter default is assumed to be identical for credate and revdate fields.
    """

    credate, revdate, default = map(str, [credate, revdate, default])

    # Get current date.
    today = datetime.today().strftime("%Y%m%d")

    # Validation.
    def validate(date):

        # Set default mod flag.
        mod_flag = False

        # Apply validation.
        if date != default:

            # Validation: length must be 4, 6, or 8.
            if len(date) not in (4, 6, 8):
                return date, 1, mod_flag

            # Rectification: default to 01 for missing month and day values.
            while len(date) in (4, 6):
                date += "01"

                # Update mod flag.
                mod_flag = True

            # Validation: valid values for day, month, year (1960+).
            year, month, day = map(int, [date[:4], date[4:6], date[6:8]])

            # Year.
            if not 1960 <= year <= int(today[:4]):
                return date, 2, mod_flag

            # Month.
            if month not in range(1, 12 + 1):
                return date, 3, mod_flag

            # Day.
            if not 1 <= day <= calendar.mdays[month]:
                if not all([day == 29, month == 2, calendar.isleap(year)]):
                    return date, 4, mod_flag

            # Validation: ensure value <= today.
            if year == today[:4]:
                if not all([month <= today[4:6], day <= today[6:8]]):
                    return date, 5, mod_flag

        return date, 0, mod_flag

    # Validation: individual date validations.
    credate, error_flag, mod_flag = validate(credate)
    if error_flag == 0:
        revdate, error_flag, mod_flag2 = validate(revdate)

        # Configure mod flag.
        if any([mod_flag, mod_flag2]):
            mod_flag = True

    if error_flag == 0:
        # Validation: ensure credate <= revdate.
        if credate != default and revdate != default:
            if not int(credate) <= int(revdate):
                error_flag = 6

    return (credate, revdate), (error_flag, mod_flag)


def validate_exitnbr_conflict(df, default):
    """
    Applies a set of validations to exitnbr field.
    Parameter default should refer to exitnbr.
    """

    errors = dict()

    # Iterate road elements comprised of multiple road segments (via nid field) and where exitnbr is not the default
    # value.
    for nid in df[(df["nid"].duplicated(keep=False)) & (df["exitnbr"] != default)]["nid"].unique():

        # Compile exitnbr values, excluding the default value.
        vals = df[(df["nid"] == nid) & (df["exitnbr"] != default)]["exitnbr"].unique()

        # Validation: ensure road element has <= 1 unique exitnbr, excluding the default value.
        if len(vals) > 1:

            # Compile error properties.
            errors[nid] = vals

    return errors


def validate_exitnbr_roadclass(exitnbr, roadclass, default):
    """
    Applies a set of validations to exitnbr and roadclass fields.
    Parameter default should refer to exitnbr.
    """

    # Validation: ensure roadclass == "Ramp" or "Service Lane" when exitnbr is not the default value.
    if str(exitnbr) != str(default):
        if roadclass not in ("Ramp", "Service Lane"):
            return 1

    return 0


def validate_nbrlanes(nbrlanes, default):
    """Applies a set of validations to nbrlanes field."""

    # Validation: ensure 1 <= nbrlanes <= 8.
    if str(nbrlanes) != str(default):
        if not 1 <= int(nbrlanes) <= 8:
            return 1

    return 0


def validate_pavement(pavstatus, pavsurf, unpavsurf):
    """Applies a set of validations to pavstatus, pavsurf, and unpavsurf fields."""

    # Validation: when pavstatus == "Paved", ensure pavsurf != "None" and unpavsurf == "None".
    if pavstatus == "Paved":
        if pavsurf == "None":
            return 1
        if unpavsurf != "None":
            return 2

    # Validation: when pavstatus == "Unpaved", ensure pavsurf == "None" and unpavsurf != "None".
    if pavstatus == "Unpaved":
        if pavsurf != "None":
            return 3
        if unpavsurf == "None":
            return 4

    return 0


def validate_roadclass_rtnumber1(roadclass, rtnumber1, default):
    """
    Applies a set of validations to roadclass and rtnumber1 fields.
    Parameter default should refer to rtnumber1.
    """

    # Validation: ensure rtnumber1 is not the default value when roadclass == "Freeway" or "Expressway / Highway".
    if roadclass in ("Freeway", "Expressway / Highway"):
        if str(rtnumber1) == str(default):
            return 1

    return 0


def validate_roadclass_self_intersection(df):
    """Applies a set of validations to roadclass and structtype fields."""

    errors = {1: pd.Series(), 2: pd.Series()}

    # Validation: for self-intersecting road segments, ensure structtype != "None".
    segments_single, errors[1] = validate_roadclass_structtype(df)

    # Validation: ensure roadclass is in ("Expressway / Highway", "Freeway", "Ramp", "Rapid Transit") for all road
    #             elements which a) self-intersect and b) touch another road segment where roadclass is in the
    #             aforementioned set.

    flag_nids = list()
    valid = ["Expressway / Highway", "Freeway", "Ramp", "Rapid Transit"]

    # Compile coords of road segments where roadclass is in the validation list.
    valid_coords = list(set(chain(
        *[itemgetter(0, -1)(geom.coords) for geom in df[df["roadclass"].isin(valid)]['geometry'].values])))

    # Single-segment road elements:

    # Compile nids of road segments with coords in the validation coords list.
    flag_intersect = np.vectorize(lambda geom: geom.coords[0] in valid_coords)(segments_single["geometry"].values)
    flag_nids.extend(segments_single[flag_intersect]["nid"].values)

    # Multi-segment road elements:

    # Compile multi-segment road elements (via non-unique nids).
    # Filter to nids with invalid roadclass (intended to reduce spatial processing).
    segments_multi = df[(df["nid"].duplicated(keep=False)) & (~df["roadclass"].isin(valid))]

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
    # Note: uuid is stored as the index.
    flag_uuids = df[(df["nid"].isin(flag_nids)) & (~df["roadclass"].isin(valid))].index.values
    errors[2] = pd.Series(df.index.map(lambda uuid: uuid in flag_uuids))

    return errors


def validate_roadclass_structtype(df):
    """Applies a set of validations to roadclass and structtype fields."""

    # Identify self-intersections formed by single-segment road elements (i.e. where nid is unique).

    # Compile single-segment road elements (via unique nids).
    segments = df[~df["nid"].duplicated(keep=False)]

    # Identify self-intersections (start coord == end coord).
    flag_self_intersect = np.vectorize(lambda geom: geom.coords[0] == geom.coords[-1])(segments["geometry"].values)
    flag_segments = segments[flag_self_intersect]

    # Validation: for self-intersecting road segments, ensure structtype != "None".
    # Note: uuid is stored as the index.
    flag_uuids = flag_segments[flag_segments["structtype"] == "None"].index.values
    flag_uuids = pd.Series(df.index.map(lambda uuid: uuid in flag_uuids))

    return flag_segments, flag_uuids


def validate_route_contiguity(df, default):
    """
    Applies a set of validations to route attributes (rows represent field groups):
        rtename1en, rtename2en, rtename3en, rtename4en,
        rtename1fr, rtename2fr, rtename3fr, rtename4fr,
        rtnumber1, rtnumber2, rtnumber3, rtnumber4, rtnumber5.
    Parameter default should be a dictionary with a key for each of the required fields.
    """

    errors = dict()

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
                if route_name not in errors.keys():
                    errors[route_name] = list()
                errors[route_name].append([field_group, deadends])

    return errors


def validate_speed(speed, default):
    """Applies a set of validations to speed field."""

    if str(speed) != str(default):

        # Validation: ensure 5 <= speed <= 120.
        if not 5 <= int(speed) <= 120:
            return 1

        # Validation: ensure speed is a multiple of 5.
        if int(speed) % 5 != 0:
            return 2

    return 0
