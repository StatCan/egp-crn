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

        if date != default:

            # Validation: length must be 4, 6, or 8.
            if len(date) not in (4, 6, 8):
                raise ValueError("Invalid length for credate / revdate = \"{}\".".format(date))

            # Rectification: default to 01 for missing month and day values.
            while len(date) in (4, 6):
                date += "01"

            # Validation: valid values for day, month, year (1960+).
            year, month, day = map(int, [date[:4], date[4:6], date[6:8]])

            # Year.
            if not 1960 <= year <= int(today[:4]):
                raise ValueError("Invalid year for credate / revdate at index 0:3 = \"{}\".".format(year))

            # Month.
            if month not in range(1, 12 + 1):
                raise ValueError("Invalid month for credate / revdate at index 4:5 = \"{}\".".format(month))

            # Day.
            if not 1 <= day <= calendar.mdays[month]:
                if not all([day == 29, month == 2, calendar.isleap(year)]):
                    raise ValueError("Invalid day for credate / revdate at index 6:7 = \"{}\".".format(day))

            # Validation: ensure value <= today.
            if year == today[:4]:
                if not all([month <= today[4:6], day <= today[6:8]]):
                    raise ValueError("Invalid date for credate / revdate = \"{}\". "
                                     "Date cannot be in the future.".format(date, today))

        return date

    # Validation: individual date validations.
    credate = validate(credate)
    revdate = validate(revdate)

    # Validation: ensure credate <= revdate.
    if credate != default and revdate != default:
        if not int(credate) <= int(revdate):
            raise ValueError("Invalid date combination for credate = \"{}\", revdate = \"{}\". "
                             "credate must precede or equal revdate.".format(credate, revdate))

    return credate, revdate


def validate_exitnbr_conflict(df, default):
    """
    Applies a set of validations to exitnbr field.
    Parameter default should refer to exitnbr.
    """

    # Iterate road elements comprised of multiple road segments (via nid field).
    for nid in df[df["nid"].duplicated(keep=False)]["nid"].unique():

        # Compile exitnbr values.
        vals = df[(df["nid"] == nid) & (df["exitnbr"] != default)]["exitnbr"].unique()

        # Validation: ensure road element has <= 1 unique exitnbr, excluding the default value.
        if len(vals) > 1:
            raise ValueError("Invalid exitnbr for road element nid = \"{}\". A road element must have <= 1 exitnbr "
                             "value, excluding the default field value. Values found: {}."
                             .format(nid, ", ".join(map('"{}"'.format, sorted(vals)))))


def validate_exitnbr_roadclass(exitnbr, roadclass, default):
    """
    Applies a set of validations to exitnbr and roadclass fields.
    Parameter default should refer to exitnbr.
    """

    # Validation: ensure roadclass == "Ramp" or "Service Lane" when exitnbr is not the default value.
    if str(exitnbr) != str(default):
        if roadclass not in ("Ramp", "Service Lane"):
            raise ValueError("Invalid value for roadclass = \"{}\". When exitnbr is not the default field value, "
                             "roadclass must be \"Ramp\" or \"Service Lane\".".format(roadclass))

    return exitnbr, roadclass


def validate_nbrlanes(nbrlanes, default):
    """Applies a set of validations to nbrlanes field."""

    # Validation: ensure 1 <= nbrlanes <= 8.
    if str(nbrlanes) != str(default):
        if not 1 <= int(nbrlanes) <= 8:
            raise ValueError("Invalid value for nbrlanes = \"{}\". Value must be between 1 and 8.".format(nbrlanes))

    return nbrlanes


def validate_pavement(pavstatus, pavsurf, unpavsurf):
    """Applies a set of validations to pavstatus, pavsurf, and unpavsurf fields."""

    # Validation: when pavstatus == "Paved", ensure pavsurf != "None" and unpavsurf == "None".
    if pavstatus == "Paved":
        if pavsurf == "None":
            raise ValueError("Invalid combination for pavstatus = \"{}\", pavsurf = \"{}\". When pavstatus is "
                             "\"Paved\", pavsurf must not be \"None\".".format(pavstatus, pavsurf))
        if unpavsurf != "None":
            raise ValueError("Invalid combination for pavstatus = \"{}\", unpavsurf = \"{}\". When pavstatus is "
                             "\"Paved\", unpavsurf must be \"None\".".format(pavstatus, unpavsurf))

    # Validation: when pavstatus == "Unpaved", ensure pavsurf == "None" and unpavsurf != "None".
    if pavstatus == "Unpaved":
        if pavsurf != "None":
            raise ValueError("Invalid combination for pavstatus = \"{}\", pavsurf = \"{}\". When pavstatus is "
                             "\"Unpaved\", pavsurf must be \"None\".".format(pavstatus, pavsurf))
        if unpavsurf == "None":
            raise ValueError("Invalid combination for pavstatus = \"{}\", unpavsurf = \"{}\". When pavstatus is "
                             "\"Unpaved\", unpavsurf must not be \"None\".".format(pavstatus, pavsurf))

    return pavstatus, pavsurf, unpavsurf


def validate_roadclass_rtnumber1(roadclass, rtnumber1, default):
    """
    Applies a set of validations to roadclass and rtnumber1 fields.
    Parameter default should refer to rtnumber1.
    """

    # Validation: ensure rtnumber1 is not the default value when roadclass == "Freeway" or "Expressway / Highway".
    if roadclass in ("Freeway", "Expressway / Highway"):
        if str(rtnumber1) == str(default):
            raise ValueError(
                "Invalid value for rtnumber1 = \"{}\". When roadclass is \"Freeway\" or \"Expressway / Highway\", "
                "rtnumber1 must not be the default field value = \"{}\".".format(rtnumber1, default))

    return roadclass, rtnumber1


def validate_roadclass_self_intersection(df, segments):
    """Applies a set of validations to roadclass and structtype fields."""

    # Validation: for self-intersecting road segments, ensure structtype != "None".
    segments_single = validate_roadclass_structtype(df)

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
    flag_uuids = df[(df["nid"].isin(flag_nids)) & (~df["roadclass"].isin(valid))]["uuid"].values

    # Raise validation.
    if len(flag_uuids):
        raise ValueError("Invalid value for roadclass. Road elements containing a self-intersection must have one of "
                         "the following roadclass values for every constituent road segment: {}."
                         "\nUpdate the following road segments' roadclass attribute (listed by uuid) with one of the "
                         "aforementioned values:\n{}"
                         .format(", ".join(map("\"{}\"".format, valid)), "\n".join(flag_uuids)))


def validate_roadclass_structtype(df):
    """Applies a set of validations to roadclass and structtype fields."""

    # Identify self-intersections formed by single-segment road elements (i.e. where nid is unique).

    # Compile single-segment road elements (via unique nids).
    segments = df[~df["nid"].duplicated(keep=False)]

    # Identify self-intersections (start coord == end coord).
    flag_self_intersect = np.vectorize(lambda geom: geom.coords[0] == geom.coords[-1])(segments["geometry"].values)
    flag_segments = segments[flag_self_intersect]

    # Validation: for self-intersecting road segments, ensure structtype != "None".
    flag_uuids = flag_segments[flag_segments["structtype"] == "None"]["uuid"].values

    if len(flag_uuids):
        raise ValueError("Invalid value for structtype = \"None\". For self-intersecting road segments, structtype "
                         "must not be \"None\"."
                         "\nReview the following road segments (listed by uuid):\n{}".format("\n".join(flag_uuids)))

    else:
        return flag_segments


def validate_route_contiguity(df, default):
    """
    Applies a set of validations to route attributes (rows represent field groups):
        rtename1en, rtename2en, rtename3en, rtename4en,
        rtename1fr, rtename2fr, rtename3fr, rtename4fr,
        rtnumber1, rtnumber2, rtnumber3, rtnumber4, rtnumber5.
    Parameter default should be a dictionary with a key for each of the required fields.
    """

    # Validation: ensure route has contiguous geometry.
    for field_group in [["rtename1en", "rtename2en", "rtename3en", "rtename4en"],
                        ["rtename1fr", "rtename2fr", "rtename3fr", "rtename4fr"],
                        ["rtnumber1", "rtnumber2", "rtnumber3", "rtnumber4", "rtnumber5"]]:

        # Compile route names.
        route_names = [df[col].unique() for col in field_group]
        # Remove default values.
        route_names = [names[np.where(names != default[field_group[index]])] for index, names in enumerate(route_names)]
        # Concatenate arrays.
        route_names = np.concatenate(route_names, axis=None)
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

                raise ValueError("Invalid route = \"{}\", based on route attributes: {}."
                                 "\nRoute must be contiguous. Review contiguity at the following endpoints:\n{}"
                                 "\nAdditionally, review the route name attributes of any ramp features connected to "
                                 "this route.".format(route_name, ", ".join(field_group), deadends))


def validate_route_text(df, default):
    """
    Applies a set of validations to route attributes:
        rtename1en, rtename2en, rtename3en, rtename4en,
        rtename1fr, rtename2fr, rtename3fr, rtename4fr.
    Parameter default should be a dictionary with a key for each of the required fields.
    """

    # Validation: set text-based route fields to title case.
    cols = ["rtename1en", "rtename2en", "rtename3en", "rtename4en",
            "rtename1fr", "rtename2fr", "rtename3fr", "rtename4fr"]
    for col in cols:
        df[col] = df[col].map(lambda route: route if route == default[col] else route.title())

    return df


def validate_speed(speed, default):
    """Applies a set of validations to speed field."""

    if str(speed) != str(default):

        # Validation: ensure 5 <= speed <= 120.
        if not 5 <= int(speed) <= 120:
            raise ValueError("Invalid value for speed = \"{}\". Value must be between 5 and 120.".format(speed))

        # Validation: ensure speed is a multiple of 5.
        if int(speed) % 5 != 0:
            raise ValueError("Invalid value for speed = \"{}\". Value must be a multiple of 5.".format(speed))

    return speed
