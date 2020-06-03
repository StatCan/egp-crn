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
from collections import Counter
from datetime import datetime
from functools import reduce
from itertools import chain, permutations
from operator import attrgetter, itemgetter, or_
from scipy.spatial import cKDTree
from shapely.geometry import Point

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import helpers


logger = logging.getLogger()


# Compile default field values and dtypes.
defaults_all = helpers.compile_default_values()
dtypes_all = helpers.compile_dtypes()


def identify_duplicate_lines(df):
    """Identifies the uuids of duplicate line geometries."""

    errors = list()

    # Keep only required fields.
    col = df["geometry"]

    # Note: filters are purely intended to reduce processing.
    # Filter geometries to those with duplicate lengths.
    s_filtered = col[col.length.duplicated(keep=False)]

    # Filter geometries to those with duplicate endpoint coordinates.
    s_filtered = s_filtered[s_filtered.map(lambda g: tuple(sorted(itemgetter(0, -1)(g.coords)))).duplicated(keep=False)]

    # Identify duplicate geometries.
    if len(s_filtered):
        mask = s_filtered.map(lambda geom1: s_filtered.map(lambda geom2: geom1.equals(geom2)).sum() > 1)

        # Compile uuids of flagged records.
        errors = s_filtered[mask].index.values

    return {"errors": errors}


def identify_duplicate_points(df):
    """Identifies the uuids of duplicate point geometries."""

    # Identify duplicated geometries.
    mask = df["geometry"].map(lambda geom: geom.coords[0]).duplicated(keep=False)

    # Compile uuids of flagged records.
    errors = df[mask].index.values

    return {"errors": errors}


def identify_isolated_lines(roadseg, ferryseg, junction):
    """Identifies the uuids of isolated road segments from the merged dataframe of road and ferry segments."""

    # Concatenate ferryseg and roadseg dataframes, keeping only required fields.
    df = gpd.GeoDataFrame(pd.concat([ferryseg[["uuid", "geometry"]], roadseg[["uuid", "geometry"]]],
                                    ignore_index=False, sort=False))

    # Compile dead end junctions.
    deadends = set(chain([geom.coords[0] for geom in junction[junction["junctype"] == "Dead End"]["geometry"].values]))

    # Identify isolated segments.
    mask = df["geometry"].map(lambda g: len(set(itemgetter(0, -1)(g.coords)) - deadends) == 0)

    # Compile uuids of flagged records.
    errors = df[mask].index.values

    return {"errors": errors}


def strip_whitespace(df):
    """Strips leading and trailing whitespace from the given value for each dataframe column."""

    mod_flag = False

    # Compile valid columns.
    cols = df.select_dtypes(include="object", exclude="geometry").columns.values

    # Iterate columns.
    for col in cols:

        # Apply modifications.
        series_orig = df[col]
        df[col] = df[col].map(str.strip)

        # Quantify modifications.
        mods = (series_orig != df[col]).sum()
        if mods:
            mod_flag = True

            # Log modifications.
            logger.warning(f"Modified {mods} record(s) in column {col}."
                           "\nModification details: Field values stripped of leading and trailing whitespace.")

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

        # Filter records to non-default values which are not already title case.
        default = defaults_all["roadseg"][col]
        s_filtered = df[df[col].map(lambda route: route != default and not route.istitle())][col]

        # Apply modifications, if required.
        if len(s_filtered):
            df.loc[s_filtered.index, col] = s_filtered.map(str.title)
            mod_flag = True

            # Log modifications.
            logger.warning(f"Modified {len(s_filtered)} record(s) in column {col}."
                           "\nModification details: Field values set to title case.")

    if mod_flag:
        return {"errors": None, "modified_dframes": df.copy(deep=True)}
    else:
        return {"errors": None}


def validate_dates(df):
    """Applies a set of validations to credate and revdate fields."""

    errors = {i: list() for i in range(1, 7+1)}
    defaults = helpers.compile_default_values()["roadseg"]
    df = df[["credate", "revdate"]].copy(deep=True)

    # Get current date.
    today = datetime.today().strftime("%Y%m%d")
    today = {"year": int(today[:4]), "month": int(today[4:6]), "day": int(today[6:8]), "full": int(today)}

    # Define functions.
    def validate_day(date):
        """Validate the day value in a date."""

        year, month, day = map(int, [date[:4], date[4:6], date[6:8]])

        if not 1 <= day <= calendar.mdays[month]:
            if not all([day == 29, month == 2, calendar.isleap(year)]):
                return False

        return True

    # Iterate credate and revdate, applying validations.
    for col in ("credate", "revdate"):

        # Subset to non-default values.
        s_filtered = df[df[col] != defaults[col]][col]

        if len(s_filtered):

            # Validation 1: date content must be numeric.
            results = s_filtered[~s_filtered.map(str.isnumeric)].index.values
            errors[1].extend(results)

            # Validation 2: length must be 4, 6, or 8.
            results = s_filtered[s_filtered.map(lambda date: len(date) not in (4, 6, 8))].index.values
            errors[2].extend(results)

            # Subset to valid records only for remaining validations.
            invalid_indexes = list(set(chain.from_iterable(errors.values())))
            s_filtered2 = s_filtered[~s_filtered.index.isin(invalid_indexes)]

            if len(s_filtered2):

                # Temporarily set missing month and day values to 01.
                series_mod = s_filtered2[s_filtered2.map(lambda date: len(date) in (4, 6))]
                if len(series_mod):
                    append_vals = {4: "0101", 6: "01"}
                    s_filtered2.loc[series_mod.index] = series_mod.map(lambda date: date + append_vals[len(date)])
                    df.loc[s_filtered2.index, col] = s_filtered2

                # Validation 3: valid date - year.
                results = s_filtered2[~s_filtered2.map(
                    lambda date: 1960 <= int(date[:4]) <= today["year"])].index.values
                errors[3].extend(results)

                # Validation 4: valid date - month.
                results = s_filtered2[~s_filtered2.map(lambda date: 1 <= int(date[4:6]) <= 12)].index.values
                errors[4].extend(results)

                # Validation 5: valid date - day.
                results = s_filtered2[~s_filtered2.map(validate_day)].index.values
                errors[5].extend(results)

                # Validation 6: ensure date <= today.
                results = s_filtered2[s_filtered2.map(lambda date: int(date) > today["full"])].index.values
                errors[6].extend(results)

    # Validation 7: ensure credate <= revdate.
    df_filtered = df[(df["credate"] != defaults["credate"]) &
                     (df["revdate"] != defaults["revdate"]) &
                     ~(df.index.isin(list(set(chain.from_iterable(itemgetter(1, 2)(errors))))))]
    if len(df_filtered):
        results = df_filtered[df_filtered["credate"].map(int) > df_filtered["revdate"].map(int)].index.values
        errors[7].extend(results)

    return {"errors": errors}


def validate_deadend_disjoint_proximity(junction, roadseg):
    """Validates the proximity of deadend junctions to disjoint / non-connected road segments."""

    # Validation: deadend junctions must be >= 5 meters from disjoint road segments.
    errors = list()

    # Filter junctions to junctype = "Dead End", keep only required fields.
    deadends = junction[junction["junctype"] == "Dead End"]["geometry"]
    roadseg = roadseg["geometry"]

    # Transform records to a meter-based crs: EPSG:3348.
    deadends = helpers.reproject_gdf(deadends, 4617, 3348)
    roadseg = helpers.reproject_gdf(roadseg, 4617, 3348)

    # Compile coordinates (used multiple times).
    deadends = deadends.map(lambda pt: itemgetter(0)(attrgetter("coords")(pt)))
    roadseg = roadseg.map(lambda g: set(attrgetter("coords")(g)))

    # Generate a lookup dict for the index of each roadseg coordinate, mapped to the full range of coordinate indexes
    # for the road segment associated with that coordinate. Therefore, the coordinate identified for exclusion at
    # distance=0 can be associated with, and expanded to include, all other coordinates along that road segment.
    # Process: get the road segment coordinate counts and cumulative counts to generate an index range for each road
    # segment. Stack the results and duplicate the ranges by the coordinate counts. Convert to a dict.
    coords_count = roadseg.map(len)
    coords_idx_cumsum = coords_count.cumsum()
    coords_full_idx_range = np.repeat(list(map(
        lambda indexes: set(range(*indexes)),
        np.column_stack((coords_idx_cumsum - coords_count, coords_idx_cumsum)))),
        coords_count)
    coords_full_idx_range_lookup = dict(zip(range(len(coords_full_idx_range)), coords_full_idx_range))

    # Generate kdtree.
    tree = cKDTree(list(chain.from_iterable(roadseg)))

    # Compile indexes of road segments within 5 meters distance of each deadend.
    proxi_idx_all = deadends.map(lambda deadend: set(chain(*tree.query_ball_point([deadend], r=5))))

    # Compile index of road segment at 0 meters distance from each deadend. These represent the connected roads.
    # Expand indexes to ranges.
    proxi_idx_exclude = deadends.map(lambda deadend: tree.query([deadend])[-1])
    proxi_idx_exclude = proxi_idx_exclude.map(lambda idx: itemgetter(*idx)(coords_full_idx_range_lookup))

    # Filter coincident indexes from all indexes. Keep only non-empty results.
    proxi_idx_keep = proxi_idx_all - proxi_idx_exclude
    proxi_idx_keep = proxi_idx_keep[proxi_idx_keep.map(len) > 0]

    # Generate a lookup dict for the index of each roadseg coordinate, mapped to the associated uuid.
    coords_idx_uuid_lookup = dict(zip(range(coords_count.sum()), np.repeat(roadseg.index.values, coords_count)))

    # Compile the uuid associated with resulting proximity point indexes for each deadend.
    proxi_results = proxi_idx_keep.map(lambda indexes: itemgetter(*indexes)(coords_idx_uuid_lookup))
    proxi_results = proxi_results.map(lambda uuids: set(uuids) if isinstance(uuids, tuple) else {uuids})

    # Compile error properties.
    for source_uuid, target_uuids in proxi_results.iteritems():
        errors.append(f"junction uuid \"{source_uuid}\" is too close to roadseg uuid(s): "
                      f"{', '.join(map(str, target_uuids))}.")

    return {"errors": errors}


def validate_exitnbr_conflict(df):
    """Applies a set of validations to exitnbr field."""

    errors = list()
    default = defaults_all["roadseg"]["exitnbr"]

    # Query multi-segment road elements (via nid field) where exitnbr is not the default value.
    df_filtered = df[(df["nid"].duplicated(keep=False)) & (df["nid"] != default) & (df["exitnbr"] != default)]

    # Group exitnbrs by nid, removing duplicate values.
    grouped = helpers.groupby_to_list(df_filtered, "nid", "exitnbr").map(np.unique)

    # Remove the default field value from each group.
    grouped = grouped.map(lambda vals: vals if default not in vals else vals.remove(default))

    # Validation: ensure road element has <= 1 unique exitnbr, excluding the default value.
    flag_nids = grouped[grouped.map(len) > 1]

    # Compile error properties.
    for nid, exitnbrs in flag_nids.iteritems():
        errors.append(f"nid: {nid}; exitnbr values: {', '.join(map(str, exitnbrs))}.")

    return {"errors": errors}


def validate_exitnbr_roadclass(df):
    """Applies a set of validations to exitnbr and roadclass fields."""

    # Subset dataframe to non-default values, keep only required fields.
    default = defaults_all["roadseg"]["exitnbr"]
    s_filtered = df[df["exitnbr"] != default]["roadclass"]

    # Validation: ensure roadclass == "Ramp" or "Service Lane" when exitnbr is not the default value.
    # Compile uuids of flagged records.
    errors = s_filtered[~s_filtered.isin(["Ramp", "Service Lane"])].index.values

    return {"errors": errors}


def validate_ferry_road_connectivity(ferryseg, roadseg, junction):
    """Validates the connectivity between ferry and road line segments."""

    errors = dict()

    # Filter dataframes to only required fields.
    ferryseg = ferryseg["geometry"]
    roadseg = roadseg["geometry"]

    # Validation 1: ensure ferry segments connect to a road segment at at least one endpoint.

    # Compile junction coordinates where junctype = "Ferry".
    ferry_junctions = list(set(chain([geom.coords[0] for geom in
                                      junction[junction["junctype"] == "Ferry"]["geometry"].values])))

    # Identify ferry segments which do not connect to any road segments.
    mask = ferryseg.map(lambda geom: not any(coords in ferry_junctions for coords in itemgetter(0, -1)(geom.coords)))

    # Compile uuids of flagged records.
    errors[1] = ferryseg[mask].index.values

    # Validation 2: ensure ferry segments connect to <= 1 road segment at either endpoint.

    # Compile road segments which connect to ferry segments.
    roads_connected = roadseg[roadseg.map(
        lambda geom: any(coords in ferry_junctions for coords in itemgetter(0, -1)(geom.coords)))]

    # Compile coordinates of connected road segments.
    road_coords_count = Counter(chain.from_iterable(roads_connected.map(
        lambda g: tuple(set(itemgetter(0, -1)(g.coords))))))

    # Identify ferry endpoints which intersect multiple road segments.
    ferry_multi_intersect = ferryseg.map(
        lambda ferry: any(itemgetter(coords)(road_coords_count) > 1 for coords in itemgetter(0, -1)(ferry.coords)))

    # Compile uuids of flagged records.
    errors[2] = ferryseg[ferry_multi_intersect].index.values

    return {"errors": errors}


def validate_ids(df):
    """
    Applies a set of validations to all id fields.
    Sets all id fields to lowercase.
    """

    errors = {1: list(), 2: list(), 3: list(), 4: list()}
    mod_flag = False

    # Identify dataframe name to configure dtypes and default values.
    table = "roadseg"
    dtypes, defaults = dtypes_all[table], defaults_all[table]
    for t in defaults_all:
        if set(defaults_all[t]).issubset(df.columns):
            dtypes, defaults, table = dtypes_all[t], defaults_all[t], t
            break

    # Iterate fields which a) end with "id", b) are str type, and c) are not uuid.
    for col in [fld for fld in df.columns.difference(["uuid"]) if fld.endswith("id") and dtypes[fld] == "str"]:

        # Subset dataframe to required column with non-default values.
        series = df[df[col] != defaults[col]][col]

        if len(series):

            # Modification: set ids to lowercase.
            # Filter records to values which are not already lowercase.
            s_filtered = series[~series.map(str.islower)]

            # Apply modification, if required.
            if len(s_filtered):
                series.loc[s_filtered.index] = s_filtered.map(str.lower)
                df.loc[series.index, col] = series
                mod_flag = True

                # Log modifications.
                logger.warning(f"Modified {len(s_filtered)} record(s) in column {col}."
                               "\nModification details: Field values set to lower case.")

            # Validation 1: ensure ids are 32 digits.
            # Compile uuids of flagged records.
            flag_uuids = series[series.map(len) != 32].index.values
            for id in flag_uuids:
                errors[1].append(f"uuid: {id}, based on attribute field: {col}.")

            # Validation 2: ensure ids are hexadecimal.
            # Compile uuids of flagged records.
            hexdigits = set(string.hexdigits)
            flag_uuids = series[series.map(lambda id: not set(id).issubset(hexdigits))].index.values
            for id in flag_uuids:
                errors[2].append(f"uuid: {id}, based on attribute field: {col}.")

    # Iterate unique id fields.
    unique_fields = {"ferrysegid", "roadsegid"}
    for col in unique_fields.intersection(set(df.columns)):

        # Filter dataframe to required column.
        series = df[col]

        # Validation 3: ensure unique id fields are unique within their column.
        # Compile uuids of flagged records.
        flag_uuids = series[series.duplicated(keep=False)].index.values
        for id in flag_uuids:
            errors[3].append(f"uuid: {id}, based on attribute field: {col}.")

        # Validation 4: ensure unique id fields are not the default field value.
        # Compile uuids of flagged records.
        flag_uuids = series[series == defaults[col]].index.values
        for id in flag_uuids:
            errors[4].append(f"uuid: {id}, based on attribute field: {col}.")

    if mod_flag:
        return {"errors": errors, "modified_dframes": df.copy(deep=True)}
    else:
        return {"errors": errors}


def validate_line_endpoint_clustering(df):
    """
    Validates the quantity of points clustered near the endpoints of line segments.
    Validation: ensure line segments have <= 3 points within 83 meters of either endpoint, inclusively.
    """

    errors = None

    # Transform records to a meter-based crs: EPSG:3348.
    series = helpers.reproject_gdf(df["geometry"], 4617, 3348)

    # Filter out records with <= 3 points or length < 83 meters.
    s_filtered = series[series.length >= 83]
    s_filtered = s_filtered[s_filtered.map(lambda geom: len(geom.coords) > 3)]

    if len(s_filtered):

        # Identify invalid records.
        # Process: either of the following must be true:
        # a) The distance of the 4th point along the linestring is < 83 meters.
        # b) The total linestring length minus the distance of the 4th-last point along the linestring is < 83 meters.
        def endpoint_clustered(geom):
            pts = itemgetter(3, -4)(geom.coords)
            return (geom.project(Point(pts[0])) < 83) or ((geom.length - geom.project(Point(pts[1]))) < 83)

        flags = np.vectorize(endpoint_clustered)(s_filtered)

        # Compile uuids of flagged records.
        errors = s_filtered[flags].index.values

    return {"errors": errors}


def validate_line_length(df):
    """Validates the minimum feature length of line geometries."""

    errors = None

    # Filter records to 0.0002 degrees length (approximately 22.2 meters).
    # Purely intended to reduce processing.
    series = df[df.length <= 0.0002]["geometry"]

    if len(series):

        # Transform records to a meter-based crs: EPSG:3348.
        series = helpers.reproject_gdf(series, 4617, 3348)

        # Validation: ensure line segments are >= 2 meters in length.
        errors = series[series.length < 2].index.values

    return {"errors": errors}


def validate_line_merging_angle(df):
    """
    Validates the merging angle of line segments.
    Validation: ensure line segments merge at angles >= 40 degrees.
    """

    errors = None

    # Transform records to a meter-based crs: EPSG:3348.
    series = helpers.reproject_gdf(df["geometry"], 4617, 3348)

    # Compile the uuid groups for all non-unique points.

    # Compile coordinates (used multiple times; only requires first 2 and last 2 points).
    # Export coordinate-uuid lookup dict.
    pts = series.map(lambda g: tuple(map(itemgetter(0, 1), itemgetter(0, 1, -2, -1)(attrgetter("coords")(g)))))
    pts_uuid_lookup = pts.to_dict()

    # Explode point groups, filter to only duplicates, and construct a dataframe of the uuids and coordinates.
    pts_exploded = pts.explode()
    pts_dups = pts_exploded[pts_exploded.duplicated(keep=False)]
    pts_df = pd.DataFrame({"coords": pts_dups, "uuid": pts_dups.index})

    # Group uuids according to coordinates.
    uuids_grouped = helpers.groupby_to_list(pts_df, "coords", "uuid")

    # Exit function if no shared points exists (b/c therefore no line merges exist).
    if len(uuids_grouped):

        # Explode grouped uuids. Maintain index point as both index and column.
        uuids_grouped_exploded = uuids_grouped.explode()
        uuids_grouped_exploded = pd.DataFrame({"coords": uuids_grouped_exploded.index, "uuid": uuids_grouped_exploded})\
            .reset_index(drop=True)

        # Compile next-to-endpoint points.
        # Process: Flag uuids according to duplication status within their group. For unique uuids, configure the
        # next-to-endpoint point based on whichever endpoint matches the common group point. For duplicated uuids
        # (which represent self-loops), the first duplicate takes the second point, the second duplicate takes the
        # second-last point - thereby avoiding the same next-to-point being taken twice for self-loop intersections.
        dup_flags = {
            "dup_none": uuids_grouped_exploded[~uuids_grouped_exploded.duplicated(keep=False)][["uuid", "coords"]],
            "dup_first": uuids_grouped_exploded[uuids_grouped_exploded.duplicated(keep="first")]["uuid"],
            "dup_last": uuids_grouped_exploded[uuids_grouped_exploded.duplicated(keep="last")]["uuid"]
        }
        dup_results = {
            "dup_none": np.vectorize(
                lambda uid, pt: pts_uuid_lookup[uid][1] if pts_uuid_lookup[uid][0] == pt else pts_uuid_lookup[uid][-2],
                otypes=[tuple])(dup_flags["dup_none"]["uuid"], dup_flags["dup_none"]["coords"]),
            "dup_first": dup_flags["dup_first"].map(lambda uid: pts_uuid_lookup[uid][1]).values,
            "dup_last": dup_flags["dup_last"].map(lambda uid: pts_uuid_lookup[uid][-2]).values
        }

        uuids_grouped_exploded["pt"] = None
        uuids_grouped_exploded.loc[dup_flags["dup_none"].index, "pt"] = dup_results["dup_none"]
        uuids_grouped_exploded.loc[dup_flags["dup_first"].index, "pt"] = dup_results["dup_first"]
        uuids_grouped_exploded.loc[dup_flags["dup_last"].index, "pt"] = dup_results["dup_last"]

        # Aggregate exploded groups.
        pts_grouped = helpers.groupby_to_list(uuids_grouped_exploded, "coords", "pt")

        # Compile the permutations of points for each point group.
        pts_grouped = pts_grouped.map(lambda pts: set(map(tuple, map(sorted, permutations(pts, r=2)))))

        # Define function to calculate and return validity of angular degrees between two intersecting lines.
        def get_invalid_angle(pt1, pt2, ref_pt):

            angle_1 = np.angle(complex(*(np.array(pt1) - np.array(ref_pt))), deg=True)
            angle_2 = np.angle(complex(*(np.array(pt2) - np.array(ref_pt))), deg=True)
            angle_1 += 360 if angle_1 < 0 else 0
            angle_2 += 360 if angle_2 < 0 else 0

            return abs(angle_1 - angle_2) < 40

        # Calculate the angular degree between each reference point and each of their point permutations.
        # Return True if any angles are invalid.
        flags = np.vectorize(
            lambda pt_groups, pt_ref: any(filter(lambda pts: get_invalid_angle(pts[0], pts[1], pt_ref), pt_groups)))(
            pts_grouped, pts_grouped.index)

        # Compile the uuid groups as errors.
        errors = uuids_grouped[flags].values

    return {"errors": errors}


def validate_line_proximity(df):
    """Validates the proximity of line segments."""

    # Validation: ensure line segments are >= 3 meters from each other, excluding connected segments.
    errors = list()

    # Transform records to a meter-based crs: EPSG:3348.
    series = helpers.reproject_gdf(df["geometry"], 4617, 3348)

    # Compile all unique segment coordinates.
    pts = series.map(lambda g: list(set(attrgetter("coords")(g))))
    pts_exploded = pts.explode()

    # Generate lookup dicts for:
    # 1) point coordinate to connected segment uuids.
    # 2) point index to segment uuid.
    pts_exploded_df = pd.DataFrame(pts_exploded).reset_index(drop=False)
    pts_uuids_lookup = helpers.groupby_to_list(pts_exploded_df, "geometry", "uuid").to_dict()
    pts_idx_uuid_lookup = pts_exploded_df["uuid"].to_dict()

    # Generate kdtree.
    tree = cKDTree(pts_exploded.to_list())

    # Compile uuids connected to each segment.
    uuids_exclude = pts.map(lambda points: set(chain.from_iterable(itemgetter(*points)(pts_uuids_lookup))))

    # Compile indexes of segment points within 3 meters distance of each segment, retrieve uuids of returned indexes.
    uuids_proxi = pts.map(
        lambda points: set(itemgetter(*chain(*tree.query_ball_point(points, r=3)))(pts_idx_uuid_lookup)))

    # Remove connected uuids from each set of uuids, keep non-empty results.
    results = uuids_proxi - uuids_exclude
    results = results[results.map(len) > 0]

    # Compile error properties.
    for source, target in results.iteritems():
        errors.append(f"Feature uuid {source} is too close to feature uuid(s) {', '.join(map(str, target))}.")

    return {"errors": errors}


def validate_nbrlanes(df):
    """Applies a set of validations to nbrlanes field."""

    # Subset dataframe to non-default values, keep only required fields.
    default = defaults_all["roadseg"]["nbrlanes"]
    s_filtered = df[df["nbrlanes"] != default]["nbrlanes"]

    # Validation: ensure 1 <= nbrlanes <= 8.
    # Compile uuids of flagged records.
    errors = s_filtered[~s_filtered.map(lambda nbrlanes: 1 <= int(nbrlanes) <= 8)].index.values

    return {"errors": errors}


# TODO: review
def validate_nid_linkages(df, dfs_all):
    """
    Validates the nid linkages for the input dataframe.
    Parameter dfs_all must be a dictionary of all nrn dataframes.
    """

    errors = list()

    # Define nid linkages.
    linkages = {
        "addrange":
            {
                "roadseg": ["adrangenid"]
            },
        "altnamlink":
            {
                "addrange": ["l_altnanid", "r_altnanid"]
            },
        "roadseg":
            {
                "blkpassage": ["roadnid"],
                "tollpoint": ["roadnid"]
            },
        "strplaname":
            {
                "addrange": ["l_offnanid", "r_offnanid"],
                "altnamlink": ["strnamenid"]
            }
    }

    # Identify dataframe name to configure nid linkages.
    id_table = None
    for table in defaults_all:
        if all(fld in defaults_all[table] for fld in df.columns.difference(["uuid", "geometry"])):
            id_table = table
            break

    # Iterate nid tables.
    for nid_table in [t for t in linkages if t in dfs_all]:

        # Retrieve nids as lowercase.
        nids = set(dfs_all[nid_table]["nid"].map(str.lower))

        # Validate table linkage.
        if id_table in linkages[nid_table]:

            # Iterate linked columns.
            for col in linkages[nid_table][id_table]:

                # Retrieve column ids as lowercase.
                ids = set(df[col].map(str.lower))

                # Validation: ensure all nid linkages are valid.
                logger.info(f"Validating nid linkage: {nid_table}.nid - {id_table}.{col}.")

                if not ids.issubset(nids):

                    # Compile invalid ids.
                    flag_ids = list(ids - nids)

                    # Configure error message.
                    if len(flag_ids):
                        vals = "\n".join(map(str, flag_ids))
                        errors.append(f"The following values from {id_table}.{col} are not present in {nid_table}.nid:"
                                      f"\n{vals}")

    return {"errors": errors}


def validate_pavement(df):
    """Applies a set of validations to pavstatus, pavsurf, and unpavsurf fields."""

    errors = dict()

    # Subset dataframe to non-default values, keep only required fields.
    default = defaults_all["roadseg"]["pavstatus"]
    df_filtered = df[df["pavstatus"] != default][["pavstatus", "pavsurf", "unpavsurf"]]

    # Apply validations and compile uuids of flagged records.

    # Validation: when pavstatus == "Paved", ensure pavsurf != "None" and unpavsurf == "None".
    paved = df_filtered[df_filtered["pavstatus"] == "Paved"]
    errors[1] = paved[paved["pavsurf"] == "None"].index.values
    errors[2] = paved[paved["unpavsurf"] != "None"].index.values

    # Validation: when pavstatus == "Unpaved", ensure pavsurf == "None" and unpavsurf != "None".
    unpaved = df_filtered[df_filtered["pavstatus"] == "Unpaved"]
    errors[3] = unpaved[unpaved["pavsurf"] != "None"].index.values
    errors[4] = unpaved[unpaved["unpavsurf"] == "None"].index.values

    return {"errors": errors}


# TODO: review
def validate_point_proximity(df):
    """Validates the proximity of points."""

    # Validation: ensure points are >= 3 meters from each other.

    # Transform records to a meter-based crs: EPSG:3348.
    df = helpers.reproject_gdf(df, 4617, 3348)

    # Generate kdtree.
    tree = cKDTree(np.concatenate(df["geometry"].map(attrgetter("coords")).to_numpy()))

    # Compile indexes of points with other points within 3 meters distance.
    proxi_idx_all = df["geometry"].map(lambda geom: list(chain(*tree.query_ball_point(geom.coords, r=3))))

    # Compile indexes of points with other points at 0 meters distance. These represent the source point.
    proxi_idx_exclude = df["geometry"].map(lambda geom: list(chain(*tree.query_ball_point(geom.coords, r=0))))

    # Filter coincident indexes from all indexes.
    proxi_idx = pd.DataFrame({"all": proxi_idx_all, "exclude": proxi_idx_exclude}, index=df.index.values)
    proxi_idx_keep = proxi_idx.apply(lambda row: set(row[0]) - set(row[1]), axis=1)

    # Compile the uuid associated with resulting proximity point indexes for each point.
    proxi_results = proxi_idx_keep.map(lambda indexes: itemgetter(*indexes)(df.index) if indexes else False)

    # Compile error properties.
    errors = list()

    for source_uuid, target_uuids in proxi_results[proxi_results != False].iteritems():
        errors.append(f"Feature uuid \"{source_uuid}\" is too close to feature uuid(s) "
                      f"{', '.join(map(str, [target_uuids] if isinstance(target_uuids, str) else target_uuids))}.")

    return {"errors": errors}


def validate_road_structures(roadseg, junction):
    """Validates the structid and structtype attributes of road segments."""

    errors = {1: list(), 2: list(), 3: list(), 4: list()}
    defaults = defaults_all["roadseg"]

    # Filter dataframes to only required fields.
    junction = junction[junction["junctype"] == "Dead End"]["geometry"]
    roadseg = roadseg[["uuid", "structid", "structtype", "geometry"]]

    # Validation 1: ensure dead end road segments have structtype = "None" or the default field value.

    # Compile dead end coordinates.
    deadend_coords = set(chain(junction.map(lambda pt: itemgetter(0)(attrgetter("coords")(pt)))))

    # Compile road segments with potentially invalid structtype.
    roadseg_invalid = roadseg[~roadseg["structtype"].isin(["None", defaults["structtype"]])]["geometry"]

    # Compile truly invalid road segments.
    roadseg_invalid = roadseg_invalid[roadseg_invalid.map(
        lambda g: any(coords in deadend_coords for coords in attrgetter("coords")(g)))]

    # Compile uuids of flagged records.
    errors[1] = roadseg_invalid.index.values

    # Validation 2: ensure structid is contiguous.

    # Compile duplicated structids, excluding default value.
    structids = roadseg[(roadseg["structid"] != defaults["structid"]) &
                        (roadseg["structid"].duplicated(keep=False))]["structid"].unique()

    if len(structids):

        # Iterate structids.
        structid_count = len(structids)
        for index, structid in enumerate(sorted(structids)):

            logger.info(f"Validating structure {index + 1} of {structid_count}: \"{structid}\".")

            # Subset dataframe to those records with current structid.
            structure = roadseg[roadseg["structid"] == structid]

            # Load structure as networkx graph.
            structure_graph = helpers.gdf_to_nx(structure, keep_attributes=False)

            # Validate contiguity (networkx connectivity).
            if not nx.is_connected(structure_graph):

                # Identify deadends (locations of discontiguity).
                deadends = [coords for coords, degree in structure_graph.degree() if degree == 1]
                deadends = "\n".join([f"{deadend[0]}, {deadend[1]}" for deadend in deadends])

                # Compile error properties.
                errors[2].append(f"Structure ID: \"{structid}\".\nEndpoints:\n{deadends}.")

    # Validation 3: ensure a single, non-default structid is applied to all contiguous road segments with the same
    #               structtype.
    # Validation 4: ensure road segments with different structtypes, excluding "None" and the default field value, are
    #               not contiguous.

    # Compile road segments with valid structtype.
    segments = roadseg[~roadseg["structtype"].isin(["None", defaults["structtype"]])]

    # Convert dataframe to networkx graph.
    segments_graph = helpers.gdf_to_nx(segments, keep_attributes=True, endpoints_only=False)

    # Configure subgraphs.
    sub_g = pd.Series(nx.connected_component_subgraphs(segments_graph))

    # Validation 3.
    default = defaults["structid"]
    structids = sub_g.map(lambda graph: set(nx.get_edge_attributes(graph, "structid").values()))
    structids_invalid = structids[structids.map(lambda vals: (len(vals) > 1) or (default in vals))]
    if len(structids_invalid):

        # Compile uuids of invalid structure.
        uuids_invalid = sub_g.loc[structids_invalid.index].map(
            lambda graph: set(nx.get_edge_attributes(graph, "uuid").values()))

        # Compile error properties.
        for index, row in pd.DataFrame({"uuids": uuids_invalid, "structids": structids_invalid}).iterrows():
            errors[3].append(f"Structure formed by uuid(s): {', '.join(map(str, row[0]))} contains the following "
                             f"structid(s): {', '.join(map(str, row[1]))}.")

    # Validation 4.
    structtypes = sub_g.map(lambda graph: set(nx.get_edge_attributes(graph, "structtype").values()))
    structtypes_invalid = structtypes[structtypes.map(len) > 1]
    if len(structtypes_invalid):

        # Compile uuids of invalid structure.
        uuids_invalid = sub_g.loc[structtypes_invalid.index].map(
            lambda graph: set(nx.get_edge_attributes(graph, "uuid").values()))

        # Compile error properties.
        for index, row in pd.DataFrame({"uuids": uuids_invalid, "structtypes": structtypes_invalid}).iterrows():
            errors[4].append(f"Structure formed by uuid(s): {', '.join(map(str, row[0]))} contains the following "
                             f"structtypes(s): {', '.join(map(str, row[1]))}.")

    return {"errors": errors}


def validate_roadclass_rtnumber1(df):
    """Applies a set of validations to roadclass and rtnumber1 fields."""

    # Filter dataframe to only required fields.
    df_filtered = df[["roadclass", "rtnumber1"]]

    # Apply validations and compile uuids of flagged records.

    # Validation: ensure rtnumber1 is not the default value when roadclass == "Freeway" or "Expressway / Highway".
    default = defaults_all["roadseg"]["rtnumber1"]
    errors = df[df["roadclass"].isin(["Freeway", "Expressway / Highway"]) &
                df["rtnumber1"].map(lambda rtnumber1: rtnumber1 == default)].index.values

    return {"errors": errors}


def validate_roadclass_self_intersection(df):
    """Applies a set of validations to roadclass and structtype fields."""

    default = defaults_all["roadseg"]["nid"]

    # Validation: ensure roadclass is in ("Expressway / Highway", "Freeway", "Ramp", "Rapid Transit") for all road
    #             elements which a) self-intersect and b) touch another road segment where roadclass is in this set.

    flag_nids = list()
    valid = ["Expressway / Highway", "Freeway", "Ramp", "Rapid Transit"]

    # Compile coords of road segments where roadclass is in the validation list.
    valid_coords = set(chain(
        *[itemgetter(0, -1)(geom.coords) for geom in df[df["roadclass"].isin(valid)]['geometry'].values]))

    # Single-segment road elements:

    # Retrieve single-segment self-intersections.
    # Function call intended to avoid duplicating logic in this current function.
    segments_single = validate_roadclass_structtype(df, return_segments_only=True)

    if not segments_single.empty:

        # Compile nids of road segments with coords in the validation coords list.
        flagged = segments_single["geometry"].map(lambda g: g.coords[0] in valid_coords)
        flag_nids.extend(segments_single[flagged]["nid"].values)

    # Multi-segment road elements:

    # Compile multi-segment road elements (via non-unique nids).
    # Filter to nids with invalid roadclass (intended to reduce spatial processing).
    segments_multi = df[(df["nid"].duplicated(keep=False)) & (~df["roadclass"].isin(valid)) & (df["nid"] != default)]

    if not segments_multi.empty:

        logger.info("Validating multi-segment road elements.")

        # Compile nids of road segments with coords in the validation coords list.
        flagged_nids = segments_multi[segments_multi["geometry"].map(
            lambda g: len(set(itemgetter(0, -1)(g.coords)).intersection(valid_coords)) > 0)]["nid"].unique()

        # Compile dataframe records with a flagged nid.
        flagged_df = df[df["nid"].isin(flagged_nids)]

        # Group geometries by nid.
        grouped_segments = helpers.groupby_to_list(flagged_df, "nid", "geometry")

        # Dissolve road segments.
        elements = grouped_segments.map(shapely.ops.linemerge)

        # Identify self-intersections and store nids.
        flag_nids.extend(elements[elements.map(lambda element: element.is_ring or not element.is_simple)].values)

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
        flag_segments = segments[segments["geometry"].map(lambda g: g.is_ring or not g.is_simple)]

        # Validation: for self-intersecting road segments, ensure structtype != "None".
        errors = flag_segments[flag_segments["structtype"] == "None"].index.values

    if return_segments_only:
        return flag_segments
    else:
        return {"errors": errors}


# TODO: exclude none names
# RUNTIMES:
# Original: 260
# New: ?
def validate_route_contiguity(roadseg, ferryseg):
    """
    Applies a set of validations to route attributes (rows represent field groups):
        rtename1en, rtename2en, rtename3en, rtename4en,
        rtename1fr, rtename2fr, rtename3fr, rtename4fr,
        rtnumber1, rtnumber2, rtnumber3, rtnumber4, rtnumber5.
    """

    errors = list()

    # Define field groups.
    field_groups = [["rtename1en", "rtename2en", "rtename3en", "rtename4en"],
                    ["rtename1fr", "rtename2fr", "rtename3fr", "rtename4fr"],
                    ["rtnumber1", "rtnumber2", "rtnumber3", "rtnumber4", "rtnumber5"]]

    # Filter dataframes to only required fields.
    keep_fields = list(chain.from_iterable([*field_groups, ["geometry"]]))
    roadseg = roadseg[keep_fields]
    ferryseg = ferryseg[keep_fields]

    # Concatenate ferryseg and roadseg.
    df = gpd.GeoDataFrame(pd.concat([ferryseg, roadseg], ignore_index=True, sort=False))

    # Validation: ensure route has contiguous geometry.
    for field_group in field_groups:

        logger.info(f"Validating routes in field group: {', '.join(map(str, field_group))}.")

        # Filter dataframe to non-default values across all fields, keep only field group.
        default = defaults_all["roadseg"][field_group[0]]
        df_filtered = df[(df[field_group].values != default).any(axis=1)][field_group]
        # TODO: not fixed beyond this point.

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

            logger.info(f"Validating route {index + 1} of {route_count}: \"{route_name}\".")

            # Subset dataframe to those records with route name in at least one field.
            route_df = df.iloc[list(np.where(df[field_group] == route_name)[0])]

            # Load dataframe as networkx graph.
            route_graph = helpers.gdf_to_nx(route_df, keep_attributes=False)

            # Validate contiguity (networkx connectivity).
            if not nx.is_connected(route_graph):

                # Identify deadends (locations of discontiguity).
                deadends = [coords for coords, degree in route_graph.degree() if degree == 1]
                deadends = "\n".join([f"{deadend[0]}, {deadend[1]}" for deadend in deadends])

                # Compile error properties.
                errors.append(f"Route name: \"{route_name}\", based on attribute fields: {', '.join(field_group)}."
                              f"\nEndpoints:\n{deadends}.")

    return {"errors": errors}


def validate_speed(df):
    """Applies a set of validations to speed field."""

    errors = dict()

    # Subset dataframe to non-default values, keep only required fields.
    default = defaults_all["roadseg"]["speed"]
    s_filtered = df[df["speed"] != default]["speed"]

    # Validation 1: ensure 5 <= speed <= 120.
    # Compile uuids of flagged records.
    errors[1] = s_filtered[~s_filtered.map(lambda speed: 5 <= int(speed) <= 120)].index.values

    # Validation 2: ensure speed is a multiple of 5.
    errors[2] = s_filtered[s_filtered.map(lambda speed: int(speed) % 5 != 0)].index.values

    return {"errors": errors}
