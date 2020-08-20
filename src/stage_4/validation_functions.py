import calendar
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
from itertools import chain, groupby, permutations, tee
from operator import attrgetter, itemgetter
from scipy.spatial import cKDTree
from shapely.geometry import Point

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import helpers


logger = logging.getLogger()


# Compile default field values and dtypes.
defaults_all = helpers.compile_default_values()
dtypes_all = helpers.compile_dtypes()


def conflicting_exitnbrs(df):
    """Applies a set of validations to exitnbr field."""

    errors = {1: list()}
    default = defaults_all["roadseg"]["exitnbr"]

    # Query multi-segment road elements (via nid field) where exitnbr is not the default value or not "None".
    df_filtered = df[(df["nid"].duplicated(keep=False)) &
                     (df["nid"] != default) &
                     (~df["exitnbr"].isin({default, "None"}))]

    if len(df_filtered):

        # Group exitnbrs by nid, removing duplicate values.
        grouped = helpers.groupby_to_list(df_filtered, "nid", "exitnbr").map(np.unique)

        # Validation: ensure road element has <= 1 unique exitnbr.
        flag_nids = grouped[grouped.map(len) > 1]

        # Compile error properties.
        for nid, exitnbrs in flag_nids.iteritems():
            exitnbrs = ", ".join(map(lambda val: f"'{val}'", exitnbrs))
            errors[1].append(f"nid '{nid}' has multiple exitnbrs: {exitnbrs}.")

    return errors


def conflicting_pavement_status(df):
    """Applies a set of validations to pavstatus, pavsurf, and unpavsurf fields."""

    errors = {i: list() for i in range(1, 4+1)}

    # Subset dataframe to non-default values, keep only required fields.
    default = defaults_all["roadseg"]["pavstatus"]
    df_filtered = df[df["pavstatus"] != default][["pavstatus", "pavsurf", "unpavsurf"]]

    # Apply validations and compile uuids of flagged records.
    if len(df_filtered):

        # Validation: when pavstatus == "Paved", ensure pavsurf != "None" and unpavsurf == "None".
        paved = df_filtered[df_filtered["pavstatus"] == "Paved"]
        errors[1] = paved[paved["pavsurf"] == "None"].index.values
        errors[2] = paved[paved["unpavsurf"] != "None"].index.values

        # Validation: when pavstatus == "Unpaved", ensure pavsurf == "None" and unpavsurf != "None".
        unpaved = df_filtered[df_filtered["pavstatus"] == "Unpaved"]
        errors[3] = unpaved[unpaved["pavsurf"] != "None"].index.values
        errors[4] = unpaved[unpaved["unpavsurf"] == "None"].index.values

    # Compile error properties.
    for code, vals in errors.items():
        if len(vals):
            errors[code] = list(map(lambda val: f"uuid: '{val}'", vals))

    return errors


def dates(df):
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
            results = s_filtered[s_filtered.map(lambda date: len(date) not in {4, 6, 8})].index.values
            errors[2].extend(results)

            # Subset to valid records only for remaining validations.
            invalid_indexes = list(set(chain.from_iterable(errors.values())))
            s_filtered2 = s_filtered[~s_filtered.index.isin(invalid_indexes)]

            if len(s_filtered2):

                # Temporarily set missing month and day values to 01.
                series_mod = s_filtered2[s_filtered2.map(lambda date: len(date) in {4, 6})]
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
                     ~(df.index.isin(set(chain.from_iterable(itemgetter(1, 2)(errors)))))]
    if len(df_filtered):
        results = df_filtered[df_filtered["credate"].map(int) > df_filtered["revdate"].map(int)].index.values
        errors[7].extend(results)

    # Compile error properties.
    for code, vals in errors.items():
        if len(vals):
            errors[code] = list(map(lambda val: f"uuid: '{val}'", vals))

    return errors


def deadend_proximity(junction, roadseg):
    """Validates the proximity of deadend junctions to disjoint / non-connected road segments."""

    # Validation: deadend junctions must be >= 5 meters from disjoint road segments.
    errors = {1: list()}

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
        target_uuids = ", ".join(map(lambda val: f"'{val}'", target_uuids))
        errors[1].append(f"junction uuid '{source_uuid}' is too close to roadseg uuid(s): {target_uuids}.")

    return errors


def duplicated_lines(df):
    """Identifies the uuids of duplicate and overlapping line geometries."""

    errors = {i: list() for i in range(1, 3+1)}

    # Keep only required fields.
    series = df["geometry"]

    # Validation 1: ensure line segments are not duplicated.

    # Filter geometries to those with duplicate lengths.
    s_filtered = series[series.length.duplicated(keep=False)]

    if len(s_filtered):

        # Filter geometries to those with duplicate endpoint coordinates.
        s_filtered = s_filtered[s_filtered.map(
            lambda g: tuple(sorted(itemgetter(0, -1)(g.coords)))).duplicated(keep=False)]

        # Identify duplicate geometries.
        if len(s_filtered):
            mask = s_filtered.map(lambda geom1: s_filtered.map(lambda geom2: geom1.equals(geom2)).sum() > 1)

            # Compile uuids of flagged records.
            errors[1] = s_filtered[mask].index.values

    # Validation 2: ensure line segments do not have repeated adjacent points.

    # Filter geometries to those with duplicated coordinates.
    s_filtered = series[series.map(lambda g: len(g.coords) != len(set(g.coords)))]

    if len(s_filtered):

        # Identify geometries with repeated adjacent coordinates.
        mask = s_filtered.map(lambda g: len(g.coords) != len(list(groupby(g.coords))))

        # Compile uuids of flagged records.
        errors[2] = s_filtered[mask].index.values

    # Validation 3: ensure line segments do not overlap (i.e. contain duplicated adjacent points).

    # Extract coordinates from geometries (used multiple times).
    series_coords = series.map(attrgetter("coords")).map(tuple)

    # Create ordered coordinate pairs, sorted.
    def ordered_pairs(coords):
        coords_1, coords_2 = tee(coords)
        next(coords_2, None)
        return sorted(zip(coords_1, coords_2))
    coord_pairs = series_coords.map(ordered_pairs).explode()

    # Remove invalid pairs (duplicated adjacent coordinates).
    coord_pairs = coord_pairs[coord_pairs.map(lambda pair: pair[0] != pair[1])]

    # Group uuids of matching pairs.
    coord_pairs_df = coord_pairs.reset_index(drop=False)
    coord_pairs_grouped = helpers.groupby_to_list(coord_pairs_df, "geometry", "uuid")
    coord_pairs_grouped = pd.DataFrame({"pairs": coord_pairs_grouped.index, "uuid": coord_pairs_grouped.values})

    # Filter to duplicated pairs.
    coord_pairs_dup = coord_pairs_grouped[coord_pairs_grouped["uuid"].map(len) > 1]
    if len(coord_pairs_dup):

        # Group duplicated pairs by sorted uuid groups.
        coord_pairs_dup["uuid"] = coord_pairs_dup["uuid"].map(sorted).map(tuple)
        coord_pairs_dup_grouped = helpers.groupby_to_list(coord_pairs_dup, "uuid", "pairs")

        # Compile error properties.
        if len(coord_pairs_dup_grouped):
            for uuid_group, pairs in coord_pairs_dup_grouped.iteritems():
                vals = ", ".join(map(lambda val: f"'{val}'", uuid_group))
                errors[3].append(f"Overlap identified for uuids: {vals}; number of overlapping segments: {len(pairs)}.")

    # Compile error properties.
    for code, vals in errors.items():
        if code in {1, 2} and len(vals):
            errors[code] = list(map(lambda val: f"uuid: '{val}'", vals))

    return errors


def duplicated_points(df):
    """Identifies the uuids of duplicate point geometries."""

    errors = {1: list()}

    # Identify duplicated geometries.
    mask = df["geometry"].map(lambda geom: geom.coords[0]).duplicated(keep=False)

    # Compile uuids of flagged records.
    errors[1] = df[mask].index.values

    # Compile error properties.
    for code, vals in errors.items():
        if len(vals):
            errors[code] = list(map(lambda val: f"uuid: '{val}'", vals))

    return errors


def exitnbr_roadclass_relationship(df):
    """Applies a set of validations to exitnbr and roadclass fields."""

    errors = {1: list()}

    # Subset dataframe to non-default and non-"None" values, keep only required fields.
    default = defaults_all["roadseg"]["exitnbr"]
    s_filtered = df[~df["exitnbr"].isin({default, "None"})]["roadclass"]

    if len(s_filtered):

        # Validation: ensure roadclass is one of: "Expressway / Highway", "Freeway", "Ramp", "Rapid Transit", "Service
        #             Lane" when exitnbr is not the default value or not "None".

        # Compile uuids of flagged records.
        errors[1] = s_filtered[~s_filtered.isin({"Expressway / Highway", "Freeway", "Ramp", "Rapid Transit",
                                                 "Service Lane"})].index.values

    # Compile error properties.
    for code, vals in errors.items():
        if len(vals):
            errors[code] = list(map(lambda val: f"uuid: '{val}'", vals))

    return errors


def ferry_road_connectivity(ferryseg, roadseg, junction):
    """Validates the connectivity between ferry and road line segments."""

    errors = {i: list() for i in range(1, 2+1)}

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

    # Compile error properties.
    for code, vals in errors.items():
        if len(vals):
            errors[code] = list(map(lambda val: f"uuid: '{val}'", vals))

    return errors


def ids(df):
    """Applies a set of validations to all id fields."""

    errors = {i: list() for i in range(1, 4+1)}

    # Identify dataframe name to configure dtypes and default values.
    table = "roadseg"
    dtypes, defaults = dtypes_all[table], defaults_all[table]
    for t in defaults_all:
        if set(defaults_all[t]).issubset(df.columns):
            dtypes, defaults, table = dtypes_all[t], defaults_all[t], t
            break

    # Iterate fields which a) end with "id", b) are str type, and c) are not uuid.
    for col in [fld for fld in df.columns.difference(["uuid"]) if fld.endswith("id") and dtypes[fld] == "str"]:

        # Subset dataframe to required column with non-default and non-"None" values.
        series = df[~df[col].isin([defaults[col], "None"])][col]

        if len(series):

            # Validation 1: ensure ids are 32 digits.
            # Compile uuids of flagged records.
            flag_uuids = series[series.map(len) != 32].index.values
            for id in flag_uuids:
                errors[1].append(f"uuid: '{id}', based on attribute field: {col}.")

            # Validation 2: ensure ids are hexadecimal.
            # Compile uuids of flagged records.
            hexdigits = set(string.hexdigits)
            flag_uuids = series[series.map(lambda id: not set(id).issubset(hexdigits))].index.values
            for id in flag_uuids:
                errors[2].append(f"uuid: '{id}', based on attribute field: {col}.")

    # Iterate unique id fields.
    unique_fields = {"ferrysegid", "roadsegid"}
    for col in unique_fields.intersection(set(df.columns)):

        # Filter dataframe to required column.
        series = df[col]

        # Validation 3: ensure unique id fields are unique within their column.
        # Compile uuids of flagged records.
        flag_uuids = series[series.duplicated(keep=False)].index.values
        for id in flag_uuids:
            errors[3].append(f"uuid: '{id}', based on attribute field: {col}.")

        # Validation 4: ensure unique id fields are not "None" nor the default field value.
        # Compile uuids of flagged records.
        flag_uuids = series[series.isin([defaults[col], "None"])].index.values
        for id in flag_uuids:
            errors[4].append(f"uuid: '{id}', based on attribute field: {col}.")

    return errors


def isolated_lines(roadseg, junction, ferryseg=None):
    """Identifies the uuids of isolated road segments from the merged dataframe of road and ferry segments."""

    errors = {1: list()}

    # Concatenate ferryseg and roadseg dataframes, keeping only required fields.
    if ferryseg is not None:
        df = gpd.GeoDataFrame(pd.concat([ferryseg[["uuid", "geometry"]], roadseg[["uuid", "geometry"]]],
                                        ignore_index=False, sort=False))
    else:
        df = roadseg.copy(deep=True)

    # Compile dead end junctions.
    deadends = set(chain([geom.coords[0] for geom in junction[junction["junctype"] == "Dead End"]["geometry"].values]))

    # Identify isolated segments.
    mask = df["geometry"].map(lambda g: len(set(itemgetter(0, -1)(g.coords)) - deadends) == 0)

    # Compile uuids of flagged records.
    errors[1] = df[mask].index.values

    # Compile error properties.
    for code, vals in errors.items():
        if len(vals):
            errors[code] = list(map(lambda val: f"uuid: '{val}'", vals))

    return errors


def line_endpoint_clustering(df):
    """
    Validates the quantity of points clustered near the endpoints of line segments.
    Validation: ensure line segments have <= 3 points within 83 meters of either endpoint, inclusively.
    """

    errors = {1: list()}

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
        errors[1] = s_filtered[flags].index.values

    # Compile error properties.
    for code, vals in errors.items():
        if len(vals):
            errors[code] = list(map(lambda val: f"uuid: '{val}'", vals))

    return errors


def line_length(df):
    """Validates the minimum feature length of line geometries."""

    errors = {1: list()}

    # Filter records to 0.0002 degrees length (approximately 22.2 meters).
    series = df[df.length <= 0.0002]["geometry"]

    if len(series):

        # Transform records to a meter-based crs: EPSG:3348.
        series = helpers.reproject_gdf(series, 4617, 3348)

        # Validation: ensure line segments are >= 2 meters in length.
        errors[1] = series[series.length < 2].index.values

    # Compile error properties.
    for code, vals in errors.items():
        if len(vals):
            errors[code] = list(map(lambda val: f"uuid: '{val}'", vals))

    return errors


def line_merging_angle(df):
    """
    Validates the merging angle of line segments.
    Validation: ensure line segments merge at angles >= 40 degrees.
    """

    errors = {1: list()}

    # Transform records to a meter-based crs: EPSG:3348.
    series = helpers.reproject_gdf(df["geometry"], 4617, 3348)

    # Compile line endpoints and their neighbours, convert to uuid-neighbour lookup dict.
    endpts_nbrs = series.map(lambda g: tuple(map(itemgetter(0, 1), itemgetter(0, 1, -2, -1)(attrgetter("coords")(g)))))
    uuid_nbr_lookup = endpts_nbrs.to_dict()

    # Compile only endpoints.
    endpts = endpts_nbrs.map(itemgetter(0, -1))

    # Explode point groups, filter to only duplicates, and construct a dataframe of the uuids and coordinates.
    pts_exploded = endpts.explode()
    pts_dups = pts_exploded[pts_exploded.duplicated(keep=False)]
    pts_df = pd.DataFrame({"coords": pts_dups, "uuid": pts_dups.index})

    # Proceed only if duplicated points exist.
    if len(pts_df):

        # Group uuids according to coordinates.
        uuids_grouped = helpers.groupby_to_list(pts_df, "coords", "uuid")

        # Explode grouped uuids. Maintain index point as both index and column.
        uuids_grouped_exploded = uuids_grouped.explode()
        uuids_grouped_exploded = pd.DataFrame({"coords": uuids_grouped_exploded.index, "uuid": uuids_grouped_exploded})\
            .reset_index(drop=True)

        # Compile endpoint-neighbouring points.
        # Process: Flag uuids according to duplication status within their group. For unique uuids, configure the
        # neighbouring point based on whichever endpoint matches the common group point. For duplicated uuids
        # (which represent self-loops), the first duplicate takes the second point, the second duplicate takes the
        # second-last point - thereby avoiding the same neighbour being taken twice for self-loop intersections.
        dup_flags = {
            "dup_none": uuids_grouped_exploded[~uuids_grouped_exploded.duplicated(keep=False)][["uuid", "coords"]],
            "dup_first": uuids_grouped_exploded[uuids_grouped_exploded.duplicated(keep="first")]["uuid"],
            "dup_last": uuids_grouped_exploded[uuids_grouped_exploded.duplicated(keep="last")]["uuid"]
        }
        dup_results = {
            "dup_none": np.vectorize(
                lambda uid, pt: uuid_nbr_lookup[uid][1] if uuid_nbr_lookup[uid][0] == pt else uuid_nbr_lookup[uid][-2],
                otypes=[tuple])(dup_flags["dup_none"]["uuid"], dup_flags["dup_none"]["coords"]),
            "dup_first": dup_flags["dup_first"].map(lambda uid: uuid_nbr_lookup[uid][1]).values,
            "dup_last": dup_flags["dup_last"].map(lambda uid: uuid_nbr_lookup[uid][-2]).values
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

            return abs(angle_1 - angle_2) < 40

        # Calculate the angular degree between each reference point and each of their point permutations.
        # Return True if any angles are invalid.
        flags = np.vectorize(
            lambda pt_groups, pt_ref: any(filter(lambda pts: get_invalid_angle(pts[0], pts[1], pt_ref), pt_groups)))(
            pts_grouped, pts_grouped.index)

        # Compile the uuid groups as errors.
        flag_uuid_groups = uuids_grouped[flags].values

        # Compile error properties.
        if len(flag_uuid_groups):
            for uuid_group in flag_uuid_groups:
                vals = ", ".join(map(lambda val: f"'{val}'", uuid_group))
                errors[1].append(f"Invalid merging angle exists at intersection of uuids: {vals}.")

    return errors


def line_proximity(df):
    """Validates the proximity of line segments."""

    # Validation: ensure line segments are >= 3 meters from each other, excluding connected segments.
    errors = {1: list()}

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
    for source_uuid, target_uuids in results.iteritems():
        target_uuids = ", ".join(map(lambda val: f"'{val}'", target_uuids))
        errors[1].append(f"uuid '{source_uuid}' is too close to uuid(s): {target_uuids}.")

    return errors


def nbrlanes(df):
    """Applies a set of validations to nbrlanes field."""

    errors = {1: list()}

    # Subset dataframe to non-default values, keep only required fields.
    default = defaults_all["roadseg"]["nbrlanes"]
    s_filtered = df[df["nbrlanes"] != default]["nbrlanes"]

    if len(s_filtered):

        # Validation: ensure 1 <= nbrlanes <= 8.
        # Compile uuids of flagged records.
        errors[1] = s_filtered[~s_filtered.map(lambda nbrlanes: 1 <= int(nbrlanes) <= 8)].index.values

    # Compile error properties.
    for code, vals in errors.items():
        if len(vals):
            errors[code] = list(map(lambda val: f"uuid: '{val}'", vals))

    return errors


def nid_linkages(df, dfs_all):
    """
    Validates the nid linkages for the input dataframe, excluding "None".
    Parameter dfs_all must be a dictionary of all nrn dataframes.
    """

    errors = {1: list()}

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

    # Identify dataframe name.
    id_table = None
    for table in defaults_all:
        if set(defaults_all[table]).issubset(df.columns):
            id_table = table
            break

    # Iterate nid tables which link to the id table.
    for nid_table in filter(lambda t: id_table in linkages[t], set(linkages).intersection(dfs_all)):

        # Retrieve nids as lowercase.
        nids = set(dfs_all[nid_table]["nid"].map(str.lower))

        # Iterate linked columns.
        for col in linkages[nid_table][id_table]:

            # Validation: ensure all nid linkages are valid.
            print(f"Validating nid linkage: {nid_table}.nid - {id_table}.{col}.")

            # Retrieve column ids as lowercase.
            ids = set(df[col].map(str.lower))

            # Compile invalid ids, excluding "None" (lower cased).
            invalid_ids = ids - nids - {"none"}

            # Configure error properties.
            if len(invalid_ids):
                for invalid_id in invalid_ids:
                    errors[1].append(f"{id_table}.{col} '{invalid_id}' is not present in {nid_table}.nid.")

    return errors


def point_proximity(df):
    """Validates the proximity of points."""

    # Validation: ensure points are >= 3 meters from each other.
    errors = {1: list()}

    # Transform records to a meter-based crs: EPSG:3348.
    series = helpers.reproject_gdf(df["geometry"], 4617, 3348)

    # Compile coordinates (used multiple times)
    pts = series.map(lambda g: itemgetter(0)(attrgetter("coords")(g)))

    # Generate kdtree.
    tree = cKDTree(pts.to_list())

    # Compile indexes of points with other points within 3 meters distance. Only keep results with > 1 match.
    proxi_idx_all = pts.map(lambda pt: set(chain(*tree.query_ball_point([pt], r=3))))
    proxi_idx_all = proxi_idx_all[proxi_idx_all.map(len) > 1]

    # Compile and filter coincident index from each set of indexes for each point, keep non-empty results.
    proxi_idx_exclude = pd.Series(range(len(pts)), index=pts.index).map(lambda index: {index})
    proxi_idx_keep = proxi_idx_all - proxi_idx_exclude.loc[proxi_idx_all.index]

    # Compile uuids associated with each index.
    pts_idx_uuid_lookup = {index: uid for index, uid in enumerate(pts.index)}
    results = proxi_idx_keep.map(lambda indexes: itemgetter(*indexes)(pts_idx_uuid_lookup))
    results = results.map(lambda vals: set(vals) if isinstance(vals, tuple) else {vals})

    # Compile error properties.
    for source_uuid, target_uuids in results.iteritems():
        target_uuids = ", ".join(map(lambda val: f"'{val}'", target_uuids))
        errors[1].append(f"uuid '{source_uuid}' is too close to uuid(s): {target_uuids}.")

    return errors


def roadclass_rtnumber_relationship(df):
    """Applies a set of validations to roadclass and rtnumber1 fields."""

    errors = {1: list()}

    # Filter dataframe to only required fields.
    df_filtered = df[["roadclass", "rtnumber1"]]

    # Apply validations and compile uuids of flagged records.

    # Validation: ensure rtnumber1 is not the default value or "None" when roadclass = "Expressway / Highway" or
    # "Freeway".
    default = defaults_all["roadseg"]["rtnumber1"]
    errors[1] = df_filtered[df_filtered["roadclass"].isin({"Expressway / Highway", "Freeway"}) &
                            df_filtered["rtnumber1"].map(lambda rtnumber1: rtnumber1 in {default, "None"})].index.values

    # Compile error properties.
    for code, vals in errors.items():
        if len(vals):
            errors[code] = list(map(lambda val: f"uuid: '{val}'", vals))

    return errors


def route_contiguity(roadseg, ferryseg=None):
    """
    Applies a set of validations to route attributes (rows represent field groups):
        rtename1en, rtename2en, rtename3en, rtename4en,
        rtename1fr, rtename2fr, rtename3fr, rtename4fr,
        rtnumber1, rtnumber2, rtnumber3, rtnumber4, rtnumber5.
    """

    errors = {1: list()}

    # Define field groups.
    field_groups = [["rtename1en", "rtename2en", "rtename3en", "rtename4en"],
                    ["rtename1fr", "rtename2fr", "rtename3fr", "rtename4fr"],
                    ["rtnumber1", "rtnumber2", "rtnumber3", "rtnumber4", "rtnumber5"]]

    # Filter dataframes to only required fields.
    keep_fields = list(chain.from_iterable([*field_groups, ["geometry"]]))
    roadseg = roadseg[keep_fields]
    if ferryseg is not None:
        ferryseg = ferryseg[keep_fields]

    # Concatenate ferryseg and roadseg.
    if ferryseg is not None:
        df = gpd.GeoDataFrame(pd.concat([ferryseg, roadseg], ignore_index=True, sort=False))
    else:
        df = roadseg.copy(deep=True)

    # Validation: ensure route has contiguous geometry.
    for field_group in field_groups:

        logger.info(f"Validating routes in field group: {', '.join(map(str, field_group))}.")

        # Filter dataframe to records with >= 1 non-default values across the field group, keep only required fields.
        default = defaults_all["roadseg"][field_group[0]]
        df_filtered = df[(df[field_group].values != default).any(axis=1)][[*field_group, "geometry"]]

        # Compile route names, excluding default value and "None".
        route_names = set(np.unique(df_filtered[field_group].values)) - {default, "None"}

        # Iterate route names.
        route_count = len(route_names)
        for index, route_name in enumerate(sorted(route_names)):

            logger.info(f"Validating route {index + 1} of {route_count}: \"{route_name}\".")

            # Subset dataframe to those records with route name in at least one field.
            route_df = df_filtered[(df_filtered[field_group].values == route_name).any(axis=1)]

            # Only process duplicated route names.
            if len(route_df) > 1:

                # Load dataframe as networkx graph.
                route_graph = helpers.gdf_to_nx(route_df, keep_attributes=False)

                # Validate contiguity (networkx connectivity).
                if not nx.is_connected(route_graph):

                    # Identify deadends (locations of discontiguity).
                    deadends = [coords for coords, degree in route_graph.degree() if degree == 1]
                    deadends = "\n".join(map(lambda deadend: f"{deadend[0]}, {deadend[1]}", deadends))

                    # Compile error properties.
                    errors[1].append(f"Discontiguous route: '{route_name}', based on attribute fields: "
                                     f"{', '.join(field_group)}."
                                     f"\nCoordinates of discontiguity:\n{deadends}\n")

    return errors


def self_intersecting_elements(df):
    """Applies a set of validations to self-intersecting road elements."""

    errors = {1: list()}
    default = defaults_all["roadseg"]["nid"]

    # Validation: ensure roadclass is in ("Expressway / Highway", "Freeway", "Ramp", "Rapid Transit", "Service Lane")
    #             for all road elements which a) self-intersect and b) touch another road segment where roadclass is in
    #             this set.

    flag_nids = list()
    valid = {"Expressway / Highway", "Freeway", "Ramp", "Rapid Transit", "Service Lane"}

    # Compile coords of road segments where roadclass is in the validation list.
    valid_coords = set(chain(
        *[itemgetter(0, -1)(geom.coords) for geom in df[df["roadclass"].isin(valid)]['geometry'].values]))

    # Single-segment road elements:

    # Retrieve single-segment self-intersections.
    # Function call intended to avoid duplicating logic in this current function.
    segments_single = self_intersecting_structures(df, return_segments_only=True)

    if not segments_single.empty:

        # Compile nids of road segments with coords in the validation coords list.
        flagged = segments_single["geometry"].map(lambda g: g.coords[0] in valid_coords)
        flag_nids.extend(segments_single[flagged]["nid"].values)

    # Multi-segment road elements:

    # Compile multi-segment road elements (via non-unique nids).
    # Filter to nids with invalid roadclass.
    segments_multi = df[(df["nid"].duplicated(keep=False)) & (~df["roadclass"].isin(valid)) & (df["nid"] != default)]

    if not segments_multi.empty:

        logger.info("Validating multi-segment road elements.")

        # Compile nids of road segments with coords in the validation coords list.
        flagged_nids = segments_multi[segments_multi["geometry"].map(
            lambda g: len(set(itemgetter(0, -1)(g.coords)).intersection(valid_coords)) > 0)]["nid"].unique()

        if len(flagged_nids):

            # Compile dataframe records with a flagged nid.
            flagged_df = df[df["nid"].isin(flagged_nids)]

            # Group geometries by nid.
            grouped_segments = helpers.groupby_to_list(flagged_df, "nid", "geometry")

            # Dissolve road segments.
            elements = grouped_segments.map(shapely.ops.linemerge)

            # Identify self-intersections and store nids.
            flag_nids.extend(elements[elements.map(lambda element: element.is_ring or not element.is_simple)].values)

    # Compile uuids of road segments with flagged nid and invalid roadclass.
    errors[1] = df[(df["nid"].isin(flag_nids)) & (~df["roadclass"].isin(valid))].index.values

    # Compile error properties.
    for code, vals in errors.items():
        if len(vals):
            errors[code] = list(map(lambda val: f"uuid: '{val}'", vals))

    return errors


def self_intersecting_structures(df, return_segments_only=False):
    """Applies a set of validations to self-intersecting road structures."""

    errors = {1: list()}
    flag_segments = pd.DataFrame()
    default = defaults_all["roadseg"]["nid"]

    # Identify self-intersections formed by single-segment road elements (i.e. where nid is unique).

    # Compile single-segment road elements (via unique nids).
    segments = df[(~df["nid"].duplicated(keep=False)) & (df["nid"] != default)]

    if not segments.empty:

        logger.info("Validating single-segment road elements.")

        # Identify self-intersections (start coord == end coord).
        flag_segments = segments[segments["geometry"].map(lambda g: g.is_ring or not g.is_simple)]

        # Validation: for self-intersecting road segments, ensure structtype != "None".
        errors[1] = flag_segments[flag_segments["structtype"] == "None"].index.values

    if return_segments_only:
        return flag_segments

    else:

        # Compile error properties.
        for code, vals in errors.items():
            if len(vals):
                errors[code] = list(map(lambda val: f"uuid: '{val}'", vals))

        return errors


def speed(df):
    """Applies a set of validations to speed field."""

    errors = {i: list() for i in range(1, 2+1)}

    # Subset dataframe to non-default values, keep only required fields.
    default = defaults_all["roadseg"]["speed"]
    s_filtered = df[df["speed"] != default]["speed"]

    if len(s_filtered):

        # Validation 1: ensure 5 <= speed <= 120.
        # Compile uuids of flagged records.
        errors[1] = s_filtered[~s_filtered.map(lambda speed: 5 <= int(speed) <= 120)].index.values

        # Validation 2: ensure speed is a multiple of 5.
        errors[2] = s_filtered[s_filtered.map(lambda speed: int(speed) % 5 != 0)].index.values

    # Compile error properties.
    for code, vals in errors.items():
        if len(vals):
            errors[code] = list(map(lambda val: f"uuid: '{val}'", vals))

    return errors


def structure_attributes(roadseg, junction):
    """Validates the structid and structtype attributes of road segments."""

    errors = {i: list() for i in range(1, 4+1)}
    defaults = defaults_all["roadseg"]

    # Filter dataframes to only required fields.
    junction = junction[junction["junctype"] == "Dead End"]["geometry"]
    roadseg = roadseg[["uuid", "structid", "structtype", "geometry"]]

    # Validation 1: ensure dead end road segments have structtype = "None" or the default field value.

    # Compile dead end coordinates.
    deadend_coords = set(chain(junction.map(lambda pt: itemgetter(0)(attrgetter("coords")(pt)))))

    # Compile road segments with potentially invalid structtype.
    roadseg_invalid = roadseg[~roadseg["structtype"].isin({"None", defaults["structtype"]})]["geometry"]

    # Compile truly invalid road segments.
    roadseg_invalid = roadseg_invalid[roadseg_invalid.map(
        lambda g: any(coords in deadend_coords for coords in attrgetter("coords")(g)))]

    # Compile uuids of flagged records, compile error properties.
    if len(roadseg_invalid):
        errors[1] = list(map(lambda val: f"uuid: '{val}'", roadseg_invalid.index.values))

    # Validation 2: ensure structid is contiguous.

    # Compile records with duplicated structids, excluding default value.
    structids_df = roadseg[(roadseg["structid"] != defaults["structid"]) &
                           (roadseg["structid"].duplicated(keep=False))]

    if len(structids_df):

        # Group records by structid.
        structures = helpers.groupby_to_list(structids_df, "structid", "geometry")

        # Load structure geometries as networkx graphs.
        structure_graphs = structures.map(
            lambda geoms: helpers.gdf_to_nx(gpd.GeoDataFrame(geometry=geoms), keep_attributes=False))

        # Validate contiguity (networkx connectivity).
        structures_invalid = structure_graphs[~structure_graphs.map(nx.is_connected)]

        if len(structures_invalid):

            # Identify deadends (locations of discontiguity).
            results = structures_invalid.map(lambda graph: [pt for pt, degree in graph.degree() if degree == 1])

            # Compile error properties.
            for structid, deadends in results.iteritems():
                deadends = "\n".join(map(lambda deadend: f"{deadend[0]}, {deadend[1]}", deadends))

                errors[2].append(f"Discontiguous structure structid: '{structid}'."
                                 f"\nCoordinates of discontiguity:\n{deadends}\n.")

    # Validation 3: ensure a single, non-default structid is applied to all contiguous road segments with the same
    #               structtype.
    # Validation 4: ensure road segments with different structtypes, excluding "None" and the default field value, are
    #               not contiguous.

    # Compile road segments with valid structtype.
    segments = roadseg[~roadseg["structtype"].isin({"None", defaults["structtype"]})]

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
            uuids = ", ".join(map(lambda val: f"'{val}'", row[0]))
            structids = ", ".join(map(lambda val: f"'{val}'", row[1]))
            errors[3].append(f"Structure formed by uuids: {uuids} contains multiple structids: {structids}.")

    # Validation 4.
    structtypes = sub_g.map(lambda graph: set(nx.get_edge_attributes(graph, "structtype").values()))
    structtypes_invalid = structtypes[structtypes.map(len) > 1]
    if len(structtypes_invalid):

        # Compile uuids of invalid structure.
        uuids_invalid = sub_g.loc[structtypes_invalid.index].map(
            lambda graph: set(nx.get_edge_attributes(graph, "uuid").values()))

        # Compile error properties.
        for index, row in pd.DataFrame({"uuids": uuids_invalid, "structtypes": structtypes_invalid}).iterrows():
            uuids = ", ".join(map(lambda val: f"'{val}'", row[0]))
            structtypes = ", ".join(map(lambda val: f"'{val}'", row[1]))
            errors[4].append(f"Structure formed by uuids: {uuids} contains multiple structtypes: {structtypes}.")

    return errors
