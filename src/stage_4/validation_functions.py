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
from itertools import chain, permutations
from operator import attrgetter, itemgetter
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

    # Note: filters are purely intended to reduce processing.
    # Filter geometries to those with duplicate lengths.
    df_sub = df[df["geometry"].length.duplicated(keep=False)]

    # Filter geometries to those with duplicate endpoint coordinates.
    df_sub = df_sub[df_sub["geometry"].map(lambda g: tuple(sorted(itemgetter(0, -1)(g.coords)))).duplicated(keep=False)]

    # Identify duplicate geometries.
    if len(df_sub):
        mask = df_sub["geometry"].map(lambda geom1: df_sub["geometry"].map(lambda geom2: geom1.equals(geom2)).sum() > 1)

        # Compile uuids of flagged records.
        errors = df_sub[mask].index.values

    return {"errors": errors}


def identify_duplicate_points(df):
    """Identifies the uuids of duplicate point geometries."""

    # Retrieve coordinates as tuples.
    coords = df["geometry"].map(lambda geom: geom.coords[0])

    # Identify duplicate geometries.
    mask = coords.duplicated(keep=False)

    # Compile uuids of flagged records.
    errors = df[mask].index.values

    return {"errors": errors}


def identify_isolated_lines(roadseg, ferryseg):
    """Identifies the uuids of isolated road segments from the merged dataframe of road and ferry segments."""

    # Concatenate ferryseg and roadseg dataframes.
    df = gpd.GeoDataFrame(pd.concat([ferryseg, roadseg], ignore_index=False, sort=False))

    # Convert dataframe to networkx graph.
    # Drop all columns except uuid and geometry to reduce processing.
    df.drop(df.columns.difference(["uuid", "geometry"]), axis=1, inplace=True)
    g = helpers.gdf_to_nx(df, keep_attributes=True, endpoints_only=False)

    # Configure subgraphs.
    sub_g = nx.connected_component_subgraphs(g)

    # Compile uuids unique to a subgraph.
    flag_uuids = list()
    for s in sub_g:
        if len(set(nx.get_edge_attributes(s, "uuid").values())) == 1:
            uuid = list(nx.get_edge_attributes(s, "uuid").values())[0]

            # Store uuid if from roadseg.
            if uuid not in ferryseg.index:
                flag_uuids.append(uuid)

    # Compile flagged records as errors.
    errors = flag_uuids

    return {"errors": errors}


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
    defaults = helpers.compile_default_values()["roadseg"]

    # Get current date.
    today = datetime.today().strftime("%Y%m%d")
    today = {"year": int(today[:4]), "month": int(today[4:6]), "day": int(today[6:8]), "full": int(today)}

    # Define functions.
    def validate_day(date):
        """Validate the day value in a date."""

        year, month, day = map(int, [date[:4], date[4:6], date[6:8]])

        if not 1 <= day <= calendar.mdays[month]:
            if not all([day == 29, month == 2, calendar.isleap(year)]):
                return True

        return False

    # Iterate credate and revdate, applying validations.
    for col in ("credate", "revdate"):

        # Subset to non-default values.
        df_sub = df[df[col] != defaults[col]]

        if len(df_sub):

            # Validation 1: date content must be numeric.
            results = df_sub[~df_sub[col].map(str.isnumeric)].index.values
            errors[1].extend(results)

            # Validation 2: length must be 4, 6, or 8.
            results = df_sub[df_sub[col].map(lambda date: len(date) not in (4, 6, 8))].index.values
            errors[2].extend(results)

            # Subset to valid records only for remaining validations.
            df_sub2 = df_sub[~df_sub.index.isin(list(set(chain.from_iterable(errors.values()))))]

            if len(df_sub2):

                # Temporarily set missing month and day values to 01.
                col_mod = df_sub2[df_sub2[col].map(lambda date: len(date) in (4, 6))][col]
                if len(col_mod):
                    append_vals = {4: "0101", 6: "01"}
                    df_sub2.loc[col_mod.index, col] = col_mod.map(lambda date: date + append_vals[len(date)])
                    df.loc[col_mod.index, col] = col_mod.map(lambda date: date + append_vals[len(date)])

                # Validation 3: valid date - year.
                results = df_sub2[~df_sub2[col].map(lambda date: 1960 <= int(date[:4]) <= today["year"])].index.values
                errors[3].extend(results)

                # Validation 4: valid date - month.
                results = df_sub2[df_sub2[col].map(lambda date: int(date[4:6]) not in range(1, 12+1))].index.values
                errors[4].extend(results)

                # Validation 5: valid date - day.
                results = df_sub2[df_sub2[col].map(lambda date: validate_day(date))].index.values
                errors[5].extend(results)

                # Validation 6: ensure date <= today.
                results = df_sub2[df_sub2[col].map(lambda date: int(date) > today["full"])].index.values
                errors[6].extend(results)

    # Validation 7: ensure credate <= revdate.
    df_sub = df[(df["credate"] != defaults["credate"]) &
                (df["revdate"] != defaults["revdate"]) &
                ~(df.index.isin(list(set(chain.from_iterable(itemgetter(1, 2)(errors))))))]
    if len(df_sub):
        results = df_sub[df_sub["credate"].map(int) > df_sub["revdate"].map(int)].index.values
        errors[7].extend(results)

    return {"errors": errors}


def validate_deadend_disjoint_proximity(junction, roadseg):
    """Validates the proximity of deadend junctions to disjoint / non-connected road segments."""

    # Validation: deadend junctions must be >= 5 meters from disjoint road segments.

    # Filter junctions to junctype = "Dead End".
    deadends = junction[junction["junctype"] == "Dead End"]

    # Transform records to a meter-based crs: EPSG:3348.
    deadends = helpers.reproject_gdf(deadends, 4617, 3348)
    roadseg = helpers.reproject_gdf(roadseg, 4617, 3348)

    # Generate kdtree.
    tree = cKDTree(np.concatenate(roadseg["geometry"].map(attrgetter("coords")).to_numpy()))

    # Compile indexes of road segments within 5 meters distance of each deadend.
    proxi_idx_all = deadends["geometry"].map(lambda geom: list(chain(*tree.query_ball_point(geom.coords, r=5))))

    # Compile index of road segment at 0 meters distance from each deadend. These represent the connected roads.
    proxi_idx_exclude = deadends["geometry"].map(lambda geom: tree.query(geom.coords)[-1])

    # Construct a uuid series aligned to the series of road segment points.
    roadseg_pts_uuid = np.concatenate([[uuid] * count for uuid, count in
                                       roadseg["geometry"].map(lambda geom: len(geom.coords)).iteritems()])

    # Retrieve the uuid associated with the exclusion indexes.
    proxi_idx_exclude = proxi_idx_exclude.map(lambda index: itemgetter(*index)(roadseg_pts_uuid))

    # Compile the range of indexes for all coordinates associated with each road segment.
    idx_ranges = dict.fromkeys(roadseg.index.values)
    base = 0
    for index, count in roadseg["geometry"].map(lambda geom: len(geom.coords)).iteritems():
        idx_ranges[index] = [base, base + count]
        base += count

    # Convert associated uuids to expanded index ranges.
    proxi_idx_exclude = proxi_idx_exclude.map(lambda uuid: list(range(*itemgetter(uuid)(idx_ranges))))

    # Filter coincident indexes from all indexes.
    proxi_idx = pd.DataFrame({"all": proxi_idx_all, "exclude": proxi_idx_exclude}, index=deadends.index.values)
    proxi_idx_keep = proxi_idx.apply(lambda row: set(row[0]) - set(row[1]), axis=1)

    # Compile the uuid associated with resulting proximity point indexes for each deadend.
    proxi_results = proxi_idx_keep.map(lambda indexes: itemgetter(*indexes)(roadseg_pts_uuid) if indexes else False)
    proxi_results = proxi_results.map(lambda uuids: set(uuids) if isinstance(uuids, tuple) else uuids)

    # Compile error properties.
    errors = list()

    for source_uuid, target_uuids in proxi_results[proxi_results != False].iteritems():
        errors.append("junction uuid \"{}\" is too close to roadseg uuid(s) {}.".format(
            source_uuid,
            ", ".join(map("\"{}\"".format, [target_uuids] if isinstance(target_uuids, str) else target_uuids))))

    return {"errors": errors}


def validate_exitnbr_conflict(df):
    """Applies a set of validations to exitnbr field."""

    errors = list()
    default = defaults_all["roadseg"]["exitnbr"]

    # Query multi-segment road elements (via nid field) where exitnbr is not the default value.
    df_sub = df[(df["nid"].duplicated(keep=False)) & (df["nid"] != default) & (df["exitnbr"] != default)]

    # Group exitnbrs by nid, removing duplicate values.
    grouped = helpers.groupby_to_list(df_sub, "nid", "exitnbr").map(np.unique)

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

    # Subset dataframe to non-default values.
    df_subset = df[df["exitnbr"] != defaults_all["roadseg"]["exitnbr"]]

    # Validation: ensure roadclass == "Ramp" or "Service Lane" when exitnbr is not the default value.
    # Compile uuids of flagged records.
    errors = df_subset[~df_subset["roadclass"].isin(["Ramp", "Service Lane"])].index.values

    return {"errors": errors}


def validate_ferry_road_connectivity(ferryseg, roadseg, junction):
    """Validates the connectivity between ferry and road line segments."""

    errors = dict()

    # Validation 1: ensure ferry segments connect to a road segment at at least one endpoint.

    # Compile junction coordinates where junctype = "Ferry".
    ferry_junctions = list(set(chain([geom.coords[0] for geom in
                                      junction[junction["junctype"] == "Ferry"]["geometry"].values])))

    # Identify ferry segments which do not connect to any road segments.
    mask = ferryseg["geometry"].map(
        lambda geom: not any([coords in ferry_junctions for coords in itemgetter(0, -1)(geom.coords)]))

    # Compile uuids of flagged records.
    errors[1] = ferryseg[mask].index.values

    # Validation 2: ensure ferry segments connect to <= 1 road segment at either endpoint.

    # Compile road segments which connect to ferry segments.
    roads_connected = roadseg[roadseg["geometry"].map(
        lambda geom: any([coords in ferry_junctions for coords in itemgetter(0, -1)(geom.coords)]))]

    # Compile coordinates of connected road segments.
    road_coords = list(chain.from_iterable(roads_connected["geometry"].map(
        lambda g: tuple(set(itemgetter(0, -1)(g.coords))))))

    # Identify ferry endpoints which intersect multiple road segments.
    ferry_multi_intersect = ferryseg["geometry"].map(
        lambda ferry: any([road_coords.count(coords) > 1 for coords in itemgetter(0, -1)(ferry.coords)]))

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


def validate_line_endpoint_clustering(df):
    """Validates the quantity of points clustered near the endpoints of line segments."""

    # Validation: ensure line segments have <= 3 points within 83 meters of either endpoint, inclusively.
    errors = None

    # Transform records to a meter-based crs: EPSG:3348.
    df = helpers.reproject_gdf(df, 4617, 3348)

    # Filter out records with <= 3 points or length < 83 meters.
    df_subset = df[~df["geometry"].map(lambda geom: len(geom.coords) <= 3 or geom.length < 83)]

    if len(df_subset):

        # Identify invalid records.
        # Process: either of the following must be true:
        # a) The distance of the 4th point along the linestring is < 83 meters.
        # b) The total linestring length minus the distance of the 4th-last point along the linestring is < 83 meters.
        flags = np.vectorize(lambda geom: (geom.project(Point(geom.coords[3])) < 83) or
                                          ((geom.length - geom.project(Point(geom.coords[-4]))) < 83)
                             )(df_subset["geometry"])

        # Compile uuids of flagged records.
        errors = df_subset[flags].index.values

    return {"errors": errors}


def validate_line_length(df):
    """Validates the minimum feature length of line geometries."""

    errors = None

    # Filter records to 0.0002 degrees length (approximately 22.2 meters).
    # Purely intended to reduce processing.
    df_sub = df[df.length <= 0.0002]

    if len(df_sub):

        # Transform records to a meter-based crs: EPSG:3348.
        df_sub = helpers.reproject_gdf(df_sub, 4617, 3348)

        # Validation: ensure line segments are >= 2 meters in length.
        errors = df_sub[df_sub.length < 2].index.values

    return {"errors": errors}


# TODO
def validate_line_merging_angle(df):
    """Validates the merging angle of line segments."""

    # Validation: ensure line segments merge at angles >= 40 degrees.

    # Transform records to a meter-based crs: EPSG:3348.
    df = helpers.reproject_gdf(df, 4617, 3348)

    # Compile the uuid groups for all non-unique points.

    # Construct a uuid series aligned to the series of points.
    pts_uuid = np.concatenate([[uuid] * count for uuid, count in
                               df["geometry"].map(lambda geom: len(geom.coords)).iteritems()])

    # Construct x- and y-coordinate series aligned to the series of points.
    # Disregard z-coordinates.
    pts_x, pts_y, pts_z = np.concatenate(df["geometry"].map(attrgetter("coords")).to_numpy()).T

    # Join the uuids, x-, and y-coordinates.
    pts_df = pd.DataFrame({"x": pts_x, "y": pts_y, "uuid": pts_uuid})

    # Filter records to only duplicated points.
    pts_df = pts_df[pts_df.duplicated(["x", "y"], keep=False)]

    # Group uuids according to x- and y-coordinates.
    uuids_grouped = pts_df.groupby(["x", "y"])["uuid"].apply(list)

    # Exit function if no shared points exists (b/c therefore no line merges exist).
    if not len(uuids_grouped):

        errors = None

    else:

        # Retrieve the next point, relative to the target point, for each grouped uuid associated with each point.

        # Compile the endpoints and next-to-endpoint points for each uuid.
        pts_uuid = dict.fromkeys(df.index.values)
        for uuid, geom in df["geometry"].iteritems():
            pts_uuid[uuid] = list(map(lambda coord: coord[:2], itemgetter(0, 1, -2, -1)(geom.coords)))

        # Explode grouped uuids. Maintain index point as both index and column.
        uuids_grouped_ex = uuids_grouped.explode().reset_index(drop=False).set_index(["x", "y"], drop=False)

        # Compile next-to-endpoint points.
        # Process: Flag uuids according to duplication status within their group. For unique uuids, configure the
        # next-to-endpoint point based on whichever endpoint matches the common group point. For duplicated uuids
        # (which represent self-loops), the first duplicate takes the second point, the second duplicate takes the
        # second-last point - thereby avoiding the same next-to-point being taken twice for self-loop intersections.
        dup_flags = {
            "dup_none": ~uuids_grouped_ex.duplicated(keep=False),
            "dup_first": uuids_grouped_ex.duplicated(keep="first"),
            "dup_last": uuids_grouped_ex.duplicated(keep="last")
        }
        dup_results = {
            "dup_none": pd.Series(np.vectorize(
                lambda uuid, index: pts_uuid[uuid][1] if pts_uuid[uuid][0] == index else pts_uuid[uuid][-2],
                otypes=[tuple])(uuids_grouped_ex[dup_flags["dup_none"]]["uuid"],
                                uuids_grouped_ex[dup_flags["dup_none"]].index)).values,
            "dup_first": pd.Series(np.vectorize(
                lambda uuid, index: pts_uuid[uuid][1],
                otypes=[tuple])(uuids_grouped_ex[dup_flags["dup_first"]]["uuid"],
                                uuids_grouped_ex[dup_flags["dup_first"]].index)).values,
            "dup_last": pd.Series(np.vectorize(
                lambda uuid, index: pts_uuid[uuid][-2],
                otypes=[tuple])(uuids_grouped_ex[dup_flags["dup_last"]]["uuid"],
                                uuids_grouped_ex[dup_flags["dup_last"]].index)).values
        }

        uuids_grouped_ex["pt"] = None
        uuids_grouped_ex.loc[dup_flags["dup_none"], "pt"] = dup_results["dup_none"]
        uuids_grouped_ex.loc[dup_flags["dup_first"], "pt"] = dup_results["dup_first"]
        uuids_grouped_ex.loc[dup_flags["dup_last"], "pt"] = dup_results["dup_last"]

        # Aggregate exploded groups.
        uuids_grouped_ex.reset_index(drop=True, inplace=True)
        pts_grouped = uuids_grouped_ex.groupby(["x", "y"])["pt"].agg(list)

        # Compile the permutations of points for each point group.
        # Recover source point as index.
        pts_grouped = pts_grouped.map(lambda pts: list(set(map(tuple, map(sorted, permutations(pts, r=2))))))
        pts_grouped.index = uuids_grouped.index

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
            lambda pt_groups, pt_ref: any(map(lambda pts: get_invalid_angle(pts[0], pts[1], pt_ref), pt_groups)))(
            pts_grouped, pts_grouped.index)

        # Compile the uuid groups as errors.
        errors = uuids_grouped[flags].values

    return {"errors": errors}


# TODO
def validate_line_proximity(df):
    """Validates the proximity of line segments."""

    # Validation: ensure line segments are >= 3 meters from each other, excluding connected segments.

    # Transform records to a meter-based crs: EPSG:3348.
    df = helpers.reproject_gdf(df, 4617, 3348)

    # Generate kdtree.
    tree = cKDTree(np.concatenate(df["geometry"].map(attrgetter("coords")).to_numpy()))

    # Compile indexes of line segments with points within 3 meters distance.
    proxi_idx_all = df["geometry"].map(lambda geom: list(chain(*tree.query_ball_point(geom, r=3))))

    # Compile indexes of line segments with points at 0 meters distance. These represent points comprising the source
    # line segment.
    proxi_idx_exclude = df["geometry"].map(lambda geom: list(chain(*tree.query_ball_point(geom, r=0))))

    # Filter coincident indexes from all indexes.
    proxi_idx = pd.DataFrame({"all": proxi_idx_all, "exclude": proxi_idx_exclude}, index=df.index.values)
    proxi_idx_keep = proxi_idx.apply(lambda row: set(row[0]) - set(row[1]), axis=1)

    # Compile the uuids of connected segments to each segment (i.e. segments connected to a given segment's endpoints).

    # Construct a uuid series aligned to the series of segment endpoints.
    endpoint_uuids = np.concatenate([[uuid, uuid] for uuid, count in
                                     df["geometry"].map(lambda geom: len(geom.coords)).iteritems()])

    # Construct x- and y-coordinate series aligned to the series of segment endpoints.
    # Disregard z-coordinates.
    endpoint_x, endpoint_y, endpoint_z = np.concatenate(
        df["geometry"].map(lambda g: itemgetter(0, -1)(attrgetter("coords")(g))).to_numpy()).T

    # Join the uuids, x-, and y-coordinates.
    endpoint_df = pd.DataFrame({"x": endpoint_x, "y": endpoint_y, "uuid": endpoint_uuids})

    # Group uuids according to x- and y-coordinates (i.e. compile uuids with a matching endpoint).
    endpoint_uuids_grouped = endpoint_df.groupby(["x", "y"])["uuid"].apply(list)

    # Compile the uuids to exclude from proximity analysis (i.e. connected segments to each source line segment).
    # Procedure: retrieve the grouped uuids associated with the endpoints for each line segment.
    df["exclude_uuids"] = df["geometry"].map(
        lambda geom: set(chain.from_iterable(itemgetter(*map(
            lambda pt: pt[:2], itemgetter(0, -1)(geom.coords)))(endpoint_uuids_grouped))))

    # Compile the range of indexes for all coordinates associated with each line segment.
    idx_ranges = dict.fromkeys(df.index.values)
    base = 0
    for index, count in df["geometry"].map(lambda geom: len(geom.coords)).iteritems():
        idx_ranges[index] = [base, base + count]
        base += count

    # Convert connected uuid lists to index range lists.
    df["exclude_ranges"] = df["exclude_uuids"].map(lambda uuids: itemgetter(*uuids)(idx_ranges))

    # Expand index ranges to full set of indexes.
    df["exclude_indexes"] = df["exclude_ranges"].map(
        lambda ranges: set(range(*ranges)) if type(ranges) == list else set(chain(*map(lambda r: range(*r), ranges))))

    # Join the remaining proximity indexes with excluded indexes.
    proxi = pd.DataFrame({"indexes": proxi_idx_keep, "exclude": df["exclude_indexes"]}, index=df.index.values)

    # Remove excluded indexes from proximity indexes.
    proxi_results = proxi.apply(lambda row: row[0] - row[1], axis=1)

    # Compile the uuid associated with every point from all line segments, in order.
    idx_all = pd.Series(np.concatenate([[uuid] * len(range(*indexes)) for uuid, indexes in idx_ranges.items()]))

    # Compile the uuid associated with resulting proximity point indexes for each line segment.
    proxi_results = proxi_results.map(lambda indexes: itemgetter(*indexes)(idx_all) if indexes else False)

    # Compile error properties.
    errors = list()

    for source_uuid, target_uuids in proxi_results[proxi_results != False].iteritems():
        errors.append("Feature uuid \"{}\" is too close to feature uuid(s) {}.".format(
            source_uuid,
            ", ".join(map("\"{}\"".format, [target_uuids] if isinstance(target_uuids, str) else target_uuids))))

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
        if all([fld in defaults_all[table] for fld in df.columns.difference(["uuid", "geometry"])]):
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
                logger.info("Validating nid linkage: {}.nid - {}.{}.".format(nid_table, id_table, col))

                if not ids.issubset(nids):

                    # Compile invalid ids.
                    flag_ids = list(ids - nids)

                    # Configure error message.
                    if len(flag_ids):
                        errors.append("The following values from {}.{} are not present in {}.nid:"
                                      "\n{}".format(id_table, col, nid_table, "\n".join(map(str, flag_ids))))

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
        errors.append("Feature uuid \"{}\" is too close to feature uuid(s) {}.".format(
            source_uuid,
            ", ".join(map("\"{}\"".format, [target_uuids] if isinstance(target_uuids, str) else target_uuids))))

    return {"errors": errors}


def validate_road_structures(roadseg, junction):
    """Validates the structid and structtype attributes of road segments."""

    errors = dict()
    defaults = defaults_all["roadseg"]

    # Validation 1: ensure dead end road segments have structtype = "None" or the default field value.

    # Compile dead end coordinates.
    deadend_coords = list(set(chain([geom.coords[0] for geom in
                                     junction[junction["junctype"] == "Dead End"]["geometry"].values])))

    # Compile road segments with potentially invalid structtype.
    roadseg_invalid = roadseg[~roadseg["structtype"].isin(["None", defaults["structtype"]])]

    # Compile truly invalid road segments.
    roadseg_invalid = roadseg_invalid[roadseg_invalid["geometry"].map(
        lambda geom: any([coords in deadend_coords for coords in itemgetter(0, -1)(geom.coords)]))]

    # Compile uuids of flagged records.
    errors[1] = roadseg_invalid.index.values

    # Validation 2: ensure structid is contiguous.
    errors[2] = list()

    # Compile structids.
    structids = roadseg["structid"].unique()

    # Remove default value.
    structids = structids[np.where(structids != defaults["structid"])]

    if len(structids):

        # Iterate structids.
        structid_count = len(structids)
        for index, structid in enumerate(sorted(structids)):

            logger.info("Validating structure {} of {}: \"{}\".".format(index + 1, structid_count, structid))

            # Subset dataframe to those records with current structid.
            structure = roadseg.iloc[list(np.where(roadseg["structid"] == structid)[0])]

            # Load structure as networkx graph.
            structure_graph = helpers.gdf_to_nx(structure, keep_attributes=False)

            # Validate contiguity (networkx connectivity).
            if not nx.is_connected(structure_graph):
                # Identify deadends (locations of discontiguity).
                deadends = [coords for coords, degree in structure_graph.degree() if degree == 1]
                deadends = "\n".join(["{}, {}".format(*deadend) for deadend in deadends])

                # Compile error properties.
                errors[2].append("Structure ID: \"{}\".\nEndpoints:\n{}.".format(structid, deadends))

    # Validation 3: ensure a single, non-default structid is applied to all contiguous road segments with the same
    #               structtype.
    # Validation 4: ensure road segments with different structtypes, excluding "None" and the default field value, are
    #               not contiguous.
    errors[3] = list()
    errors[4] = list()

    # Compile road segments with valid structtype.
    segments = roadseg[~roadseg["structtype"].isin(["None", defaults["structtype"]])]

    # Convert dataframe to networkx graph.
    # Drop all columns except uuid, structid, structtype, and geometry to reduce processing.
    segments.drop(segments.columns.difference(["uuid", "structid", "structtype", "geometry"]), axis=1, inplace=True)
    segments_graph = helpers.gdf_to_nx(segments, keep_attributes=True, endpoints_only=False)

    # Configure subgraphs.
    sub_g = nx.connected_component_subgraphs(segments_graph)

    # Iterate subgraphs and apply validations.
    for index, s in enumerate(sub_g):

        # Validation 3.
        structids = set(nx.get_edge_attributes(s, "structid").values())
        if len(structids) > 1 or defaults["structid"] in structids:

            # Compile error properties.
            uuids = list(set(nx.get_edge_attributes(s, "uuid").values()))
            errors[3].append("Structure: {}. Structure uuids: {}. Structure IDs: {}.".format(
                index, ", ".join(map("\"{}\"".format, uuids)), ", ".join(map("\"{}\"".format, structids))))

        # Validation 4.
        structtypes = set(nx.get_edge_attributes(s, "structtype").values())
        if len(structtypes) > 1:

            # Compile error properties.
            uuids = list(set(nx.get_edge_attributes(s, "uuid").values()))
            errors[4].append("Structure: {}. Structure uuids: {}. Structure types: {}.".format(
                index, ", ".join(map("\"{}\"".format, uuids)), ", ".join(map("\"{}\"".format, structtypes))))

    return {"errors": errors}


def validate_roadclass_rtnumber1(df):
    """Applies a set of validations to roadclass and rtnumber1 fields."""

    # Apply validations and compile uuids of flagged records.

    # Validation: ensure rtnumber1 is not the default value when roadclass == "Freeway" or "Expressway / Highway".
    errors = df[df["roadclass"].isin(["Freeway", "Expressway / Highway"]) &
                df["rtnumber1"].map(lambda rtnumber1: rtnumber1 == defaults_all["roadseg"]["rtnumber1"])].index.values

    return {"errors": errors}


# TODO: use groupby to optimize the iteration of road elements. The preceding bits of this function have been optimized.
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
        flag_intersect = segments_single["geometry"].map(lambda g: g.coords[0] in valid_coords)
        flag_nids.extend(segments_single[flag_intersect]["nid"].values)

    # Multi-segment road elements:

    # Compile multi-segment road elements (via non-unique nids).
    # Filter to nids with invalid roadclass (intended to reduce spatial processing).
    segments_multi = df[(df["nid"].duplicated(keep=False)) & (~df["roadclass"].isin(valid)) & (df["nid"] != default)]

    if not segments_multi.empty:

        logger.info("Validating multi-segment road elements.")

        # Compile nids of road segments with coords in the validation coords list.
        nids = segments_multi[segments_multi["geometry"].map(
            lambda g: len(set(itemgetter(0, -1)(g.coords)).intersection(valid_coords)) > 0)]["nid"].unique()

        # Iterate flagged elements to identify self-intersections.
        nid_count = len(nids)
        for index, nid in enumerate(nids):

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


# TODO: exclude none names
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
