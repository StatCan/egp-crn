import fiona
import geopandas as gpd
import logging
import networkx as nx
import numpy as np
import os
import pandas as pd
import sys
from itertools import chain, permutations
from operator import itemgetter
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

    # Filter geometries to those with duplicate lengths.
    df_same_len = df[df["geometry"].length.duplicated(keep=False)]

    # Identify duplicate geometries.
    mask = df_same_len["geometry"].map(lambda geom1: df_same_len["geometry"].map(lambda geom2:
                                                                                 geom1.equals(geom2)).sum() > 1)

    # Compile uuids of flagged records.
    errors = df_same_len[mask].index.values

    return {"errors": errors, "modifications": None}


def identify_duplicate_points(df):
    """Identifies the uuids of duplicate point geometries."""

    # Retrieve coordinates as tuples.
    coords = df["geometry"].map(lambda geom: geom.coords[0])

    # Identify duplicate geometries.
    mask = coords.duplicated(keep=False)

    # Compile uuids of flagged records.
    errors = df[mask].index.values

    return {"errors": errors, "modifications": None}


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

    return {"errors": errors, "modifications": None}


def validate_deadend_disjoint_proximity(junction, roadseg):
    """Validates the proximity of deadend junctions to disjoint / non-connected road segments."""

    # Validation: deadend junctions must be >= 5 meters from disjoint road segments.

    # Filter junctions to junctype = "Dead End".
    deadends = junction[junction["junctype"] == "Dead End"]

    # Transform records to a meter-based crs: EPSG:3348.
    deadends = helpers.reproject_gdf(deadends, 4617, 3348)
    roadseg = helpers.reproject_gdf(roadseg, 4617, 3348)

    # Generate kdtree.
    tree = cKDTree(np.concatenate([np.array(geom.coords) for geom in roadseg["geometry"]]))

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

    return {"errors": errors, "modifications": None}


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

    # Identify ferry endpoints which intersect multiple road segments.
    ferry_multi_intersect = ferryseg["geometry"].map(
        lambda ferry: any([roads_connected["geometry"].map(
            lambda road: any([road_coords == ferry.coords[i] for road_coords in itemgetter(0, -1)(road.coords)]))
                          .sum() > 1 for i in (0, -1)]))

    # Compile uuids of flagged records.
    errors[2] = ferryseg[ferry_multi_intersect].index.values

    return {"errors": errors, "modifications": None}


def validate_line_endpoint_clustering(df):
    """Validates the quantity of points clustered near the endpoints of line segments."""

    # Validation: ensure line segments have <= 3 points within 83 meters of either endpoint, inclusively.

    # Transform records to a meter-based crs: EPSG:3348.
    df = helpers.reproject_gdf(df, 4617, 3348)

    # Filter out records with <= 3 points or length < 83 meters.
    df_subset = df[~df["geometry"].map(lambda geom: len(geom.coords) <= 3 or geom.length < 83)]

    # Identify invalid records.
    # Process: either of the following must be true:
    # a) The distance of the 4th point along the linestring is < 83 meters.
    # b) The total linestring length minus the distance of the 4th-last point along the linestring is < 83 meters.
    flags = np.vectorize(lambda geom: (geom.project(Point(geom.coords[3])) < 83) or
                                      ((geom.length - geom.project(Point(geom.coords[-4]))) < 83)
                         )(df_subset["geometry"])

    # Compile uuids of flagged records.
    errors = df_subset[flags].index.values

    return {"errors": errors, "modifications": None}


def validate_line_length(df):
    """Validates the minimum feature length of line geometries."""

    # Filter records to 0.0002 degrees length (approximately 22.2 meters).
    # Purely intended to reduce processing.
    df_sub = df[df.length <= 0.0002]

    # Transform records to a meter-based crs: EPSG:3348.
    df_sub = helpers.reproject_gdf(df_sub, 4617, 3348)

    # Validation: ensure line segments are >= 2 meters in length.
    errors = df_sub[df_sub.length < 2].index.values

    return {"errors": errors, "modifications": None}


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
    # Disregard z-values.
    pts_x, pts_y, pts_z = np.concatenate([np.array(geom.coords) for geom in df["geometry"]]).T

    # Join the uuids, x-, and y-coordinates.
    pts_df = pd.DataFrame({"x": pts_x, "y": pts_y, "uuid": pts_uuid})

    # Filter records to only duplicated points.
    pts_df = pts_df[pts_df.duplicated(["x", "y"], keep=False)]

    # Group uuids according to x- and y-coordinates.
    uuids_grouped = pts_df.groupby(["x", "y"])["uuid"].apply(list)

    # Exit function if no shared points exists (b/c therefore no line merges exist).
    if not len(uuids_grouped):

        return list()

    else:

        # Retrieve the next point, relative to the target point, for each grouped uuid associated with each point.

        # Compile the endpoints and next-to-endpoint points for each uuid.
        pts_uuid = dict.fromkeys(df.index.values)
        for uuid, geom in df["geometry"].iteritems():
            pts_uuid[uuid] = list(map(lambda coord: coord[:2], itemgetter(0, 1, -2, -1)(geom.coords)))

        # Retrieve the next point for each grouped uuid associated with each point.
        pts_grouped = pd.Series(np.vectorize(lambda uuids, index: map(
            lambda uuid: pts_uuid[uuid][1] if pts_uuid[uuid][0] == index else pts_uuid[uuid][-2], uuids))(
            uuids_grouped, uuids_grouped.index))\
            .map(lambda vals: list(vals))

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

        # Compile the original crs coordinates of all flagged intersections.

        # Filter flagged intersection points (stored as index).
        flagged_pts = pts_grouped[flags].index.values

        # Revert to original crs: EPSG:4617.
        flagged_pts = gpd.GeoDataFrame(geometry=gpd.GeoSeries(map(Point, flagged_pts)))
        flagged_pts.crs = dict()
        flagged_pts = helpers.reproject_gdf(flagged_pts, 3348, 4617)

        # Compile resulting points as errors.
        errors = list(map(lambda pt: pt.coords[0][:2], flagged_pts["geometry"]))

        return {"errors": errors, "modifications": None}


def validate_line_proximity(df):
    """Validates the proximity of line segments."""

    # Validation: ensure line segments are >= 3 meters from each other, excluding connected segments.

    # Transform records to a meter-based crs: EPSG:3348.
    df = helpers.reproject_gdf(df, 4617, 3348)

    # Generate kdtree.
    tree = cKDTree(np.concatenate([np.array(geom.coords) for geom in df["geometry"]]))

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
    # Disregard z-values.
    endpoint_x, endpoint_y, endpoint_z = np.concatenate([itemgetter(0, -1)(geom.coords) for geom in df["geometry"]]).T

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

    return {"errors": errors, "modifications": None}


def validate_point_proximity(df):
    """Validates the proximity of points."""

    # Validation: ensure points are >= 3 meters from each other.

    # Transform records to a meter-based crs: EPSG:3348.
    df = helpers.reproject_gdf(df, 4617, 3348)

    # Generate kdtree.
    tree = cKDTree(np.concatenate([np.array(geom.coords) for geom in df["geometry"]]))

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

    return {"errors": errors, "modifications": None}


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

    return {"errors": errors, "modifications": None}
