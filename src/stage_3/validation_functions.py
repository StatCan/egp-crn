import logging
import networkx as nx
import numpy as np
import os
import pandas as pd
import sys
from itertools import chain
from operator import itemgetter
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


def identify_isolated_lines(df):
    """Identifies the uuids of isolated line segments."""

    # Convert dataframe to networkx graph.
    # Drop all columns except uuid and geometry to reduce processing.
    df.reset_index(drop=False, inplace=True)
    df.drop(df.columns.difference(["uuid", "geometry"]), axis=1, inplace=True)
    g = helpers.gdf_to_nx(df, keep_attributes=True, endpoints_only=False)

    # Configure subgraphs.
    sub_g = nx.connected_component_subgraphs(g)

    # Compile uuids unique to a subgraph.
    flag_uuids = list()
    for s in sub_g:
        if len(set(nx.get_edge_attributes(s, "uuid").values())) == 1:
            uuid = list(nx.get_edge_attributes(s, "uuid").values())[0]
            flag_uuids.append(uuid)

    # Compile flagged records as errors.
    errors = pd.Series(df.index.isin(flag_uuids), index=df.index)

    return errors


def validate_ferry_road_connectivity(ferryseg, roadseg, junction):
    """Validates the connectivity between ferry and road line segments."""

    # Validation 1: ensure ferry segments connect to a road segment at at least one endpoint.

    # Compile junction coordinates where junctype = "Ferry".
    ferry_junctions = list(set(chain([geom[0].coords[0] for geom in
                                      junction[junction["junctype"] == "Ferry"]["geometry"].values])))

    # Identify ferry segments which do not connect to any road segments.
    mask = ferryseg["geometry"].map(
        lambda geom: not any([coords in ferry_junctions for coords in itemgetter(0, -1)(geom.coords)]))

    # Compile uuids of flagged records.
    flag_uuids = ferryseg[mask].index.values
    errors = pd.Series(ferryseg.index.isin(flag_uuids), index=ferryseg.index).astype("int")

    # Validation 2: ensure ferry segments connect to <= 1 road segment at either endpoint.

    # Compile road segments which connect to ferry segments.
    roads_connected = roadseg[roadseg["geometry"].map(
        lambda geom: any([coords in ferry_junctions for coords in itemgetter(0, -1)(geom.coords)]))]

    # Identify ferry endpoints which intersect multiple road segments.
    ferry_multi_intersect = ferryseg["geometry"]\
        .map(lambda ferry: [roads_connected["geometry"]
             .map(lambda road: any([road_coords == ferry.coords[i] for road_coords in itemgetter(0, -1)(road.coords)]))
             .sum() > 1 for i in (0, -1)])

    # Compile uuids of flagged records.
    flag_uuids = ferryseg[ferry_multi_intersect].index.values
    errors[pd.Series(ferryseg.index.isin(flag_uuids), index=ferryseg.index)] = 2

    return errors


def validate_min_length(df):
    """Validates the minimum feature length of line geometries."""

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


def validate_road_structures(roadseg, junction, default):
    """
    Validates the structid and structtype attributes of road segments.
    Parameter default should be a dictionary with a key for each of structid and structtype for roadseg.
    """

    # Validation 1: ensure dead end road segments have structtype = "None" or the default field value.

    # Compile dead end coordinates.
    deadend_coords = list(set(chain([geom[0].coords[0] for geom in
                                     junction[junction["junctype"] == "Dead End"]["geometry"].values])))

    # Compile road segments with potentially invalid structtype.
    roadseg_invalid = roadseg[~roadseg["structtype"].isin(["None", default["structtype"]])]

    # Compile truly invalid road segments.
    roadseg_invalid = roadseg_invalid[roadseg_invalid["geometry"].map(
        lambda geom: any([coords in deadend_coords for coords in itemgetter(0, -1)(geom.coords)]))]

    # Compile uuids of flagged records.
    flag_uuids = roadseg_invalid.index.values
    errors = pd.Series(roadseg.index.isin(flag_uuids), index=roadseg.index).astype("int")

    # Validation 2: ensure structid is contiguous.
    errors_2 = list()

    # Compile structids.
    structids = roadseg["structid"].unique()

    # Remove default value.
    structids = structids[np.where(structids != default["structid"])]

    if len(structids):

        # Iterate structids.
        for structid in structids:

            logger.info("Validating structure: \"{}\".".format(structid))

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
                errors_2.append("Structure ID: \"{}\".\nEndpoints:\n{}.".format(structid, deadends))

    # Validation 3: ensure structid is applied to all contiguous road segments with the same structtype.
    errors_3 = list()

    # Compile road segments with valid structtype.
    segments = roadseg[~roadseg["structtype"].isin(["None", default["structtype"]])]

    # Convert dataframe to networkx graph.
    # Drop all columns except uuid, structid, and geometry to reduce processing.
    segments.reset_index(drop=False, inplace=True)
    segments.drop(segments.columns.difference(["uuid", "structid", "geometry"]), axis=1, inplace=True)
    segments_graph = helpers.gdf_to_nx(segments, keep_attributes=True, endpoints_only=False)

    # Configure subgraphs.
    sub_g = nx.connected_component_subgraphs(segments_graph)

    # Compile uuids of subgraph if multiple or invalid (default) structids are present.
    for index, s in enumerate(sub_g):
        structids = set(nx.get_edge_attributes(s, "structid").values())
        if len(structids) > 1 or default["structid"] in structids:

            # Compile error properties.
            uuids = list(set(nx.get_edge_attributes(s, "uuid").values()))
            errors_3.append("Structure {}. Structure uuids: {}. Structure IDs: {}."
                            .format(index, ", ".join(map("\"{}\"", uuids)), ", ".join(map("\"{}\"", structids))))

    return errors, errors_2, errors_3
