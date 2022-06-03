import datetime
import geopandas as gpd
import logging
import numpy as np
import pandas as pd
import sqlite3
import string
import sys
import time
import uuid
import yaml
from itertools import chain
from operator import attrgetter, itemgetter
from osgeo import ogr, osr
from pathlib import Path
from shapely.geometry import LineString, Point
from tqdm import tqdm
from typing import Any, Dict, List, Tuple, Union


# Set logger.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)


# Enable ogr exceptions.
ogr.UseExceptions()


class Timer:
    """Tracks stage runtime."""

    def __init__(self) -> None:
        """Initializes the Timer class."""

        self.start_time = None

    def __enter__(self) -> None:
        """Starts the timer."""

        logger.info("Started.")
        self.start_time = time.time()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """
        Computes and returns the elapsed time.

        :param Any exc_type: required parameter for __exit__.
        :param Any exc_val: required parameter for __exit__.
        :param Any exc_tb: required parameter for __exit__.
        """

        total_seconds = time.time() - self.start_time
        delta = datetime.timedelta(seconds=total_seconds)
        logger.info(f"Finished. Time elapsed: {delta}.")


def explode_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Explodes MultiLineStrings to LineStrings.

    :param gpd.GeoDataFrame gdf: GeoDataFrame.
    :return gpd.GeoDataFrame: updated GeoDataFrame.
    """

    # Explode.
    if "MultiLineString" in set(gdf.geom_type):

        # Separate multi- and single-type records.
        multi = gdf.loc[gdf.geom_type == "MultiLineString"]
        single = gdf.loc[~gdf.index.isin(multi.index)]

        # Explode multi-type geometries.
        multi_exploded = multi.explode().reset_index(drop=True)

        # Merge all records.
        merged = gpd.GeoDataFrame(pd.concat([single, multi_exploded], ignore_index=True), crs=gdf.crs)

        logger.warning(f"Exploded {len(multi)} MultiLineString to {len(multi_exploded)} LineString geometries.")

        return merged.copy(deep=True)

    else:
        return gdf.copy(deep=True)


def export(df: gpd.GeoDataFrame, dst: Path, name: str) -> None:
    """
    Exports a GeoDataFrame to a GeoPackage.

    :param gpd.GeoDataFrame df: GeoDataFrame containing LineStrings.
    :param Path dst: output GeoPackage path.
    :param str name: output GeoPackage layer name.
    """

    try:

        # Open GeoPackage.
        driver = ogr.GetDriverByName("GPKG")
        gpkg = driver.Open(str(dst), update=1)

        # Configure spatial reference system.
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(df.crs.to_epsg())

        # Create GeoPackage layer.
        geom_type = attrgetter(f"wkb{df.geom_type.iloc[0]}")(ogr)
        layer = gpkg.CreateLayer(name=name, srs=srs, geom_type=geom_type, options=["OVERWRITE=YES"])

        # Convert float fields to int.
        for col in df.columns:
            if df[col].dtype.kind == "f":
                df.loc[df[col].isna(), col] = -1
                df[col] = df[col].astype(int)

        # Set field definitions.
        ogr_field_map = {"b": ogr.OFTInteger, "i": ogr.OFTInteger, "O": ogr.OFTString}
        for field_name, field_dtype in df.dtypes.to_dict().items():
            if field_name != "geometry":
                field_defn = ogr.FieldDefn(field_name, ogr_field_map[field_dtype.kind])
                if field_dtype.kind == "b":
                    field_defn.SetSubType(ogr.OFSTBoolean)
                layer.CreateField(field_defn)

        # Write layer.
        layer.StartTransaction()

        for feat in tqdm(df.itertuples(index=False), total=len(df),
                         desc=f"Writing to file: {gpkg.GetName()}|layer={name}",
                         bar_format="{desc}: |{bar}| {percentage:3.0f}% {r_bar}"):

            # Instantiate feature.
            feature = ogr.Feature(layer.GetLayerDefn())

            # Compile feature properties.
            properties = feat._asdict()

            # Set feature geometry.
            geom = ogr.CreateGeometryFromWkb(properties.pop("geometry").wkb)
            feature.SetGeometry(geom)

            # Iterate and set feature properties (attributes).
            for field_index, prop in enumerate(properties.items()):
                feature.SetField(field_index, prop[-1])

            # Create feature.
            layer.CreateFeature(feature)

            # Clear pointer for next iteration.
            feature = None

        layer.CommitTransaction()

    except (KeyError, ValueError, sqlite3.Error) as e:
        logger.exception(f"Error raised when writing output: {dst}|layer={name}.")
        logger.exception(e)
        sys.exit(1)


def groupby_to_list(df: Union[gpd.GeoDataFrame, pd.DataFrame], group_field: Union[List[str], str], list_field: str) -> \
        pd.Series:
    """
    Faster alternative to :func:`~pd.groupby.apply/agg(list)`.
    Groups records by one or more fields and compiles an output field into a list for each group.

    :param Union[gpd.GeoDataFrame, pd.DataFrame] df: (Geo)DataFrame.
    :param Union[List[str], str] group_field: field or list of fields by which the (Geo)DataFrame records will be
        grouped.
    :param str list_field: (Geo)DataFrame field to output, based on the record groupings.
    :return pd.Series: Series of grouped values.
    """

    if isinstance(group_field, list):
        for field in group_field:
            if df[field].dtype.name != "geometry":
                df[field] = df[field].astype("U")
        transpose = df.sort_values(group_field)[[*group_field, list_field]].values.T
        keys, vals = np.column_stack(transpose[:-1]), transpose[-1]
        keys_unique, keys_indexes = np.unique(keys.astype("U") if isinstance(keys, np.object) else keys,
                                              axis=0, return_index=True)

    else:
        keys, vals = df.sort_values(group_field)[[group_field, list_field]].values.T
        keys_unique, keys_indexes = np.unique(keys, return_index=True)

    vals_arrays = np.split(vals, keys_indexes[1:])

    return pd.Series([list(vals_array) for vals_array in vals_arrays], index=keys_unique).copy(deep=True)


def load_yaml(path: Union[Path, str]) -> Any:
    """
    Loads the content of a YAML file as a Python object.

    :param Union[Path, str] path: path to the YAML file.
    :return Any: Python object consisting of the YAML content.
    """

    path = Path(path).resolve()

    with open(path, "r", encoding="utf8") as f:

        try:

            return yaml.safe_load(f)

        except (ValueError, yaml.YAMLError):
            logger.exception(f"Unable to load yaml: {path}.")


def snap_nodes(df: gpd.GeoDataFrame, prox: float = 0.1, prox_boundary: float = 0.01) -> Tuple[gpd.GeoDataFrame, bool]:
    """
    Snaps NGD arcs to NRN arcs (node-to-node) if they are <= the snapping proximity threshold.

    :param gpd.GeoDataFrame df: GeoDataFrame containing both NRN and NGD arcs.
    :param float prox: max snapping distance (same unit as GeoDataFrame CRS), default=0.1.
    :param float prox_boundary: max snapping distance (same unit as GeoDataFrame CRS) for boundary arcs, default=0.01.
    :return Tuple[gpd.GeoDataFrame, bool]: updated GeoDataFrame and flag indicating if records have been modified.
    """

    logger.info(f"Snapping to NRN nodes.")

    # Compile nodes.
    nrn_flag = (df["segment_id_orig"].map(str) != "None") & (df["segment_type"] != 2)
    nrn_nodes = set(df.loc[nrn_flag, "geometry"].map(
        lambda g: tuple(set(itemgetter(0, -1)(attrgetter("coords")(g))))).explode())
    ngd_nodes = df.loc[(~nrn_flag) & (df["segment_type"] != 2), "geometry"].map(
        lambda g: tuple(set(itemgetter(0, -1)(attrgetter("coords")(g))))).explode()
    ngd_boundary_nodes = set(df.loc[(~nrn_flag) & (df["segment_type"] != 2) & (df["boundary"] == 1), "geometry"].map(
        lambda g: tuple(set(itemgetter(0, -1)(attrgetter("coords")(g))))).explode())

    # Compile snappable ngd nodes (ngd nodes not connected to an nrn node).
    snap_nodes = ngd_nodes.loc[~ngd_nodes.isin(nrn_nodes)].copy(deep=True)
    if len(snap_nodes):

        # Compile nrn nodes as Points.
        nrn_nodes = gpd.GeoSeries(map(Point, set(nrn_nodes)), crs=df.crs)

        # Generate simplified ngd node buffers using distance tolerance.
        snap_node_buffers = snap_nodes.map(
            lambda pt: Point(pt).buffer({True: prox_boundary, False: prox}[pt in ngd_boundary_nodes], resolution=5))

        # Query nrn node which intersect each ngd node buffer.
        # Construct DataFrame containing results.
        snap_features = pd.DataFrame({
            "from_node": snap_nodes,
            "to_node": snap_node_buffers.map(lambda buffer: set(nrn_nodes.sindex.query(buffer, predicate="intersects")))
        })

        # Filter snappable nodes to those intersecting >= 1 nrn node.
        snap_nodes = snap_features.loc[snap_features["to_node"].map(len) >= 1].copy(deep=True)
        if len(snap_nodes):

            # Create idx-node lookup for target nodes.
            to_node_idxs = set(chain.from_iterable(snap_nodes["to_node"]))
            to_nodes = nrn_nodes.loc[nrn_nodes.index.isin(to_node_idxs)]
            to_node_lookup = dict(zip(to_nodes.index, to_nodes.map(lambda pt: itemgetter(0)(attrgetter("coords")(pt)))))

            # Replace target node indexes with actual nodes tuple of first result in each instance.
            snap_nodes["to_node"] = snap_nodes["to_node"].map(lambda idxs: itemgetter(tuple(idxs)[0])(to_node_lookup))

            # Create node snapping lookup dictionary and update required arcs.
            snap_nodes_lookup = dict(zip(snap_nodes["from_node"], snap_nodes["to_node"]))
            snap_arc_ids = set(snap_nodes.index)
            df.loc[df.index.isin(snap_arc_ids), "geometry"] = df.loc[df.index.isin(snap_arc_ids), "geometry"].map(
                lambda g: update_nodes(g, node_map=snap_nodes_lookup))

            logger.info(f"Snapped {len(snap_nodes)} non-NRN nodes to NRN nodes based on proximity={prox}.")

    return df.copy(deep=True), bool(len(snap_nodes))


def standardize(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Applies a series of geometry and attribute standardizations and rules:
    1) ensures geometries are LineString;
    2) enforces domain restrictions and dtypes;
    3) enforces attribute rules:
        i) bo_new = 1 must result in segment_type = 3.
        ii) completely new bos must have both bo_new = 1 and segment_type = 3.
        iii) NRN records must not have modified values for bo_new, boundary, and segment_type.

    :param gpd.GeoDataFrame gdf: GeoDataFrame.
    :return gpd.GeoDataFrame: updated GeoDataFrame.
    """

    logger.info("Standardizing data.")

    identifier = "segment_id"
    nrn_identifier = "segment_id_orig"

    try:

        # Standardization - geometry type.
        flag_geom = ~gdf.geom_type.isin({"LineString", "MultiLineString"})
        if sum(flag_geom):
            gdf = gdf.loc[~flag_geom].copy(deep=True)

            logger.warning(f"Dropped {sum(flag_geom)} non-(Multi)LineString geometries.")

        # Standardization - multi-type geometries.
        gdf = explode_geometry(gdf)

        # Standardization - domains and dtypes.
        specs = {
            "bo_new": {"domain": {"0": 0, "0.0": 0, "1": 1, "1.0": 1}, "default": 0, "dtype": int},
            "boundary": {"domain": {"0": 0, "0.0": 0, "1": 1, "1.0": 1}, "default": 0, "dtype": int},
            "ngd_uid": {"domain": None, "default": -1, "dtype": int},
            "segment_id_orig": {"domain": None, "default": -1, "dtype": str},
            "segment_type": {"domain": {"1": 1, "1.0": 1, "2": 2, "2.0": 2, "3": 3, "3.0": 3},
                             "default": 1, "dtype": int},
            "structure_type": {"domain": {
                "-1": "Unknown", "-1.0": "Unknown", "Unknown": "Unknown",
                "0": "None", "0.0": "None", "None": "None",
                "1": "Bridge", "1.0": "Bridge", "Bridge": "Bridge",
                "2": "Bridge covered", "2.0": "Bridge covered", "Bridge covered": "Bridge covered",
                "3": "Bridge moveable", "3.0": "Bridge moveable", "Bridge moveable": "Bridge moveable",
                "4": "Bridge unknown", "4.0": "Bridge unknown", "Bridge unknown": "Bridge unknown",
                "5": "Tunnel", "5.0": "Tunnel", "Tunnel": "Tunnel",
                "6": "Snowshed", "6.0": "Snowshed", "Snowshed": "Snowshed",
                "7": "Dam", "7.0": "Dam", "Dam": "Dam"
            }, "default": "Unknown", "dtype": str}
        }

        for col, params in specs.items():

            # Copy original series as object dtype.
            s_orig = gdf[col].copy(deep=True).astype(object)

            # Set Nulls to default value.
            flag_null = gdf[col].isna()
            gdf.loc[flag_null, col] = params["default"]

            # Set invalid values (not within domain) to default value.
            if params["domain"]:
                flag_domain = ~gdf[col].astype(str).isin(params["domain"])
                gdf.loc[flag_domain, col] = params["default"]

            # Map values via domain.
            if params["domain"]:
                flag_cast = ~gdf[col].isin(params["domain"].values())
                gdf.loc[flag_cast, col] = gdf.loc[flag_cast, col].astype(str).map(params["domain"])

            # Cast dtype.
            def _cast_dtype(val):
                try:
                    return params["dtype"](val)
                except ValueError:
                    return params["default"]

            gdf[col] = gdf[col].map(_cast_dtype)

            # Log results.
            flag_invalid = pd.Series(s_orig.map(str) != gdf[col].map(str))
            if sum(flag_invalid):
                logger.warning(f"Standardized domain and dtype for {sum(flag_invalid)} records for \"{col}\".")

        # Standardization - identifier.

        # Flag invalid identifiers.
        hexdigits = set(string.hexdigits)
        flag_len = gdf[identifier].map(len) != 32
        flag_non_hex = gdf[identifier].map(lambda val: not set(val).issubset(hexdigits))
        flag_dups = gdf[identifier].duplicated(keep=False)
        flag_invalid = flag_len | flag_non_hex | flag_dups

        # Resolve invalid identifiers and assign attribute as index.
        if sum(flag_invalid):
            gdf.loc[flag_invalid, identifier] = [uuid.uuid4().hex for _ in range(sum(flag_invalid))]
            gdf.index = gdf[identifier]

            logger.warning(f"Resolved {sum(flag_invalid)} invalid identifiers for \"segment_id\".")

        # Rules - New BOs (must be done prior to validating NRN record integrity.

        # Converted ngd road.
        flag_invalid = (gdf["bo_new"] == 1) & (gdf["segment_type"] != 3)
        if sum(flag_invalid):
            gdf.loc[flag_invalid, "segment_type"] = 3

            logger.warning(f"Set \"segment_type\" = 3 for {sum(flag_invalid)} NGD roads converted to BOs.")

        # Completely new bo.
        flag_invalid = (gdf["ngd_uid"] == -1) & (gdf["bo_new"] != 1) & (gdf["segment_type"] == 3)
        if sum(flag_invalid):
            gdf.loc[flag_invalid, "bo_new"] = 1

            logger.warning(f"Set \"bo_new\" = 1 for {sum(flag_invalid)} completely new BOs.")

        # Rules - NRN record integrity.

        # Standardize NRN identifier.
        flag_invalid = gdf[nrn_identifier].astype(str).map(len) != 32
        if sum(flag_invalid):
            gdf.loc[flag_invalid, nrn_identifier] = -1

            logger.warning(f"Resolved {sum(flag_invalid)} invalid NRN identifiers for \"segment_id_orig\".")

        # Revert modified attributes.
        for col, domain in {"bo_new": {0}, "boundary": {0}, "segment_type": {1, 2}}.items():
            flag_invalid = (gdf[nrn_identifier].astype(str).map(len) == 32) & (~gdf[col].isin(domain))
            if sum(flag_invalid):
                gdf.loc[flag_invalid, col] = specs[col]["default"]

                logger.warning(f"Reverted {sum(flag_invalid)} NRN record values for \"{col}\".")

        logger.info("Finished standardizing data.")

        return gdf.copy(deep=True)

    except (TypeError, ValueError) as e:
        logger.exception(e)
        logger.exception(f"Unable to complete dataset standardizations.")
        sys.exit(1)


def update_nodes(g: LineString, node_map: Dict[tuple, tuple]) -> LineString:
    """
    Updates one or both nodes in the LineString.

    :param LineString g: LineString to be updated.
    :param Dict[tuple, tuple] node_map: mapping of from and to nodes.
    :return LineString: updated LineString.
    """

    # Compile coordinates.
    coords = list(attrgetter("coords")(g))

    # Conditionally update nodes.
    for idx in (0, -1):
        try:
            coords[idx] = itemgetter(coords[idx])(node_map)
        except KeyError:
            pass

    return LineString(coords)
