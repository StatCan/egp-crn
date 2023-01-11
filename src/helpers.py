import datetime
import fiona
import geopandas as gpd
import logging
import pandas as pd
import sqlite3
import string
import sys
import time
import uuid
import yaml
from itertools import chain, groupby
from operator import attrgetter, itemgetter
from osgeo import ogr, osr
from pathlib import Path
from shapely.geometry import LineString, Point
from tqdm import tqdm
from typing import Any, Dict, Union


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

        \b
        :param Any exc_type: required parameter for __exit__.
        :param Any exc_val: required parameter for __exit__.
        :param Any exc_tb: required parameter for __exit__.
        """

        total_seconds = time.time() - self.start_time
        delta = datetime.timedelta(seconds=total_seconds)
        logger.info(f"Finished. Time elapsed: {delta}.")


def create_gpkg(path: Union[Path, str]) -> None:
    """
    Creates a GeoPackage.

    \b
    :param Union[Path, str] path: A valid path with .gpkg extension.
    """

    # Resolve inputs.
    path = str(path)

    logger.info(f"Creating GeoPackage: {path}.")

    # Create GeoPackage.
    driver = ogr.GetDriverByName("GPKG")
    driver.CreateDataSource(path)

    del driver


def delete_layers(dst: Union[Path, str], layers: Union[list[str, ...], str]) -> None:
    """
    Deletes one or more layers from a GeoPackage.

    \b
    :param Union[Path, str] dst: An existing GeoPackage.
    :param Union[list[str, ...], str] layers: layer(s) to be deleted.
    """

    # Resolve inputs.
    if isinstance(layers, str):
        layers = [layers]
    dst = str(dst)

    logger.info(f"Deleting layer(s): {', '.join(layers)} from \"{dst}\".")

    # Open Geopackage.
    driver = ogr.GetDriverByName("GPKG")
    gpkg = driver.Open(dst, update=1)

    # Delete layer(s).
    for layer in set(layers).intersection(set(fiona.listlayers(dst))):
        gpkg.DeleteLayer(layer)

    del driver, gpkg


def explode_geometry(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Explodes MultiLineStrings to LineStrings.

    \b
    :param gpd.GeoDataFrame df: GeoDataFrame.
    :return gpd.GeoDataFrame: updated GeoDataFrame.
    """

    # Explode.
    if "MultiLineString" in set(df.geom_type):

        # Separate multi- and single-type records.
        multi = df.loc[df.geom_type == "MultiLineString"]
        single = df.loc[~df.index.isin(multi.index)]

        # Explode multi-type geometries.
        multi_exploded = multi.explode().reset_index(drop=True)

        # Merge all records.
        merged = gpd.GeoDataFrame(pd.concat([single, multi_exploded], ignore_index=True), crs=df.crs)

        logger.warning(f"Exploded {len(multi)} MultiLineString to {len(multi_exploded)} LineString geometries.")

        return merged.copy(deep=True)

    else:
        return df.copy(deep=True)


def export(df: gpd.GeoDataFrame, dst: Path, name: str) -> None:
    """
    Exports a GeoDataFrame to a GeoPackage.

    \b
    :param gpd.GeoDataFrame df: GeoDataFrame.
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


def load_yaml(path: Union[Path, str]) -> Any:
    """
    Loads the content of a YAML file as a Python object.

    \b
    :param Union[Path, str] path: path to the YAML file.
    :return Any: Python object consisting of the YAML content.
    """

    path = Path(path).resolve()

    with open(path, "r", encoding="utf8") as f:

        try:

            return yaml.safe_load(f)

        except (ValueError, yaml.YAMLError):
            logger.exception(f"Unable to load yaml: {path}.")


def round_coordinates(df: gpd.GeoDataFrame, precision: int = 5) -> gpd.GeoDataFrame:
    """
    Rounds the LineString coordinates to a specified decimal precision.
    Only the first 2 values (x, y) are kept for each coordinate, effectively flattening the geometry to 2-dimensions.
    Duplicated adjacent vertices are removed.

    \b
    :param gpd.GeoDataFrame df: GeoDataFrame of LineStrings.
    :param int precision: decimal precision to round coordinates to.
    :return gpd.GeoDataFrame: GeoDataFrame with modified decimal precision.
    """

    logger.info(f"Rounding coordinates to decimal precision: {precision}.")

    try:

        # Ensure valid geometry types.
        if len(set(df.geom_type) - {"LineString"}):
            raise TypeError("Non-LineString geometries detected for GeoDataFrame.")

        # Round coordinates.
        coords = df["geometry"].map(lambda g: map(
            lambda pt: (round(itemgetter(0)(pt), precision), round(itemgetter(1)(pt), precision)),
            attrgetter("coords")(g))).map(tuple)

        # Remove duplicated adjacent vertices.
        flag = coords.map(set).map(len) >= 2
        coords.loc[flag] = coords.loc[flag].map(lambda g: tuple(map(itemgetter(0), groupby(g))))

        df["geometry"] = coords.map(LineString)

        return df.copy(deep=True)

    except (TypeError, ValueError) as e:
        logger.exception(e)
        logger.exception("Unable to round coordinates for GeoDataFrame.")
        sys.exit(1)


def snap_nodes(df: gpd.GeoDataFrame, prox: float = 0.1, prox_boundary: float = 0.01) -> gpd.GeoDataFrame:
    """
    Snaps NGD arcs to NRN arcs (node-to-node) if they are <= the snapping proximity threshold.

    \b
    :param gpd.GeoDataFrame df: GeoDataFrame containing both NRN and NGD arcs.
    :param float prox: max snapping distance (same unit as GeoDataFrame CRS), default=0.1.
    :param float prox_boundary: max snapping distance (same unit as GeoDataFrame CRS) for boundary arcs, default=0.01.
    :return gpd.GeoDataFrame: updated GeoDataFrame.
    """

    logger.info(f"Snapping to NRN nodes.")

    # Compile nodes.
    nrn_flag = (df["segment_id_orig"].map(len) == 32) & (df["segment_type"] == 1)
    nrn_nodes = set(df.loc[nrn_flag, "geometry"].map(
        lambda g: set(itemgetter(0, -1)(attrgetter("coords")(g)))).explode())
    ngd_nodes = df.loc[~nrn_flag, "geometry"].map(
        lambda g: set(itemgetter(0, -1)(attrgetter("coords")(g)))).explode()
    ngd_boundary_nodes = set(df.loc[(~nrn_flag) & (df["boundary"] == 1), "geometry"].map(
        lambda g: set(itemgetter(0, -1)(attrgetter("coords")(g)))).explode())

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

    return df.copy(deep=True)


def standardize(df: gpd.GeoDataFrame, round_coords: bool = True) -> gpd.GeoDataFrame:
    """
    Applies a series of geometry and attribute standardizations and rules:
    1) ensures geometries are LineString;
    2) drops zero-length geometries;
    3) removes null coordinates;
    4) rounds coordinates;
    5) enforces domain restrictions and dtypes;
    6) enforces attribute-specific rules:
        i) bo_new = 1 must result in segment_type = 2.
        ii) completely new bos must have both bo_new = 1 and segment_type = 2.
        iii) NRN records must not have modified values for bo_new, boundary, and segment_type.
    7) drops any existing validation attributes (v#+).
    8) assign identifier attribute (segment_id) as index.

    \b
    :param gpd.GeoDataFrame df: GeoDataFrame.
    :param bool round_coords: indicates if coordinates are to be rounded.
    :return gpd.GeoDataFrame: updated GeoDataFrame.
    """

    logger.info("Standardizing data.")

    identifier = "segment_id"
    nrn_identifier = "segment_id_orig"

    try:

        # 1) Ensure geometries are LineString.

        # Enforce geometry type.
        flag_geom = ~df.geom_type.isin({"LineString", "MultiLineString"})
        if sum(flag_geom):
            df = df.loc[~flag_geom].copy(deep=True)

            logger.warning(f"Dropped {sum(flag_geom)} non-(Multi)LineString geometries.")

        # Explode MultiLineStrings.
        df = explode_geometry(df)

        # 2) Drop zero-length geometries.
        flag_zero_len = df.length == 0
        if sum(flag_zero_len):
            df = df.loc[~flag_zero_len].copy(deep=True)

            logger.warning(f"Dropped {sum(flag_zero_len)} zero-length geometries.")

        # 3) Remove null coordinates.
        flag_null_len = df.length.isna()
        if sum(flag_null_len):

            # Compile valid coordinates for flagged geometries.
            df_ = df.loc[flag_null_len].copy(deep=True)
            df_["valid_coords"] = df_["geometry"].map(
                lambda g: tuple(filter(lambda pt: not (pd.isna(pt[0]) or pd.isna(pt[1])), attrgetter("coords")(g))))

            # Flag geometries with valid (non-null) coordinates.
            flag_has_valid_coords = df_["valid_coords"].map(len) >= 2

            # Update geometries - Has valid coordinates: Replace geometry.
            df.loc[df_.loc[flag_has_valid_coords].index, "geometry"] = \
                df_.loc[flag_has_valid_coords, "valid_coords"].map(LineString)

            # Update geometries - No valid coordinates: Drop geometry.
            df = df.loc[~df.index.isin(df_.loc[~flag_has_valid_coords].index)].copy(deep=True)

            logger.warning(f"Removed null coordinates from {sum(flag_null_len)} geometries: geometries updated = "
                           f"{sum(flag_has_valid_coords)}, geometries dropped = {sum(~flag_has_valid_coords)}.")

        # 4) Round coordinates.
        if round_coords:
            df = round_coordinates(df)

        # 5) Enforce domains and dtypes.

        # Define attribute specifications.
        specs = {
            "bo_new": {"domain": {"0": 0, "0.0": 0, "1": 1, "1.0": 1}, "default": 0, "dtype": int},
            "boundary": {"domain": {"0": 0, "0.0": 0, "1": 1, "1.0": 1}, "default": 0, "dtype": int},
            "ngd_uid": {"domain": None, "default": -1, "dtype": int},
            "segment_id": {"domain": None, "default": "-1", "dtype": str},
            "segment_id_orig": {"domain": None, "default": "-1", "dtype": str},
            "segment_type": {"domain": {"1": 1, "1.0": 1, "2": 2, "2.0": 2}, "default": 1, "dtype": int},
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

        # Iterate attribute specifications.
        for col, params in specs.items():

            # Copy original series as object dtype.
            s_orig = df[col].copy(deep=True).astype(object)

            # Set Nulls to default value.
            flag_null = df[col].isna()
            df.loc[flag_null, col] = params["default"]

            # Set invalid values (not within domain) to default value.
            if params["domain"]:
                flag_domain = ~df[col].astype(str).isin(params["domain"])
                df.loc[flag_domain, col] = params["default"]

            # Map values via domain.
            if params["domain"]:
                flag_cast = ~df[col].isin(params["domain"].values())
                df.loc[flag_cast, col] = df.loc[flag_cast, col].astype(str).map(params["domain"])

            # Cast dtype.
            def _cast_dtype(val):
                try:
                    return params["dtype"](val)
                except ValueError:
                    return params["default"]

            df[col] = df[col].map(_cast_dtype)

            # Log results.
            flag_invalid = pd.Series(s_orig.map(str) != df[col].map(str))
            if sum(flag_invalid):
                logger.warning(f"Standardized domain and dtype for {sum(flag_invalid)} records for \"{col}\".")

        # Standardize domain - identifier.

        # Flag invalid identifiers.
        hexdigits = set(string.hexdigits)
        flag_len = df[identifier].map(len) != 32
        flag_non_hex = df[identifier].map(lambda val: not set(val).issubset(hexdigits))
        flag_dups = df[identifier].duplicated(keep=False)
        flag_invalid = flag_len | flag_non_hex | flag_dups

        # Resolve invalid identifiers and assign attribute as index.
        if sum(flag_invalid):
            df.loc[flag_invalid, identifier] = [uuid.uuid4().hex for _ in range(sum(flag_invalid))]
            df.index = df[identifier]

            logger.warning(f"Resolved {sum(flag_invalid)} invalid identifiers for \"segment_id\".")

        # 6) Enforce attribute-specific rules.

        # i) New BOs (converted ngd road; must be done prior to validating NRN record integrity).
        flag_invalid = (df["bo_new"] == 1) & (df["segment_type"] != 2)
        if sum(flag_invalid):
            df.loc[flag_invalid, "segment_type"] = 2

            logger.warning(f"Set \"segment_type\" = 2 for {sum(flag_invalid)} NGD roads converted to BOs.")

        # ii) New BOs (completely new feature; must be done prior to validating NRN record integrity).
        flag_invalid = (df["ngd_uid"] == -1) & (df["bo_new"] != 1) & (df["segment_type"] == 2)
        if sum(flag_invalid):
            df.loc[flag_invalid, "bo_new"] = 1

            logger.warning(f"Set \"bo_new\" = 1 for {sum(flag_invalid)} completely new BOs.")

        # iii) NRN record integrity.

        # Standardize NRN identifier.
        flag_invalid = (df[nrn_identifier].map(len) != 32) & (df[nrn_identifier] != specs[nrn_identifier]["default"])
        if sum(flag_invalid):
            df.loc[flag_invalid, nrn_identifier] = specs[identifier]["default"]

            logger.warning(f"Resolved {sum(flag_invalid)} invalid NRN identifiers for \"segment_id_orig\".")

        # Revert modified attributes.
        for col, domain in {"bo_new": {0}, "boundary": {0}, "segment_type": {1}}.items():
            flag_invalid = (df[nrn_identifier].map(len) == 32) & (~df[col].isin(domain))
            if sum(flag_invalid):
                df.loc[flag_invalid, col] = specs[col]["default"]

                logger.warning(f"Reverted {sum(flag_invalid)} NRN record values for \"{col}\".")

        # 7) Drop existing validation attributes.
        cols = set(df.filter(regex="v[0-9]+$").columns)
        if cols:
            df.drop(columns=cols, inplace=True)

            logger.warning(f"Dropped {len(cols)} existing validation attributes: {', '.join(cols)}.")

        # 8) Assign identifier attribute as index.
        df.index = df[identifier]

        logger.info("Finished standardizing data.")

        return df.copy(deep=True)

    except (TypeError, ValueError) as e:
        logger.exception(e)
        logger.exception(f"Unable to complete dataset standardizations.")
        sys.exit(1)


def update_nodes(g: LineString, node_map: Dict[tuple, tuple]) -> LineString:
    """
    Updates one or both nodes in the LineString.

    \b
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
