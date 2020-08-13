import datetime
import fiona
import geopandas as gpd
import geoparquet as gpq
import logging
import networkx as nx
import numpy as np
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyproj
import re
import requests
import sqlite3
import subprocess
import sys
import time
import yaml
from collections import defaultdict
from copy import deepcopy
from operator import attrgetter
from osgeo import ogr, osr
from shapely.geometry import LineString, Point
from shapely.wkt import loads
from tqdm import tqdm


logger = logging.getLogger()
ogr.UseExceptions()


class TempHandlerSwap:
    """Temporarily swaps all stream handlers with a file handler."""

    def __init__(self, class_logger, log_path):
        self.logger = class_logger
        self.log_path = log_path

        # Store stream handlers.
        self.stream_handlers = [h for h in self.logger.handlers if isinstance(h, logging.StreamHandler)]

        # Define file handler.
        self.file_handler = logging.FileHandler(self.log_path)
        self.file_handler.setLevel(logging.INFO)
        self.file_handler.setFormatter(self.logger.handlers[0].formatter)

    def __enter__(self):
        """Remove stream handlers and add file handler."""
        logger.info(f"Temporarily redirecting stream logging to file: {self.log_path}.")
        for handler in self.stream_handlers:
            self.logger.removeHandler(handler)
        self.logger.addHandler(self.file_handler)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Remove file handler and add stream handlers."""
        for handler in self.stream_handlers:
            self.logger.addHandler(handler)
        self.logger.removeHandler(self.file_handler)

        logger.info("File logging complete; reverted logging to stream.")


class Timer:
    """Tracks stage runtime."""

    def __init__(self):
        self.start_time = None

    def __enter__(self):
        logger.info("Started.")
        self.start_time = time.time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        total_seconds = time.time() - self.start_time
        delta = datetime.timedelta(seconds=total_seconds)
        logger.info(f"Finished. Time elapsed: {delta}.")


def apply_domain(series, domain, default):
    """
    Applies a domain restriction to the given pandas series based on the provided domain dictionary.
    Replaces missing or invalid values with the default parameter.

    Non-dictionary domains are treated as null. Values are left as-is excluding null types and empty strings, which are
    replaced with the default parameter.
    """

    # Validate against domain dictionary.
    if isinstance(domain, dict):

        # Convert keys to lowercase strings.
        domain = {str(k).lower(): v for k, v in domain.items()}

        # Configure lookup function, convert invalid values to default.
        def get_value(val):
            try:
                return domain[str(val).lower()]
            except KeyError:
                return default

        # Get values.
        return series.map(get_value)

    else:

        # Convert empty strings and null types to default.
        series.loc[(series.map(str).isin(["", "nan"])) | (series.isna())] = default
        return series


def compile_default_values(lang="en"):
    """Compiles the default value for each field in each table."""

    dft_vals = load_yaml(os.path.abspath(f"../field_domains_{lang}.yaml"))["default"]
    dist_format = load_yaml(os.path.abspath("../distribution_format.yaml"))
    defaults = dict()

    try:

        # Iterate tables.
        for name in dist_format:
            defaults[name] = dict()

            # Iterate fields.
            for field, dtype in dist_format[name]["fields"].items():

                # Configure default value.
                key = "label" if dtype[0] == "str" else "code"
                defaults[name][field] = dft_vals[key]

    except (AttributeError, KeyError, ValueError):
        logger.exception(f"Invalid schema definition for one or more yamls:"
                         f"\nDefault values: {dft_vals}"
                         f"\nDistribution format: {dist_format}")
        sys.exit(1)

    return defaults


def compile_domains(mapped_lang="en"):
    """
    Returns a dictionary containing the following for each field in each table:
    1) 'values': all English and French values and numeric keys flattened into a single list.
    2) 'lookup': a lookup dictionary mapping each English and French value and numeric key to the value of the given
    map language.
    """

    # Compile field domains.
    domains = defaultdict(dict)

    # Load domain yamls.
    domain_yamls = {lang: load_yaml(f"../field_domains_{lang}.yaml") for lang in ("en", "fr")}

    # Iterate tables and fields with domains.
    for table in domain_yamls["en"]["tables"]:
        for field in domain_yamls["en"]["tables"][table]:

            try:

                # Compile domains.
                domain_en = domain_yamls["en"]["tables"][table][field]
                domain_fr = domain_yamls["fr"]["tables"][table][field]

                # Configure mapped and non-mapped output domain.
                domain_mapped = domain_en if mapped_lang == "en" else domain_fr
                domain_non_mapped = domain_en if mapped_lang != "en" else domain_fr

                # Compile all domain values and domain lookup table, separately.
                if domain_en is None:
                    domains[table][field] = {"values": None, "lookup": None}

                elif isinstance(domain_en, list):
                    domains[table][field] = {
                        "values": sorted(list({*domain_en, *domain_fr}), reverse=True),
                        "lookup": dict([*zip(domain_en, domain_mapped), *zip(domain_fr, domain_mapped)])
                    }

                elif isinstance(domain_en, dict):
                    domains[table][field] = {
                        "values": sorted(list({*domain_en.values(), *domain_fr.values()}), reverse=True),
                        "lookup": {**domain_mapped,
                                   **{v: v for v in domain_mapped.values()},
                                   **{v: domain_mapped[k] for k, v in domain_non_mapped.items()}}
                    }

                    # Add integer keys as floats to accommodate incorrectly casted data.
                    domains[table][field]["lookup"].update({str(float(k)): v for k, v in domain_mapped.items()})

                else:
                    raise TypeError

            except (AttributeError, KeyError, TypeError, ValueError):
                yaml_paths = ", ".join(os.path.abspath(f"../field_domains_{lang}.yaml") for lang in ("en", "fr"))
                logger.exception(f"Unable to compile domains from config yamls: {yaml_paths}. Invalid schema "
                                 f"definition for table: {table}, field: {field}.")
                sys.exit(1)

    return domains


def compile_dtypes(length=False):
    """Compiles the dtype for each field in each table. Optionally returns a list to include the field length."""

    dist_format = load_yaml(os.path.abspath("../distribution_format.yaml"))
    dtypes = dict()

    try:

        # Iterate tables.
        for name in dist_format:
            dtypes[name] = dict()

            # Iterate fields.
            for field, dtype in dist_format[name]["fields"].items():

                # Compile dtype and field length.
                dtypes[name][field] = dtype if length else dtype[0]

    except (AttributeError, KeyError, ValueError):
        logger.exception(f"Invalid schema definition: {dist_format}.")
        sys.exit(1)

    return dtypes


def explode_geometry(gdf):
    """Explodes MultiLineStrings and MultiPoints to LineStrings and Points, respectively."""

    logger.info("Exploding multi-type geometries.")

    multi_types = {"MultiLineString", "MultiPoint"}
    if len(set(gdf.geom_type.unique()).intersection(multi_types)):

        # Separate multi- and single-type records.
        multi = gdf[gdf.geom_type.isin(multi_types)]
        single = gdf[~gdf.index.isin(multi.index)]

        # Explode multi-type geometries.
        multi_exploded = multi.explode().reset_index(drop=True)

        # Merge all records.
        merged = gpd.GeoDataFrame(pd.concat([single, multi_exploded], ignore_index=True), crs=gdf.crs)
        return merged.copy(deep=True)

    else:
        return gdf.copy(deep=True)


def export_gpkg(dataframes, output_path, export_schemas=None):
    """
    Receives a dictionary of (Geo)pandas (Geo)DataFrames and exports them as GeoPackage layers.
    Optionally accepts an export schemas yaml path which will override the default distribution format column names.
    """

    output_path = os.path.abspath(output_path)
    logger.info(f"Exporting dataframe(s) to GeoPackage: {output_path}.")

    try:

        logger.info(f"Creating / opening data source: {output_path}.")

        # Create / open GeoPackage.
        driver = ogr.GetDriverByName("GPKG")
        if os.path.exists(output_path):
            gpkg = driver.Open(output_path, update=1)
        else:
            gpkg = driver.CreateDataSource(output_path)

        # Compile schemas.
        schemas = load_yaml(os.path.abspath("../distribution_format.yaml"))
        if export_schemas:
            export_schemas = load_yaml(export_schemas)["conform"]
        else:
            export_schemas = defaultdict(dict)
            for table in schemas:
                export_schemas[table]["fields"] = {field: field for field in schemas[table]["fields"]}

        # Iterate dataframes.
        for table_name, df in dataframes.items():

            logger.info(f"Layer {table_name}: creating layer.")

            # Configure layer shape type and spatial reference.
            if isinstance(df, gpd.GeoDataFrame):

                srs = osr.SpatialReference()
                srs.ImportFromEPSG(df.crs.to_epsg())

                if len(df.geom_type.unique()) > 1:
                    raise ValueError(f"Multiple geometry types detected for dataframe {table_name}: "
                                     f"{', '.join(map(str, df.geom_type.unique()))}.")
                elif df.geom_type[0] in {"Point", "MultiPoint", "LineString", "MultiLineString"}:
                    shape_type = attrgetter(f"wkb{df.geom_type[0]}")(ogr)
                else:
                    raise ValueError(f"Invalid geometry type(s) for dataframe {table_name}: "
                                     f"{', '.join(map(str, df.geom_type.unique()))}.")
            else:
                shape_type = ogr.wkbNone
                srs = None

            # Create layer.
            layer = gpkg.CreateLayer(name=table_name, srs=srs, geom_type=shape_type, options=["OVERWRITE=YES"])

            logger.info(f"Layer {table_name}: configuring schema.")

            # Configure layer schema (field definitions).
            ogr_field_map = {"float": ogr.OFTReal, "int": ogr.OFTInteger, "str": ogr.OFTString}

            for field_name, mapped_field_name in export_schemas[table_name]["fields"].items():
                field_type, field_width = schemas[table_name]["fields"][field_name]
                field_defn = ogr.FieldDefn(mapped_field_name, ogr_field_map[field_type])
                field_defn.SetWidth(field_width)
                layer.CreateField(field_defn)

            # Map dataframe column names (does nothing if already mapped).
            df.rename(columns=export_schemas[table_name]["fields"], inplace=True)

            # Add uuid field to schema, if available.
            if "uuid" in df.columns:
                field_defn = ogr.FieldDefn("uuid", ogr.OFTString)
                field_defn.SetWidth(32)
                layer.CreateField(field_defn)

            # Filter invalid columns from dataframe.
            invalid_cols = set(df.columns) - {*export_schemas[table_name]["fields"].values(), "uuid", "geometry"}
            if invalid_cols:
                logger.warning(f"Layer {table_name}: extraneous columns detected and will not be written to output: "
                               f"{', '.join(map(str, invalid_cols))}.")

            # Write layer.
            layer.StartTransaction()

            for feat in tqdm(df.itertuples(index=False), total=len(df), desc=f"Layer {table_name}: writing to file"):

                # Instantiate feature.
                feature = ogr.Feature(layer.GetLayerDefn())

                # Set feature properties.
                properties = feat._asdict()
                for prop in set(properties) - invalid_cols - {"geometry"}:
                    field_index = feature.GetFieldIndex(prop)
                    feature.SetField(field_index, properties[prop])

                # Set feature geometry, if spatial.
                if srs:
                    geom = ogr.CreateGeometryFromWkb(properties["geometry"].wkb)
                    feature.SetGeometry(geom)

                # Create feature.
                layer.CreateFeature(feature)

                # Clear pointer for next iteration.
                feature = None

            layer.CommitTransaction()

    except (Exception, KeyError, ValueError, sqlite3.Error) as e:
        logger.exception(f"Error raised when writing to GeoPackage: {output_path}.")
        logger.exception(e)
        sys.exit(1)


def gdf_to_nx(gdf, keep_attributes=True, endpoints_only=False):
    """Converts a pandas dataframe to a networkx graph."""

    logger.info("Loading GeoPandas GeoDataFrame into NetworkX graph.")

    # Generate graph from GeoDataFrame of LineStrings, keeping crs property and (optionally) fields.
    g = nx.Graph()
    g.graph['crs'] = gdf.crs
    fields = list(gdf.columns) if keep_attributes else None

    # Iterate rows.
    for index, row in gdf.iterrows():

        # Compile geometry as edges.
        coords = [*row.geometry.coords]
        if endpoints_only:
            edges = [[coords[0], coords[-1]]]
        else:
            edges = [[coords[i], coords[i + 1]] for i in range(len(coords) - 1)]

        # Compile attributes.
        attributes = dict()
        if keep_attributes:
            data = [row[field] for field in fields]
            attributes = dict(zip(fields, data))

        # Add edges.
        g.add_edges_from(edges, **attributes)

    logger.info("Successfully loaded GeoPandas GeoDataFrame into NetworkX graph.")

    return g


def get_url(url, max_attempts=10, **kwargs):
    """Attempts to retrieve a url."""

    attempt = 1
    while attempt <= max_attempts:

        try:

            logger.info(f"Connecting to url (attempt {attempt} of {max_attempts}): {url}")

            # Get url response.
            response = requests.get(url, **kwargs)

            return response

        except (TimeoutError, requests.exceptions.RequestException) as e:

            if attempt == max_attempts:
                logger.warning("Failed to get url response.")
                logger.exception(e)
                logger.warning("Maximum attempts exhausted. Exiting program.")
                sys.exit(1)
            else:
                logger.warning("Failed to get url response. Retrying...")
                attempt += 1
                time.sleep(5)
                continue


def groupby_to_list(df, group_field, list_field):
    """
    Faster alternative to pandas groupby.apply/agg(list).
    Groups records by one or more fields and compiles an output field into a list for each group.
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


def load_gpkg(gpkg_path, find=False, layers=None):
    """
    Returns a dictionary of geopackage layers as pandas or geopandas (geo)dataframes.
    Parameter find will creating a mapping for geopackage layer names which contain, but do not exactly match the
    expected NRN layer names.
    Parameter layers accepts an iterable of table names if only a subset of GeoPackage layers are required.
    """

    dframes = dict()
    distribution_format = load_yaml(os.path.abspath("../distribution_format.yaml"))

    if os.path.exists(gpkg_path):

        # Filter layers to load.
        if layers:
            distribution_format = {k: v for k, v in distribution_format.items() if k in layers}

        try:

            # Create sqlite connection.
            con = sqlite3.connect(gpkg_path)
            cur = con.cursor()

            # Load GeoPackage table names.
            gpkg_layers = list(zip(*cur.execute("select name from sqlite_master where type='table';").fetchall()))[0]

            # Create table name mapping.
            layers_map = dict()
            if find:
                for table_name in distribution_format:
                    for layer_name in gpkg_layers:
                        if layer_name.lower().find(table_name) >= 0:
                            layers_map[table_name] = layer_name
                            break
            else:
                layers_map = {name: name for name in set(distribution_format).intersection(set(gpkg_layers))}

        except sqlite3.Error:
            logger.exception(f"Unable to connect to GeoPackage: {gpkg_path}.")
            sys.exit(1)

        # Compile missing layers.
        missing_layers = set(distribution_format) - set(layers_map)
        if missing_layers:
            logger.warning(f"Missing one or more expected layers: {', '.join(map(str, sorted(missing_layers)))}. An "
                           f"exception may be raised later on if any of these layers are required.")

        # Load GeoPackage layers as (geo)dataframes.
        # Convert column names to lowercase on import.
        for table_name in layers_map:

            logger.info(f"Loading layer: {table_name}.")

            try:

                # Spatial data.
                if distribution_format[table_name]["spatial"]:
                    df = gpd.read_file(gpkg_path, layer=layers_map[table_name], driver="GPKG").rename(columns=str.lower)

                # Tabular data.
                else:
                    df = pd.read_sql_query(f"select * from {layers_map[table_name]}", con).rename(columns=str.lower)

                # Set index field: uuid.
                if "uuid" in df.columns:
                    df.index = df["uuid"]

                # Drop fid field (this field is automatically generated and not part of the NRN).
                if "fid" in df.columns:
                    df.drop(columns=["fid"], inplace=True)

                # Store result.
                dframes[table_name] = df.copy(deep=True)
                logger.info(f"Successfully loaded layer as dataframe: {table_name}.")

            except (fiona.errors.DriverError, pd.io.sql.DatabaseError, sqlite3.Error):
                logger.exception(f"Unable to load layer: {table_name}.")
                sys.exit(1)

    else:
        logger.exception(f"GeoPackage does not exist: {gpkg_path}.")
        sys.exit(1)

    return dframes


def load_yaml(path):
    """Loads and returns a yaml file."""

    with open(path, "r", encoding="utf8") as f:

        try:

            return yaml.safe_load(f)

        except (ValueError, yaml.YAMLError):
            logger.exception(f"Unable to load yaml: {path}.")


def nx_to_gdf(g, nodes=True, edges=True):
    """Converts a networkx graph to pandas dataframe."""

    logger.info("Loading NetworkX graph into GeoPandas GeoDataFrame.")

    # Generate GeoDataFrames for both networkx nodes and edges.
    gdf_nodes, gdf_edges = None, None

    # Compile node geometry and attributes.
    if nodes:
        node_xy, node_data = zip(*g.nodes(data=True))
        gdf_nodes = gpd.GeoDataFrame(list(node_data), geometry=[Point(i, j) for i, j in node_xy])
        gdf_nodes.crs = g.graph['crs']

    # Compile edge geometry and attributes.
    if edges:
        starts, ends, edge_data = zip(*g.edges(data=True))
        gdf_edges = gpd.GeoDataFrame(list(edge_data))
        gdf_edges.crs = g.graph['crs']

    logger.info("Successfully loaded GeoPandas GeoDataFrame into NetworkX graph.")

    # Conditionally return nodes and / or edges.
    if all([nodes, edges]):
        return gdf_nodes, gdf_edges
    elif nodes is True:
        return gdf_nodes
    else:
        return gdf_edges


def ogr2ogr(expression, log=None, max_attempts=5):
    """Runs an ogr2ogr subprocess. Input expression must be a dictionary of ogr2ogr parameters."""

    # Write log.
    if log:
        logger.info(log)

    # Format ogr2ogr command.
    expression = f"ogr2ogr {' '.join(map(str, expression.values()))}"

    # Execute ogr2ogr.
    attempt = 1
    while attempt <= max_attempts:

        try:

            # Run subprocess.
            subprocess.run(expression, shell=True, check=True)
            break

        except subprocess.CalledProcessError as e:

            if attempt == max_attempts:
                logger.exception("Unable to transform data source.")
                logger.exception(f"ogr2ogr error: {e}")
                logger.warning("Maximum attempts reached. Exiting program.")
                sys.exit(1)
            else:
                logger.warning(f"Attempt {attempt} of {max_attempts} failed. Retrying.")
                attempt += 1
                continue


def reproject_gdf(gdf, epsg_source, epsg_target):
    """Transforms a GeoDataFrame's geometry column or GeoSeries between EPSGs."""

    logger.info(f"Reprojecting geometry from EPSG:{epsg_source} to EPSG:{epsg_target}.")

    series_flag = True if isinstance(gdf, gpd.GeoSeries) else False

    # Return empty dataframe.
    if not len(gdf):
        return gdf

    # Deep copy dataframe to avoid reprojecting original.
    # Explicitly copy crs property since it is excluded from default copy method.
    gdf = gpd.GeoDataFrame(gdf.copy(deep=True), crs=deepcopy(gdf.crs))

    # Define transformation.
    prj_source, prj_target = osr.SpatialReference(), osr.SpatialReference()
    prj_source.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    prj_target.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    prj_source.ImportFromEPSG(epsg_source)
    prj_target.ImportFromEPSG(epsg_target)
    prj_transformer = osr.CoordinateTransformation(prj_source, prj_target)

    # Transform Records.
    # Process: pass reversed xy coordinates to proj transformer, load result as shapely geometry.
    if len(gdf.geom_type.unique()) > 1:
        raise Exception("Multiple geometry types detected for dataframe.")

    elif gdf.geom_type.iloc[0] == "LineString":
        gdf["geometry"] = gdf["geometry"].map(
            lambda geom: LineString(prj_transformer.TransformPoints(list(zip(*geom.coords.xy)))))

    elif gdf.geom_type.iloc[0] == "Point":
        gdf["geometry"] = gdf["geometry"].map(
            lambda geom: Point(prj_transformer.TransformPoint(*list(zip(*geom.coords.xy))[0])))

    else:
        raise Exception("Geometry type not supported for EPSG transformation.")

    # Update crs attribute.
    gdf.crs = f"epsg:{epsg_target}"

    if series_flag:
        return gdf["geometry"]
    else:
        return gdf


def round_coordinates(gdf, precision=7):
    """Rounds the coordinates of the geometry column to the specified decimal precision."""

    logger.info(f"Rounding coordinates to decimal precision: {precision}.")

    try:

        gdf["geometry"] = gdf["geometry"].map(
            lambda g: loads(re.sub(r"\d*\.\d+", lambda m: f"{float(m.group(0)):.{precision}f}", g.wkt)))

        return gdf

    except (TypeError, ValueError) as e:
        logger.exception("Unable to round coordinates for GeoDataFrame.")
        logger.exception(e)
        sys.exit(1)


def to_geoparquet(self: gpd.GeoDataFrame, path: str):
    """
    A copy of the geoparquet.to_geoparquet function as of commit: b09b12d.
    Reference: https://github.com/darcy-r/geoparquet-python/blob/master/geoparquet/__init__.py

    The current geoparquet release and, therefore, geopandas.GeoDataFrame.to_geoparquet, does not have the latest commit
    which handles CRS in an acceptible way. This function overwrite geopandas.GeoDataFrame.to_parquet and should be
    removed once GeoPandas updates to_parquet.
    """

    field_name = self.geometry.name
    crs = pyproj.CRS.from_user_input(self.crs).to_wkt(version="WKT2_2018")
    crs_format = "WKT2_2018"
    geometry_types = self.geometry.geom_type.unique().tolist()
    self = self._serialise_geometry(field_name)
    self = pa.Table.from_pandas(self)
    geometry_metadata = {
        "geometry_fields": [
            {
                "field_name": field_name,
                "geometry_format": "wkb",
                "geometry_types": geometry_types,
                "crs": crs,
                "crs_format": crs_format,
            }
        ]
    }
    self = gpq._update_metadata(self, new_metadata=geometry_metadata)
    pq.write_table(self, path)
    return

gpd.GeoDataFrame.to_parquet = to_geoparquet
