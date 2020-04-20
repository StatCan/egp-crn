import datetime
import fiona
import geopandas as gpd
import logging
import networkx as nx
import os
import pandas as pd
import requests
import shutil
import sqlite3
import subprocess
import sys
import time
import yaml
from copy import deepcopy
from geoalchemy2 import WKTElement, Geometry
from itertools import compress
from osgeo import ogr, osr
from shapely.geometry import LineString, Point


logger = logging.getLogger()


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
        logger.info("Temporarily redirecting stream logging to file: {}.".format(self.log_path))
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
        logger.info("Finished. Time elapsed: {}.".format(delta))


def compile_default_values(lang="en"):
    """Compiles the default value for each field in each table."""

    dft_vals = load_yaml(os.path.abspath("../field_domains_{}.yaml".format(lang)))["default"]
    dist_format = load_yaml(os.path.abspath("../distribution_format.yaml"))
    defaults = dict()

    try:

        # Iterate tables.
        for name in dist_format:
            defaults[name] = dict()

            # Iterate fields.
            for field, dtype in dist_format[name]["fields"].items():

                # Configure default value.
                key = "label" if dtype[0] in ("bytes", "str", "unicode") else "code"
                defaults[name][field] = dft_vals[key]

    except (AttributeError, KeyError, ValueError):
        logger.exception("Invalid schema definition for either \"{}\" or \"{}\".".format(dft_vals, dist_format))
        sys.exit(1)

    return defaults


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
        logger.exception("Invalid schema definition for \"{}\".".format(dist_format))
        sys.exit(1)

    return dtypes


def export_gpkg(dataframes, output_path, empty_gpkg_path=os.path.abspath("../../data/empty.gpkg")):
    """Receives a dictionary of pandas dataframes and exports them as geopackage layers."""

    # Create gpkg from template if it doesn't already exist.
    if not os.path.exists(output_path):
        shutil.copyfile(empty_gpkg_path, output_path)

    # Export target dataframes to GeoPackage layers.
    try:

        # Create sqlite and ogr GeoPackage connections.
        con = sqlite3.connect(output_path)
        con_ogr = ogr.GetDriverByName("GPKG").Open(output_path, update=1)

        # Iterate dataframes.
        for table_name, df in dataframes.items():

            # Remove pre-existing layer from GeoPackage.
            if table_name in [layer.GetName() for layer in con_ogr]:

                logger.info("Layer already exists: \"{}\". Removing layer from GeoPackage.".format(table_name))
                con_ogr.DeleteLayer(table_name)

                # Remove metadata table.
                con.cursor().execute("delete from gpkg_contents where table_name = '{}';".format(table_name))
                con.commit()

            # Write to GeoPackage.
            logger.info("Writing to GeoPackage: \"{}\", layer: \"{}\".".format(output_path, table_name))

            # Spatial data.
            if "geometry" in dir(df):

                # Open GeoPackage.
                with fiona.open(output_path, "w", overwrite=True, layer=table_name, driver="GPKG", crs=df.crs.to_wkt(),
                                schema=gpd.io.file.infer_schema(df)) as gpkg:

                    # Write to GeoPackage.
                    gpkg.writerecords(df.iterfeatures())

            # Tabular data.
            else:

                # Write to GeoPackage.
                df.to_sql(table_name, con, if_exists="replace", index=False)

                # Add metedata record to gpkg_contents.
                con.cursor().execute("insert or ignore into gpkg_contents (table_name, data_type) values "
                                     "('{}', 'attributes');".format(table_name))
                con.commit()

            logger.info("Successfully exported layer: \"{}\".".format(table_name))

        # Commit and close db connection.
        con.commit()
        con.close()
        del con_ogr

    except (ValueError, fiona.errors.FionaValueError, fiona.errors.TransactionError, sqlite3.Error):
        logger.exception("Error raised when writing to GeoPackage: \"{}\".".format(output_path))
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


def gdf_to_postgis(gdf, name, engine, **kwargs):
    """
    Converts a GeoPandas GeoDataFrame to a PostGIS database table.
    **kwargs: Keyword args for GeoPandas.GeoDataFrame.to_sql method.
    """

    logger.info("Loading GeoDataFrame into PostGIS via SQLAlchemy engine url: \"{}\".".format(repr(engine.url)))

    # Copy input GeoDataFrame.
    gdf = gdf.copy(deep=True)

    # Compile geometry attributes
    srid = gdf.crs.to_epsg()
    geom_type = gdf.geom_type.iloc[0].upper()

    # Store geometry as geom.
    gdf["geom"] = gdf["geometry"].map(lambda geom: WKTElement(geom.wkt, srid=srid))
    gdf.drop("geometry", axis=1, inplace=True)

    # Call GeoPandas.GeoDataFrame.to_sql method.
    gdf.to_sql(name=name, con=engine, dtype={"geom": Geometry(geometry_type=geom_type, srid=srid)}, **kwargs)


def get_url(url, max_attempts=10, **kwargs):
    """Attempts to retrieve a url."""

    attempt = 1
    while attempt <= max_attempts:

        try:

            logger.info("Connecting to url (attempt {} of {}): {}".format(attempt, max_attempts, url))

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


def load_gpkg(gpkg_path, find=False):
    """
    Returns a dictionary of geopackage layers loaded into pandas or geopandas (geo)dataframes.
    Parameter find will creating a mapping for geopackage layer names which contain, but do not exactly match the
    expected NRN layer names.
    """

    dframes = dict()
    distribution_format = load_yaml(os.path.abspath("../distribution_format.yaml"))
    missing_flag = False

    if os.path.exists(gpkg_path):

        try:

            # Create sqlite connection.
            con = sqlite3.connect(gpkg_path)

            # Load gpkg table names.
            cur = con.cursor()
            query = "select name from sqlite_master where type='table';"
            layers = list(zip(*cur.execute(query).fetchall()))[0]

            # Create table name mapping.
            gpkg_tables = dict()
            if find:
                for table_name in distribution_format:
                    results = [name.lower().find(table_name) >= 0 for name in layers]
                    if any(results):
                        gpkg_tables[table_name] = list(compress(layers, results))[0]
            else:
                gpkg_tables = {name: name for name in layers}

        except sqlite3.Error:
            logger.exception("Unable to connect to GeoPackage: \"{}\".".format(gpkg_path))
            sys.exit(1)

        # Load GeoPackage layers into pandas or geopandas.
        for table_name in distribution_format:

            logger.info("Loading layer: \"{}\".".format(table_name))

            try:

                if table_name in gpkg_tables:

                    # Spatial data.
                    if distribution_format[table_name]["spatial"]:
                        df = gpd.read_file(gpkg_path, layer=gpkg_tables[table_name], driver="GPKG")

                    # Tabular data.
                    else:
                        df = pd.read_sql_query("select * from {}".format(gpkg_tables[table_name]), con)

                    # Set index field: uuid.
                    if "uuid" in df.columns:
                        df.index = df["uuid"]

                    # Store result.
                    dframes[table_name] = df.copy(deep=True)
                    logger.info("Successfully loaded layer into dataframe: \"{}\".".format(table_name))

                else:
                    logger.warning("GeoPackage layer not found: \"{}\".".format(table_name))
                    missing_flag = True

            except (fiona.errors.DriverError, pd.io.sql.DatabaseError, sqlite3.Error):
                logger.exception("Unable to load GeoPackage layer: \"{}\".".format(table_name))
                sys.exit(1)

    else:
        logger.exception("GeoPackage does not exist: \"{}\".".format(gpkg_path))
        sys.exit(1)

    # Provide warning for missing GeoPackage layers.
    if missing_flag:
        logger.warning("Missing tables indicated. An exception may be raised later on if the table is required.")

    return dframes


def load_yaml(path):
    """Loads and returns a yaml file."""

    with open(path, "r", encoding="utf8") as f:

        try:
            return yaml.safe_load(f)
        except (ValueError, yaml.YAMLError):
            logger.exception("Unable to load yaml file: \"{}\".".format(path))


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
    expression = "ogr2ogr {}".format(" ".join(map(str, expression.values())))

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
                logger.exception("ogr2ogr error: {}".format(e))
                logger.warning("Maximum attempts reached. Exiting program.")
                sys.exit(1)
            else:
                logger.warning("Attempt {} of {} failed. Retrying.".format(attempt, max_attempts))
                attempt += 1
                continue


def reproject_gdf(gdf, epsg_source, epsg_target):
    """Transforms a GeoDataFrame's geometry column between EPSGs."""

    # Return empty dataframe.
    if not len(gdf):
        return gdf

    # Deep copy dataframe to avoid reprojecting original.
    # Explicitly copy crs property since it is excluded from default copy method.
    gdf = gpd.GeoDataFrame(gdf.copy(deep=True), crs=deepcopy(gdf.crs))

    # Define transformation.
    prj_source, prj_target = osr.SpatialReference(), osr.SpatialReference()
    prj_source.ImportFromEPSG(epsg_source)
    prj_target.ImportFromEPSG(epsg_target)
    prj_transformer = osr.CoordinateTransformation(prj_source, prj_target)

    # Transform Records.
    if len(gdf.geom_type.unique()) > 1:
        raise Exception("Multiple geometry types detected for dataframe.")
    elif gdf.geom_type.iloc[0] == "LineString":
        gdf["geometry"] = gdf["geometry"].map(lambda geom: LineString(prj_transformer.TransformPoints(
            list(map(lambda coord: coord[::-1], geom.coords)))))
    elif gdf.geom_type.iloc[0] == "Point":
        gdf["geometry"] = gdf["geometry"].map(lambda geom: Point(prj_transformer.TransformPoint(*geom.coords[0][::-1])))
    else:
        raise Exception("Geometry type not supported for EPSG transformation.")

    # Update crs attribute.
    gdf.crs = "epsg:{}".format(epsg_target)

    return gdf
