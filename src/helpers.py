import datetime
import fiona
import geopandas as gpd
import logging
import networkx as nx
import os
import pandas as pd
import shutil
import sqlite3
import sys
import time
import yaml
from osgeo import ogr, osr
from shapely.geometry import LineString, Point


logger = logging.getLogger()


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


def compile_default_values():
    """Compiles the default value for each field in each table."""

    dft_vals = load_yaml(os.path.abspath("../field_domains_en.yaml"))["default"]
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
                with fiona.open(output_path, "w", overwrite=True, layer=table_name, driver="GPKG", crs=df.crs,
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


def load_gpkg(gpkg_path):
    """Returns a dictionary of geopackage layers loaded into pandas or geopandas (geo)dataframes."""

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
            gpkg_tables = list(zip(*cur.execute(query).fetchall()))[0]

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
                        df = gpd.read_file(gpkg_path, layer=table_name, driver="GPKG")

                    # Tabular data.
                    else:
                        df = pd.read_sql_query("select * from {}".format(table_name), con)

                    # Set index field: uuid.
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

def reproject_gdf(gdf, epsg_source, epsg_target):
    """Transforms a GeoDataFrame's geometry column between EPSGs."""

    # Define transformation.
    prj_source, prj_target = osr.SpatialReference(), osr.SpatialReference()
    prj_source.ImportFromEPSG(epsg_source)
    prj_target.ImportFromEPSG(epsg_target)
    prj_transformer = osr.CoordinateTransformation(prj_source, prj_target)

    # Transform Records.
    if gdf.geom_type[0] == "LineString":
        gdf["geometry"] = gdf["geometry"].map(lambda geom: LineString(prj_transformer.TransformPoints(geom.coords)))
    elif gdf.geom_type[0] == "Point":
        gdf["geometry"] = gdf["geometry"].map(lambda geom: Point(prj_transformer.TransformPoint(*geom.coords[0])))
    else:
        raise Exception("Geometry type not supported for EPSG transformation.")

    # Update crs attribute.
    gdf.crs["init"] = "epsg:{}".format(epsg_target)

    return gdf
