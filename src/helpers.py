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
from shapely.geometry.point import Point


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


def export_gpkg(dataframes, output_path, empty_gpkg_path=os.path.abspath("../../data/empty.gpkg")):
    """Receives a dictionary of pandas dataframes and exports them as geopackage layers."""

    # Create gpkg from template if it doesn't already exist.
    if not os.path.exists(output_path):
        shutil.copyfile(empty_gpkg_path, output_path)

    # Export target dataframes to GeoPackage layers.
    try:
        for table_name, df in dataframes.items():

            logger.info("Writing to GeoPackage {}, layer={}.".format(output_path, table_name))

            # Reset index to preserve attribute as column.
            df.reset_index(inplace=True)

            # Spatial data.
            if "geometry" in dir(df):
                # Open GeoPackage.
                with fiona.open(output_path, "w", layer=table_name, driver="GPKG", crs=df.crs,
                                schema=gpd.io.file.infer_schema(df)) as gpkg:

                    # Write to GeoPackage.
                    gpkg.writerecords(df.iterfeatures())

            # Tabular data.
            else:
                # Create sqlite connection.
                con = sqlite3.connect(output_path)

                # Write to GeoPackage.
                df.to_sql(table_name, con)

                # Insert record into gpkg_contents metadata table.
                con.cursor().execute("insert into 'gpkg_contents' ('table_name', 'data_type') values "
                                     "('{}', 'attributes');".format(table_name))

                # Commit and close db connection.
                con.commit()
                con.close()

            logger.info("Successfully exported layer.")

    except (ValueError, fiona.errors.FionaValueError):
        logger.exception("ValueError raised when writing GeoPackage layer.")
        sys.exit(1)


def load_gpkg(gpkg_path):
    """Returns a dictionary of geopackage layers loaded into pandas or geopandas (geo)dataframes."""

    dframes = dict()
    distribution_format = load_yaml(os.path.abspath("../distribution_format.yaml"))

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
                    df.set_index("uuid", inplace=True)

                    # Store result.
                    dframes[table_name] = df
                    logger.info("Successfully loaded layer into dataframe.")

                else:
                    logger.warning("GeoPackage layer not found: \"{}\".".format(table_name))

            except (fiona.errors.DriverError, pd.io.sql.DatabaseError, sqlite3.Error):
                logger.exception("Unable to load GeoPackage layer: \"{}\".".format(table_name))
                sys.exit(1)

    else:
        logger.exception("GeoPackage does not exist: \"{}\".".format(gpkg_path))
        sys.exit(1)

    return dframes


def load_yaml(path):
    """Loads and returns a yaml file."""

    with open(path, "r", encoding="utf8") as f:

        try:
            return yaml.safe_load(f)
        except (ValueError, yaml.YAMLError):
            logger.exception("Unable to load yaml file: {}.".format(path))


# source:
# https://www.reddit.com/r/gis/comments/b1ui7h/geopandas_how_to_make_a_graph_out_of_a/
def gdf_to_nx(gdf_network):
    # generate graph from GeoDataFrame of LineStrings
    net = nx.Graph()
    net.graph['crs'] = gdf_network.crs
    fields = list(gdf_network.columns)

    for index, row in gdf_network.iterrows():
        first = row.geometry.coords[0]
        last = row.geometry.coords[-1]

        data = [row[f] for f in fields]
        attributes = dict(zip(fields, data))
        net.add_edge(first, last, **attributes)

    return net


def nx_to_gdf(net, nodes=True, edges=True):
    # generate nodes and edges geodataframes from graph
    if nodes is True:
        node_xy, node_data = zip(*net.nodes(data=True))
        gdf_nodes = gpd.GeoDataFrame(list(node_data), geometry=[Point(i, j) for i, j in node_xy])
        gdf_nodes.crs = net.graph['crs']

    if edges is True:
        starts, ends, edge_data = zip(*net.edges(data=True))
        gdf_edges = gpd.GeoDataFrame(list(edge_data))
        gdf_edges.crs = net.graph['crs']

    if nodes is True and edges is True:
        return gdf_nodes, gdf_edges
    elif nodes is True and edges is False:
        return gdf_nodes
    else:
        return gdf_edges