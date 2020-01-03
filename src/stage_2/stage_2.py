import geopandas as gpd
import logging
import networkx as nx
import os
import pandas as pd
import subprocess
import sys
import uuid
import urllib.request
import zipfile
from datetime import datetime
from geopandas_postgis import PostGIS
from osgeo import ogr
from sqlalchemy import *
from sqlalchemy.engine.url import URL
from shapely.geometry.multipoint import MultiPoint
from shapely.geometry.point import Point
from psycopg2 import connect, extensions, sql

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import helpers

# Set logger.
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)

# start script timer
startTime = datetime.now()


class Stage:

    def __init__(self):

        # Configure and validate input data path.
        self.data_path = os.path.abspath("../../data/interim/nb.gpkg")
        if not os.path.exists(self.data_path):
            logger.exception("Input data not found: \"{}\".".format(self.data_path))
            sys.exit(1)

    def create_db(self):
        """Creates the PostGIS database needed for Stage 2."""

        # database name which will be used for stage 2
        nrn_db = "nrn"

        # default postgres connection needed to create the nrn database
        conn = connect(
            dbname="postgres",
            user="postgres",
            host="localhost",
            password="password"
        )

        # postgres database url for geoprocessing
        nrn_url = URL(
            drivername='postgresql+psycopg2', host='localhost',
            database=nrn_db, username='postgres',
            port='5432', password='password'
        )

        # engine to connect to nrn database
        self.engine = create_engine(nrn_url)

        # get the isolation level for autocommit
        autocommit = extensions.ISOLATION_LEVEL_AUTOCOMMIT

        # set the isolation level for the connection's cursors
        # will raise ActiveSqlTransaction exception otherwise
        conn.set_isolation_level(autocommit)

        # connect to default connection
        cursor = conn.cursor()

        # drop the nrn database if it exists, then create it if not
        try:
            logging.info("Dropping PostgreSQL database.")
            cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {};").format(sql.Identifier(nrn_db)))
        except Exception:
            logging.exception("Could not drop database.")

        try:
            logging.info("Creating PostgreSQL database.")
            cursor.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(nrn_db)))
        except Exception:
            logging.exception("Failed to create PostgreSQL database.")

        logging.info("Closing default PostgreSQL connection.")
        cursor.close()
        conn.close()

        # connection parameters for newly created database
        nrn_conn = connect(
            dbname=nrn_db,
            user="postgres",
            host="localhost",
            password="password"
        )

        nrn_conn.set_isolation_level(autocommit)

        # connect to nrn database
        nrn_cursor = nrn_conn.cursor()
        try:
            logging.info("Creating spatially enabled PostgreSQL database.")
            nrn_cursor.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS postgis;"))
        except Exception:
            logging.exception("Cannot create PostGIS extension.")

        logging.info("Closing NRN PostgreSQL connection.")
        nrn_cursor.close()
        nrn_conn.close()

    def load_gpkg(self):
        """Loads input GeoPackage layers into dataframes."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)
        # print(self.dframes["roadseg"])

    def gen_dead_end(self):
        """Generates dead end junctions with NetworkX."""

        logging.info("Applying CRS EPSG:4617 to roadseg geodataframe.")
        self.df_roadseg = gpd.GeoDataFrame(self.dframes["roadseg"], geometry='geometry')
        self.df_roadseg.crs = {'init': 'epsg:4617'}

        logging.info("Convert roadseg geodataframe to NetX graph.")
        graph = helpers.gdf_to_nx(self.df_roadseg)

        logging.info("Create an empty graph for dead ends junctions.")
        dead_ends = nx.Graph()

        logging.info("Applying CRS EPSG:4617 to dead ends graph.")
        dead_ends.graph['crs'] = self.df_roadseg.crs

        logging.info("Filter for dead end junctions.")
        dead_ends_filter = [node for node, degree in graph.degree() if degree == 1]

        logging.info("Insert filtered dead end junctions into empty graph.")
        dead_ends.add_nodes_from(dead_ends_filter)

        logging.info("Convert dead end graph to geodataframe.")
        self.dead_end_gdf = helpers.nx_to_gdf(dead_ends, nodes=True, edges=False)

        logging.info("Apply dead end junctype to junctions.")
        self.dead_end_gdf["junctype"] = "Dead End"
        print(self.dead_end_gdf)

    def gen_intersections(self):
        """Generates intersection junction types."""

        logging.info("Importing roadseg geodataframe into PostGIS.")
        self.df_roadseg.postgis.to_postgis(con=self.engine, table_name="stage_2", geometry="LineString", if_exists="replace")

        logging.info("Loading SQL yaml.")
        self.sql = helpers.load_yaml("../sql.yaml")

        # source:
        # https://gis.stackexchange.com/questions/20835/identifying-road-intersections-using-postgis
        logging.info("Executing SQL injection for junction intersections.")
        inter_filter = self.sql["intersections"]["query"]

        logging.info("Creating junction intersection geodataframe.")
        self.inter_gdf = gpd.GeoDataFrame.from_postgis(inter_filter, self.engine, geom_col="geometry")

        logging.info("Apply intersection junctype to junctions.")
        self.inter_gdf["junctype"] = "Intersection"
        self.inter_gdf.crs = {'init': 'epsg:4617'}
        print(self.inter_gdf)

    def gen_ferry(self):
        """Generates ferry junctions with NetworkX."""

        logging.info("Applying CRS EPSG:4617 to roadseg geodataframe.")
        self.df_ferryseg = gpd.GeoDataFrame(self.dframes["ferryseg"], geometry='geometry')
        self.df_ferryseg.crs = {'init': 'epsg:4617'}

        logging.info("Convert ferryseg geodataframe to NetX graph.")
        graph_ferry = helpers.gdf_to_nx(self.df_ferryseg)

        logging.info("Create an empty graph for ferry junctions.")
        ferry_junc = nx.Graph()

        logging.info("Applying CRS EPSG:4617 to dead ends graph.")
        ferry_junc.graph['crs'] = self.df_ferryseg.crs

        logging.info("Filter for ferry junctions.")
        ferry_filter = [node for node, degree in graph_ferry.degree() if degree > 0]

        logging.info("Insert filtered ferry junctions into empty graph.")
        ferry_junc.add_nodes_from(ferry_filter)

        logging.info("Convert dead end graph to geodataframe.")
        self.ferry_gdf = helpers.nx_to_gdf(ferry_junc, nodes=True, edges=False)

        logging.info("Apply dead end junctype to junctions.")
        self.ferry_gdf["junctype"] = "Ferry"
        print(self.ferry_gdf)

    def combine(self):
        """Combine geodataframes."""

        logging.info("Combining ferry, dead end and intersection junctions.")
        junctions = gpd.GeoDataFrame(pd.concat([self.ferry_gdf, self.dead_end_gdf, self.inter_gdf], sort=False))
        junctions = junctions[['junctype', 'geometry']]
        junctions.crs = {'init': 'epsg:4617'}
        print(junctions)

        # source:
        # https://gis.stackexchange.com/questions/311320/casting-geometry-to-multi-using-geopandas
        junctions["geometry"] = [MultiPoint([feature]) if type(feature) == Point else feature for feature in junctions["geometry"]]
        self.ferry_gdf["geometry"] = [MultiPoint([feature]) if type(feature) == Point else feature for feature in self.ferry_gdf["geometry"]]

        logging.info("Importing merged junctions into PostGIS.")
        junctions.postgis.to_postgis(con=self.engine, table_name='stage_2_junc', geometry='MULTIPOINT', if_exists='replace')
        self.ferry_gdf.postgis.to_postgis(con=self.engine, table_name='stage_2_ferry_junc', geometry='MULTIPOINT', if_exists='replace')

    def fix_junctype(self):
        """Generate attributes."""

        logging.info("Downloading administrative boundary file.")
        adm_file = "http://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/2016/lpr_000a16a_e.zip"
        urllib.request.urlretrieve(adm_file, '../../data/raw/boundary.zip')
        with zipfile.ZipFile("../../data/raw/boundary.zip", "r") as zip_ref:
            zip_ref.extractall("../../data/raw/boundary")

        logging.info("Reading administrative boundary file.")
        subprocess.run("ogr2ogr -f GPKG -where PRUID='13' ../../data/raw/boundary/boundary.gpkg ../../data/raw/boundary/lpr_000a16a_e.shp -t_srs EPSG:4617 -nlt MULTIPOLYGON")
        bound_adm = gpd.read_file("../../data/raw/boundary/boundary.gpkg", driver="GeoPackage")
        bound_adm.crs = {'init': 'epsg:4617'}

        logging.info("Importing administrative boundary into PostGIS.")
        bound_adm.postgis.to_postgis(con=self.engine, table_name="adm", if_exists="replace", geometry='MultiPolygon')

        attr_fix = self.sql["attributes"]["query"]

        logging.info("Testing for junction equality and altering attributes.")
        attr_equality = gpd.GeoDataFrame.from_postgis(attr_fix, self.engine)

        print(attr_equality)

    def compile_target_attributes(self):
        """Compiles the target (distribution format) yaml file into a dictionary."""

        logger.info("Compiling target attribute yaml.")
        self.target_attributes = dict()

        # Load yaml.
        target_attributes_yaml = helpers.load_yaml(os.path.abspath("../distribution_format.yaml"))

        # Store yaml contents for all contained table names.
        logger.info("Compiling attributes for target tables.")

        for table in target_attributes_yaml:
            self.target_attributes[table] = {"spatial": target_attributes_yaml[table]["spatial"], "fields": dict()}

            for field, vals in target_attributes_yaml[table]["fields"].items():
                # Compile field attributes.
                try:
                    self.target_attributes[table]["fields"][field] = str(vals[0])
                except (AttributeError, KeyError, ValueError):
                    logger.exception("Invalid schema definition for table: {}, field: {}.".format(table, field))
                    sys.exit(1)

        print(self.target_attributes)

    def gen_target_dataframes(self):
        """Creates empty dataframes for all applicable output tables based on the input data field mapping."""

        logger.info("Creating target dataframes for applicable tables.")
        self.target_gdframes = dict()

        # Retrieve target table name from source attributes.
        for source, source_yaml in self.source_attributes.items():
            for table in source_yaml["conform"]:

                logger.info("Creating target dataframe: {}.".format(table))

                # Spatial.
                if self.target_attributes[table]["spatial"]:

                    # Generate target dataframe from source uuid and geometry fields.
                    gdf = gpd.GeoDataFrame(self.source_gdframes[source][["uuid"]],
                                           geometry=self.source_gdframes[source].geometry)

                # Tabular.
                else:

                    # Generate target dataframe from source uuid field.
                    gdf = pd.DataFrame(self.source_gdframes[source][["uuid"]])

                # Add target field schema.
                gdf = gdf.assign(**{field: pd.Series(dtype=dtype) for field, dtype in
                                    self.target_attributes[table]["fields"].items()})

                # Store result.
                self.target_gdframes[table] = gdf
                logger.info("Successfully created target dataframe: {}.".format(table))

        print(self.target_gdframes[table])

    def execute(self):
        """Executes an NRN stage."""

        self.create_db()
        self.load_gpkg()
        self.gen_dead_end()
        self.gen_intersections()
        self.gen_ferry()
        self.combine()
        self.fix_junctype()


def main():

    stage = Stage()
    stage.execute()


if __name__ == "__main__":

    try:
        main()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: exiting program.")
        sys.exit(1)

# output execution time
print("Total execution time: ", datetime.now() - startTime)