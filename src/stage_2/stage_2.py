import click
import fiona
import geopandas as gpd
import logging
import networkx as nx
import os
import pandas as pd
import shutil
import subprocess
import sys
import urllib.request
import uuid
import zipfile
from datetime import datetime
from geopandas_postgis import PostGIS
from psycopg2 import connect, extensions, sql
from shapely.geometry.multipoint import MultiPoint
from shapely.geometry.point import Point
from sqlalchemy import *
from sqlalchemy.engine.url import URL

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import helpers


# Set logger.
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)


class Stage:
    """Defines an NRN stage."""

    def __init__(self, source):
        self.stage = 2
        self.source = source.lower()

        # Configure and validate input data path.
        self.data_path = os.path.abspath("../../data/interim/{}.gpkg".format(self.source))
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
            logger.info("Dropping PostgreSQL database.")
            cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {};").format(sql.Identifier(nrn_db)))
        except Exception:
            logger.exception("Could not drop database.")

        try:
            logger.info("Creating PostgreSQL database.")
            cursor.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(nrn_db)))
        except Exception:
            logger.exception("Failed to create PostgreSQL database.")

        logger.info("Closing default PostgreSQL connection.")
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
            logger.info("Creating spatially enabled PostgreSQL database.")
            nrn_cursor.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS postgis;"))
        except Exception:
            logger.exception("Cannot create PostGIS extension.")

        logger.info("Closing NRN PostgreSQL connection.")
        nrn_cursor.close()
        nrn_conn.close()

    def load_gpkg(self):
        """Loads input GeoPackage layers into dataframes."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

    def gen_dead_end(self):
        """Generates dead end junctions with NetworkX."""

        logger.info("Convert roadseg geodataframe to NetX graph.")
        graph = helpers.gdf_to_nx(self.dframes["roadseg"])

        logger.info("Create an empty graph for dead ends junctions.")
        dead_ends = nx.Graph()

        logger.info("Applying CRS EPSG:4617 to dead ends graph.")
        dead_ends.graph['crs'] = self.dframes["roadseg"].crs

        logger.info("Filter for dead end junctions.")
        dead_ends_filter = [node for node, degree in graph.degree() if degree == 1]

        logger.info("Insert filtered dead end junctions into empty graph.")
        dead_ends.add_nodes_from(dead_ends_filter)

        logger.info("Convert dead end graph to geodataframe.")
        self.dead_end_gdf = helpers.nx_to_gdf(dead_ends, nodes=True, edges=False)

        logger.info("Apply dead end junctype to junctions.")
        self.dead_end_gdf["junctype"] = "Dead End"

    def gen_intersections(self):
        """Generates intersection junction types."""

        logger.info("Importing roadseg geodataframe into PostGIS.")
        self.dframes["roadseg"].postgis.to_postgis(con=self.engine, table_name="stage_{}".format(self.stage),
                                                   geometry="LineString", if_exists="replace", index=False)

        logger.info("Loading SQL yaml.")
        self.sql = helpers.load_yaml("../sql.yaml")

        # source:
        # https://gis.stackexchange.com/questions/20835/identifying-road-intersections-using-postgis
        logger.info("Executing SQL injection for junction intersections.")
        inter_filter = self.sql["intersections"]["query"].format(self.stage)

        logger.info("Creating junction intersection geodataframe.")
        self.inter_gdf = gpd.GeoDataFrame.from_postgis(inter_filter, self.engine, geom_col="geometry")

        logger.info("Apply intersection junctype to junctions.")
        self.inter_gdf["junctype"] = "Intersection"
        self.inter_gdf.crs = self.dframes["roadseg"].crs

    def gen_ferry(self):
        """Generates ferry junctions with NetworkX."""

        logger.info("Convert ferryseg geodataframe to NetX graph.")
        graph = helpers.gdf_to_nx(self.dframes["ferryseg"], endpoints_only=True)

        logger.info("Convert dead end graph to geodataframe.")
        self.ferry_gdf = helpers.nx_to_gdf(graph, nodes=True, edges=False)

        logger.info("Apply dead end junctype to junctions.")
        self.ferry_gdf["junctype"] = "Ferry"

    def compile_target_attributes(self):
        """Compiles the target (distribution format) yaml file into a dictionary."""

        logger.info("Compiling target attribute yaml.")
        table = field = None

        # Load yaml.
        self.target_attributes = helpers.load_yaml(os.path.abspath("../distribution_format.yaml"))

        # Remove field length from dtype attribute.
        logger.info("Configuring target attributes.")
        try:

            for table in self.target_attributes:
                for field, vals in self.target_attributes[table]["fields"].items():
                    self.target_attributes[table]["fields"][field] = vals[0]

        except (AttributeError, KeyError, ValueError):
            logger.exception("Invalid schema definition for table: {}, field: {}.".format(table, field))
            sys.exit(1)

    def gen_target_junction(self):

        self.junctions = gpd.GeoDataFrame()

        self.junctions = self.junctions.assign(**{field: pd.Series(dtype=dtype) for field, dtype in
                                                  self.target_attributes["junction"]["fields"].items()})

    def combine(self):
        """Combine geodataframes."""

        logger.info("Combining ferry, dead end and intersection junctions.")
        combine = gpd.GeoDataFrame(pd.concat([self.ferry_gdf, self.dead_end_gdf, self.inter_gdf], sort=False))
        combine = combine[['junctype', 'geometry']]
        self.junctions = self.junctions.append(combine)
        self.junctions.crs = self.dframes["roadseg"].crs

        # source:
        # https://gis.stackexchange.com/questions/311320/casting-geometry-to-multi-using-geopandas
        self.junctions["geometry"] = [MultiPoint([feature]) if type(feature) == Point else feature for feature in
                                      self.junctions["geometry"]]
        self.ferry_gdf["geometry"] = [MultiPoint([feature]) if type(feature) == Point else feature for feature in
                                      self.ferry_gdf["geometry"]]

        logger.info("Importing merged junctions into PostGIS.")
        self.junctions.postgis.to_postgis(con=self.engine, table_name='stage_{}_junc'.format(self.stage),
                                          geometry='MULTIPOINT', if_exists='replace')
        self.ferry_gdf.postgis.to_postgis(con=self.engine, table_name='stage_{}_ferry_junc'.format(self.stage),
                                          geometry='MULTIPOINT', if_exists='replace')

    def fix_junctype(self):
        """Fix junctype of junctions outside of administrative boundaries."""

        # Download administrative boundary file.
        logger.info("Downloading administrative boundary file.")
        adm_file = "http://www12.statcan.gc.ca/census-recensement/2011/geo/bound-limit/files-fichiers/2016/" \
                   "lpr_000a16a_e.zip"
        try:
            urllib.request.urlretrieve(adm_file, '../../data/raw/boundary.zip')
        except urllib.error.URLError as e:
            logger.exception("Unable to download administrative boundary file: \"{}\".".format(adm_file))
            logger.exception("urllib error: {}".format(e))
            sys.exit(1)

        # Extract zipped file.
        logger.info("Extracting zipped administrative boundary file.")
        with zipfile.ZipFile("../../data/raw/boundary.zip", "r") as zip_ref:
            zip_ref.extractall("../../data/raw/boundary")

        # Transform administrative boundary file to GeoPackage layer with crs EPSG:4617.
        logger.info("Transforming administrative boundary file.")
        try:
            subprocess.run("ogr2ogr -f GPKG -where PRUID='{}' ../../data/raw/boundary.gpkg "
                           "../../data/raw/boundary/lpr_000a16a_e.shp -t_srs EPSG:4617 -nlt MULTIPOLYGON -nln {} "
                           "-lco overwrite=yes "
                           .format({"ab": 48, "bc": 59, "mb": 46, "nb": 13, "nl": 10, "ns": 12, "nt": 61, "nu": 62,
                                    "on": 35, "pe": 11, "qc": 24, "sk": 47, "yt": 60}[self.source], self.source))
        except subprocess.CalledProcessError as e:
            logger.exception("Unable to transform data source to EPSG:4617.")
            logger.exception("ogr2ogr error: {}".format(e))
            sys.exit(1)

        logger.info("Remove temporary administrative boundary files and directories.")
        paths = ["../../data/raw/boundary", "../../data/raw/boundary.zip"]
        for path in paths:
            if os.path.exists(path):
                try:
                    os.remove(path) if os.path.isfile(path) else shutil.rmtree(path)
                except OSError as e:
                    logger.warning("Unable to remove directory: \"{}\".".format(os.path.abspath(paths[0])))
                    logger.warning("OSError: {}.".format(e))
                    continue

        bound_adm = gpd.read_file("../../data/raw/boundary.gpkg", layer=self.source)
        bound_adm.crs = self.dframes["roadseg"].crs

        logger.info("Importing administrative boundary into PostGIS.")
        bound_adm.postgis.to_postgis(con=self.engine, table_name="adm", if_exists="replace", geometry='MultiPolygon')

        attr_fix = self.sql["attributes"]["query"].format(self.stage)

        logger.info("Testing for junction equality and altering attributes.")
        self.attr_equality = gpd.GeoDataFrame.from_postgis(attr_fix, self.engine, geom_col="geom")
        self.attr_equality = self.attr_equality.rename(columns={"geom": "geometry"}).set_geometry("geometry")

    def gen_junctions(self):
        """Generate final dataset."""

        # Set standard field values.
        self.attr_equality["uuid"] = [uuid.uuid4().hex for _ in range(len(self.attr_equality))]
        self.attr_equality["credate"] = datetime.today().strftime("%Y%m%d")
        self.attr_equality["datasetnam"] = self.dframes["roadseg"]["datasetnam"][0]
        self.dframes["junction"] = self.attr_equality

        # Apply field domains.
        self.apply_domains()

        # Convert geometry from multipoint to point.
        if self.dframes["junction"].geom_type[0] == "MultiPoint":
            self.multipoint_to_point()

    def apply_domains(self):
        """Applies the field domains to each column in the target dataframes."""

        logging.info("Applying field domains to junction.")
        defaults = helpers.compile_default_values()
        dtypes = helpers.compile_dtypes()
        field = None

        try:

            for field, domains in defaults["junction"].items():

                logger.info("Target field \"{}\": Applying domain.".format(field))

                # Apply domains to dataframe.
                default = defaults["junction"][field]
                self.dframes["junction"][field] = self.dframes["junction"][field].map(
                    lambda val: default if val == "" or pd.isna(val) else val)

                # Force adjust data type.
                self.dframes["junction"][field] = self.dframes["junction"][field].astype(dtypes["junction"][field])

        except (AttributeError, KeyError, ValueError):
            logger.exception("Invalid schema definition for table: junction, field: {}.".format(field))
            sys.exit(1)

    def multipoint_to_point(self):
        """Converts junction geometry from multipoint to point."""

        self.dframes["junction"]["geometry"] = self.dframes["junction"]["geometry"].map(lambda geom: geom[0])

    def export_gpkg(self):
        """Exports the junctions dataframe as a GeoPackage layer."""

        logger.info("Exporting junctions dataframe to GeoPackage layer.")

        # Export junctions dataframe to GeoPackage layer.
        helpers.export_gpkg({"junction": self.dframes["junction"]}, self.data_path)

    def execute(self):
        """Executes an NRN stage."""

        self.create_db()
        self.load_gpkg()
        self.gen_dead_end()
        self.gen_intersections()
        self.gen_ferry()
        self.compile_target_attributes()
        self.gen_target_junction()
        self.combine()
        self.fix_junctype()
        self.gen_junctions()
        self.export_gpkg()


@click.command()
@click.argument("source", type=click.Choice("ab bc mb nb nl ns nt nu on pe qc sk yt parks_canada".split(), False))
def main(source):
    """Executes an NRN stage."""

    try:

        with helpers.Timer():
            stage = Stage(source)
            stage.execute()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: exiting program.")
        sys.exit(1)

if __name__ == "__main__":
    main()
