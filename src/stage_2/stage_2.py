import click
import fiona
import geopandas as gpd
import logging
import networkx as nx
import numpy as np
import os
import pandas as pd
import psycopg2
import requests
import shutil
import sqlalchemy.exc
import sys
import uuid
import zipfile
from datetime import datetime
from itertools import chain
from operator import itemgetter
from psycopg2 import connect, extensions, sql
from scipy.spatial import cKDTree
from shapely.geometry import MultiPoint, Point
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
        self.junctions_dframes = dict()

        # Configure and validate input data path.
        self.data_path = os.path.abspath("../../data/interim/{}.gpkg".format(self.source))
        if not os.path.exists(self.data_path):
            logger.exception("Input data not found: \"{}\".".format(self.data_path))
            sys.exit(1)

        # Compile database configuration variables.
        self.db_config = helpers.load_yaml(os.path.abspath("db_config.yaml"))

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

    def create_db(self):
        """Creates the PostGIS database needed for Stage 2."""

        logger.info("Establishing default PostgreSQL database connection.")

        # database name which will be used for stage 2
        nrn_db = "nrn"

        # default postgres connection needed to create the nrn database
        conn = connect(
            dbname=self.db_config["dbname"],
            user=self.db_config["user"],
            host=self.db_config["host"],
            password=self.db_config["password"]
        )

        # postgres database url for geoprocessing
        nrn_url = URL(
            drivername=self.db_config["drivername"],
            host=self.db_config["host"],
            database=nrn_db,
            username=self.db_config["username"],
            port=self.db_config["port"],
            password=self.db_config["password"]
        )

        # engine to connect to nrn database
        self.engine = create_engine(nrn_url)

        # get the isolation level for autocommit
        autocommit = extensions.ISOLATION_LEVEL_AUTOCOMMIT

        # set the isolation level for the connection's cursors (otherwise ActiveSqlTransaction exception will be raised)
        conn.set_isolation_level(autocommit)

        # connect to default connection
        cursor = conn.cursor()

        # drop the nrn database if it exists, then create it if not
        logger.info("Creating / replacing database: {}.".format(nrn_db))

        try:
            cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {};").format(sql.Identifier(nrn_db)))
        except (psycopg2.Error, sqlalchemy.exc.SQLAlchemyError) as e:
            logger.exception("Unable to drop database: {}.".format(nrn_db))
            logger.exception(e)
            sys.exit(1)

        try:
            cursor.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(nrn_db)))
        except (psycopg2.Error, sqlalchemy.exc.SQLAlchemyError) as e:
            logger.exception("Unable to create database: {}.".format(nrn_db))
            logger.exception(e)
            sys.exit(1)

        # Close default postgresql connection.
        cursor.close()
        conn.close()

        logger.info("Establishing database connection: {}.".format(nrn_db))

        # connection parameters for newly created database
        nrn_conn = connect(
            dbname=nrn_db,
            user=self.db_config["user"],
            host=self.db_config["host"],
            password=self.db_config["password"]
        )

        nrn_conn.set_isolation_level(autocommit)

        # create postgis extension.
        logger.info("Creating PostGIS spatial extension for database: {}.".format(nrn_db))

        nrn_cursor = nrn_conn.cursor()
        try:
            nrn_cursor.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS postgis;"))
        except (psycopg2.Error, sqlalchemy.exc.SQLAlchemyError) as e:
            logger.exception("Unable to create PostGIS extension.")
            logger.exception(e)
            sys.exit(1)

        nrn_cursor.close()
        nrn_conn.close()

    def load_gpkg(self):
        """Loads input GeoPackage layers into dataframes."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

    def gen_dead_end(self):
        """Generates dead end junctions with NetworkX."""

        logger.info("Generating junction type: Dead End.")

        # Convert roadseg to networkx graph.
        g = helpers.gdf_to_nx(self.dframes["roadseg"])

        # Create empty networkx graph.
        dead_ends = nx.Graph()
        dead_ends.graph["crs"] = g.graph["crs"]

        # Identify and populate empty graph with dead end nodes.
        dead_ends_filter = [node for node, degree in g.degree() if degree == 1]
        dead_ends.add_nodes_from(dead_ends_filter)

        # Convert dead end graph to geodataframe.
        self.junctions_dframes["deadend"] = helpers.nx_to_gdf(dead_ends, nodes=True, edges=False)

        # Populate junctype field.
        self.junctions_dframes["deadend"]["junctype"] = "Dead End"

    def gen_intersections(self):
        """Generates intersection junction types."""

        logger.info("Generating junction type: Intersection.")

        # Import roadseg into postgis database.
        helpers.gdf_to_postgis(self.dframes["roadseg"], name="stage_{}".format(self.stage), engine=self.engine,
                               if_exists="replace", index=False)

        # Load sql yaml.
        self.sql = helpers.load_yaml("sql.yaml")

        # Inject sql filter into postgis database to retrieve intersection junctions.
        # Retrieve results as geodataframe.
        inter_filter = self.sql["intersections"]["query"].format(self.stage)
        self.junctions_dframes["intersection"] = gpd.GeoDataFrame.from_postgis(inter_filter, self.engine,
                                                                               geom_col="geometry")

        # Populate junctype field.
        self.junctions_dframes["intersection"]["junctype"] = "Intersection"

    def gen_ferry(self):
        """Generates ferry junctions."""

        if "ferryseg" in self.dframes:

            logger.info("Generating junction type: Ferry.")
            df = self.dframes["ferryseg"].copy(deep=True)

            # Duplicate and concatenate ferryseg records, keeping the first and last points as separate records.
            self.junctions_dframes["ferry"] = gpd.GeoDataFrame(pd.concat([
                gpd.GeoDataFrame(df, geometry=df["geometry"].map(lambda g: Point(g.coords[0]))).copy(deep=True),
                gpd.GeoDataFrame(df, geometry=df["geometry"].map(lambda g: Point(g.coords[-1]))).copy(deep=True)
            ]))

            # Populate junctype field.
            self.junctions_dframes["ferry"]["junctype"] = "Ferry"

    def compile_target_attributes(self):
        """Compiles the target (distribution format) yaml file into a dictionary."""

        logger.info("Compiling target attributes yaml.")
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

        logger.info("Creating target dataframe.")

        self.junctions = gpd.GeoDataFrame().assign(**{field: pd.Series(dtype=dtype) for field, dtype in
                                                      self.target_attributes["junction"]["fields"].items()})

    def combine(self):
        """Combine geodataframes for all junction types."""

        logger.info("Combining junction types into a single geodataframe.")

        # Combine junction types.
        combine = gpd.GeoDataFrame(pd.concat(self.junctions_dframes, sort=False))
        self.junctions = self.junctions.append(combine[["junctype", "geometry"]], ignore_index=True, sort=False)
        self.junctions.crs = self.dframes["roadseg"].crs

        # Cast geometry to MultiPoint.
        self.junctions["geometry"] = [MultiPoint([feature]) if type(feature) == Point else feature for feature in
                                      self.junctions["geometry"]]

        # Export junctions to PostGIS.
        helpers.gdf_to_postgis(self.junctions, name="stage_{}_junc".format(self.stage), engine=self.engine,
                               if_exists="replace")

    def fix_junctype(self):
        """
        Fix junctype for:
        1) NatProvTer: junctions outside of administrative boundaries.
        2) Self-intersections.
        """

        logger.info("Classifying NatProvTer and fixing self-intersecting junctions.")

        # Download administrative boundary file.
        logger.info("Downloading administrative boundary file.")
        source = helpers.load_yaml("../downloads.yaml")["provincial_boundaries"]
        download_url, filename = itemgetter("url", "filename")(source)

        try:

            # Get raw content stream from download url.
            download = helpers.get_url(download_url, stream=True, timeout=30)

            # Copy download content to file.
            with open("../../data/interim/boundaries.zip", "wb") as f:
                shutil.copyfileobj(download.raw, f)

        except (requests.exceptions.RequestException, shutil.Error) as e:
            logger.exception("Unable to download administrative boundary file: \"{}\".".format(download_url))
            logger.exception(e)
            sys.exit(1)

        # Extract zipped file.
        logger.info("Extracting zipped administrative boundary file.")
        with zipfile.ZipFile("../../data/interim/boundaries.zip", "r") as zip:
            zip.extractall("../../data/interim/boundaries")

        # Transform administrative boundary file to GeoPackage layer with crs EPSG:4617.
        logger.info("Transforming administrative boundary file.")
        helpers.ogr2ogr({
            "query": "-where \"\\\"PRUID\\\"='{}'\"".format(
                {"ab": 48, "bc": 59, "mb": 46, "nb": 13, "nl": 10, "ns": 12, "nt": 61, "nu": 62, "on": 35, "pe": 11,
                 "qc": 24, "sk": 47, "yt": 60}[self.source]),
            "dest": os.path.abspath("../../data/interim/boundaries.geojson"),
            "src": os.path.abspath("../../data/interim/boundaries/{}".format(filename)),
            "options": "-t_srs EPSG:4617 -nlt MULTIPOLYGON"
        })

        # Load boundaries into PostGIS.
        logger.info("Importing administrative boundary into PostGIS.")
        bound_adm = gpd.read_file("../../data/interim/boundaries.geojson", crs=self.dframes["roadseg"].crs)
        helpers.gdf_to_postgis(bound_adm, name="adm", engine=self.engine, if_exists="replace")

        # Update junctions via postgis sql injection.
        # Retrieve updated junctions as geodataframe.
        logger.info("Applying junction updates.")

        attr_fix = self.sql["attributes"]["query"].format(self.stage)

        self.attr_equality = gpd.GeoDataFrame.from_postgis(attr_fix, self.engine, geom_col="geom")
        self.attr_equality = self.attr_equality.rename(columns={"geom": "geometry"}).set_geometry("geometry")
        self.dframes["junction"] = self.attr_equality.copy(deep=True)

        # Remove temporary files.
        logger.info("Removing temporary administrative boundary files and directories.")
        for f in os.listdir("../../data/interim"):
            if os.path.splitext(f)[0] == "boundaries":
                path = os.path.join("../../data/interim", f)
                try:
                    os.remove(path) if os.path.isfile(path) else shutil.rmtree(path)
                except (OSError, shutil.Error) as e:
                    logger.warning("Unable to remove directory or file: \"{}\".".format(os.path.abspath(path)))
                    logger.warning(e)
                    continue

    def gen_output(self):
        """Generate final dataset."""

        logger.info("Generating final output dataset.")

        def compute_connected_attribute(junction, attribute):
            """
            Computes the given attribute from connected features to the given junction dataframe.
            Currently supported attributes: 'accuracy', 'exitnbr'.
            """

            # Validate input attribute.
            if attribute not in ("accuracy", "exitnbr"):
                logger.exception("Unsupported attribute provided: {}.".format(attribute))
                sys.exit(1)

            # Compile default field value.
            default = helpers.compile_default_values()["junction"][attribute]

            # Concatenate ferryseg and roadseg, if possible.
            if "ferryseg" in self.dframes:
                df = gpd.GeoDataFrame(pd.concat(itemgetter("ferryseg", "roadseg")(self.dframes), ignore_index=False,
                                                sort=False))
            else:
                df = self.dframes["roadseg"].copy(deep=True)

            # Generate kdtree.
            tree = cKDTree(np.concatenate([np.array(geom.coords) for geom in df["geometry"]]))

            # Compile indexes of segments at 0 meters distance from each junction. These represent connected segments.
            connected_idx = junction["geometry"].map(lambda geom: list(chain(*tree.query_ball_point(geom.coords, r=0))))

            # Construct a uuid series aligned to the series of segment points.
            pts_uuid = np.concatenate([[uuid] * count for uuid, count in
                                       df["geometry"].map(lambda geom: len(geom.coords)).iteritems()])

            # Retrieve the uuid associated with the connected indexes.
            connected_uuid = connected_idx.map(lambda index: itemgetter(*index)(pts_uuid))

            # Compile the attribute for all segment uuids.
            attribute_uuid = df[attribute].to_dict()

            # Convert associated uuids to attributes.
            # Return a series of the attribute default if an unsupported attribute was specified.

            # Attribute: accuracy.
            if attribute == "accuracy":
                connected_attribute = connected_uuid.map(
                    lambda uuid: max(itemgetter(*uuid)(attribute_uuid)) if isinstance(uuid, tuple) else
                    itemgetter(uuid)(attribute_uuid))

            # Attribute: exitnbr.
            if attribute == "exitnbr":
                connected_attribute = connected_uuid.map(
                    lambda uuid: tuple(set(itemgetter(*uuid)(attribute_uuid))) if isinstance(uuid, tuple) else
                    (itemgetter(uuid)(attribute_uuid),))

                # Concatenate, sort, and remove invalid attribute tuples.
                connected_attribute = connected_attribute.map(
                    lambda vals: ", ".join(sorted([str(val) for val in vals if val != default and not pd.isna(val)])))

            # Populate empty results with default.
            connected_attribute = connected_attribute.map(lambda val: val if len(str(val)) else default)

            return connected_attribute.copy(deep=True)

        # Convert geometry from multipoint to point.
        if self.dframes["junction"].geom_type.iloc[0] == "MultiPoint":
            self.dframes["junction"]["geometry"] = self.dframes["junction"]["geometry"].map(lambda geom: geom[0])

        # Set additional field values, if possible.
        self.dframes["junction"]["uuid"] = [uuid.uuid4().hex for _ in range(len(self.dframes["junction"]))]
        self.dframes["junction"]["acqtech"] = "Computed"
        self.dframes["junction"]["metacover"] = "Complete"
        self.dframes["junction"]["credate"] = datetime.today().strftime("%Y%m%d")
        self.dframes["junction"]["datasetnam"] = self.dframes["roadseg"]["datasetnam"][0]
        self.dframes["junction"]["accuracy"] = compute_connected_attribute(self.dframes["junction"], "accuracy")
        self.dframes["junction"]["provider"] = "Federal"
        self.dframes["junction"]["exitnbr"] = compute_connected_attribute(self.dframes["junction"], "exitnbr")

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
        self.gen_output()
        self.apply_domains()
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
