import click
import fiona
import geopandas as gpd
import logging
import os
import pandas as pd
import sys
import uuid
import yaml

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
        self.stage = 1
        self.source = source.lower()

        # Configure raw data path.
        self.data_path = os.path.abspath("../../data/raw/{}".format(self.source))

        # Configure source attribute path.
        self.source_attribute_path = os.path.abspath("sources/{}".format(self.source))

        # Create temp dir.
        self.temp_dir = helpers.create_temp_directory(self.stage)

    def gen_source_geodataframes(self):
        """Loads input data into a GeoPandas dataframe."""

        logger.info("Loading input data as geodataframes.")
        self.source_gdframes = dict()

        for source, source_yaml in self.source_attributes.items():
            # Configure filename attribute absolute path.
            source_yaml["data"]["filename"] = os.path.join(self.data_path, source_yaml["data"]["filename"])

            # Convert None layer attribute to python NoneType.
            if source_yaml["data"]["layer"].upper() == "NONE":
                source_yaml["data"]["layer"] = None

            # Load source into geodataframe.
            self.source_gdframes[source] = gpd.read_file(**source_yaml["data"])
            logger.info("Successfully loaded geodataframe for {}, layer={}".format(
                os.path.basename(source_yaml["data"]["filename"]), source_yaml["data"]["layer"]))

            # Add uuid field.
            logger.info("Adding temporary uuid field to geodataframe.")
            self.source_gdframes[source]["UUID"] = [uuid.uuid4().hex for _ in range(len(self.source_gdframes[source]))]

    def gen_target_geodataframes(self):
        """Creates empty geodataframes for all applicable output tables based on the input data field mappings."""

        logger.info("Compiling applicable target table names.")
        target_tables = list()

        for source, source_yaml in self.source_attributes.items():
            for table in source_yaml["conform"]:
                target_tables.append(table)

        logger.info("Creating target geodataframes for applicable tables.")
        self.target_gdframes = dict()

        for table in target_tables:
            self.target_gdframes[table] = gpd.GeoDataFrame({field: pd.Series(dtype=self.target_attributes[table][field])
                                                            for field in self.target_attributes[table]})

    def load_source_attributes(self):
        """Loads the yaml files in the sources' directory into a dictionary."""

        logger.info("Identifying source attribute files.")
        files = [os.path.join(self.source_attribute_path, f) for f in os.listdir(self.source_attribute_path) if
                 f.endswith(".yaml")]

        logger.info("Loading source attribute yamls.")
        self.source_attributes = dict()

        for f in files:
            # Load yaml.
            with open(f, "r") as source_attributes_file:
                try:
                    source_attributes_yaml = yaml.safe_load(source_attributes_file)
                except yaml.YAMLError:
                    logger.error("Unable to load yaml file: {}.".format(f))

            # Store yaml contents.
            self.source_attributes[os.path.splitext(os.path.basename(f))[0]] = source_attributes_yaml

    def load_target_attributes(self):
        """Loads the target (distribution format) yaml file into a dictionary."""

        logger.info("Loading target attribute yaml.")
        self.target_attributes = dict()
        target_attributes_path = os.path.abspath("../distribution_format.yaml")

        # Load yaml.
        with open(target_attributes_path, "r") as target_attributes_file:
            try:
                target_attributes_yaml = yaml.safe_load(target_attributes_file)
            except yaml.YAMLError:
                logger.error("Unable to load yaml file: {}".format(target_attributes_path))

        logger.info("Compiling attributes for target tables.")
        # Store yaml contents for all contained table names.
        for table in target_attributes_yaml:
            self.target_attributes[table] = dict()

            for field, vals in target_attributes_yaml[table].items():
                # Compile field attributes.
                try:
                    self.target_attributes[table][field] = str(vals[0])
                except ValueError:
                    logger.error("Invalid schema definition for table: {}, field: {}".format(table, field))

    def execute(self):
        """Executes an NRN stage."""

        self.load_source_attributes()
        self.load_target_attributes()
        self.gen_source_geodataframes()
        self.gen_target_geodataframes()

        for source in self.source_gdframes:
            sys.stdout.write("\nSOURCE:\n")
            sys.stdout.write(str(self.source_gdframes[source]))
        for target in self.target_gdframes:
            sys.stdout.write("\nTARGET:\n")
            sys.stdout.write(str(self.target_gdframes[target]))


@click.command()
@click.argument("source", type=click.Choice(["ab", "bc", "mb", "nb", "nl", "ns", "nt", "nu", "on", "pe", "qc", "sk",
                                             "yt", "parks_canada"], case_sensitive=False))
def main(source):
    """Executes an NRN stage."""

    logger.info("Started.")

    stage = Stage(source)
    stage.execute()

    logger.info("Finished.")

if __name__ == "__main__":
    try:

        main()

    except KeyboardInterrupt:
        sys.stdout.write("KeyboardInterrupt exception: exiting program.")
