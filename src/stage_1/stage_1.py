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
import field_map_functions
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

    def apply_field_mapping(self):
        """Maps the source geodataframes to the target geodataframes via user-specific field mapping functions."""

        logger.info("Applying field mapping.")

        # Retrieve source attributes and geodataframe.
        for source_name, source_attributes in self.source_attributes.items():
            source_gdf = self.source_gdframes[source_name]

            # Retrieve target attributes.
            for target_name in source_attributes["conform"]:
                logger.info("Applying field mapping from {} to {}.".format(source_name, target_name))

                # Retrieve table field mapping attributes.
                map_attributes = source_attributes["conform"][target_name]

                # Field mapping.
                for target_field, source_field in map_attributes.items():

                    # Retrieve target geodataframe.
                    target_gdf = self.target_gdframes[target_name]

                    # No mapping.
                    if source_field is None:
                        logger.info("Target field \"{}\": No mapping provided.".format(target_field))

                    # Non-function mapping.
                    elif isinstance(source_field, str):

                        # Field mapping type: 1:1.
                        if source_field in source_gdf.columns:
                            logger.info("Target field \"{}\": Applying 1:1 field mapping.".format(target_field))

                            target_gdf[target_field] = target_gdf["uuid"].map(
                                source_gdf.set_index("uuid")[source_field])

                        # Field mapping type: raw value.
                        else:
                            logger.info("Target field \"{}\": Applying raw value field mapping.".format(target_field))
                            target_gdf[target_field] = source_field

                    # Advanced function mapping - split_record.
                    elif "split_record" in source_field["functions"].keys():
                        logger.info("Target field \"{}\": Applying advanced function mapping - "
                                    "\"split_record\".".format(target_field))

                        logger.info("\nCONTENT COMING: split_record.\n")

                    # Chained function mapping.
                    else:
                        logger.info("Target field \"{}\": Applying chained function mapping.".format(target_field))

                        # Create mapped dataframe from source and target geodataframes, keeping only the source field.
                        merged_df = target_gdf["uuid"].map(source_gdf.set_index("uuid")[source_field["field"]])

                        # Iterate field mapping functions, storing interim results in merged dataframe.
                        for func, params in source_field["functions"].items():

                            # Function: strip_attribute.
                            if func == "strip_attribute":
                                # . . . .

                            else:
                                merged_df = merged_df.apply(lambda val:
                                                            eval("field_map_functions.{}(val='{}', **{})".format(
                                                                func, val.replace("'", "\\'"), params)))

                        # Update target geodataframe.
                        target_gdf[target_field] = merged_df

                    # Store updated target geodataframe.
                    self.target_gdframes[target_name] = target_gdf

    def gen_source_geodataframes(self):
        """Loads input data into a GeoPandas dataframe."""

        logger.info("Loading input data as geodataframes.")
        self.source_gdframes = dict()

        for source, source_yaml in self.source_attributes.items():
            # Configure filename attribute absolute path.
            source_yaml["data"]["filename"] = os.path.join(self.data_path, source_yaml["data"]["filename"])

            # Load source into geodataframe.
            gdf = gpd.read_file(**source_yaml["data"])

            # Force lowercase field names.
            gdf.columns = map(str.lower, gdf.columns)

            # Add uuid field.
            gdf["uuid"] = [uuid.uuid4().hex for _ in range(len(gdf))]

            # Store result.
            self.source_gdframes[source] = gdf
            logger.info("Successfully loaded geodataframe for {}, layer={}.".format(
                os.path.basename(source_yaml["data"]["filename"]), source_yaml["data"]["layer"]))

    def gen_target_geodataframes(self):
        """Creates empty geodataframes for all applicable output tables based on the input data field mapping."""

        logger.info("Creating target geodataframes for applicable tables.")
        self.target_gdframes = dict()

        # Retrieve target table name from source attributes.
        for source, source_yaml in self.source_attributes.items():
            for table in source_yaml["conform"]:

                logger.info("Creating target geodataframe: {}.".format(table))

                # Generate target geodataframe from source uuid and geometry fields.
                gdf = gpd.GeoDataFrame(self.source_gdframes[source][["uuid"]],
                                       geometry=self.source_gdframes[source].geometry)

                # Add target field schema.
                gdf = gdf.assign(**{field: pd.Series(dtype=self.target_attributes[table][field]) for field in
                                    self.target_attributes[table]})

                # Store result.
                self.target_gdframes[table] = gdf
                logger.info("Successfully created target geodataframe: {}.".format(table))

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
                    sys.exit(1)

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
                logger.error("Unable to load yaml file: {}.".format(target_attributes_path))
                sys.exit(1)

        logger.info("Compiling attributes for target tables.")
        # Store yaml contents for all contained table names.
        for table in target_attributes_yaml:
            self.target_attributes[table] = dict()

            for field, vals in target_attributes_yaml[table].items():
                # Compile field attributes.
                try:
                    self.target_attributes[table][field] = str(vals[0])
                except ValueError:
                    logger.error("Invalid schema definition for table: {}, field: {}.".format(table, field))
                    sys.exit(1)

    def execute(self):
        """Executes an NRN stage."""

        self.load_source_attributes()
        self.load_target_attributes()
        self.gen_source_geodataframes()
        self.gen_target_geodataframes()
        self.apply_field_mapping()


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
        logger.error("KeyboardInterrupt exception: exiting program.")
        sys.exit(1)
