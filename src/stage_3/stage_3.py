import click
import logging
import os
import pandas as pd
import sys
from copy import deepcopy
from operator import itemgetter

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import validation_functions
import helpers


# Suppress pandas chained assignment warning.
pd.options.mode.chained_assignment = None


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
        self.stage = 3
        self.source = source.lower()

        # Configure and validate input data path.
        self.data_path = os.path.abspath("../../data/interim/{}.gpkg".format(self.source))
        if not os.path.exists(self.data_path):
            logger.exception("Input data not found: \"{}\".".format(self.data_path))
            sys.exit(1)

        # Compile default field values.
        self.defaults = helpers.compile_default_values()

    def compile_tables(self):
        """Compiles the required GeoPackage layers for validation. Groups the layers by geometry type."""

        try:

            # Verify tables.
            for table in ("ferryseg", "junction", "roadseg"):
                if table not in self.dframes:
                    raise KeyError("Missing required layer: \"{}\".".format(table))

            # Group tables by geometry type.
            self.df_lines = {name: df.copy(deep=True) for name, df in self.dframes.items() if name in
                             ("ferryseg", "roadseg")}
            self.df_points = {name: df.copy(deep=True) for name, df in self.dframes.items() if name in
                              ("blkpassage", "junction", "tollpoint")}

        except (KeyError, SyntaxError, ValueError):
            logger.exception("Unable to compile dataframes.")
            sys.exit(1)

    def export_gpkg(self):
        """Exports the dataframes as GeoPackage layers."""

        logger.info("Exporting dataframes to GeoPackage layers.")

        # Filter dataframe which require exporting.
        dframes = dict()
        for name, df in self.dframes.items():
            if any([len(v) for k, v in self.flags[name]["modifications"].items()]):
                dframes[name] = df

        # Export target dataframes to GeoPackage layers.
        if len(dframes):
            helpers.export_gpkg(dframes, self.data_path)
        else:
            logger.info("Export not required, no dataframe modifications detected.")

    def gen_flag_variables(self):
        """Generates variables required for storing and logging error and modification flags for records."""

        logger.info("Generating flag variables.")

        # Create flag dictionary entry for each gpkg dataframe.
        self.flags = {name: {"modifications": dict(), "errors": dict()} for name in self.dframes.keys()}

        # Load flag messages yaml.
        self.flag_messages_yaml = helpers.load_yaml(os.path.abspath("flag_messages.yaml"))

    def load_gpkg(self):
        """Loads input GeoPackage layers into dataframes."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

    def log_messages(self):
        """Logs any errors and modification messages flagged by the attribute validations."""

        logger.info("Compiling modification and error logs.")

        # Iterate dataframe flags.
        for name in self.flags.keys():

            # Iterate non-empty flag types.
            for flag_typ in [typ for typ in ("modifications", "errors") if len(self.flags[name][typ])]:

                # Iterate non-empty flag tables (validations).
                for validation in [val for val, data in self.flags[name][flag_typ].items() if len(data)]:

                    # Retrieve flags.
                    flags = self.flags[name][flag_typ][validation]

                    # Log error messages, iteratively if multiple error codes stored in dictionary.
                    if isinstance(flags, dict):
                        for code, code_flags in [[k, v] for k, v in flags.items() if len(v)]:

                            # Log messages.
                            vals = "\n".join(map(str, code_flags))
                            logger.info(self.flag_messages_yaml[validation][flag_typ][code].format(name, vals))

                    else:

                        # Log messages.
                        vals = "\n".join(map(str, flags))
                        logger.info(self.flag_messages_yaml[validation][flag_typ][1].format(name, vals))

    def validations(self):
        """Applies a set of geometry-based validations unique to one or more fields and / or tables."""

        logger.info("Applying geometry validations.")

        try:

            # Validation: identify duplicate line features.
            for table, df in self.df_lines.items():
                logger.info("Applying validation: identify duplicate line features. Target dataframe: {}."
                            .format(table))

                # Apply function.
                self.flags[table]["errors"]["identify_duplicate_lines"] = validation_functions\
                    .identify_duplicate_lines(df.copy(deep=True))

            # Validation: identify duplicate point features.
            for table, df in self.df_points.items():
                logger.info("Applying validation: identify duplicate point features. Target dataframe: {}."
                            .format(table))

                # Apply function.
                self.flags[table]["errors"]["identify_duplicate_points"] = validation_functions\
                    .identify_duplicate_points(df.copy(deep=True))

            # Validation: minimum feature length.
            logger.info("Applying validation: minimum feature length. Target dataframe: roadseg.")

            # Apply function.
            self.flags["roadseg"]["errors"]["validate_min_length"] = validation_functions\
                .validate_min_length(self.df_lines["roadseg"].copy(deep=True))

            # Validation: identify isolated line features.
            logger.info("Applying validation: identify isolated line features. Target dataframe: ferryseg + roadseg.")

            # Apply function.
            self.flags["roadseg"]["errors"]["identify_isolated_lines"] = validation_functions\
                .identify_isolated_lines(*map(deepcopy, itemgetter("ferryseg", "roadseg")(self.df_lines)))

            # Validation: validate ferry-road connectivity.
            logger.info("Applying validation: ferry-road connectivity. Target dataframe: ferryseg.")

            # Apply function.
            self.flags["ferryseg"]["errors"]["validate_ferry_road_connectivity"] = validation_functions\
                .validate_ferry_road_connectivity(
                *map(deepcopy, itemgetter("ferryseg", "roadseg", "junction")(self.dframes)))

            # Validation: validate road structures.
            logger.info("Applying validation: road structures. Target dataframe: roadseg.")

            # Apply function.
            self.flags["roadseg"]["errors"]["validate_road_structures"] = validation_functions\
                .validate_road_structures(
                *map(deepcopy, itemgetter("roadseg", "junction")(self.dframes)), default=self.defaults["roadseg"])

            # Validation: validate line proximity.
            for table, df in self.df_lines.items():
                logger.info("Applying validation: line proximity. Target dataframe: {}.".format(table))

                # Apply function.
                self.flags[table]["errors"]["validate_line_proximity"] = validation_functions\
                    .validate_line_proximity(df.copy(deep=True))

            # Validation: validate line merging angle.
            for table, df in self.df_lines.items():
                logger.info("Applying validation: line merging angle. Target dataframe: {}.".format(table))

                # Apply function.
                self.flags[table]["errors"]["validate_line_merging_angle"] = validation_functions\
                    .validate_line_merging_angle(df.copy(deep=True))

            # Validation: validate line endpoint clustering.
            for table, df in self.df_lines.items():
                logger.info("Applying validation: line endpoint clustering. Target dataframe: {}.".format(table))

                # Apply function.
                self.flags[table]["errors"]["validate_line_endpoint_clustering"] = validation_functions\
                    .validate_line_endpoint_clustering(df.copy(deep=True))

            # Validation: validate point proximity.
            for table, df in self.df_points.items():
                logger.info("Applying validation: point proximity. Target dataframe: {}.".format(table))

                # Apply function.
                self.flags[table]["errors"]["validate_point_proximity"] = validation_functions\
                    .validate_point_proximity(df.copy(deep=True))

            # Validation: validate deadend-disjoint proximity.
            logger.info("Applying validation: deadend-disjoint proximity. Target dataframe: junction.")

            # Apply function.
            self.flags["junction"]["errors"]["validate_deadend_disjoint_proximity"] = validation_functions\
                .validate_deadend_disjoint_proximity(*map(deepcopy, itemgetter("junction", "roadseg")(self.dframes)))

        except (KeyError, SyntaxError, ValueError):
            logger.exception("Unable to apply validation.")
            sys.exit(1)

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.gen_flag_variables()
        self.compile_tables()
        self.validations()
        self.log_messages()
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
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)

if __name__ == "__main__":
    main()
