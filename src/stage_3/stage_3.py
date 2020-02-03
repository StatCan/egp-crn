import click
import fiona
import geopandas as gpd
import logging
import numpy as np
import os
import pandas as pd
import re
import sys
from itertools import chain

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
            self.df_lines = {name: df for name, df in self.dframes.items() if name in ("ferryseg", "roadseg")}
            self.df_points = {name: df for name, df in self.dframes.items() if name in ("blkpassage", "junction",
                                                                                        "tollpoint")}

        except (KeyError, SyntaxError, ValueError):
            logger.exception("Unable to compile dataframes.")
            sys.exit(1)

    def export_gpkg(self):
        """Exports the dataframes as GeoPackage layers."""

        logger.info("Exporting dataframes to GeoPackage layers.")

        # Export target dataframes to GeoPackage layers.
        helpers.export_gpkg(self.dframes, self.data_path)

    def gen_flag_variables(self):
        """Generates variables required for storing and logging error and modification flags for records."""

        logger.info("Generating flag variables.")

        # Create flag dataframes for each gpkg dataframe.
        self.flags = {name: pd.DataFrame(index=df.index) for name, df in self.dframes.items()}

        # Create custom key for error / mod messages that aren't uuid based.
        self.flags["custom"] = dict()

        # Load flag messages yaml.
        self.flag_messages_yaml = helpers.load_yaml(os.path.abspath("flag_messages.yaml"))

    def load_gpkg(self):
        """Loads input GeoPackage layers into dataframes."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

    def log_messages(self):
        """Logs any errors and modification messages flagged by the attribute validations."""

        logger.info("Compiling modification and error logs.")

        # Log standardized modification and error messages.
        for table in [t for t in self.flags.keys() if t != "custom"]:

            # Log message type.
            for typ in ("modifications", "errors"):

                # Iterate message flag columns.
                for col in [c for c in self.flags[table].columns if re.findall("_" + typ + "$", c)]:

                    # Retrieve data series.
                    # Force data type to int.
                    series = self.flags[table][col].astype(int)

                    # Iterate message codes.
                    # Excludes values evaluating to False (i.e. 0, nan, False).
                    for mcode in sorted([code for code in series.unique() if code]):

                        # Log messages.
                        vals = series[series == mcode].index
                        validation = re.sub("_" + typ + "$", "", col)
                        args = [table, validation, "\n".join(vals)]
                        logger.info(self.flag_messages_yaml[validation][typ][mcode].format(*args))

        # Log non-standardized error messages.
        for key, vals in self.flags["custom"].items():
            if self.flags["custom"][key]:

                # Log messages.
                validation = re.sub("_errors$", "", key)
                args = [validation, "\n".join(map(str, vals))]
                logger.info(self.flag_messages_yaml[validation]["errors"][1].format(*args))

    def validations(self):
        """Applies a set of geometry-based validations unique to one or more fields and / or tables."""

        logger.info("Applying geometry validations.")

        try:

            # Validation: identify duplicate line features.
            for table, df in self.df_lines.items():
                logger.info("Applying validation: identify duplicate line features. Target dataframe: {}."
                            .format(table))

                # Apply function.
                self.flags[table]["identify_duplicate_lines_errors"] = validation_functions.identify_duplicate_lines(df)

            # Validation: identify duplicate point features.
            for table, df in self.df_points.items():
                logger.info("Applying validation: identify duplicate point features. Target dataframe: {}."
                            .format(table))

                # Apply function.
                self.flags[table]["identify_duplicate_points_errors"] = \
                    validation_functions.identify_duplicate_points(df)

            # Validation: minimum feature length.
            logger.info("Applying validation: minimum feature length. Target dataframe: roadseg.")

            # Apply function.
            self.flags["roadseg"]["validate_min_length_errors"] = \
                validation_functions.validate_min_length(self.df_lines["roadseg"])

            # Validation: identify isolated line features.
            logger.info("Applying validation: identify isolated line features. Target dataframe: ferryseg + roadseg.")

            # Concatenate dataframes, apply function.
            df = gpd.GeoDataFrame(pd.concat(self.df_lines.values(), ignore_index=False, sort=False))
            self.flags["roadseg"]["identify_isolated_lines_errors"] = validation_functions.identify_isolated_lines(df)

            # Validation: validate ferry-road connectivity.
            logger.info("Applying validation: ferry-road connectivity. Target dataframe: ferryseg.")

            # Apply function.
            self.flags["ferryseg"]["validate_ferry_road_connectivity_errors"] = \
                validation_functions.validate_ferry_road_connectivity(self.dframes["ferryseg"], self.dframes["roadseg"],
                                                                      self.dframes["junction"])

            # Validation: validate road structures.
            logger.info("Applying validation: road structures. Target dataframe: roadseg.")

            # Apply function.
            cols = ["validate_road_structures_{}errors".format(suf) for suf in ("", "2_", "3_", "4_")]
            results = validation_functions.validate_road_structures(self.dframes["roadseg"], self.dframes["junction"],
                                                                    self.defaults["roadseg"])
            self.flags["roadseg"][cols[0]], self.flags["custom"][cols[1]], self.flags["custom"][cols[2]], \
            self.flags["custom"][cols[3]] = results

            # Validation: line proximity.
            for validation in ("line proximity", "line merging angle"):
                for table, df in self.df_lines.items():
                    logger.info("Applying validation: {}. Target dataframe: {}.".format(validation, table))

                    # Apply function.
                    self.flags["custom"]["validate_{}_{}".format(validation.replace(" ", "_"), table)] = \
                        eval("validation_functions.validate_{}(df)".format(validation.replace(" ", "_")))

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
