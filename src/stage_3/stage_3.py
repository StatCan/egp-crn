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

            # Group tables by geometry type.
            self.df_by_type = dict()
            self.df_by_type["lines"] = {name: df.copy(deep=True) for name, df in self.dframes.items() if name in
                                        ("ferryseg", "roadseg")}
            self.df_by_type["points"] = {name: df.copy(deep=True) for name, df in self.dframes.items() if name in
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

        # Define validation logger.
        validation_logger = logging.getLogger()
        validation_logger.setLevel(logging.INFO)
        handler = logging.FileHandler(os.path.abspath("../../data/interim/stage_{}_{}.log"
                                                      .format(self.stage, self.source)))
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
        validation_logger.addHandler(handler)

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
                            validation_logger.info(
                                self.flag_messages_yaml[validation][flag_typ][code].format(name, vals))

                    else:

                        # Log messages.
                        vals = "\n".join(map(str, flags))
                        validation_logger.info(self.flag_messages_yaml[validation][flag_typ][1].format(name, vals))

    def validations(self):
        """Applies a set of geometry-based validations unique to one or more fields and / or tables."""

        logger.info("Applying geometry validations.")

        try:

            # Iterate standard validations.
            # Description: validations that apply to any single dataset and only capture errors.

            funcs_by_type = {"lines": ("identify_duplicate_lines", "validate_line_proximity",
                                       "validate_line_merging_angle", "validate_line_endpoint_clustering",
                                       "validate_line_length"),
                             "points": ("identify_duplicate_points", "validate_point_proximity")}

            for geom_type, funcs in funcs_by_type.items():
                for func in funcs:
                    for table, df in self.df_by_type[geom_type].items():

                        logger.info("Applying validation: {}. Target dataframe: {}."
                                    .format(func.replace("_", " "), table))

                        # Apply function.
                        args = (df.copy(deep=True),)
                        self.flags[table]["errors"][func] = eval("validation_functions.{}(*args)".format(func))

            # Iterate non-standard validations.
            # Description: validations that are (any): dataset-specific, receive multiple parameters,
            # capture modifications.

            # Iterate non-standard validations - errors only.
            funcs = {"identify_isolated_lines": {"tables": ["roadseg", "ferryseg"], "kwargs": {}},
                     "validate_ferry_road_connectivity": {"tables": ["ferryseg", "roadseg", "junction"], "kwargs": {}},
                     "validate_road_structures": {"tables": ["roadseg", "junction"],
                                                  "kwargs": {"default": self.defaults["roadseg"]}},
                     "validate_deadend_disjoint_proximity": {"tables": ["junction", "roadseg"], "kwargs": {}}}

            for func, params in funcs.items():

                logger.info("Applying validation: {}. Target dataframe: {}."
                            .format(func.replace("_", " "), params["tables"][0]))

                # Verify dataframe availability.
                missing = [name for name in params["tables"] if name not in self.dframes]
                if any(missing):
                    logger.warning("Missing required layer(s): {}. Skipping validation."
                                   .format(", ".join(map("\"{}\"".format, missing))))
                    continue

                else:
                    # Apply function.
                    args = map(deepcopy, itemgetter(*params["tables"])(self.dframes))
                    kwargs = params["kwargs"]
                    self.flags[params["tables"][0]]["errors"][func] = eval("validation_functions.{}(*args, **kwargs)"
                                                                           .format(func))

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
