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
        self.stage = 4
        self.source = source.lower()

        # Configure and validate input data path.
        self.data_path = os.path.abspath("../../data/interim/{}.gpkg".format(self.source))
        if not os.path.exists(self.data_path):
            logger.exception("Input data not found: \"{}\".".format(self.data_path))
            sys.exit(1)

        # Compile default field values.
        self.defaults = helpers.compile_default_values()

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

    def unique_attr_validation(self):
        """Applies a set of attribute validations unique to one or more fields and / or tables."""

        logger.info("Applying validation set: unique attribute validations.")

        try:

            # Validation: nbrlanes.
            logger.info("Applying validation: nbrlanes. Target dataframe: roadseg.")

            # Apply function.
            self.flags["roadseg"]["errors"]["validate_nbrlanes"] = validation_functions.validate_nbrlanes(
                self.dframes["roadseg"].copy(deep=True), self.defaults["roadseg"]["nbrlanes"])

            # Validation: speed.
            logger.info("Applying validation: speed. Target dataframe: roadseg.")

            # Apply function.
            self.flags["roadseg"]["errors"]["validate_speed"] = validation_functions\
                .validate_speed(self.dframes["roadseg"].copy(deep=True), self.defaults["roadseg"]["speed"])

            # Validation: pavement.
            logger.info("Applying validation: pavement. Target dataframe: roadseg.")

            # Apply function.
            self.flags["roadseg"]["errors"]["validate_pavement"] = validation_functions\
                .validate_pavement(self.dframes["roadseg"].copy(deep=True))

            # Validation: roadclass-rtnumber1.
            for table in ("ferryseg", "roadseg"):
                logger.info("Applying validation: roadclass-rtnumber1. Target dataframe: {}.".format(table))

                # Apply function.
                self.flags[table]["errors"]["validate_roadclass_rtnumber1"] = validation_functions\
                    .validate_roadclass_rtnumber1(
                    self.dframes[table].copy(deep=True), self.defaults[table]["rtnumber1"])

            # Validation: route text.
            for table in ("roadseg", "ferryseg"):
                logger.info("Applying validation: route text. Target dataframe: {}.".format(table))

                # Apply function, store modifications and flags.
                self.dframes[table], self.flags[table]["modifications"]["title_route_text"] = validation_functions\
                    .title_route_text(self.dframes[table].copy(deep=True), self.defaults[table])

            # Validation: route contiguity.
            logger.info("Applying validation: route contiguity. Target dataframe: ferryseg + roadseg.")

            # Apply function.
            self.flags["roadseg"]["errors"]["validate_route_contiguity"] = validation_functions\
                .validate_route_contiguity(
                *map(deepcopy, itemgetter("ferryseg", "roadseg")(self.dframes)), default=self.defaults["roadseg"])

            # Validation: exitnbr-roadclass.
            logger.info("Applying validation: exitnbr-roadclass. Target dataframe: roadseg.")

            # Apply function.
            self.flags["roadseg"]["errors"]["validate_exitnbr_roadclass"] = validation_functions\
                .validate_exitnbr_roadclass(
                self.dframes["roadseg"].copy(deep=True), self.defaults["roadseg"]["exitnbr"])

            # Validation: exitnbr conflict.
            logger.info("Applying validation: exitnbr conflict. Target dataframe: roadseg.")

            # Apply function.
            self.flags["roadseg"]["errors"]["validate_exitnbr_conflict"] = validation_functions\
                .validate_exitnbr_conflict(self.dframes["roadseg"].copy(deep=True), self.defaults["roadseg"]["exitnbr"])

            # Validation: roadclass self-intersection.
            logger.info("Applying validation: roadclass self-intersection. Target dataframe: roadseg.")

            # Apply function.
            self.flags["roadseg"]["errors"]["validate_roadclass_structtype"], \
            self.flags["roadseg"]["errors"]["validate_roadclass_self_intersection"] = validation_functions\
                .validate_roadclass_self_intersection(self.dframes["roadseg"].copy(deep=True),
                                                      self.defaults["roadseg"]["nid"])

        except (KeyError, SyntaxError, ValueError):
            logger.exception("Unable to apply validation.")
            sys.exit(1)

    def universal_attr_validation(self):
        """Applies a set of universal attribute validations (all fields and / or all tables)."""

        logger.info("Applying validation set: universal attribute validations.")

        # Iterate data frames.
        for name, df in self.dframes.items():

            try:

                # Validation: strip whitespace.
                logger.info("Applying validation: strip whitespace. Target dataframe: {}.".format(name))

                # Apply function.
                # Store modifications and flags.
                df, self.flags[name]["modifications"]["strip_whitespace"] = \
                    validation_functions.strip_whitespace(df.copy(deep=True))

                # Validation: dates.
                logger.info("Applying validation: dates. Target dataframe: {}.".format(name))

                # Apply function.
                results = validation_functions.validate_dates(df.copy(deep=True), self.defaults[name]["credate"])

                # Store modifications, error flags, and modification flags.
                df[["credate", "revdate"]] = results[0]
                self.flags[name]["errors"]["validate_dates"] = results[1]
                self.flags[name]["modifications"]["validate_dates"] = results[2]

                # Validation: ids.
                logger.info("Apply validation: ids. Target dataframe: {}.".format(name))

                # Apply function.
                results = validation_functions.validate_ids(name, df.copy(deep=True), self.defaults[name])

                # Store modifications, error flags, and modification flags.
                df = results[0]
                self.flags[name]["errors"]["validate_ids"] = results[1]
                self.flags[name]["modifications"]["validate_ids"] = results[2]

                # Store results.
                self.dframes[name] = df

            except (KeyError, SyntaxError, ValueError):
                logger.exception("Unable to apply validation.")
                sys.exit(1)

    def validate_tables(self):
        """Validates the required GeoPackage layers."""

        try:

            for table in ("ferryseg", "roadseg"):
                if table not in self.dframes:
                    raise KeyError("Missing required layers: \"{}\".".format(table))

        except KeyError:
            sys.exit(1)

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.gen_flag_variables()
        self.validate_tables()
        self.universal_attr_validation()
        self.unique_attr_validation()
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
