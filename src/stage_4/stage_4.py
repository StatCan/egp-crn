import click
import fiona
import geopandas as gpd
import logging
import numpy as np
import os
import pandas as pd
import sys

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

            for typ in ("modifications", "errors"):

                # Log message type.
                for col in [c for c in self.flags[table].columns if c.endswith(typ[0]) and self.flags[table][c].any()]:

                    # Retrieve data series.
                    series = self.flags[table][col]

                    # Iterate message codes.
                    for mcode in [code for code in series.unique() if code]:

                        # Log messages.
                        vals = series[series == mcode].index
                        args = [table, col.rstrip("_" + typ), "\n".join(vals)]
                        logger.info(self.flag_messages_yaml[col.rstrip("_" + typ)][typ][mcode].format(*args))

        # Log non-standardized error messages.
        for key, vals in self.flags["custom"].items():
            if self.flags["custom"][key]:

                # Log messages.
                args = [key.rstrip("_errors"), "\n".join(map(str, vals))]
                logger.info(self.flag_messages_yaml[key.rstrip("_errors")]["errors"][1].format(*args))

    def unique_attr_validation(self):
        """Applies a set of attribute validations unique to one or more fields and / or tables."""

        logger.info("Applying validation set: unique attribute validations.")

        try:

            # Verify tables.
            for table in ("ferryseg", "roadseg"):
                if table not in self.dframes:
                    raise KeyError("Missing required layer: \"{}\".".format(table))

            # Validation: nbrlanes.
            logger.info("Applying validation: nbrlanes. Target dataframe: roadseg.")

            # Apply function directly to target field.
            self.flags["roadseg"]["validate_nbrlanes_errors"] = self.dframes["roadseg"]["nbrlanes"].map(
                lambda val: validation_functions.validate_nbrlanes(val, default=self.defaults["roadseg"]["nbrlanes"]))

            # Validation: speed.
            logger.info("Applying validation: speed. Target dataframe: roadseg.")

            # Apply function directly to target field.
            self.flags["roadseg"]["validate_speed_errors"] = self.dframes["roadseg"]["speed"].map(
                lambda val: validation_functions.validate_speed(val, default=self.defaults["roadseg"]["speed"]))

            # Validation: pavement.
            logger.info("Applying validation: pavement. Target dataframe: roadseg.")

            # Apply function directly to target fields.
            cols = ["pavstatus", "pavsurf", "unpavsurf"]
            args = [self.dframes["roadseg"][col].values for col in cols]
            self.flags["roadseg"]["validate_pavement_errors"] = np.vectorize(
                validation_functions.validate_pavement)(*args)

            # Validation: roadclass-rtnumber1.
            cols = ["roadclass", "rtnumber1"]
            for table in ("ferryseg", "roadseg"):
                logger.info("Applying validation: roadclass-rtnumber1. Target dataframe: {}.".format(table))

                # Compile valid fields, apply function.
                df = self.dframes[table]
                args = [df[col].values for col in cols] + [self.defaults[table][cols[1]]]
                self.flags[table]["validate_roadclass_rtnumber1_errors"] = np.vectorize(
                    validation_functions.validate_roadclass_rtnumber1)(*args)

            # Validation: route text.
            for table in ("roadseg", "ferryseg"):
                logger.info("Applying validation: route text. Target dataframe: {}.".format(table))

                # Apply function, store results.
                self.dframes[table], self.flags[table]["title_route_text_modifications"] = \
                    validation_functions.title_route_text(self.dframes[table], self.defaults[table])

            # Validation: route contiguity.
            logger.info("Applying validation: route contiguity. Target dataframe: ferryseg + roadseg.")

            # Concatenate dataframes, apply function.
            df = gpd.GeoDataFrame(pd.concat([self.dframes["ferryseg"], self.dframes["roadseg"]], ignore_index=True,
                                            sort=False))
            self.flags["custom"]["validate_route_contiguity_errors"] = validation_functions.validate_route_contiguity(
                df, self.defaults["roadseg"])

            # Validation: exitnbr-roadclass.
            logger.info("Applying validation: exitnbr-roadclass. Target dataframe: roadseg.")

            # Apply function directly to target fields.
            cols = ["exitnbr", "roadclass"]
            args = [self.dframes["roadseg"][col].values for col in cols] + [self.defaults["roadseg"][cols[0]]]
            self.flags["roadseg"]["validate_exitnbr_roadclass_errors"] = np.vectorize(
                validation_functions.validate_exitnbr_roadclass)(*args)

            # Validation: exitnbr conflict.
            logger.info("Applying validation: exitnbr conflict. Target dataframe: roadseg.")

            # Apply function.
            self.flags["custom"]["validate_exitnbr_conflict_errors"] = validation_functions.validate_exitnbr_conflict(
                self.dframes["roadseg"], self.defaults["roadseg"]["exitnbr"])

            # Validation: roadclass self-intersection.
            logger.info("Applying validation: roadclass self-intersection. Target dataframe: roadseg.")

            # Apply function.
            cols = ["validate_roadclass_structtype_errors", "validate_roadclass_self_intersection_errors"]
            self.flags["roadseg"][cols[0]], self.flags["roadseg"][cols[1]] = \
                validation_functions.validate_roadclass_self_intersection(self.dframes["roadseg"],
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

                # Compile valid fields, apply function.
                df_valid = df.select_dtypes(include="object")
                if "geometry" in df_valid.columns:
                    df_valid.drop("geometry", axis=1, inplace=True)
                df[df_valid.columns] = df_valid.applymap(validation_functions.strip_whitespace)

                # Validation: dates.
                logger.info("Applying validation: dates. Target dataframe: {}.".format(name))

                # Compile valid fields, apply function.
                cols = ["credate", "revdate"]
                args = [df[col].values for col in cols] + [self.defaults[name][cols[0]]]
                results = pd.DataFrame(np.column_stack(np.vectorize(validation_functions.validate_dates)(*args)))
                df[cols] = results[[0, 1]].values
                self.flags[name]["validate_dates_errors"], self.flags[name]["validate_dates_modifications"] = \
                    results[2].values, results[3].values

                # Store results.
                self.dframes[name] = df

            except (KeyError, SyntaxError, ValueError):
                logger.exception("Unable to apply validation.")
                sys.exit(1)

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.gen_flag_variables()
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
