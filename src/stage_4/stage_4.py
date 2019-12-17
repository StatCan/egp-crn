import click
import fiona
import geopandas as gpd
import logging
import numpy as np
import os
import pandas as pd
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import attr_rect_functions
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

    def load_gpkg(self):
        """Loads input GeoPackage layers into dataframes."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

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
            self.dframes["roadseg"]["nbrlanes"] = self.dframes["roadseg"]["nbrlanes"].map(
                lambda val: attr_rect_functions.validate_nbrlanes(val, default=self.defaults["roadseg"]["nbrlanes"]))

            # Validation: speed.
            logger.info("Applying validation: speed. Target dataframe: roadseg.")

            # Apply function directly to target field.
            self.dframes["roadseg"]["speed"] = self.dframes["roadseg"]["speed"].map(
                lambda val: attr_rect_functions.validate_speed(val, default=self.defaults["roadseg"]["speed"]))

            # Validation: pavement.
            logger.info("Applying validation: pavement. Target dataframe: roadseg.")

            # Apply function directly to target fields.
            cols = ["pavstatus", "pavsurf", "unpavsurf"]
            args = [self.dframes["roadseg"][col].values for col in cols]
            self.dframes["roadseg"][cols] = np.column_stack(np.vectorize(attr_rect_functions.validate_pavement)(*args))

            # Validation: roadclass-rtnumber1.
            cols = ["roadclass", "rtnumber1"]
            for table in ("ferryseg", "roadseg"):
                logger.info("Applying validation: roadclass-rtnumber1. Target dataframe: {}.".format(table))

                # Compile valid fields, apply function.
                df = self.dframes[table]
                args = [df[col].values for col in cols] + [self.defaults[table][cols[1]]]
                df[cols] = np.column_stack(np.vectorize(attr_rect_functions.validate_roadclass_rtnumber1)(*args))

                # Store results.
                self.dframes[table] = df

            # Validation: route text.
            for table in ("roadseg", "ferryseg"):
                logger.info("Applying validation: route text. Target dataframe: {}.".format(table))

                # Apply function, store results.
                self.dframes[table] = attr_rect_functions.validate_route_text(self.dframes[table], self.defaults[table])

            # Validation: route contiguity.
            logger.info("Applying validation: route contiguity. Target dataframe: ferryseg and roadseg.")

            # Concatenate dataframes, apply function.
            df = gpd.GeoDataFrame(pd.concat([self.dframes["ferryseg"], self.dframes["roadseg"]], ignore_index=True))
            attr_rect_functions.validate_route_contiguity(df, self.defaults["roadseg"])

        except (KeyError, ValueError):
            logger.exception("Unable to apply validation.")
            sys.exit(1)
        except SyntaxError as e:
            logger.exception("Unable to apply validation.")
            logger.exception(e)
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
                df[df_valid.columns] = df_valid.applymap(attr_rect_functions.strip_whitespace)

                # Validation: dates.
                logger.info("Applying validation: dates. Target dataframe: {}.".format(name))

                # Compile valid fields, apply function.
                cols = ["credate", "revdate"]
                args = [df[col].values for col in cols] + [self.defaults[name][cols[0]]]
                df[cols] = np.column_stack(np.vectorize(attr_rect_functions.validate_dates)(*args))

                # Store results.
                self.dframes[name] = df

            except ValueError:
                logger.exception("Unable to apply validation.")
                sys.exit(1)
            except SyntaxError as e:
                logger.exception("Unable to apply validation.")
                logger.exception(e)
                sys.exit(1)

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.universal_attr_validation()
        self.unique_attr_validation()


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
