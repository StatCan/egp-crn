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

    def universal_attr_validation(self):
        """Applies a set of universal attribute validations (all fields and / or all tables)."""

        logger.info("Applying validation: universal attribute validations.")

        # Iterate data frames.
        for name, df in self.dframes.items():

            logger.info("Target dataframe: {}.".format(name))

            try:

                # Validation: strip whitespace.
                logger.info("Applying validation: strip whitespace.")

                # Compile valid fields, apply function.
                df_valid = df.select_dtypes(include="object")
                if "geometry" in df_valid.columns:
                    df_valid.drop("geometry", axis=1, inplace=True)
                df[df_valid.columns] = df_valid.applymap(attr_rect_functions.strip_whitespace)

                # Validation: dates.
                logger.info("Applying validation: dates.")

                # Compile valid fields, apply function.
                cols = ["credate", "revdate"]
                args = [df[col].values for col in cols] + [self.defaults[name][cols[0]]]
                df[cols] = np.column_stack(np.vectorize(attr_rect_functions.validate_dates)(*args))

                # Store results.
                self.dframes[name] = df

            except (SyntaxError, ValueError):
                logger.exception("Unable to apply validation.")
                sys.exit(1)

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.universal_attr_validation()


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
