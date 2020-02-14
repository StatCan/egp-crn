import click
import logging
import os
import pandas as pd
import sys
import uuid
from datetime import datetime
from itertools import chain

sys.path.insert(1, os.path.join(sys.path[0], ".."))
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
        self.stage = 6
        self.source = source.lower()
        self.altnamlink_required = True

        # Configure and validate input data path.
        self.data_path = os.path.abspath("../../data/interim/{}.gpkg".format(self.source))
        if not os.path.exists(self.data_path):
            logger.exception("Input data not found: \"{}\".".format(self.data_path))
            sys.exit(1)

        # Compile default field values and dtypes.
        self.defaults = helpers.compile_default_values()
        self.dtypes = helpers.compile_dtypes()["altnamlink"]

    def apply_altnamlink_domains(self):
        """Applies the field domains to each column in the altnamlink dataframe."""

        if "altnamlink" in self.dframes:

            logging.info("Applying field domains to altnamlink.")
            field = None

            try:

                for field, default in self.defaults["altnamlink"].items():
                    logger.info("Target field \"{}\": Applying domain.".format(field))

                    # Apply domains to dataframe.
                    self.dframes["altnamlink"][field] = self.dframes["altnamlink"][field].map(
                        lambda val: default if val == "" or pd.isna(val) else val)

                    # Force adjust data type.
                    self.dframes["altnamlink"][field] = self.dframes["altnamlink"][field].astype(self.dtypes[field])

            except (AttributeError, KeyError, ValueError):
                logger.exception("Invalid schema definition for table: altnamlink, field: {}.".format(field))
                sys.exit(1)

    def export_gpkg(self):
        """Exports the dataframes as GeoPackage layers."""

        logger.info("Exporting dataframes to GeoPackage layers.")

        # Export target dataframes to GeoPackage layers.
        helpers.export_gpkg(self.dframes, self.data_path)

    def filter_duplicates(self):
        """Filter duplicate records from addrange and strplaname to simplify linkages."""

        logger.info("Filtering duplicates from addrange and strplaname.")

        # Filter duplicate records (ignoring uuid and nid columns).
        for name, df in self.dframes.items():

            if name in ("addrange", "strplaname"):

                kwargs = {"subset": df.columns.difference(["uuid", "nid"]), "keep": "first", "inplace": True}
                self.dframes[name] = df.drop_duplicates(**kwargs)

    def gen_altnamlink(self):
        """Generate altnamlink dataframe."""

        logger.info("Validating altnamlink requirement.")

        # Check if altnamlink is required.
        # Process: altnamlink is only required if addrange l_altnanid or r_altnanid contain non-default values.
        addrange = self.dframes["addrange"]
        altnanids = set(chain.from_iterable(addrange[addrange[col] != self.defaults["addrange"][col]][col] for
                                            col in ("l_altnanid", "r_altnanid")))
        if len(altnanids):

            logger.info("Validation = True: altnamlink required.")
            self.altnamlink_required = True

            logger.info("Generating altnamlink dataframe.")

            # Generate altnamlink dataframe from addrange l_altnanid and r_altnanid values.
            altnamlink = pd.DataFrame(
                {field: pd.Series(list(altnanids), dtype=dtype) if field.lower() == "nid" else pd.Series(dtype=dtype)
                 for field, dtype in self.dtypes.items()})

            # Force lowercase field names.
            altnamlink.columns = map(str.lower, altnamlink.columns)

            # Populate all possible fields.
            altnamlink["uuid"] = [uuid.uuid4().hex for _ in range(len(altnamlink))]
            altnamlink["credate"] = datetime.today().strftime("%Y%m%d")
            altnamlink["datasetnam"] = self.dframes["roadseg"]["datasetnam"][0]

            # Store result.
            self.dframes["altnamlink"] = altnamlink

        else:

            logger.info("Validation = False: altnamlink not required.")
            self.altnamlink_required = False

    def load_gpkg(self):
        """Loads input GeoPackage layers into dataframes."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

    def populate_altnamlink(self):
        """Populates the altnamlink dataframe."""

        if self.altnamlink_required:

            # . . . .

    def verify_tables(self):
        """Verifies the existence of required GeoPackage layers: addrange and strplaname."""

        try:

            # Verify tables.
            for table in ("addrange", "strplaname"):
                if table not in self.dframes:
                    raise KeyError("Missing required layer: \"{}\".".format(table))

        except KeyError:
            logger.exception("")
            sys.exit(1)

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.verify_tables()
        self.gen_altnamlink()
        self.apply_altnamlink_domains()
        self.filter_duplicates()
        self.populate_altnamlink()
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
