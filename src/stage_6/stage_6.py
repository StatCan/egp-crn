import click
import logging
import os
import pandas as pd
import sys

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
        self.mods = list()

        # Configure and validate input data path.
        self.data_path = os.path.abspath("../../data/interim/{}.gpkg".format(self.source))
        if not os.path.exists(self.data_path):
            logger.exception("Input data not found: \"{}\".".format(self.data_path))
            sys.exit(1)

    def export_gpkg(self):
        """Exports the dataframes as GeoPackage layers."""

        logger.info("Exporting dataframes to GeoPackage layers.")

        # Export target dataframes to GeoPackage layers.
        dframes = {name: df for name, df in self.dframes.items() if name in self.mods}

        if len(dframes):
            helpers.export_gpkg(dframes, self.data_path)
        else:
            logger.info("Export not required, no dataframe modifications detected.")

    def filter_duplicates(self):
        """
        Filter duplicate records from addrange and strplaname, only if altnamlink does not exist.
        This is intended to simplify tables and linkages.
        """

        if "altnamlink" not in self.dframes:

            logger.info("Filtering duplicates from addrange and strplaname.")

            # Filter duplicate records (ignoring uuid and nid columns).
            for name, df in self.dframes.items():

                if name in ("addrange", "strplaname"):

                    # Drop duplicates.
                    kwargs = {"subset": df.columns.difference(["uuid", "nid"]), "keep": "first", "inplace": False}
                    new_df = df.drop_duplicates(**kwargs)

                    # Replace original dataframe only if modifications were made.
                    if len(df) != len(new_df):
                        self.dframes[name] = new_df
                        self.mods.append(name)

    def load_gpkg(self):
        """Loads input GeoPackage layers into dataframes."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

    def validate_nid_linkages(self):
        """Validate the nid linkages between all required dataframes."""

        logger.info("Validating nid linkages.")
        errors = list()

        # Define linkages.
        linkages = {
            "addrange":
                {
                    "altnamlink": ["l_altnanid", "r_altnanid"],
                    "strplaname": ["l_offnanid", "r_offnanid"]
                },
            "altnamlink":
                {
                    "strplaname": ["strnamenid"]
                },
            "blkpassage":
                {
                    "roadseg": ["roadnid"]
                },
            "roadseg":
                {
                    "addrange": ["adrangenid"]
                },
            "tollpoint":
                {
                    "roadseg": ["roadnid"]
                }
        }

        # Iterate tables with nid linkages.
        for source in [s for s in linkages if s in self.dframes]:

            # Iterate linked tables (targets).
            for target in [t for t in linkages[source] if t in self.dframes]:

                # Retrieve nid from target.
                target_ids = self.dframes[target]["nid"]

                # Iterate source columns with nid linkages.
                for col in linkages[source][target]:

                    # Retrieve source column ids.
                    source_ids = self.dframes[source][col]

                    # Validate linkages.
                    logger.info("Validating table linkage: {}.{} - {}.nid.".format(source, col, target))
                    if not set(source_ids).issubset(target_ids):

                        # Compile invalid values and configure error messages.
                        flag_vals = "\n".join(list(set(source_ids) - set(target_ids)))
                        errors.append("Invalid table linkage. The following values from {}.{} are not present in "
                                      "{}.{}: {}.".format(source, col, target, flag_vals))

        # Log error messages.
        if len(errors):
            logger.info("Invalid nid linkages identified.")

            for error in errors:
                logger.info(error)

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.filter_duplicates()
        self.validate_nid_linkages()
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
