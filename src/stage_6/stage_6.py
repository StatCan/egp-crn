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

    def validate_linkages(self):
        """Validate the linkages between all required dataframes."""

        logger.info("Validating table linkages.")
        errors = list()

        # Define linkages.
        linkages = [
            ["addrange", "l_altnanid", "altnamlink", "nid"],
            ["addrange", "r_altnanid", "altnamlink", "nid"],
            ["addrange", "l_offnanid", "strplaname", "nid"],
            ["addrange", "r_offnanid", "strplaname", "nid"],
            ["altnamlink", "strnamenid", "strplaname", "nid"],
            ["blkpassage", "roadnid", "roadseg", "nid"],
            ["roadseg", "adrangenid", "addrange", "nid"],
            ["tollpoint", "roadnid", "roadseg", "nid"]
        ]

        # Validate linkages.
        for linkage in [link for link in linkages if all([table in self.dframes for table in (link[0], link[2])])]:

            logger.info("Validating table linkage: {}.{} - {}.{}.".format(*linkage))
            source, target = self.dframes[linkage[0]][linkage[1]], self.dframes[linkage[2]][linkage[3]]

            if not set(source).issubset(target):

                # Compile invalid values and configure error messages.
                flag_vals = "\n".join(list(set(source) - set(target)))
                errors.append("Invalid table linkage. The following values from {1}.{2} are not present in {3}.{4}: "
                              "{0}.".format(flag_vals, *linkage))

        # Log error messages.
        if len(errors):
            logger.info("Invalid table linkages identified.")

            for error in errors:
                logger.info(error)

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.filter_duplicates()
        self.validate_linkages()
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
