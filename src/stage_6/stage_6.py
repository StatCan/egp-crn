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
        self.required = True

        # Configure and validate input data path.
        self.data_path = os.path.abspath("../../data/interim/{}.gpkg".format(self.source))
        if not os.path.exists(self.data_path):
            logger.exception("Input data not found: \"{}\".".format(self.data_path))
            sys.exit(1)

    def export_gpkg(self):
        """Exports the dataframes as GeoPackage layers."""

        logger.info("Exporting dataframes to GeoPackage layers.")

        # Export target dataframes to GeoPackage layers.
        dframes = {name: df for name, df in self.dframes.items() if name in ("addrange", "altnamlink", "strplaname")}
        helpers.export_gpkg(dframes, self.data_path)

    def filter_duplicates(self):
        """
        Filter duplicate records from addrange and strplaname to simplify linkages.
        This task occurs regardless of altnamlink production requirement.
        """

        logger.info("Filtering duplicates from addrange and strplaname.")

        # Filter duplicate records (ignoring uuid and nid columns).
        for name, df in self.dframes.items():

            if name in ("addrange", "strplaname"):

                kwargs = {"subset": df.columns.difference(["uuid", "nid"]), "keep": "first", "inplace": True}
                self.dframes[name] = df.drop_duplicates(**kwargs)

    def load_gpkg(self):
        """Loads input GeoPackage layers into dataframes."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

    def validate_linkages(self):
        """Validate the linkages between all required dataframes."""

        logger.info("Validating table linkages.")

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
        try:

            flag = False

            for linkage in [link for link in linkages if all([table in self.dframes for table in (link[0], link[2])])]:

                logger.info("Validating table linkage: {}.{} - {}.{}.".format(*linkage))
                source, target = self.dframes[linkage[0]][linkage[1]], self.dframes[linkage[2]][linkage[3]]

                if not set(source).issubset(target):

                    flag = True

                    # Compile invalid values.
                    flag_vals = ", ".join(list(set(source) - set(target)))
                    logger.info("Invalid table linkage. The following values from {1}.{2} are not present in {3}.{4}: "
                                "{0}.".format(flag_vals, *linkage))

            if flag:
                raise ValueError("Invalid table linkages identified.")

        except ValueError:
            logger.exception("")
            sys.exit(1)

    def verify_altnamlink_requirement(self):
        """
        Verifies the requirement to process altnamlink via validating the existence of required GeoPackage layers:
        addrange, altnamlink, and strplaname.
        """

        logger.info("Verifying altnamlink processing requirement.")

        try:

            # Verify tables.
            if not all([table in self.dframes for table in ("addrange", "altnamlink", "strplaname")]):

                # Ensure altnamlink doesn't exist without associated linked tables.
                if "altnamlink" in self.dframes:
                    raise ValueError("Unable to validate altnamlink without both addrange and strplaname.")

                self.required = False

            # Log verification results.
            if self.required:
                logger.info("Verification = True: altnamlink processing required.")
            else:
                logger.info("Verification = False: altnamlink processing not required.")

        except ValueError:
            logger.exception("")
            sys.exit(1)

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.filter_duplicates()
        self.verify_altnamlink_requirement()
        if self.required:
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
