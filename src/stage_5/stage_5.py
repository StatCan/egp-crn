import click
import fiona
import geopandas as gpd
import logging
import numpy as np
import os
import pandas as pd
import sys
import uuid

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
        self.stage = 5
        self.source = source.lower()

        # Configure and validate input data path.
        self.data_path = os.path.abspath("../../data/interim/{}.gpkg".format(self.source))
        if not os.path.exists(self.data_path):
            logger.exception("Input data not found: \"{}\".".format(self.data_path))
            sys.exit(1)

    def export_gpkg(self):
        """Exports the dataframes as GeoPackage layers."""

        logger.info("Exporting dataframes to GeoPackage layers.")

        # Export target dataframes to GeoPackage layers.
        helpers.export_gpkg(self.dframes, self.data_path)

    def gen_nids_roadseg(self):
        """Groups roadseg records and assigns nid values."""

        # Compile match fields (fields which must be equal across records).
        match_fields = ["namebody", "strtypre", "strtysuf", "dirprefix", "dirsuffix"]

        # Compile match fields via dataframe merges.
        # Keep only necessary fields.
        roadseg = self.dframes["roadseg"][["uuid", "nid", "adrangenid"]].copy(deep=True)
        addrange = self.dframes["addrange"][["nid", "l_offnanid", "r_offnanid"]].copy(deep=True)
        strplaname = self.dframes["strplaname"][["nid", *match_fields]].copy(deep=True)

        roadseg = roadseg.merge(
            addrange, how="left", left_on="adrangenid", right_on="nid", suffixes=("", "_addrange")).merge(
            strplaname, how="left", left_on=["l_offnanid", "r_offnanid"], right_on=["nid", "nid"],
            suffixes=("", "_strplaname"))

        # Group uuids by match fields.
        grouped_uuids = roadseg.groupby(match_fields)["uuid"].apply(list)

    def load_gpkg(self):
        """Loads input GeoPackage layers into dataframes."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.gen_nids_roadseg()
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
