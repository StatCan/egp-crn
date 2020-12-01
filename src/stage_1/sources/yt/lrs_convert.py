import click
import geopandas as gpd
import logging
import numpy as np
import os
import pandas as pd
import shapely
import sqlite3
import sys
import uuid
from collections import Counter
from itertools import chain
from operator import attrgetter, itemgetter
from osgeo import ogr, osr
from shapely.geometry import LineString, MultiLineString
from tqdm import tqdm

sys.path.insert(1, os.path.join(sys.path[0], "../../../"))
import helpers


# Set logger.
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)


class LRS:
    """Class to convert Yukon data from Linear Reference System (LRS) to GeoPackage."""

    def __init__(self, src, dst):
        self.nrn_datasets = dict()
        self.source_datasets = dict()

        # Validate src.
        self.src = os.path.abspath(src)
        if os.path.splitext(self.src)[-1] != ".gdb":
            logger.exception(f"Invalid src input: {src}. Must be a File GeoDatabase.")
            sys.exit(1)

        # Validate dst.
        self.dst = os.path.abspath(dst)
        if os.path.splitext(self.dst)[-1] != ".gpkg":
            logger.exception(f"Invalid dst input: {dst}. Must be a GeoPackage.")
            sys.exit(1)
        if os.path.exists(self.dst):
            logger.exception(f"Invalid dst input: {dst}. File already exists.")

    def execute(self):
        """Executes class functionality."""

        # ...


@click.command()
@click.argument("src", type=click.Path(exists=True))
@click.option("--dst", type=click.Path(exists=False), default=os.path.abspath("../../../../data/raw/yt/yt.gpkg"),
              show_default=True)
def main(src, dst):
    """Executes the LRS class."""

    try:

        with helpers.Timer():
            lrs = LRS(src, dst)
            lrs.execute()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
