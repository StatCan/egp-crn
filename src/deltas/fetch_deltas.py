import click
import fiona
import geopandas as gpd
import logging
import pandas as pd
import sys
from copy import deepcopy
from itertools import chain
from operator import attrgetter, itemgetter
from pathlib import Path
from shapely.geometry import Point
from shapely.ops import polygonize, unary_union
from tabulate import tabulate

filepath = Path(__file__).resolve()
sys.path.insert(1, str(Path(__file__).resolve().parents[1]))
import helpers

# Set logger.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)


class CRNDeltas:
    """Defines the CRN deltas class."""

    def __init__(self, source: str) -> None:
        """
        Initializes the CRN class.

        :param str source: abbreviation for the source province / territory.
        """

        self.source = source
        self.layer = f"nrn_bo_{source}"
        self.layer_ngd = f"..."
        self.layer_nrn = f"..."

        self.id = "segment_id"
        self.ngd_id = "ngd_uid"
        self.nrn_id = "..."

        self.src = Path(filepath.parents[2] / "data/egp_data.gpkg")
        self.src_ngd = Path(filepath.parents[2] / "data/....gpkg")
        self.src_nrn = Path(filepath.parents[2] / "data/....gpkg")

        # Configure source path and layer name.
        for src in (self.src, self.src_ngd, self.src_nrn):
            if src.exists():
                layer = {self.src: self.layer, self.src_ngd: self.layer_ngd, self.src_nrn: self.layer_nrn}[src]
                if layer not in set(fiona.listlayers(src)):
                    logger.exception(f"Layer \"{layer}\" not found within source: \"{src}\".")
                    sys.exit(1)
            else:
                logger.exception(f"Source not found: \"{src}\".")
                sys.exit(1)

        # Load source data.
        logger.info(f"Loading source data: {self.src}|layer={self.layer}.")
        self.crn = gpd.read_file(self.src, layer=self.layer)
        logger.info("Successfully loaded source data.")

        # Standardize data and snap nodes.
        self.crn = helpers.standardize(self.crn)
        self.crn = helpers.snap_nodes(self.crn)

        # Load NGD data.
        logger.info(f"Loading NGD data: {self.src_ngd}|layer={self.layer_ngd}.")
        self.ngd = gpd.read_file(self.src_ngd, layer=self.layer_ngd)
        logger.info("Successfully loaded NGD data.")

        # Load NRN data.
        logger.info(f"Loading NRN data: {self.src_nrn}|layer={self.layer_nrn}.")
        self.nrn = gpd.read_file(self.src_nrn, layer=self.layer_nrn)
        logger.info("Successfully loaded NRN data.")

    def __call__(self) -> None:
        """Executes the CRN class."""

        self.fetch_ngd_deltas()
        self.fetch_nrn_deltas()

    def fetch_ngd_deltas(self) -> None:
        """Identifies and retrieves NGD deltas."""

        logger.info("Fetching NGD deltas.")

        # TODO

    def fetch_nrn_deltas(self) -> None:
        """Identifies and retrieves NRN deltas."""

        logger.info("Fetching NRN deltas.")

        # TODO


@click.command()
@click.argument("source", type=click.Choice(helpers.load_yaml("../config.yaml")["sources"], False))
def main(source: str) -> None:
    """
    Instantiates and executes the CRN class.

    :param str source: abbreviation for the source province / territory.
    """

    try:

        with helpers.Timer():
            crn = CRNDeltas(source)
            crn()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
