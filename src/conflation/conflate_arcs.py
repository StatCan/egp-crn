import click
import fiona
import geopandas as gpd
import logging
import pandas as pd
import sys
from collections import Counter
from operator import attrgetter, itemgetter
from pathlib import Path
from shapely.ops import polygonize, unary_union
from tabulate import tabulate

filepath = Path(__file__).resolve()
sys.path.insert(1, str(filepath.parents[1]))
import helpers


# Set logger.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)


class CRNArcConflation:
    """Defines the CRN arc conflation class."""

    def __init__(self, source: str) -> None:
        """
        Initializes the CRN class.

        :param str source: abbreviation for the source province / territory.
        """

        self.source = source

        self.src = Path(filepath.parents[2] / "data/interim/egp_data.gpkg")
        self.layer_arc = f"nrn_bo_{self.source}"
        self.layer_meshblock = f"meshblock_{self.source}"

        self.src_ngd = Path(filepath.parents[2] / "data/interim/ngd.zip")
        self.layer_arc_ngd = f"ngd_al_{self.source}"
        self.layer_meshblock_ngd = f"ngd_a_{self.source}"

        self.id_arc = "segment_id"
        self.id_arc_ngd = "ngd_uid"
        self.id_meshblock_ngd = "bb_uid"

        # Configure source path and layer name.
        for src in (self.src, self.src_ngd):
            if src.exists():
                layers = set(fiona.listlayers("zip://" + str(src) if src.suffix == "zip" else src))
                for layer in {self.src: (self.layer_arc, self.layer_meshblock),
                              self.src_ngd: (self.layer_arc_ngd, self.layer_meshblock_ngd)}[src]:
                    if layer not in layers:
                        logger.exception(f"Layer \"{layer}\" not found within source: \"{src}\".")
                        sys.exit(1)
            else:
                logger.exception(f"Source not found: \"{src}\".")
                sys.exit(1)

        # Load source data.
        logger.info(f"Loading source data: {self.src}|layers={self.layer_arc},{self.layer_meshblock}.")
        self.arcs = gpd.read_file(self.src, layer=self.layer_arc)
        self.meshblock = gpd.read_file(self.src, layer=self.layer_meshblock)
        logger.info("Successfully loaded source data.")

        # Load ngd data.
        logger.info(f"Loading ngd data: {self.src_ngd}|layers={self.layer_arc_ngd},{self.layer_meshblock_ngd}.")
        self.arcs_ngd = gpd.read_file(self.src_ngd, layer=self.layer_arc_ngd)
        self.meshblock_ngd = gpd.read_file(self.src_ngd, layer=self.layer_meshblock_ngd)
        logger.info("Successfully loaded ngd data.")

    def __call__(self) -> None:
        """Executes the CRN class."""

        self.link_arcs_to_meshblock()
        self.conflation()
        self.output_results()

    def conflation(self) -> None:
        """Performs the arc conflation."""

        logger.info(f"Performing arc conflation.")

        # TODO

    def output_results(self) -> None:
        """Outputs conflation results."""

        logger.info(f"Outputting results.")

        # TODO

    def link_arcs_to_meshblock(self) -> None:
        """Links each arc to a meshblock polygon."""

        logger.info("Linking arcs to meshblock polygons.")


@click.command()
@click.argument("source", type=click.Choice("ab bc mb nb nl ns nt nu on pe qc sk yt".split(), False))
def main(source: str) -> None:
    """
    Instantiates and executes the CRN class.

    :param str source: abbreviation for the source province / territory.
    """

    try:

        with helpers.Timer():
            crn = CRNArcConflation(source)
            crn()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
