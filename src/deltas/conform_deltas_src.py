import click
import fiona
import geopandas as gpd
import logging
import pandas as pd
import subprocess
import sys
from pathlib import Path

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

    def __init__(self, src: Path, source: str, mode: str) -> None:
        """
        Initializes the CRN class.

        \b
        :param Path src: path to the source GeoPackage (NRN) or ESRI File Geodatabase (NGD).
        :param str source: code for the source province / territory.
        :param mode: the type of deltas to be returned: {'ngd', 'nrn'}.
        """

        self.src = src
        self.source = source
        self.mode = mode

        # Validate src.
        # TODO: ...

        # Validate and create dst.

    def __call__(self) -> None:
        """Executes the CRN class."""

        # TODO: find input layers and load data
        # TODO: for nrn data, create nrn_full_attribution layer (maybe I can skip this since we dont know which records are deltas at this point)
        # TODO: create simplified NRN / NGD data which resembled CRN schema
        # Export (new gpkg in src location.


@click.command()
@click.argument("src", type=click.Path(exists=True, dir_okay=False, resolve_path=True, path_type=Path))
@click.argument("source", type=click.Choice("ab bc mb nb nl ns nt nu on pe qc sk yt".split(), False))
@click.argument("mode", type=click.Choice(["ngd", "nrn"], False))
def main(src: Path, source: str, mode: str) -> None:
    """
    Instantiates and executes the CRN class.

    \b
    :param Path src: path to the source GeoPackage (NRN) or ESRI File Geodatabase (NGD).
    :param str source: code for the source province / territory.
    :param str mode: the type of deltas to be returned: {'ngd', 'nrn'}.
    """

    try:

        with helpers.Timer():
            deltas = CRNDeltas(src, source, mode)
            deltas()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
