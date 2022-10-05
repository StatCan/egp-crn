import click
import fiona
import geopandas as gpd
import logging
import pandas as pd
import sys
from collections import Counter
from operator import attrgetter, itemgetter
from pathlib import Path
from shapely.geometry import Point
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


class CRNCrossings:
    """Defines the CRN crossings class."""

    def __init__(self, source: str) -> None:
        """
        Initializes the CRN class.

        :param str source: code for the source region (working area).
        """

        self.source = source
        self.layer = f"crn_{source}"
        self.layer_crossings = f"{source}_crossings"
        self.layer_deltas = f"{source}_crossings_deltas"
        self.src = Path(filepath.parents[2] / "data/crn.gpkg")
        self.crossings = None
        self.crossings_old = None
        self.crossings_deltas = None
        self.min_count = 4

        # Configure source path and layer name.
        if self.src.exists():
            if self.layer not in set(fiona.listlayers(self.src)):
                logger.exception(f"Layer \"{self.layer}\" not found within source: \"{self.src}\".")
                sys.exit(1)
        else:
            logger.exception(f"Source not found: \"{self.src}\".")
            sys.exit(1)

        # Load source data.
        logger.info(f"Loading source data: {self.src}|layer={self.layer}.")
        self.crn = gpd.read_file(self.src, layer=self.layer)
        logger.info("Successfully loaded source data.")

        # Standardize data and filter to roads.
        self.crn = helpers.standardize(self.crn)
        self.crn = self.crn.loc[self.crn["segment_type"] == 1].copy(deep=True)

        # Load existing crossings data, if possible.
        if self.layer_crossings in set(fiona.listlayers(self.src)):
            logger.info("Loading existing crossings data.")
            self.crossings_old = gpd.read_file(self.src, layer=self.layer_crossings)
            logger.info("Successfully loaded existing crossings data.")

    def __call__(self) -> None:
        """Executes the CRN class."""

        self.gen_crossings()
        if isinstance(self.crossings_old, pd.DataFrame):
            self.fetch_deltas()

            # Export required dataset.
            if isinstance(self.crossings_deltas, pd.DataFrame):
                helpers.export(self.crossings_deltas, dst=self.src, name=self.layer_deltas)

                logger.info("Results: Exported crossings deltas dataset.")

            else:
                logger.info("Results: No export required.")

        else:

            # Export required dataset.
            helpers.export(self.crossings, dst=self.src, name=self.layer_crossings)
            logger.info("Results: Exported crossings dataset.")

    def fetch_deltas(self) -> None:
        """Fetches crossings deltas (additions, deletions, modifications)."""

        logger.info("Fetching crossings deltas.")

        # Extract crossings data as tuple sets.
        crossings = pd.DataFrame({"count": self.crossings["count"],
                                  "geometry": self.crossings["geometry"].map(
                                      lambda g: itemgetter(0)(attrgetter("coords")(g)))})
        crossings_old = pd.DataFrame({"count": self.crossings_old["count"],
                                      "geometry": self.crossings_old["geometry"].map(
                                          lambda g: itemgetter(0)(attrgetter("coords")(g)))})

        # Merge data.
        deltas = crossings.merge(crossings_old, on="geometry", how="outer", suffixes=("", "_old"))

        # Compile delta classifications.
        deltas["status"] = -1
        deltas.loc[deltas["count_old"].isna(), "status"] = "Additions"
        deltas.loc[deltas["count"].isna(), "status"] = "Deletions"
        deltas.loc[~(deltas["count"].isna() | deltas["count_old"].isna()) &
                   (deltas["count"] != deltas["count_old"]), "status"] = "Modifications"

        # Create delta GeoDataFrame.
        deltas = deltas.loc[deltas["status"] != -1].fillna(0).copy(deep=True)
        if len(deltas):
            self.crossings_deltas = gpd.GeoDataFrame(deltas, geometry=list(map(Point, deltas["geometry"])),
                                                     crs=self.crn.crs)

            # Log results.
            table = tabulate([[k, f"{v:,}"] for k, v in Counter(self.crossings_deltas["status"]).items()],
                             headers=["Crossing Status", "Count"], tablefmt="rst", colalign=("left", "right"))
            logger.info("\n" + table)

    def gen_crossings(self) -> None:
        """Compiles crossing points (nodes with count >= 4)."""

        logger.info("Compiling crossing points.")

        # Extract duplicated nodes.
        nodes = self.crn["geometry"].map(lambda g: itemgetter(0, -1)(attrgetter("coords")(g))).explode()
        nodes = nodes.loc[nodes.duplicated(keep=False)].copy(deep=True)

        # Compile counts, filter to threshold.
        counts = Counter(nodes)
        counts_df = pd.DataFrame({"count": counts.values(), "geometry": counts.keys()})
        counts_df = counts_df.loc[counts_df["count"] >= self.min_count].copy(deep=True)

        # Compile crossings as GeoDataFrame.
        self.crossings = gpd.GeoDataFrame(counts_df, geometry=list(map(Point, counts_df["geometry"])), crs=self.crn.crs)
        self.crossings["overpass_flag"] = -1


@click.command()
@click.argument("source", type=click.Choice(helpers.load_yaml("../config.yaml")["sources"], False))
def main(source: str) -> None:
    """
    Instantiates and executes the CRN class.

    \b
    :param str source: code for the source region (working area).
    """

    try:

        with helpers.Timer():
            crn = CRNCrossings(source)
            crn()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
