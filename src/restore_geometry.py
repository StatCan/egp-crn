import click
import fiona
import geopandas as gpd
import logging
import re
import sys
from pathlib import Path
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


class EGPRestoreGeometry:
    """Defines the EGP geometry restoration class."""

    def __init__(self, source: str) -> None:
        """
        Initializes the EGP class.

        :param str source: abbreviation for the source province / territory.
        """

        self.source = source
        self.layer = f"nrn_bo_{source}"
        self.nrn_id = "segment_id_orig"
        self.bo_id = "ngd_uid"
        self.src = Path(filepath.parents[2] / "data/interim/egp_data.gpkg")
        self.src_restore = Path(filepath.parents[2] / "data/interim/nrn_bo_restore.gpkg")
        self.missing_nrn = set()
        self.missing_bo = set()

        # Configure source path and layer name.
        for src in (self.src, self.src_restore):
            if src.exists():
                if self.layer not in set(fiona.listlayers(src)):
                    logger.exception(f"Layer \"{self.layer}\" not found within source: \"{src}\".")
                    sys.exit(1)
            else:
                logger.exception(f"Source not found: \"{src}\".")
                sys.exit(1)

        # Load source data.
        logger.info(f"Loading source data: {self.src}|layer={self.layer}.")
        self.df = gpd.read_file(self.src, layer=self.layer)
        logger.info("Successfully loaded source data.")

        # Load source restoration data.
        logger.info(f"Loading source restoration data: {self.src_restore}|layer={self.layer}.")
        self.df_restore = gpd.read_file(self.src_restore, layer=self.layer)
        logger.info("Successfully loaded source restoration data.")

    def restore_data(self) -> None:
        """Identifies partially or completely missing restoration data geometries from the source dataset."""

        logger.info("Identifying missing data.")

        # Define flags.
        flag_nrn = self.df["segment_type"].astype(str).isin({"1", "2", "1.0", "2.0"})
        flag_nrn_restore = self.df_restore["segment_type"].astype(str).isin({"1", "2", "1.0", "2.0"})
        flag_bo = self.df["segment_type"].astype(str).isin({"3", "3.0"})
        flag_bo_restore = self.df_restore["segment_type"].astype(str).isin({"3", "3.0"})

        # Identify missing nrn arcs.
        self.missing_nrn.update(set(self.df_restore.loc[flag_nrn_restore, self.nrn_id]) -
                                set(self.df.loc[flag_nrn, self.nrn_id]))

        # Identify missing bo arcs.
        self.missing_bo.update(set(self.df_restore.loc[flag_bo_restore, self.bo_id]) -
                               set(self.df.loc[flag_bo, self.bo_id]))

        # Filter out non-NRN arcs and dissolve geometries on original identifier values.
        df = self.df.loc[(~self.df[self.identifier].isna()) &
                         (self.df[self.identifier] != "None")].dissolve(by=self.identifier, sort=False)

    def execute(self) -> None:
        """Executes the EGP class."""

        self.restore_data()


@click.command()
@click.argument("source", type=click.Choice("ab bc mb nb nl ns nt nu on pe qc sk yt".split(), False))
def main(source: str) -> None:
    """
    Instantiates and executes the EGP class.

    :param str source: abbreviation for the source province / territory.
    """

    try:

        with helpers.Timer():
            egp = EGPRestoreGeometry(source)
            egp.execute()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
