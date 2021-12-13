import click
import fiona
import geopandas as gpd
import logging
import re
import sys
from operator import itemgetter
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
        self.modified_nrn = set()
        self.modified_bo = set()

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

        # Define flags to classify arcs.
        flag_nrn = self.df["segment_type"].astype(str).isin({"1", "2", "1.0", "2.0"})
        flag_bo = self.df["segment_type"].astype(str).isin({"3", "3.0"})
        flag_nrn_restore = self.df_restore["segment_type"].astype(str).isin({"1", "2", "1.0", "2.0"})
        flag_bo_restore = self.df_restore["segment_type"].astype(str).isin({"3", "3.0"})

        # Filter to exclusively nrn and bo arcs and dissolve geometries on identifiers.
        nrn_dissolved = self.df.loc[flag_nrn].dissolve(by=self.nrn_id, sort=False)
        bo_dissolved = self.df.loc[flag_bo].dissolve(by=self.bo_id, sort=False)

        # Create identifier - geometry lookups for new geometries.
        nrn_id_geom_lookup = dict(zip(nrn_dissolved.index, nrn_dissolved["geometry"]))
        bo_id_geom_lookup = dict(zip(bo_dissolved.index, bo_dissolved["geometry"]))

        # Define flags to query arcs linkage.
        flag_nrn_link = self.df_restore[self.nrn_id].isin(nrn_id_geom_lookup)
        flag_bo_link = self.df_restore[self.bo_id].isin(bo_id_geom_lookup)

        # Compile new arc associated with each original arc.
        self.df_restore["geometry_orig"] = None
        self.df_restore.loc[flag_nrn_link, "geometry_orig"] = self.df_restore.loc[flag_nrn_link, self.nrn_id]\
            .map(lambda val: itemgetter(val)(nrn_id_geom_lookup))
        self.df_restore.loc[flag_bo_link, "geometry_orig"] = self.df_restore.loc[flag_bo_link, self.bo_id]\
            .map(lambda val: itemgetter(val)(bo_id_geom_lookup))

        # Validate geometry equality.
        self.df_restore["equals"] = False
        self.df_restore.loc[~self.df_restore["geometry_orig"].isna(), "equals"] = \
            self.df_restore.loc[~self.df_restore["geometry_orig"].isna(), ["geometry", "geometry_orig"]]\
                .apply(lambda row: row[0].equals(row[1]), axis=1)

        # Store identifiers of modified arcs.
        self.modified_nrn.update(set(self.df_restore.loc[flag_nrn_restore & (~self.df_restore["equals"]), self.nrn_id]))
        self.modified_bo.update(set(self.df_restore.loc[flag_bo_restore & (~self.df_restore["equals"]), self.bo_id]))

        # TODO: refine equals criteria... perhaps dont include those with the same approximate length...

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
