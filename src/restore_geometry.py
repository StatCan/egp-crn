import click
import fiona
import geopandas as gpd
import logging
import sys
from operator import attrgetter, itemgetter
from pathlib import Path
from shapely.geometry import LineString
from shapely.ops import linemerge
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
        self.export_layer = f"restore_{source}"
        self.nrn_id = "segment_id_orig"
        self.bo_id = "ngd_uid"
        self.src = Path(filepath.parents[1] / "data/interim/egp_data.gpkg")
        self.src_restore = Path(filepath.parents[1] / "data/interim/nrn_bo_restore.gpkg")
        self.modified_nrn = set()
        self.modified_bo = set()

        # Define thresholds.
        self._rnd_prec = 2
        self._len_prec = 0

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

        # Round coordinates to defined decimal precision.
        # Note: This accounts for point snapping which, within a threshold, should not be considered a modification.
        logger.info(f"Rounding coordinates to decimal precision: {self._rnd_prec} (0.{'0'*(self._rnd_prec-1)}1).")

        self.df["geometry"] = self.df["geometry"].map(lambda g: LineString(map(
            lambda pt: [round(itemgetter(0)(pt), self._rnd_prec), round(itemgetter(1)(pt), self._rnd_prec)],
            attrgetter("coords")(g))))
        self.df_restore["geometry_rnd"] = self.df_restore["geometry"].map(lambda g: LineString(map(
            lambda pt: [round(itemgetter(0)(pt), self._rnd_prec), round(itemgetter(1)(pt), self._rnd_prec)],
            attrgetter("coords")(g))))

    def identify_mods(self) -> None:
        """Identifies partially or completely missing restoration data geometries from the source dataset."""

        logger.info("Identifying missing data.")

        # Define flags to classify arcs.
        flag_nrn = self.df["segment_type"].astype(str).isin({"1", "2", "1.0", "2.0"})
        flag_bo = self.df["segment_type"].astype(str).isin({"3", "3.0"})
        flag_nrn_restore = self.df_restore["segment_type"].astype(str).isin({"1", "2", "1.0", "2.0"})
        flag_bo_restore = self.df_restore["segment_type"].astype(str).isin({"3", "3.0"})

        # Filter to exclusively nrn and bo arcs and dissolve geometries on identifiers.
        nrn_dissolved = helpers.groupby_to_list(self.df.loc[flag_nrn], group_field=self.nrn_id, list_field="geometry")\
            .map(lambda geoms: geoms[0] if len(geoms) == 1 else linemerge(geoms))
        bo_dissolved = helpers.groupby_to_list(self.df.loc[flag_bo], group_field=self.bo_id, list_field="geometry")\
            .map(lambda geoms: geoms[0] if len(geoms) == 1 else linemerge(geoms))

        # Create identifier - geometry lookups for new geometries.
        nrn_id_geom_lookup = dict(zip(nrn_dissolved.index, nrn_dissolved.values))
        bo_id_geom_lookup = dict(zip(bo_dissolved.index, bo_dissolved.values))

        # Define flags to query arcs linkage.
        flag_nrn_link = self.df_restore[self.nrn_id].isin(nrn_id_geom_lookup)
        flag_bo_link = self.df_restore[self.bo_id].isin(bo_id_geom_lookup)

        # Compile new arc associated with each original arc.
        self.df_restore["geometry_new"] = None
        self.df_restore.loc[flag_nrn_link, "geometry_new"] = self.df_restore.loc[flag_nrn_link, self.nrn_id]\
            .map(lambda val: itemgetter(val)(nrn_id_geom_lookup))
        self.df_restore.loc[flag_bo_link, "geometry_new"] = self.df_restore.loc[flag_bo_link, self.bo_id]\
            .map(lambda val: itemgetter(val)(bo_id_geom_lookup))

        # Validate geometry equality.
        self.df_restore["equals"] = False
        self.df_restore.loc[~self.df_restore["geometry_new"].isna(), "equals"] = \
            self.df_restore.loc[~self.df_restore["geometry_new"].isna(), ["geometry_rnd", "geometry_new"]]\
                .apply(lambda row: row[0].equals(row[1]), axis=1)

        # Refinement 1) update equality status based on geometry length, rounded to a defined decimal precision.
        refinement_flag = ~(self.df_restore["equals"] | self.df_restore["geometry_new"].isna())
        self.df_restore.loc[refinement_flag, "equals"] =\
            self.df_restore.loc[refinement_flag, ["geometry_rnd", "geometry_new"]].apply(
                lambda row: round(row[0].length, self._len_prec) == round(row[1].length, self._len_prec), axis=1)

        # Store identifiers of modified arcs.
        self.modified_nrn.update(set(self.df_restore.loc[flag_nrn_restore & (~self.df_restore["equals"]), self.nrn_id]))
        self.modified_bo.update(set(self.df_restore.loc[flag_bo_restore & (~self.df_restore["equals"]), self.bo_id]))

    def restore_and_log_mods(self) -> None:
        """Exports records of modified geometries and logs results."""

        logger.info(f"Restoring and logging modified data.")

        # Compile modified records, drop supplementary attribution, and export results.
        export_df = self.df_restore.loc[~self.df_restore["equals"]].copy(deep=True)
        export_df.drop(columns=["geometry_rnd", "geometry_new", "equals"], inplace=True)
        helpers.export(export_df, dst=self.src, name=self.export_layer)

        # Log modification summary.
        table = tabulate([["NRN", len(self.modified_nrn)], ["BO", len(self.modified_bo)]],
                         headers=["Arc Type", "Count"], tablefmt="rst", colalign=("left", "right"))
        logger.info("Summary of restored data:\n" + table)

    def execute(self) -> None:
        """Executes the EGP class."""

        self.identify_mods()
        self.restore_and_log_mods()


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
