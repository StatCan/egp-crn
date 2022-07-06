import click
import fiona
import geopandas as gpd
import logging
import sys
from itertools import compress
from operator import itemgetter
from pathlib import Path
from shapely.ops import unary_union
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


class CRNRestoreGeometry:
    """Defines the CRN geometry restoration class."""

    def __init__(self, source: str, distance: int = 2) -> None:
        """
        Initializes the CRN class.

        :param str source: abbreviation for the source province / territory.
        :param int distance: the radius of the buffer, default = 2.
        """

        self.source = source
        self.distance = distance
        self.layer = f"nrn_bo_{source}"
        self.export_layer = f"{source}_restore"
        self.nrn_id = "segment_id_orig"
        self.bo_id = "ngd_uid"
        self.src = Path(filepath.parents[1] / "data/egp_data.gpkg")
        self.src_restore = Path(filepath.parents[1] / "data/nrn_bo_restore.gpkg")
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
        self.crn = gpd.read_file(self.src, layer=self.layer)
        logger.info("Successfully loaded source data.")

        # Standardize data.
        self.crn = helpers.standardize(self.crn)

        # Load source restoration data.
        logger.info(f"Loading source restoration data: {self.src_restore}|layer={self.layer}.")
        self.crn_restore = gpd.read_file(self.src_restore, layer=self.layer)
        self.crn_restore_cols = set(self.crn_restore.columns)
        logger.info("Successfully loaded source restoration data.")

    def __call__(self) -> None:
        """Executes the CRN class."""

        self.identify_mods()
        self.restore_and_log_mods()

    def identify_mods(self) -> None:
        """Identifies original geometries which have been removed or modified in the source dataset."""

        logger.info("Identifying modified data.")

        # Define flags to classify arcs.
        flag_nrn_restore = self.crn_restore["segment_type"].isin({1, 2})
        flag_bo_restore = self.crn_restore["segment_type"] == 3

        # Flag missing arcs based on identifiers and store results.
        self.modified_nrn.update(set(self.crn_restore.loc[flag_nrn_restore, self.nrn_id]) - set(self.crn[self.nrn_id]))
        self.modified_bo.update(set(self.crn_restore.loc[flag_bo_restore, self.bo_id]) - set(self.crn[self.bo_id]))

        # Flag modified arcs based on buffer intersection.

        # Create buffers and index-buffer lookup dict from new arcs.
        buffers = self.crn.buffer(self.distance, resolution=5)
        idx_buffer_lookup = dict(buffers)

        # Compile and dissolve all buffer polygons intersecting each original arc.
        self.crn_restore["buffer_idxs"] = self.crn_restore["geometry"].map(
            lambda g: buffers.sindex.query(g, predicate="intersects"))
        self.crn_restore["buffer"] = self.crn_restore["buffer_idxs"].map(
            lambda idxs: unary_union(itemgetter(*idxs)(idx_buffer_lookup)) if len(idxs) else None)

        # Flag arcs not completely within the intersecting buffer.
        flag_buffer = ~self.crn_restore["buffer"].isna()
        flag_mods = ~gpd.GeoSeries(self.crn_restore.loc[flag_buffer, ["geometry", "buffer"]]
                                   .apply(lambda row: row[0].difference(row[1]), axis=1)).is_empty
        mods_idxs = set(compress(self.crn_restore[flag_buffer].index, flag_mods))
        flag_mods = self.crn_restore.index.isin(mods_idxs)

        # Store results.
        self.modified_nrn.update(set(self.crn_restore.loc[flag_mods & flag_nrn_restore, self.nrn_id]))
        self.modified_bo.update(set(self.crn_restore.loc[flag_mods & flag_bo_restore, self.bo_id]))

    def restore_and_log_mods(self) -> None:
        """Exports records of modified geometries and logs results."""

        logger.info(f"Restoring and logging modified data.")

        # Compile modified records, drop supplementary attribution, and export results.
        export_df = self.crn_restore.loc[(self.crn_restore[self.nrn_id].isin(self.modified_nrn)) |
                                         (self.crn_restore[self.bo_id].isin(self.modified_bo))].copy(deep=True)
        export_df.drop(columns=set(self.crn_restore.columns)-self.crn_restore_cols, inplace=True)
        helpers.export(export_df, dst=self.src, name=self.export_layer)

        # Log modification summary.
        table = tabulate([["NRN", len(self.modified_nrn)], ["BO", len(self.modified_bo)]],
                         headers=["Arc Type", "Count"], tablefmt="rst", colalign=("left", "right"))
        logger.info("Summary of restored data:\n" + table)


@click.command()
@click.argument("source", type=click.Choice(helpers.load_yaml("config.yaml")["sources"], False))
@click.option("--distance", "-d", type=click.IntRange(min=1), default=2, show_default=True,
              help="The radius of the buffer.")
def main(source: str, distance: int = 2) -> None:
    """
    Instantiates and executes the CRN class.

    \b
    :param str source: abbreviation for the source province / territory.
    :param int distance: the radius of the buffer, default = 2.
    """

    try:

        with helpers.Timer():
            crn = CRNRestoreGeometry(source, distance)
            crn()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
