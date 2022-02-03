import click
import fiona
import geopandas as gpd
import logging
import sys
from collections import Counter
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


class EGPMeshblockConflation:
    """Defines the EGP meshblock conflation class."""

    def __init__(self, source: str, threshold: int = 80) -> None:
        """
        Initializes the EGP class.

        :param str source: abbreviation for the source province / territory.
        :param int threshold: the percentage of area intersection which constitutes a match, default=80.
        """

        self.source = source
        self.threshold = threshold / 100
        self.layer = f"nrn_bo_{source}"
        self.src = Path(filepath.parents[2] / "data/interim/egp_data.gpkg")
        self.layer_ngd = f"ngd_a_{source}"
        self.src_ngd = Path(filepath.parents[2] / "data/interim/ngd_a.gpkg")
        self.id_ngd = "bb_uid"

        # Configure source path and layer name.
        for src in (self.src, self.src_ngd):
            if src.exists():
                layer = {self.src: self.layer, self.src_ngd: self.layer_ngd}[src]
                if layer not in set(fiona.listlayers(src)):
                    logger.exception(f"Layer \"{layer}\" not found within source: \"{src}\".")
                    sys.exit(1)
            else:
                logger.exception(f"Source not found: \"{src}\".")
                sys.exit(1)

        # Load source data and generate meshblock (all non-ferry and non-exclude arcs).
        logger.info(f"Loading and generating meshblock from source data: {self.src}|layer={self.layer}.")

        df = gpd.read_file(self.src, layer=self.layer)
        meshblock_input = df.loc[(df["segment_type"] != 2) & (df["meshblock_exclude"] != 1)].copy(deep=True)
        self.meshblock = gpd.GeoDataFrame(geometry=list(polygonize(unary_union(meshblock_input["geometry"].to_list()))),
                                          crs=meshblock_input.crs)

        logger.info("Successfully loaded and generated meshblock from source data.")

        # Load ngd meshblock data.
        logger.info(f"Loading ngd meshblock data: {self.src_ngd}|layer={self.layer_ngd}.")
        self.meshblock_ngd = gpd.read_file(self.src_ngd, layer=self.layer_ngd).copy(deep=True)
        logger.info("Successfully loaded ngd meshblock data.")

    def __call__(self) -> None:
        """Executes the EGP class."""

        self.conflation()
        self.output_results()

    def conflation(self) -> None:
        """Validates the meshblock conflation."""

        logger.info("Validating meshblock conflation.")

        meshblock = self.meshblock.copy(deep=True)

        # Generate ngd meshblock lookup dictionaries.
        ngd_idx_id_lookup = dict(zip(self.meshblock_ngd.index, self.meshblock_ngd[self.id_ngd]))
        ngd_id_poly_lookup = dict(zip(self.meshblock_ngd[self.id_ngd], self.meshblock_ngd["geometry"]))

        # Compile the index of each ngd polygon intersecting each egp polygon.
        meshblock["ngd_id"] = meshblock["geometry"]\
            .map(lambda g: self.meshblock_ngd.sindex.query(g, predicate="intersects"))

        # Explode on ngd index groups.
        meshblock = meshblock.explode(column="ngd_id")

        # Compile identifier and poly associated with each ngd index.
        meshblock["ngd_id"] = meshblock["ngd_id"].map(ngd_idx_id_lookup)
        meshblock["ngd_poly"] = meshblock["ngd_id"].map(ngd_id_poly_lookup)

        # Validate cardinality (valid: one-to-one and many-to-one based on egp-to-ngd direction).
        cardinality = meshblock["geometry"].intersection(
            gpd.GeoSeries(meshblock["ngd_poly"], crs=self.meshblock_ngd.crs)
        ).area >= (meshblock.area * self.threshold)

        # Compile valid ngd identifiers based on cardinality.
        valid_ngd_ids = set(meshblock.loc[cardinality, "ngd_id"])

        # Assign validity status as attribute to ngd meshblock.
        self.meshblock_ngd["valid"] = self.meshblock_ngd[self.id_ngd].isin(valid_ngd_ids)

    def output_results(self) -> None:
        """Outputs conflation results."""

        logger.info(f"Outputting results.")

        # Export ngd meshblock with conflation indicator.
        helpers.export(self.meshblock[["geometry"]], dst=self.src, name=f"meshblock_{self.source}")
        helpers.export(self.meshblock_ngd[[self.id_ngd, "valid", "geometry"]], dst=self.src,
                       name=f"meshblock_ngd_{self.source}")

        # Log conflation progress.
        table = tabulate([[k, f"{v:,}"] for k, v in Counter(self.meshblock_ngd["valid"]).items()],
                         headers=["Valid Cardinality", "Count"], tablefmt="rst", colalign=("left", "right"))
        logger.info("\n" + table)


@click.command()
@click.argument("source", type=click.Choice("ab bc mb nb nl ns nt nu on pe qc sk yt".split(), False))
@click.option("--threshold", "-t", type=click.IntRange(min=1, max=99), default=80, show_default=True,
              help="The percentage of area intersection which constitutes a match.")
def main(source: str, threshold: int = 80) -> None:
    """
    Instantiates and executes the EGP class.

    :param str source: abbreviation for the source province / territory.
    :param int threshold: the percentage of area intersection which constitutes a match, default=80.
    """

    try:

        with helpers.Timer():
            egp = EGPMeshblockConflation(source, threshold)
            egp()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
