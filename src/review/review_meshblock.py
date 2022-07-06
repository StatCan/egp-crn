import click
import fiona
import geopandas as gpd
import logging
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


class CRNMeshblockReview:
    """Defines the CRN meshblock review class."""

    def __init__(self, source: str) -> None:
        """
        Initializes the CRN class.

        :param str source: abbreviation for the source province / territory.
        """

        self.source = source
        self.src = Path(filepath.parents[2] / "data/egp_data.gpkg")
        self.layer = f"meshblock_{self.source}"
        self.src_ngd = Path(filepath.parents[2] / "data/ngd.gpkg")
        self.layer_ngd = f"ngd_a_{self.source.split('_')[0]}"
        self.id = "bb_uid"
        self.meshblock_invalid = None

        # Configure source path and layer name.
        for src in (self.src, self.src_ngd):
            if src.exists():
                layers = set(fiona.listlayers(src))
                layer = {self.src: self.layer, self.src_ngd: self.layer_ngd}[src]
                if layer not in layers:
                    logger.exception(f"Layer \"{layer}\" not found within source: \"{src}\".")
                    sys.exit(1)
            else:
                logger.exception(f"Source not found: \"{src}\".")
                sys.exit(1)

        # Load source data.
        logger.info(f"Loading source data: {self.src}|layer={self.layer}.")
        self.meshblock = gpd.read_file(self.src, layer=self.layer)
        logger.info("Successfully loaded source data.")

        # Load ngd data.
        logger.info(f"Loading ngd data: {self.src_ngd}|layers={self.layer_ngd}.")
        self.meshblock_ngd = gpd.read_file(self.src_ngd, layer=self.layer_ngd)
        logger.info("Successfully loaded ngd data.")

    def __call__(self) -> None:
        """Executes the CRN class."""

        self.compare_neighbours()
        self.output_results()

    def compare_neighbours(self) -> None:
        """
        Compiles and identifies any difference in the set of neighbouring bb identifiers for each linked bb between the
        egp and ngd meshblock networks.
        """

        logger.info("Performing neighbour comparison.")

        # Dissolve egp meshblock based on identifer.
        meshblock = self.meshblock.dissolve(by=self.id, as_index=False)

        # Create neighbour index-identifier lookup dicts.
        meshblock_idx_id_lookup = dict(zip(range(len(meshblock)), meshblock[self.id]))
        meshblock_ngd_idx_id_lookup = dict(zip(range(len(self.meshblock_ngd)), self.meshblock_ngd[self.id]))

        # Compile neighbouring identifiers as sets.
        meshblock["nbrs"] = meshblock["geometry"]\
            .map(lambda g: meshblock.sindex.query(g, predicate="touches"))\
            .map(lambda idxs: itemgetter(*idxs)(meshblock_idx_id_lookup))\
            .map(lambda ids: (ids,) if not isinstance(ids, tuple) else ids).map(set)
        self.meshblock_ngd["nbrs"] = self.meshblock_ngd["geometry"]\
            .map(lambda g: self.meshblock_ngd.sindex.query(g, predicate="touches"))\
            .map(lambda idxs: itemgetter(*idxs)(meshblock_ngd_idx_id_lookup))\
            .map(lambda ids: (ids,) if not isinstance(ids, tuple) else ids).map(set)

        # Create ngd identifier-neighbours lookup dict.
        meshblock_ngd_id_nbrs_lookup = dict(zip(self.meshblock_ngd[self.id], self.meshblock_ngd["nbrs"]))

        # Compile ngd neighbours for each egp bb.
        meshblock["nbrs_ngd"] = meshblock[self.id].map(lambda val: itemgetter(val)(meshblock_ngd_id_nbrs_lookup))

        # Flag egp bbs with different neighbours than their linked ngd bbs.
        self.meshblock_invalid = meshblock.loc[meshblock["nbrs"] != meshblock["nbrs_ngd"]].copy(deep=True)

    def output_results(self) -> None:
        """Outputs results."""

        logger.info(f"Outputting results.")

        # Compile export attributes (extra and missing values).
        self.meshblock_invalid["extra"] = (self.meshblock_invalid["nbrs"] - self.meshblock_invalid["nbrs_ngd"])\
            .map(lambda vals: ",".join(map(str, vals)) if len(vals) else None)
        self.meshblock_invalid["missing"] = (self.meshblock_invalid["nbrs_ngd"] - self.meshblock_invalid["nbrs"])\
            .map(lambda vals: ",".join(map(str, vals)) if len(vals) else None)

        # Filter attributes.
        self.meshblock_invalid = self.meshblock_invalid[[self.id, "extra", "missing", "geometry"]].copy(deep=True)

        # Explode multi-part geometries.
        self.meshblock_invalid = self.meshblock_invalid.explode().reset_index(drop=True)

        # Export data.
        helpers.export(self.meshblock_invalid, dst=self.src, name=f"{self.source}_review_meshblock")

        # Log results.
        invalid = set(self.meshblock_invalid[self.id])
        valid = set(self.meshblock_ngd[self.id]) - invalid
        table = tabulate([["True", f"{len(valid):,}"], ["False", f"{len(invalid):,}"]],
                         headers=["Identical Neighbourhood", "Count"], tablefmt="rst", colalign=("left", "right"))
        logger.info("\n" + table)


@click.command()
@click.argument("source", type=click.Choice(helpers.load_yaml("../config.yaml")["sources"], False))
def main(source: str) -> None:
    """
    Instantiates and executes the CRN class.

    \b
    :param str source: abbreviation for the source province / territory.
    """

    try:

        with helpers.Timer():
            crn = CRNMeshblockReview(source)
            crn()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
