import click
import fiona
import geopandas as gpd
import logging
import numpy as np
import sys
from collections import Counter
from operator import attrgetter, itemgetter
from pathlib import Path
from shapely.geometry import Polygon
from shapely.ops import polygonize, unary_union
from tabulate import tabulate
from typing import Tuple

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

    def __init__(self, source: str) -> None:
        """
        Initializes the EGP class.

        :param str source: abbreviation for the source province / territory.
        """

        self.source = source
        self.layer = f"nrn_bo_{source}"
        self.src = Path(filepath.parents[2] / "data/interim/egp_data.gpkg")
        self.layer_ngd = f"ngd_a_{source}"
        self.src_ngd = Path(filepath.parents[2] / "data/interim/ngd_a.gpkg")
        self.id_ngd = "bb_uid"

        # Define thresholds.
        self._threshold = 0.95

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

    def _validate_one_to_one(self, base: Polygon, targets: Tuple[Polygon, ...]) -> int:
        """
        Validates if the relationship between the base Polygon and intersecting (target) Polygon(s) is 1:1 when
        factoring in a tolerance for the size of the area of intersection.

        Example, given a tolerance of 90%:
        Index = At least 90% of the base Polygon intersects the target Polygon AND at least 90% of the target Polygon
                intersects the base Polygon AND this is only true for 1 target Polygon.
        None = Any other result.

        :param Polygon base: the base Polygon.
        :param Tuple[Polygon, ...] targets: one or more intersecting Polygon(s).
        :return int: -1 or the index of the target Polygon which is 1:1 with the base Polygon.
        """

        # Calculate areas, multiplied by the size threshold for the independent (non-intersection) sizes.
        base_area = attrgetter("area")(base) * self._threshold
        target_areas = np.array(tuple(map(lambda poly: attrgetter("area")(poly) * self._threshold, targets)))
        intersection_areas = np.array(tuple(map(lambda poly: attrgetter("area")(base.intersection(poly)), targets)))

        # Determine if there is any 1:1 relationship between base and targets; if True, return index of target.
        try:
            return tuple((intersection_areas >= base_area) & (intersection_areas >= target_areas)).index(True)
        except ValueError:
            return -1

    def conflation(self) -> None:
        """Validates the meshblock conflation."""

        logger.info("Validating meshblock conflation.")

        # Generate ngd meshblock lookup dictionaries.
        ngd_idx_id_lookup = dict(zip(self.meshblock_ngd.index, self.meshblock_ngd[self.id_ngd]))
        ngd_id_poly_lookup = dict(zip(self.meshblock_ngd[self.id_ngd], self.meshblock_ngd["geometry"]))

        # Compile the identifier for each ngd polygon intersecting each egp polygon.
        self.meshblock["ngd_ids"] = self.meshblock["geometry"]\
            .map(lambda g: self.meshblock_ngd.sindex.query(g, predicate="intersects"))\
            .map(lambda idxs: itemgetter(*idxs)(ngd_idx_id_lookup))\
            .map(lambda ids: ids if isinstance(ids, tuple) else (ids,))

        # Compile the poly associated with each ngd identifier.
        self.meshblock["ngd_polys"] = self.meshblock["ngd_ids"]\
            .map(lambda ids: itemgetter(*ids)(ngd_id_poly_lookup))\
            .map(lambda polys: polys if isinstance(polys, tuple) else (polys,))

        # Validate 1:1 meshblock relationships.
        self.meshblock["one_to_one_idx"] = self.meshblock[["geometry", "ngd_polys"]]\
            .apply(tuple, axis=1)\
            .map(lambda vals: self._validate_one_to_one(*vals))

        # Assign 1:1 status as attribute - new meshblock.
        self.meshblock["one_to_one"] = self.meshblock["one_to_one_idx"] >= 0

        # Compile 1:1 ngd identifiers based on 1:1 validation results.
        one_to_one_ngd_ids = set(self.meshblock.loc[self.meshblock["one_to_one"], ["one_to_one_idx", "ngd_ids"]]
                                 .apply(lambda row: itemgetter(row[0])(row[1]), axis=1))

        # Assign 1:1 status as attribute - ngd meshblock.
        self.meshblock_ngd["one_to_one"] = self.meshblock_ngd[self.id_ngd].isin(one_to_one_ngd_ids)

    def output_results(self) -> None:
        """Outputs conflation results."""

        logger.info(f"Outputting results.")

        # Export ngd meshblock with conflation indicator.
        helpers.export(self.meshblock_ngd[[self.id_ngd, "one_to_one", "geometry"]], dst=self.src,
                       name=f"meshblock_ngd_{self.source}")

        # Log conflation progress.
        table = tabulate([[k, f"{v:,}"] for k, v in Counter(self.meshblock_ngd["one_to_one"]).items()],
                         headers=["Is Conflated (1:1)", "Count"], tablefmt="rst", colalign=("left", "right"))
        logger.info("\n" + table)


@click.command()
@click.argument("source", type=click.Choice("ab bc mb nb nl ns nt nu on pe qc sk yt".split(), False))
def main(source: str) -> None:
    """
    Instantiates and executes the EGP class.

    :param str source: abbreviation for the source province / territory.
    """

    try:

        with helpers.Timer():
            egp = EGPMeshblockConflation(source)
            egp()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
