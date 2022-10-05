import click
import fiona
import geopandas as gpd
import logging
import pandas as pd
import sys
from itertools import chain
from operator import itemgetter
from pathlib import Path

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

        :param str source: code for the source region (working area).
        """

        self.source = source

        self.src = Path(filepath.parents[2] / "data/crn.gpkg")
        self.layer_arc = f"crn_{self.source}"
        self.layer_meshblock = f"meshblock_{self.source}"

        self.src_ngd = Path(filepath.parents[2] / "data/ngd.gpkg")
        self.layer_arc_ngd = f"ngd_al_{self.source.split('_')[0]}"
        self.layer_meshblock_ngd = f"ngd_a_{self.source.split('_')[0]}"

        self.id_arc = "segment_id"
        self.id_arc_ngd = "ngd_uid"
        self.id_meshblock_ngd = "bb_uid"
        self.id_meshblock_l_ngd = "bb_uid_l"
        self.id_meshblock_r_ngd = "bb_uid_r"

        # Configure source path and layer name.
        for src in (self.src, self.src_ngd):
            if src.exists():
                layers = set(fiona.listlayers(src))
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

        self.conflation()
        self.output_results()

    def conflation(self) -> None:
        """Performs the arc conflation."""

        logger.info(f"Performing arc conflation.")

        # Define linkage attribution.
        for new_col in ("meshblock_idx", f"{self.id_meshblock_ngd}_linked", f"{self.id_arc_ngd}_linked"):
            self.arcs[new_col] = ((-1,),) * len(self.arcs)

        # Filter source data to non-ferry arcs.
        arcs = self.arcs.loc[self.arcs["segment_type"] != 2].copy(deep=True)

        # Compile the meshblock index associated with each arc - an arc will be covered by or contained by a polygon.
        meshblock_boundaries = self.meshblock.boundary
        arcs["meshblock_idx"] = arcs["geometry"].map(
            lambda g: set(meshblock_boundaries.sindex.query(g, predicate="covered_by")))
        arcs.loc[arcs["meshblock_idx"].map(len) == 0, "meshblock_idx"] = \
            arcs.loc[arcs["meshblock_idx"].map(len) == 0, "geometry"].map(
                lambda g: set(self.meshblock.sindex.query(g, predicate="within")))

        # Retrieve the ngd meshblock identifier linking to each new meshblock.
        meshblock_idx_ngd_id = dict(zip(self.meshblock.index, self.meshblock[self.id_meshblock_ngd]))
        arcs[f"{self.id_meshblock_ngd}_linked"] = arcs["meshblock_idx"]\
            .map(lambda idxs: itemgetter(*idxs)(meshblock_idx_ngd_id))\
            .map(lambda vals: vals if isinstance(vals, tuple) else (vals,)).map(set).map(tuple)

        # Create ngd meshblock - arc identifiers lookup. Add -1 to dict for non linkages.
        arcs_ngd_both_sides = pd.DataFrame().append([
            self.arcs_ngd[[self.id_arc_ngd, self.id_meshblock_l_ngd]]
                .rename(columns={self.id_meshblock_l_ngd: self.id_meshblock_ngd}),
            self.arcs_ngd[[self.id_arc_ngd, self.id_meshblock_r_ngd]]
                .rename(columns={self.id_meshblock_r_ngd: self.id_meshblock_ngd})
        ])
        ngd_meshblock_id_to_arc_ids = arcs_ngd_both_sides.groupby(
            by=self.id_meshblock_ngd, axis=0, as_index=True)[self.id_arc_ngd].agg(tuple).to_dict()
        ngd_meshblock_id_to_arc_ids[-1] = (-1,)

        # Compile ngd arc identifiers associated with each linked ngd meshblock.
        arcs[f"{self.id_arc_ngd}_linked"] = arcs[f"{self.id_meshblock_ngd}_linked"]\
            .map(lambda ids: itemgetter(*ids)(ngd_meshblock_id_to_arc_ids))\
            .map(lambda vals: tuple(chain.from_iterable(vals) if isinstance(vals[0], tuple) else vals))

        # Update original dataset with linkage attribution.
        self.arcs.loc[arcs.index] = arcs.copy(deep=True)

    def output_results(self) -> None:
        """Outputs conflation results."""

        logger.info(f"Outputting results.")

        # Delete temporary attributes.
        self.arcs.drop(columns=["meshblock_idx"], inplace=True)

        # Convert list-like attributes to comma-delimited strings.
        for col in (f"{self.id_meshblock_ngd}_linked", f"{self.id_arc_ngd}_linked"):
            self.arcs[col] = self.arcs[col].map(lambda vals: ",".join(map(str, vals)))

        # Export arcs with linked ngd meshblock and arc identifiers.
        helpers.export(self.arcs, dst=self.src, name=self.layer_arc)


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
            crn = CRNArcConflation(source)
            crn()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
