import click
import fiona
import geopandas as gpd
import logging
import pandas as pd
import sys
from operator import attrgetter, itemgetter
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


class CRNMeshblockConflation:
    """Defines the CRN meshblock conflation class."""

    def __init__(self, source: str, threshold: int = 80) -> None:
        """
        Initializes the CRN class.

        \b
        :param str source: code for the source region (working area).
        :param int threshold: the percentage of area intersection which constitutes a match, default=80.
        """

        self.source = source
        self.threshold = threshold / 100

        self.src = Path(filepath.parents[2] / "data/crn.gpkg")
        self.dst = Path(filepath.parents[2] / "data/crn.gpkg")
        self.layer_arc = f"crn_{self.source}"

        self.src_ngd = helpers.load_yaml("../config.yaml")["filepaths"]["ngd"]
        self.layer_meshblock_ngd = f"ngd_a_{self.source}"

        self.id_arc_ngd = "ngd_uid"
        self.id_meshblock_ngd = "bb_uid"

        # Configure src / dst paths and layer name.
        if self.dst.exists():
            if self.layer_arc not in set(fiona.listlayers(self.dst)):
                self.src = helpers.load_yaml("../config.yaml")["filepaths"]["crn"]
        else:
            helpers.create_gpkg(self.dst)
            self.src = helpers.load_yaml("../config.yaml")["filepaths"]["crn"]

        # Load source data.
        logger.info(f"Loading source data: {self.src}|layer={self.layer_arc}.")
        df = gpd.read_file(self.src, layer=self.layer_arc)

        # Standardize data and snap nodes.
        df = helpers.standardize(df)
        df = helpers.snap_nodes(df)

        # Generate meshblock (all non-deadend and non-ferry arcs).
        logger.info(f"Generating meshblock from source data.")

        nodes = df["geometry"].map(lambda g: itemgetter(0, -1)(attrgetter("coords")(g))).explode()
        deadends = set(nodes.loc[~nodes.duplicated(keep=False)].index)
        meshblock_input = df.loc[(~df.index.isin(deadends)) & (df["segment_type"] != 2)].copy(deep=True)
        self.meshblock = gpd.GeoDataFrame(geometry=list(polygonize(unary_union(meshblock_input["geometry"].to_list()))),
                                          crs=meshblock_input.crs)

        logger.info("Successfully loaded and generated meshblock from source data.")

        # Load ngd meshblock data.
        logger.info(f"Loading ngd meshblock data: {self.src_ngd}|layer={self.layer_meshblock_ngd}.")
        self.meshblock_ngd = gpd.read_file(self.src_ngd, layer=self.layer_meshblock_ngd).copy(deep=True)
        logger.info("Successfully loaded ngd meshblock data.")

        # Export data.
        helpers.export(df, dst=self.dst, name=self.layer_arc)

    def __call__(self) -> None:
        """Executes the CRN class."""

        self.conflation()
        self.output_results()

    def conflation(self) -> None:
        """Performs the meshblock conflation."""

        logger.info("Performing meshblock conflation.")

        meshblock = self.meshblock.copy(deep=True)

        # Generate ngd meshblock lookup dictionaries.
        ngd_idx_id_lookup = dict(zip(self.meshblock_ngd.index, self.meshblock_ngd[self.id_meshblock_ngd]))
        ngd_id_poly_lookup = dict(zip(self.meshblock_ngd[self.id_meshblock_ngd], self.meshblock_ngd["geometry"]))

        # Compile the index of each ngd polygon intersecting each crn polygon.
        meshblock["ngd_id"] = meshblock["geometry"]\
            .map(lambda g: self.meshblock_ngd.sindex.query(g, predicate="intersects"))

        # Explode on ngd index groups.
        meshblock = meshblock.explode(column="ngd_id")

        # Compile identifier and poly associated with each ngd index.
        meshblock["ngd_id"] = meshblock["ngd_id"].map(ngd_idx_id_lookup)
        meshblock["ngd_poly"] = meshblock["ngd_id"].map(ngd_id_poly_lookup)

        # Validate cardinality (valid: one-to-one and many-to-one based on crn-to-ngd direction).
        meshblock["occupation_area"] = meshblock["geometry"].intersection(
            gpd.GeoSeries(meshblock["ngd_poly"], crs=self.meshblock_ngd.crs)
        ).area / meshblock.area

        # Compile valid identifiers based on cardinality.
        flag_valid = meshblock["occupation_area"] >= self.threshold
        valid_meshblock_idx_ngd_id_lookup = dict(zip(meshblock.loc[flag_valid].index,
                                                     meshblock.loc[flag_valid, "ngd_id"]))
        valid_ngd_ids = set(valid_meshblock_idx_ngd_id_lookup.values())
        valid_meshblock_idxs = set(valid_meshblock_idx_ngd_id_lookup)

        # Compile maximum occupation percentage for each invalid ngd meshblock as a lookup dictionary.
        flag_invalid = ~meshblock["ngd_id"].isin(valid_ngd_ids)
        occupation_pct_ngd = pd.DataFrame({
            "ngd_id": meshblock.loc[flag_invalid, "ngd_id"].values,
            "occupation_pct": (meshblock.loc[flag_invalid, "occupation_area"] * 100).fillna(0).map(int).values})\
            .sort_values(by=["ngd_id", "occupation_pct"])\
            .drop_duplicates(subset="ngd_id", keep="last")
        occupation_pct_ngd = dict(zip(occupation_pct_ngd["ngd_id"], occupation_pct_ngd["occupation_pct"]))

        # Compile maximum occupation percentage for each invalid meshblock as a lookup dictionary.
        flag_invalid = ~meshblock.index.isin(valid_meshblock_idxs)
        occupation_pct_meshblock = pd.DataFrame({
            "idx": meshblock.loc[flag_invalid].index.values,
            "occupation_pct": (meshblock.loc[flag_invalid, "occupation_area"] * 100).fillna(0).map(int).values})\
            .sort_values(by=["idx", "occupation_pct"])\
            .drop_duplicates(subset="idx", keep="last")
        occupation_pct_meshblock = dict(zip(occupation_pct_meshblock["idx"],
                                            occupation_pct_meshblock["occupation_pct"]))

        # Assign validity status and occupation percentage as attributes to ngd meshblock.
        self.meshblock_ngd["valid"] = self.meshblock_ngd[self.id_meshblock_ngd].isin(valid_ngd_ids)
        self.meshblock_ngd["occupation_pct"] = self.meshblock_ngd[self.id_meshblock_ngd]\
            .map(occupation_pct_ngd).fillna(-1)

        # Assign ngd bb identifier, validity status, and occupation percentage as attributes to meshblock.
        meshblock_index = pd.Series(self.meshblock.index, index=self.meshblock.index)
        self.meshblock["valid"] = meshblock_index.isin(valid_meshblock_idxs)
        self.meshblock["occupation_pct"] = meshblock_index.map(occupation_pct_meshblock).fillna(-1)
        self.meshblock[self.id_meshblock_ngd] = meshblock_index\
            .map(valid_meshblock_idx_ngd_id_lookup).fillna(-1).map(int)

    def output_results(self) -> None:
        """Outputs conflation results."""

        logger.info(f"Outputting results.")

        # Export meshblock layers with conflation indicator.
        for layer, df in {"meshblock": self.meshblock, "meshblock_ngd": self.meshblock_ngd}.items():
            helpers.export(df[[self.id_meshblock_ngd, "valid", "occupation_pct", "geometry"]], dst=self.dst,
                           name=f"{self.source}_{layer}")

        # Log conflation progress.
        count_ngd = sum(~self.meshblock_ngd["valid"])
        count_crn = sum(~self.meshblock["valid"])
        count_total = count_ngd + count_crn
        table = tabulate([["NGD", f"{count_ngd:,}"], ["CRN", f"{count_crn:,}"], ["Total", f"{count_total:,}"]],
                         headers=["Invalid Blocks", "Count"], tablefmt="rst", colalign=("left", "right"))
        logger.info("\n" + table)


@click.command()
@click.argument("source", type=click.Choice(helpers.load_yaml("../config.yaml")["sources"], False))
@click.option("--threshold", "-t", type=click.IntRange(min=60, max=99), default=80, show_default=True,
              help="The percentage of area intersection which constitutes a match.")
def main(source: str, threshold: int = 80) -> None:
    """
    Instantiates and executes the CRN class.

    \b
    :param str source: code for the source region (working area).
    :param int threshold: the percentage of area intersection which constitutes a match, default=80.
    """

    try:

        with helpers.Timer():
            crn = CRNMeshblockConflation(source, threshold)
            crn()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
