import click
import fiona
import geopandas as gpd
import logging
import pandas as pd
import sys
from operator import itemgetter
from pathlib import Path
from shapely.ops import unary_union
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


def get_finished_sources() -> list:
    """
    Returns source codes where all constituent regions have been finished.

    \b
    :return list: a list of valid source codes.
    """

    # Compile original and finished regions.
    regions_orig = set(helpers.load_yaml("../config.yaml")["sources"])
    regions_finished = set(map(lambda r: r.split("crn_")[1],
                               fiona.listlayers(helpers.load_yaml("../config.yaml")["filepaths"]["crn_finished"])))

    # Configure valid sources from regions.
    sources = set(map(lambda r: r.split("_")[0], regions_orig)) - \
              set(map(lambda r: r.split("_")[0], regions_orig.difference(regions_finished)))

    return sorted(sources)


class CRNDeltas:
    """Defines the CRN deltas class."""

    def __init__(self, source: str, mode: str, vintage: int, radius: int = 5) -> None:
        """
        Initializes the CRN class.

        \b
        :param str source: code for the source province / territory.
        :param str mode: the type of deltas to be returned: {'ngd', 'nrn'}.
        :param int vintage: deltas date, expected to be suffixed to the source file name.
        :param int radius: CRN buffer radius used for NRN delta detection, default=5.
        """

        self.source = source
        self.mode = mode
        self.vintage = vintage
        self.radius = radius

        self.dst = Path(filepath.parents[2] / f"data/crn_deltas_{self.mode}_{self.source}_{self.vintage}.gpkg")
        self.flag_new_gpkg = False
        self.delta_ids = {delta_type: set() for delta_type in ("ngd_add", "ngd_del", "nrn_mod")}
        self.export = dict.fromkeys(map(lambda name: f"{self.source}_{name}", ("ngd_add", "nrn_mod")))

        # CRN
        self.crn = None
        self.src_crn = self.dst
        self.crn_regions = dict.fromkeys(map(lambda r: f"crn_{r}",
                                             filter(lambda r: r.startswith(self.source),
                                                    helpers.load_yaml("../config.yaml")["sources"])))

        # NRN / NGD
        self.df = None

        if self.mode == "ngd":
            self.layer = f"ngd_al_{self.source}"
            self.id = "ngd_uid"
            self.src = Path(helpers.load_yaml("../config.yaml")["filepaths"]["deltas_ngd"]
                            .replace("vintage", str(self.vintage)))

        elif self.mode == "nrn":
            self.layer = f"nrn_{self.source}"
            self.id = "segment_id"
            self.src = Path(helpers.load_yaml("../config.yaml")["filepaths"]["deltas_nrn"]
                            .replace("vintage", str(self.vintage)))

        # Configure src and layer.
        if self.src.exists():
            if self.layer not in set(fiona.listlayers(self.src)):
                logger.exception(f"Layer {self.layer} does not exist in source {self.src}.")
                sys.exit(1)
        else:
            logger.exception(f"Source does not exist: {self.src}.")
            sys.exit(1)

        # Configure dst.
        if self.dst.exists():
            missing = set(self.crn_regions) - set(fiona.listlayers(self.dst))
            if len(missing):
                logger.exception(f"Finished CRN regions are missing from dst {self.dst}: {*missing,}.")
                sys.exit(1)
        else:
            helpers.create_gpkg(self.dst)
            self.flag_new_gpkg = True
            self.src_crn = Path(helpers.load_yaml("../config.yaml")["filepaths"]["crn_finished"])

    def __call__(self) -> None:
        """Executes the CRN class."""

        self._load_data()

        if self.mode == "ngd":
            self.fetch_ngd_deltas()
        elif self.mode == "nrn":
            self.fetch_nrn_deltas()

        self._write_deltas()

    def _load_data(self) -> None:
        """Loads and standardizes CRN and NGD / NRN data."""

        # Load source data - CRN.
        for layer in self.crn_regions:
            logger.info(f"Loading CRN source data: {self.src_crn}|layer={layer}.")
            self.crn_regions[layer] = gpd.read_file(self.src_crn, layer=layer).copy(deep=True)

        logger.info(f"Successfully loaded CRN source data.")

        # Standardize data - CRN.
        for layer, df in self.crn_regions.items():
            logger.info(f"Standardizing CRN data, layer={layer}.")
            df = helpers.standardize(df, round_coords=False)
            self.crn_regions[layer] = df.copy(deep=True)
        self.crn = pd.concat(self.crn_regions.values(), ignore_index=True).copy(deep=True)

        # Load source data - NGD / NRN.
        logger.info(f"Loading source data: {self.src}|layer={self.layer}.")
        self.df = gpd.read_file(self.src, layer=self.layer)
        self.df.index = self.df[self.id]
        logger.info(f"Successfully loaded {self.mode.upper()} source data.")

    def _write_deltas(self) -> None:
        """Write output datasets and logs."""

        logger.info(f"Writing delta outputs.")

        # Export required datasets / execute required subprocesses.
        if not self.flag_new_gpkg:
            helpers.delete_layers(dst=self.dst, layers=self.export.keys())
        for layer, df in {**self.crn_regions, **self.export}.items():
            if isinstance(df, pd.DataFrame):
                helpers.export(df, dst=self.dst, name=layer)

        # Log results summary.
        summary = tabulate([["NGD Additions", len(self.delta_ids["ngd_add"]) if self.mode == "ngd" else "N/A"],
                            ["NGD Deletions", len(self.delta_ids["ngd_del"]) if self.mode == "ngd" else "N/A"],
                            ["NRN Modifications", len(self.delta_ids["nrn_mod"]) if self.mode == "nrn" else "N/A"]],
                           headers=["Delta Type", "Count"], tablefmt="rst", colalign=("left", "right"))

        logger.info("Deltas results:\n" + summary)

    def fetch_ngd_deltas(self) -> None:
        """Identifies and retrieves NGD deltas."""

        logger.info("Fetching NGD deltas.")

        # Additions.
        self.delta_ids["ngd_add"].update(set(self.df.loc[self.df["segment_type"] == 3, self.id]) -
                                         set(self.crn[self.id]))

        # Deletions.
        self.delta_ids["ngd_del"].update(set(self.crn[self.id]) - set(self.df[self.id]) - {-1})

        # Construct export dataset - NGD Additions.
        if len(self.delta_ids["ngd_add"]):
            self.export[f"{self.source}_ngd_add"] = \
                self.df.loc[self.df.index.isin(self.delta_ids["ngd_add"])].copy(deep=True)

        # Add flags to CRN dataset - NGD Deletions.
        if len(self.delta_ids["ngd_del"]):
            for layer, df in self.crn_regions.items():
                self.crn_regions[layer]["ngd_del"] = df[self.id].isin(self.delta_ids["ngd_del"]).map(int)

    def fetch_nrn_deltas(self) -> None:
        """Identifies and retrieves NRN deltas."""

        logger.info("Fetching NRN deltas.")

        # Filter CRN to exclusively roads and ferries.
        crn = self.crn.loc[self.crn["segment_type"].isin({1, 2})].copy(deep=True)

        # Generate CRN buffers and index-geometry lookup.
        crn_buffers = crn.buffer(self.radius, resolution=5)
        crn_idx_buffer_lookup = dict(zip(range(len(crn_buffers)), crn_buffers))

        # Query CRN buffers which contain each NRN arc.
        within = self.df["geometry"].map(lambda g: set(crn_buffers.sindex.query(g, predicate="within")))

        # Filter to NRN arcs which are not within any CRN buffers.
        nrn_ = self.df.loc[within.map(len) == 0].copy(deep=True)
        if len(nrn_):

            # Query CRN buffers which intersect each remaining NRN arc.
            intersects = nrn_["geometry"].map(lambda g: set(crn_buffers.sindex.query(g, predicate="intersects")))

            # Compile identifiers of NRN arcs not intersecting any CRN buffers.
            self.delta_ids["nrn_mod"].update(set(intersects.loc[intersects.map(len) == 0].index))

            # Filter to NRN arcs which intersect CRN buffers.
            nrn_ = nrn_.loc[intersects.map(len) > 0].copy(deep=True)
            intersects = intersects.loc[intersects.map(len) > 0].copy(deep=True)
            if len(nrn_):

                # Compile and dissolve all intersecting buffers.
                dissolved_buffers = gpd.GeoSeries(
                    intersects.map(lambda idxs: itemgetter(*idxs)(crn_idx_buffer_lookup))
                    .map(lambda val: unary_union(val) if isinstance(val, tuple) else val),
                    crs=self.crn.crs)

                # Test within predicate between NRN and dissolved buffers.
                within = nrn_.within(dissolved_buffers)

                # Compile identifiers of NRN arcs not contained within any dissolved CRN buffers.
                self.delta_ids["nrn_mod"].update(set(within.loc[~within].index))

        # Construct export dataset.
        if len(self.delta_ids["nrn_mod"]):
            self.export[f"{self.source}_nrn_mod"] = \
                self.df.loc[self.df.index.isin(self.delta_ids["nrn_mod"])].copy(deep=True)


@click.command()
@click.argument("source", type=click.Choice(get_finished_sources(), False))
@click.argument("mode", type=click.Choice(["ngd", "nrn"], False))
@click.argument("vintage", type=click.INT)
@click.option("--radius", "-r", type=click.INT, default=5, show_default=True,
              help="CRN buffer radius used for NRN delta detection.")
def main(source: str, mode: str, vintage: int, radius: int = 5) -> None:
    """
    Instantiates and executes the CRN class.

    \b
    :param str source: code for the source province / territory.
    :param str mode: the type of deltas to be returned: {'ngd', 'nrn'}.
    :param int vintage: deltas date, expected to be suffixed to the source file name.
    :param int radius: CRN buffer radius used for NRN delta detection, default=5.
    """

    try:

        with helpers.Timer():
            deltas = CRNDeltas(source, mode, vintage, radius)
            deltas()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
