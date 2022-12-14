import click
import fiona
import geopandas as gpd
import logging
import pandas as pd
import sqlite3
import sys
import uuid
from pathlib import Path
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
        self.export = {
            f"{self.source}_nrn_mod": None,
            f"{self.source}_crn_buffers": None
        }

        # CRN
        self.crn = None
        self.crn_regions = dict.fromkeys(map(lambda r: f"crn_{r}",
                                             filter(lambda r: r.startswith(self.source),
                                                    helpers.load_yaml("../config.yaml")["sources"])))
        self.id_crn = "segment_id"
        self.src_crn = self.dst

        # NRN
        self.nrn = None
        self.layer_nrn = f"nrn_{self.source}"
        self.id_nrn = "uuid"
        self.src_nrn = Path(helpers.load_yaml("../config.yaml")["filepaths"]["deltas_nrn"]
                            .replace("vintage", str(self.vintage)))

        # NGD
        self.con = None
        self.ngd_al = None
        self.ngd_prov_code = helpers.load_yaml("../config.yaml")["ngd_prov_codes"][self.source]
        self.layer_ngd_al = f"ngd_al"
        self.id_ngd = "ngd_uid"
        self.src_ngd = Path(helpers.load_yaml("../config.yaml")["filepaths"]["deltas_ngd"]
                            .replace("vintage", str(self.vintage)))

        # Configure src paths and layer names.
        src, layer = {"ngd": (self.src_ngd, self.layer_ngd_al), "nrn": (self.src_nrn, self.layer_nrn)}[self.mode]
        if src.exists():
            if layer not in set(fiona.listlayers(src)):
                logger.exception(f"Layer {layer} does not exist in source {src}.")
                sys.exit(1)
        else:
            logger.exception(f"Source does not exist: {src}.")
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
        if self.mode == "nrn":
            self.fetch_nrn_deltas()

        self._write_deltas()

    def _load_data(self) -> None:
        """Loads and standardizes CRN, NGD, and NRN data."""

        # Load source data - CRN.
        for layer in self.crn_regions:
            logger.info(f"Loading CRN source data: {self.src_crn}|layer={layer}.")
            self.crn_regions[layer] = gpd.read_file(self.src_crn, layer=layer).copy(deep=True)

        logger.info(f"Successfully loaded CRN source data.")

        # Load source data - NGD.
        if self.mode == "ngd":
            logger.info(f"Loading NGD source data: {self.src_ngd}|layer={self.layer_ngd_al}.")

            self.con = sqlite3.connect(self.src_ngd)
            self.ngd_al = pd.read_sql_query(
                f"SELECT {self.id_ngd.upper()} FROM {self.layer_ngd_al} WHERE "
                f"SUBSTR(CSD_UID_L, 1, 2) == '{self.ngd_prov_code}' OR "
                f"SUBSTR(CSD_UID_R, 1, 2) == '{self.ngd_prov_code}'", con=self.con)

            logger.info("Successfully loaded NGD source data.")

        # Load source data - NRN.
        if self.mode == "nrn":
            logger.info(f"Loading NRN source data: {self.src_nrn}|layer={self.layer_nrn}.")

            self.nrn = gpd.read_file(self.src_nrn, layer=self.layer_nrn)

            logger.info("Successfully loaded NRN source data.")

        # Standardize data - CRN.
        for layer, df in self.crn_regions.items():
            logger.info(f"Standardizing CRN data, layer={layer}.")
            df = helpers.standardize(df, round_coords=False)
            df = helpers.snap_nodes(df)
            self.crn_regions[layer] = df.copy(deep=True)
        self.crn = pd.concat(self.crn_regions.values(), ignore_index=True).copy(deep=True)

        # Standardize data - NGD.
        if self.mode == "ngd":
            logger.info(f"Standardizing NGD data.")
            self.ngd_al.columns = map(str.lower, self.ngd_al.columns)
            self.ngd_al.index = self.ngd_al[self.id_ngd]

        # Standardize data - NRN.
        if self.mode == "nrn":
            logger.info(f"Standardizing NRN data.")
            self.nrn.columns = map(str.lower, self.nrn.columns)
            if self.nrn.crs.to_epsg() != self.crn.crs.to_epsg():
                self.nrn = self.nrn.to_crs(self.crn.crs)
            self.nrn[self.id_nrn] = [uuid.uuid4().hex for _ in range(len(self.nrn))]
            self.nrn.index = self.nrn[self.id_nrn]

    def _write_deltas(self) -> None:
        """Write output datasets and logs."""

        logger.info(f"Writing delta outputs.")

        # Export required datasets.
        if not self.flag_new_gpkg:
            helpers.delete_layers(dst=self.dst, layers=self.export.keys())
        for layer, df in {**self.crn_regions, **self.export}.items():
            if isinstance(df, pd.DataFrame):
                helpers.export(df, dst=self.dst, name=layer)

        # Update existing dataset - NGD.
        if len(self.delta_ids["ngd_add"]):

            # Conditionally add flag column.
            if "ngd_add" not in pd.read_sql_query(f"SELECT * FROM {self.layer_ngd_al} LIMIT 0", con=self.con).columns:
                _ = self.con.execute(f"ALTER {self.layer_ngd_al} ADD COLUMN ngd_add INT DEFAULT 0")

            # Populate flag column.
            _ = self.con.execute(f"UPDATE {self.layer_ngd_al} SET ngd_add = 1 WHERE {self.id_ngd} IN "
                                 f"{*self.delta_ids['ngd_add'],}".replace(",)", ")"))

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
        self.delta_ids["ngd_add"] = set(self.ngd_al.loc[self.ngd_al["segment_type"] == 3, self.id_ngd]) - \
                                    set(self.crn[self.id_ngd])

        # Deletions.
        self.delta_ids["ngd_del"] = set(self.crn[self.id_ngd]) - set(self.ngd_al[self.id_ngd]) - {-1}

        # Add flags to CRN dataset.
        if len(self.delta_ids["ngd_del"]):
            for layer, df in self.crn_regions.items():
                self.crn_regions[layer]["ngd_del"] = df[self.id_ngd].isin(self.delta_ids["ngd_del"]).map(int)

    def fetch_nrn_deltas(self) -> None:
        """Identifies and retrieves NRN deltas."""

        logger.info("Fetching NRN deltas.")

        # Filter CRN to exclusively roads and ferries.
        crn = self.crn.loc[self.crn["segment_type"].isin({1, 2})]

        # Generate CRN buffers.
        crn_buffers = crn.buffer(self.radius, resolution=5)

        # Query CRN buffers which contain each NRN arc.
        within = self.nrn["geometry"].map(lambda g: set(crn_buffers.sindex.query(g, predicate="within")))

        # Compile identifiers of NRN arcs not contained within any CRN buffers.
        self.delta_ids["nrn_mod"] = set(within.loc[within.map(len) == 0].index)

        # Construct export datasets.
        if len(self.delta_ids["nrn_mod"]):

            # Export dataset - NRN modifications.
            self.export[f"{self.source}_nrn_mod"] = \
                self.nrn.loc[self.nrn.index.isin(self.delta_ids["nrn_mod"])].copy(deep=True)

            # Export dataset - CRN buffers.
            # TODO - remove export once finished fixing buffer network
            self.export[f"{self.source}_crn_buffers"] = gpd.GeoDataFrame({self.id_crn: crn[self.id_crn]},
                                                                         geometry=list(crn_buffers), crs=self.crn.crs)


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
