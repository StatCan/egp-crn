# TODO: make the following mods:
# 1) use regions instead of provinces (change click argument and also change difference detection to reference the concatenated regions dataframe) - CANCEL - use provs and concat split crn data
# 2) src must reference a gpkg other than crn_restore.gpkg (also included _restore for comparison) and ngd.gpkg (perhaps crn_date.gpkg and ngd_date.gpkg)
# 3) dst can still work like the rest to create a new crn.gpkg in the data directory and use the same layer names with validations as attributes.
# 4) add parameter to CRN scripts to tell it to use delta as src data (see point #2) instead of crn_restore.gpkg and ngd.gpkg.
# NGD adds/removes: set intersection
# NGD mods: get mods by attribute query, verify fixes via crn meshblock_conflation against new ngd_a
# NRN deltas (adds/removes/mods): network buffer (similar to restore_geometry.py) since this accounts for minor differences.

import click
import fiona
import geopandas as gpd
import logging
import pandas as pd
import sqlite3
import sys
import uuid
from copy import deepcopy
from itertools import chain
from operator import attrgetter, itemgetter
from pathlib import Path
from shapely.geometry import Point, LineString, MultiLineString
from shapely.ops import unary_union
from tabulate import tabulate
from collections import Counter

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


def get_finished_provs() -> list:
    """
    Returns all provincial / territorial abbreviations where all constituent regions have been finished.

    \b
    :return list: a list of provincial / territorial abbreviations.
    """

    # Compile original and finished sources.
    sources_orig = set(helpers.load_yaml("../config.yaml")["sources"])
    sources_finished = set(map(lambda source: source.split("crn_")[1],
                               fiona.listlayers(helpers.load_yaml("../config.yaml")["filepaths"]["crn_finished"])))

    # Configure finished provinces / territories.
    provs = set(map(lambda s: s.split("_")[0], sources_orig)) - \
            set(map(lambda s: s.split("_")[0], sources_orig.difference(sources_finished)))

    return sorted(provs)


class CRNDeltas:
    """Defines the CRN deltas class."""

    def __init__(self, source: str, vintage: int, mode: str, base_vintage: int = 20210601, radius: int = 5) -> None:
        """
        Initializes the CRN class.

        \b
        :param str source: code for the source province / territory.
        :param int vintage: deltas date, expected to be suffixed to the source file name.
        :param str mode: the type of deltas to be returned:
                         ngd: NGD only
                         nrn: NRN only
        :param int base_vintage: inclusive date from which NGD deltas will be detected, default=20210601.
        :param int radius: CRN buffer radius used for NRN delta detection, default=5.
        """

        self.source = source
        self.vintage = vintage
        self.mode = mode
        self.base_vintage = base_vintage
        self.radius = radius

        self.dst = Path(filepath.parents[2] / f"data/crn_deltas_{self.vintage}.gpkg")
        self.flag_new_gpkg = False
        self.delta_ids = {delta_type: set() for delta_type in ("ngd_additions", "ngd_deletions", "nrn_modifications")}
        self.export = {
            f"{self.source}_ngd_additions": None,
            f"{self.source}_ngd_deletions": None,
            f"{self.source}_nrn_modifications": None
        }

        # CRN
        self.crn = None
        self.layer_crn = f"crn_{source}"
        self.id_crn = "segment_id"
        self.src_crn = self.dst

        # NRN
        self.nrn = None
        self.layer_nrn = f"nrn_{self.source}"
        self.id_nrn = "uuid"
        self.src_nrn = Path(helpers.load_yaml("../config.yaml")["filepaths"]["deltas_nrn"].replace("vintage", vintage))

        # NGD
        self.ngd_al = None
        self.ngd_prov_code = helpers.load_yaml("../config.yaml")["ngd_prov_codes"][self.source]
        self.layer_ngd_al = f"ngd_al_{self.source}"
        self.id_ngd = "ngd_uid"
        self.src_ngd = Path(helpers.load_yaml("../config.yaml")["filepaths"]["deltas_ngd"].replace("vintage", vintage))

        # Configure dst path and layer name.
        if self.dst.exists():
            if self.layer_crn not in set(fiona.listlayers(self.dst)):
                self.src_crn = Path(helpers.load_yaml("../config.yaml")["filepaths"]["crn_finished"])
        else:
            helpers.create_gpkg(self.dst)
            self.flag_new_gpkg = True
            self.src_crn = Path(helpers.load_yaml("../config.yaml")["filepaths"]["crn_finished"])

        # Configure src paths and layer names.
        for src, layer in {self.src_crn: self.layer_crn,
                           **{"ngd": {self.src_ngd: self.layer_ngd_al},
                              "nrn": {self.src_nrn: self.layer_nrn}}[self.mode]
                           }.items():

            if src.exists():
                if layer not in set(fiona.listlayers(src)):
                    logger.exception(f"Layer {layer} does not exist in source {src}.")
                    sys.exit(1)
            else:
                logger.exception(f"Source does not exist: {src}.")
                sys.exit(1)

        # Load source data - CRN.
        logger.info(f"Loading CRN source data: {self.src_crn}.")

        # Compile and concatenate individual regions.
        crn = list()
        for layer in sorted(filter(lambda l: l.startswith(f"crn_{self.source}"), fiona.listlayers(self.src_crn))):
            logger.info(f"Loading CRN source layer: {layer}.")
            crn.append(gpd.read_file(self.src_crn, layer=layer).copy(deep=True))

        self.crn = pd.concat(crn, ignore_index=True).copy(deep=True)

        logger.info(f"Successfully loaded CRN source data.")

        # Load source data - NGD.
        if self.mode == "ngd":
            logger.info(f"Loading NGD source data: {self.src_ngd}|layer={self.layer_ngd_al}.")

            con = sqlite3.connect(self.src_ngd)
            self.ngd_al = pd.read_sql_query(
                f"select {self.id_ngd}, segment_type from {self.layer_ngd_al} where "
                f"substr(csd_uid_l, 1, 2) == '{self.ngd_prov_code}' or "
                f"substr(csd_uid_r, 1, 2) == '{self.ngd_prov_code}'", con=con)

            logger.info("Successfully loaded NGD source data.")

        # Load source data - NRN.
        if self.mode == "nrn":
            logger.info(f"Loading NRN source data: {self.src_nrn}|layer={self.layer_nrn}.")

            self.nrn = gpd.read_file(self.src_nrn, layer=self.layer_nrn)

            logger.info("Successfully loaded NRN source data.")

        # Standardize data - CRN.
        self.crn = helpers.standardize(self.crn)
        self.crn = helpers.snap_nodes(self.crn)

        # Standardize data - NGD.
        if self.mode == "ngd":
            self.ngd_al.index = self.ngd_al[self.id_ngd]

        # Standardize data - NRN.
        if self.mode == "nrn":
            self.nrn = self.nrn.to_crs(self.crn.crs)
            self.nrn = helpers.round_coordinates(self.nrn)
            self.nrn[self.id_nrn] = [uuid.uuid4().hex for _ in range(len(self.nrn))]
            self.nrn.index = self.nrn[self.id_nrn]

    def __call__(self) -> None:
        """Executes the CRN class."""

        if self.mode == "ngd":
            self.fetch_ngd_deltas()
        if self.mode == "nrn":
            self.fetch_nrn_deltas()

        self._write_deltas()

    def _write_deltas(self) -> None:
        """Write output datasets and logs."""

        # TODO: write output data, del if already exists, print tabulate to console.

    def fetch_ngd_deltas(self) -> None:
        """Identifies and retrieves NGD deltas."""

        logger.info("Fetching NGD deltas.")

        # Additions.
        self.delta_ids["ngd_additions"] = set(self.ngd_al.loc[self.ngd_al["segment_type"] == 3, self.id_ngd]) - \
                                          set(self.crn[self.id_ngd])

        # Deletions.
        self.delta_ids["ngd_deletions"] = set(self.crn[self.id_ngd]) - set(self.ngd_al[self.id_ngd]) - {-1}

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
        self.delta_ids["nrn_modifications"] = set(within.loc[within.map(len) == 0].index)


@click.command()
@click.argument("source", type=click.Choice(get_finished_provs(), False))
@click.argument("vintage", type=click.INT)
@click.argument("mode", type=click.Choice(["ngd", "nrn"], False))
@click.option("--base_vintage", "-bv", type=click.INT, default=20210601, show_default=True,
              help="Inclusive date from which NGD deltas will be detected.")
@click.option("--radius", "-r", type=click.INT, default=5, show_default=True,
              help="CRN buffer radius used for NRN delta detection.")
def main(source: str, vintage: int, mode: str, base_vintage: int = 20210601, radius: int = 5) -> None:
    """
    Instantiates and executes the CRN class.

    \b
    :param str source: code for the source province / territory.
    :param int vintage: deltas date, expected to be suffixed to the source file name.
    :param str mode: the type of deltas to be returned:
                     ngd: NGD only
                     nrn: NRN only
    :param int base_vintage: inclusive date from which NGD deltas will be detected, default=20210601.
    :param int radius: CRN buffer radius used for NRN delta detection, default=5.
    """

    try:

        with helpers.Timer():
            deltas = CRNDeltas(source, vintage, mode, base_vintage, radius)
            deltas()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
