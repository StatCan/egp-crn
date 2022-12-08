# TODO: make the following mods:
# 1) use regions instead of provinces (change click argument and also change difference detection to reference the concatenated regions dataframe) - CANCEL - use provs
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


class CRNDeltas:
    """Defines the CRN deltas class."""

    def __init__(self, source: str, vintage: int, mode: str, base_vintage: int = 20210601) -> None:
        """
        Initializes the CRN class.

        \b
        :param str source: code for the source province / territory.
        :param int vintage: deltas date, expected to be suffixed to the source file name.
        :param str mode: the type of deltas to be returned:
                         ngd: NGD only
                         nrn: NRN only
        :param int base_vintage: inclusive date from which NGD deltas will be detected, default=20210601.
        """

        self.source = source
        self.vintage = vintage
        self.mode = mode
        self.base_vintage = base_vintage

        self.dst = Path(filepath.parents[2] / f"data/crn_deltas_{self.vintage}.gpkg")
        self.flag_new_gpkg = False
        self._nrn_buffer = 5
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
        self.src_nrn = Path(helpers.load_yaml("../config.yaml")["filepaths"]["deltas_nrn"]
                            .replace("<vintage>", vintage))

        # NGD
        self.ngd_al = None
        self.ngd_prov_code = {"ab": 48, "bc": 59, "mb": 46, "nb": 13, "nl": 10, "ns": 12, "nt": 61, "nu": 62, "on": 35,
                              "pe": 11, "qc": 24, "sk": 47, "yt": 60}[self.source]
        self.layer_ngd_al = f"ngd_al_{self.source}"
        self.id_ngd = "ngd_uid"
        self.src_ngd = Path(helpers.load_yaml("../config.yaml")["filepaths"]["deltas_ngd"]
                            .replace("<vintage>", vintage))

        # Delta identifiers.
        self.delta_ids = {delta_type: set() for delta_type in ("ngd_additions", "ngd_deletions", "nrn_modifications")}

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

        self.crn = pd.concat(crn).copy(deep=True)

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

        # Standardize data - NRN.
        if self.mode == "nrn":
            self.nrn = self.nrn.to_crs(self.crn.crs)
            self.nrn = helpers.round_coordinates(self.nrn)
            self.nrn[self.id_nrn] = [uuid.uuid4().hex for _ in range(len(self.nrn))]

    def __call__(self) -> None:
        """Executes the CRN class."""

        if self.mode == "ngd":
            self.fetch_ngd_deltas()
        if self.mode == "nrn":
            self.fetch_nrn_deltas()

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

        # TODO: replace this function with logic from restore_geometry.

        # Compile CRN road and BO and NRN vertices as sets.
        nrn_nodes = set(self.nrn["geometry"].map(lambda g: attrgetter("coords")(g)).explode())
        crn_nodes = set(self.crn_rd["geometry"].map(lambda g: attrgetter("coords")(g)).explode())
        crn_added_nodes = set(self.crn_added["geometry"].map(lambda g: attrgetter("coords")(g)).explode())

        # Configure NRN deltas.
        additions = nrn_nodes - crn_nodes
        deletions = crn_nodes - nrn_nodes
        deletions_fltr = deletions - crn_added_nodes

        # Create NRN deltas GeoDataFrames.
        self.del_nrn = gpd.GeoDataFrame(geometry=list(map(Point, deletions_fltr)), crs=self.crn.crs)
        self.del_nrn["status"] = "delete"
        self.add_nrn = gpd.GeoDataFrame(geometry=list(map(Point, additions)), crs=self.crn.crs)
        self.add_nrn["status"] = "add"

        # Merge NRN deltas GeoDataframes.
        nrn_deltas = self.del_nrn.merge(self.add_nrn, on="geometry", how="outer", suffixes=("_del", "_add"))

        # Compile NRN delta classifications.
        nrn_deltas["status"] = -1
        nrn_deltas.loc[nrn_deltas["status_del"].isna(), "status"] = "addition"
        nrn_deltas.loc[nrn_deltas["status_add"].isna(), "status"] = "deletion"
        nrn_deltas = nrn_deltas.loc[nrn_deltas["status"] != -1].fillna(0).copy(deep=True)

        # Export NRN deltas GeoDataFrame to GeoPackage.
        if len(nrn_deltas):
            helpers.export(nrn_deltas, dst=self.src, name=f"{self.source}_nrn_deltas")

    # TODO: create progress logger.


@click.command()
@click.argument("source", type=click.Choice(["ab", "bc", "mb", "nb", "nl", "nt", "ns",
                                             "nu", "on", "pe", "qc", "sk", "yt"], False))
@click.argument("vintage", type=click.INT)
@click.argument("mode", type=click.Choice(["ngd", "nrn"], False))
@click.option("--base_vintage", "-bv", type=click.INT, default=20210601, show_default=True,
              help="Inclusive date from which NGD deltas will be detected.")
def main(source: str, vintage: int, mode: str, base_vintage: int = 20210601) -> None:
    """
    Instantiates and executes the CRN class.

    \b
    :param str source: code for the source province / territory.
    :param int vintage: deltas date, expected to be suffixed to the source file name.
    :param str mode: the type of deltas to be returned:
                     ngd: NGD only
                     nrn: NRN only
    :param int base_vintage: inclusive date from which NGD deltas will be detected, default=20210601.
    """

    try:

        with helpers.Timer():
            deltas = CRNDeltas(source, vintage, mode, base_vintage)
            deltas()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
