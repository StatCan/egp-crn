# TODO: make the following mods:
# 1) use regions instead of provinces (change click argument and also change difference detection to reference the concatenated regions dataframe)
# 2) src must reference a gpkg other than crn_restore.gpkg and ngd.gpkg (perhaps crn_date.gpkg and ngd_date.gpkg)
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
import sys
from copy import deepcopy
from itertools import chain
from operator import attrgetter, itemgetter
from pathlib import Path
from shapely.geometry import Point, LineString, MultiLineString
from shapely.ops import polygonize, unary_union
from tabulate import tabulate
from collections import Counter

filepath = Path(__file__).resolve()
sys.path.insert(1, str(Path(__file__).resolve().parents[1]))


import dltshelper
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

    def __init__(self, source: str, mode: str = "both") -> None:
        """
        Initializes the CRN class.

        \b
        :param str source: abbreviation for the source province / territory.
        :param str mode: the type of deltas to be returned:
            both: NGD and NRN (default)
            ngd: NGD only
            nrn: NRN only
        """

        self.source = source
        self.mode = mode
        self.process_ngd = mode in {"both", "ngd"}
        self.process_nrn = mode in {"both", "nrn"}
        self.layer = f"crn_{source}"
        self.layer_ngd = f"ngd_al_{source}"
        self.layer_ngd_current = f"ngd_al_current_{source}"
        self.layer_nrn = f"nrn_{source}"
        self.id = "segment_id"
        self.ngd_id = "ngd_uid"
        self.nrn_id = "nid"
        self.src = Path(filepath.parents[2] / "data/crn.gpkg")
        self.src_ngd = Path(filepath.parents[2] / "data/ngd_al.gpkg")
        self.src_ngd_current = Path(filepath.parents[2] / "data/ngd_al_current.gpkg")
        self.src_nrn = Path(filepath.parents[2] / "data/nrn.gpkg")
        self.del_nrn = None
        self.add_nrn = None
        self.nrn_deltas = None

        # Configure source path and layer name.
        for src in (self.src, self.src_ngd, self.src_nrn):
            if src.exists():
                layer = {self.src: self.layer, self.src_ngd: self.layer_ngd, self.src_nrn: self.layer_nrn}[src]
                if layer not in set(fiona.listlayers(src)):
                    logger.exception(f"Layer \"{layer}\" not found within source: \"{src}\".")
                    sys.exit(1)
            else:
                logger.exception(f"Source not found: \"{src}\".")
                sys.exit(1)

        # Load source data.
        logger.info(f"Loading source data: {self.src}|layer={self.layer}.")
        self.crn = gpd.read_file(self.src, layer=self.layer)
        logger.info("Successfully loaded source data.")

        # Standardize crn data and filter to roads or BOs.
        logger.info("Standardizing CRN data")
        crn_identifier = "segment_id_orig"
        self.crn = helpers.standardize(self.crn)
        self.crn = helpers.snap_nodes(self.crn)
        self.crn_rd = self.crn.loc[(self.crn["segment_type"] == 1) &
                                   (self.crn[crn_identifier].map(len) == 32)].copy(deep=True)
        self.crn_added = self.crn.loc[self.crn[crn_identifier].map(len) != 32].copy(deep=True)
        logger.info("Finished standardizing CRN data")

        # Load NGD data.
        if self.process_ngd:
            logger.info(f"Loading NGD data: {self.src_ngd}|layer={self.layer_ngd}.")
            self.ngd = gpd.read_file(self.src_ngd, layer=self.layer_ngd)
            self.ngd_current = gpd.read_file(self.src_ngd_current, layer=self.layer_ngd_current)
            logger.info("Successfully loaded NGD data.")

            # Standardize and filter NGD data.
            logger.info("Standardizing NGD data.")
            self.ngd = dltshelper.drop_zero_geom(self.ngd)
            self.ngd = helpers.round_coordinates(self.ngd)
            self.ngd["SGMNT_DTE"] = self.ngd.SGMNT_DTE.dt.strftime("%Y%m%d").astype(int)
            logger.info("Finished standardizing NGD data.")

        # Load NRN data.
        if self.process_nrn:
            logger.info(f"Loading NRN data: {self.src_nrn}|layer={self.layer_nrn}.")
            self.nrn = gpd.read_file(self.src_nrn, layer=self.layer_nrn)
            logger.info("Successfully loaded NRN data.")

            # Standardize NRN data
            logger.info("Standardizing NRN data.")
            self.nrn = self.nrn.to_crs(3347)
            self.nrn = helpers.round_coordinates(self.nrn)
            logger.info("Finished standardizing NRN data.")

    def __call__(self) -> None:
        """Executes the CRN class."""

        if self.process_ngd:
            self.fetch_ngd_deltas()
        if self.process_nrn:
            self.fetch_nrn_deltas()

    def fetch_ngd_deltas(self) -> None:
        """Identifies and retrieves NGD deltas."""

        logger.info("Fetching NGD deltas.")

        # Filter NGD arcs based on date created.
        ngd_additions = set(self.ngd.loc[self.ngd["SGMNT_DTE"] >= 20210601, "geometry"].map
                            (lambda g: attrgetter("coords")(g)))
        logger.info(f"There were {len(ngd_additions)} features added to the NGD")

        # Extract NGD identifiers as sets.
        ngd_current_ids = set(self.ngd_current["ngd_uid"])
        ngd_al_ids = set(self.ngd["NGD_UID"])

        # Configure NGD deletions.
        ngd_del_ids = ngd_current_ids - ngd_al_ids
        logger.info("Finished listing deleted ids")
        logger.info(f"There were {len(ngd_del_ids)} features deleted from the NGD")

        # Extract NGD deleted arcs and create NGD deletions set.
        ngd_del_arcs = self.ngd_current.loc[self.ngd_current["ngd_uid"].isin(ngd_del_ids)].copy(deep=True)
        ngd_deletions = set(ngd_del_arcs["geometry"].map(lambda g: attrgetter("coords")(g)))

        # Create NGD deltas GeoDatFrames.
        ngd_add = gpd.GeoDataFrame(geometry=list(map(LineString, ngd_additions)), crs=self.crn.crs)
        ngd_add["status"] = "add"
        ngd_del = gpd.GeoDataFrame(geometry=list(map(LineString, ngd_deletions)), crs=self.crn.crs)
        ngd_del["status"] = "delete"

        # Merge NGD deltas GeoDataFrames.
        ngd_deltas = ngd_del.merge(ngd_add, on="geometry", how="outer", suffixes=("_del", "_add"))

        # Compile NGD delta classifications.
        ngd_deltas["status"] = -1
        ngd_deltas.loc[ngd_deltas["status_del"].isna(), "status"] = "Addition"
        ngd_deltas.loc[ngd_deltas["status_add"].isna(), "status"] = "Deletion"
        ngd_deltas = ngd_deltas.loc[ngd_deltas["status"] != -1].fillna(0).copy(deep=True)

        # Export NGD deltas GeoDataFrame to GeoPackage
        if len(ngd_deltas):
            helpers.export(ngd_deltas, dst=self.src, name=f"{self.source}_ngd_deltas")

    def fetch_nrn_deltas(self) -> None:
        """Identifies and retrieves NRN deltas."""

        logger.info("Fetching NRN deltas.")

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
@click.argument("source", type=click.Choice(["ab", "bc", "mb", "nb", "nl", "nt", "ns", "nu",
                                            "on", "pe", "qc", "sk", "yt"], False))
@click.option("--mode", "-m", type=click.Choice(["both", "ngd", "nrn"], False), default="both", show_default=True,
              help="The type of deltas to be returned.")
def main(source: str, mode: str = "both") -> None:
    """
    Instantiates and executes the CRN class.

    \b
    :param str source: abbreviation for the source province / territory.
    :param str mode: the type of deltas to be returned:
        both: NGD and NRN (default)
        ngd: NGD only
        nrn: NRN only
    """

    try:

        with helpers.Timer():
            deltas = CRNDeltas(source, mode)
            deltas()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
