import click
import fiona
import geopandas as gpd
import logging
import pandas as pd
import subprocess
import sys
import uuid
from pathlib import Path

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

    def __init__(self, src: Path, source: str, mode: str, vintage: int) -> None:
        """
        Initializes the CRN class.

        \b
        :param Path src: path to the source GeoPackage (NRN) or ESRI File Geodatabase (NGD).
        :param str source: code for the source province / territory.
        :param mode: the type of data being processed: {'ngd_a', 'ngd_al', 'nrn'}.
        :param int vintage: deltas date, will be suffixed to the dst file name.
        """

        self.src = src
        self.source = source
        self.mode = mode
        self.vintage = vintage

        self.dst = Path(helpers.load_yaml("../config.yaml")["filepaths"][f"deltas_{self.mode.split('_')[0]}"]
                        .replace("vintage", str(self.vintage)))
        self.dst_layer = f"{self.mode}_{self.source}"
        self.df = None
        self.ngd_prov_code = helpers.load_yaml("../config.yaml")["ngd_prov_codes"][self.source]

        # Validate src.
        if self.src.exists():
            suffix = {"ngd_a": ".gdb", "ngd_al": ".gdb", "nrn": ".gpkg"}[self.mode]
            if self.src.suffix != suffix:
                logger.exception(f"Invalid source file extension. Current={self.src.suffix}, expected={suffix}.")
                sys.exit(1)
        else:
            logger.exception(f"Source file does not exist: {self.src}.")
            sys.exit(1)

        # Validate and create dst.
        if not self.dst.exists():
            helpers.create_gpkg(self.dst)

    def __call__(self) -> None:
        """Executes the CRN class."""

        # Load / conform data.
        if self.mode == "ngd_a":
            self._conform_ngd_a()
        elif self.mode == "ngd_al":
            self._conform_ngd_al()
        elif self.mode == "nrn":
            self._conform_nrn()

        # Export data.
        helpers.export(self.df, dst=self.dst, name=self.dst_layer)

    def _conform_ngd_a(self) -> None:
        """Conforms source data to CRN schema - NGD_A."""

        logger.info("Conforming data.")

        # Validate layers.
        missing = {"CB", "NGD_A"}.difference(set(fiona.listlayers(self.src)))
        if len(missing):
            logger.exception(f"Cannot find layer(s) {*missing,} in source {self.src}.")
            sys.exit(1)

        # Load data - subset CB layer via ogr2ogr (interim layer).
        logger.info(f"Running ogr2ogr subprocess for output: {self.dst}|layer=interim_cb.")
        _ = subprocess.run(
            f"ogr2ogr -overwrite -sql \"SELECT CB_UID FROM CB WHERE PRCODE = '{self.ngd_prov_code}'\" {self.dst} "
            f"{self.src} -nln interim_cb")

        # Load data - transfer NGD_A via ogr2ogr (interim layer).
        logger.info(f"Running ogr2ogr subprocess for output: {self.dst}|layer=interim_ngd_a.")
        _ = subprocess.run(
            f"ogr2ogr -overwrite -sql \"SELECT BB_UID, CB_UID, Shape FROM NGD_A\" {self.dst} {self.src} "
            f"-nln interim_ngd_a -nlt POLYGON")

        # Load data - subset NGD_A via ogr2ogr using interim layers.
        logger.info(f"Running ogr2ogr subprocess for output: {self.dst}|layer={self.dst_layer}.")
        _ = subprocess.run(
            f"ogr2ogr -overwrite -sql \"SELECT BB_UID, Shape FROM interim_ngd_a WHERE CB_UID IN (SELECT CB_UID FROM "
            f"interim_cb)\" {self.dst} {self.dst} -nln {self.dst_layer}")

        # Delete interim layers.
        helpers.delete_layers(dst=self.dst, layers=["interim_cb", "interim_ngd_a"])

        # Load data as GeoDataFrame.
        logger.info(f"Loading data as GeoDataFrame.")
        self.df = gpd.read_file(self.dst, layer=self.dst_layer)

        logger.info(f"Standardizing data.")

        # Standardize data.
        self.df.columns = map(str.lower, self.df.columns)

        # Subset data.
        self.df = self.df[["bb_uid", "geometry"]].copy(deep=True)

    def _conform_ngd_al(self) -> None:
        """Conforms source data to CRN schema - NGD_AL."""

        logger.info("Conforming data.")

        # Validate layer.
        if "NGD_AL" not in set(fiona.listlayers(self.src)):
            logger.exception(f"Cannot find layer NGD_AL in source {self.src}.")
            sys.exit(1)

        # Load data - subset via ogr2ogr.
        logger.info(f"Running ogr2ogr subprocess for output: {self.dst}|layer={self.dst_layer}.")
        _ = subprocess.run(
            f"ogr2ogr -overwrite -sql \"SELECT NGD_UID, SGMNT_TYP_CDE, BB_UID_L, BB_UID_R, CSD_UID_L, CSD_UID_R, Shape "
            f"FROM NGD_AL WHERE SUBSTR(CSD_UID_L, 1, 2) = '{self.ngd_prov_code}' OR SUBSTR(CSD_UID_R, 1, 2) = "
            f"'{self.ngd_prov_code}'\" {self.dst} {self.src} -nln {self.dst_layer} -nlt LINESTRING")

        # Load data as GeoDataFrame.
        logger.info(f"Loading data as GeoDataFrame.")
        self.df = gpd.read_file(self.dst, layer=self.dst_layer)

        logger.info(f"Standardizing data.")

        # Standardize data.
        self.df.columns = map(str.lower, self.df.columns)

        # Standardize data - add / modify attribution.
        self.df["segment_type"] = self.df["sgmnt_typ_cde"].map({1: 2, 2: 1})
        self.df["boundary"] = pd.Series(self.df["csd_uid_l"] != self.df["csd_uid_r"]).astype(int)

        # Subset data.
        self.df = self.df[["ngd_uid", "bb_uid_l", "bb_uid_r", "segment_type", "boundary", "geometry"]].copy(deep=True)

    def _conform_nrn(self) -> None:
        """Conforms source data to CRN schema - NRN."""

        logger.info("Conforming data.")

        # Configure source layer name.
        layer = [layer for layer in fiona.listlayers(self.src) if layer.lower().find("roadseg") >= 0][0]

        # Validate layer.
        if not len(layer):
            logger.exception(f"Cannot find layer roadseg (or any layer containing this word) in source {self.src}.")
            sys.exit(1)

        # Load data.
        logger.info(f"Loading data as GeoDataFrame.")
        self.df = gpd.read_file(self.src, layer=layer)

        logger.info(f"Standardizing data.")

        # Standardize data.
        self.df.columns = map(str.lower, self.df.columns)
        self.df = self.df.to_crs("EPSG:3347")

        # Standardize data - add / modify attribution.
        self.df["segment_id"] = [uuid.uuid4().hex for _ in range(len(self.df))]
        self.df["segment_id_orig"] = self.df["segment_id"]
        self.df["structure_type"] = self.df["structtype"].fillna("Unknown")
        self.df["segment_type"] = 1
        self.df["ngd_uid"] = -1
        self.df["boundary"] = 0
        self.df["bo_new"] = 0

        # Subset data.
        self.df = self.df[["segment_id", "segment_id_orig", "structure_type", "segment_type", "ngd_uid", "boundary",
                           "bo_new", "geometry"]].copy(deep=True)


@click.command()
@click.argument("src", type=click.Path(exists=True, dir_okay=True, resolve_path=True, path_type=Path))
@click.argument("source", type=click.Choice("ab bc mb nb nl ns nt nu on pe qc sk yt".split(), False))
@click.argument("mode", type=click.Choice(["ngd_a", "ngd_al", "nrn"], False))
@click.argument("vintage", type=click.INT)
def main(src: Path, source: str, mode: str, vintage: int) -> None:
    """
    Instantiates and executes the CRN class.

    \b
    :param Path src: path to the source GeoPackage (NRN) or ESRI File Geodatabase (NGD).
    :param str source: code for the source province / territory.
    :param mode: the type of data being processed: {'ngd_a', 'ngd_al', 'nrn'}.
    :param int vintage: deltas date, will be suffixed to the dst file name.
    """

    try:

        with helpers.Timer():
            deltas = CRNDeltas(src, source, mode, vintage)
            deltas()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
