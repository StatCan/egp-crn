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
<<<<<<< HEAD

# import hazhelper
=======
>>>>>>> 3c5d0d92d4d8c04014f32dde0fd346671b75b7cb
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

        self.layer = f"nrn_bo_{source}"
        self.layer_ngd = f"ngd_al_{source}"
        self.layer_nrn = f"nrn_{source}"

        self.id = "segment_id"
        self.ngd_id = "ngd_uid"
        self.nrn_id = "nid"

        self.src = Path(filepath.parents[2] / "data/egp_data.gpkg")
        self.src_ngd = Path(filepath.parents[2] / "data/ngd_al.gpkg")
        self.src_nrn = Path(filepath.parents[2] / "data/nrn.gpkg")

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

        # Standardize data and snap nodes.
        print(Counter(self.crn.segment_type))
        self.crn = helpers.standardize(self.crn)
        print(Counter(self.crn.segment_type))
        self.crn = helpers.snap_nodes(self.crn)

        # Load NGD data.
        if self.process_ngd:
            logger.info(f"Loading NGD data: {self.src_ngd}|layer={self.layer_ngd}.")
            self.ngd = gpd.read_file(self.src_ngd, layer=self.layer_ngd)
            logger.info("Successfully loaded NGD data.")

            # Standardize data.
            # self.ngd = hazhelper.drop_zero_geom(self.ngd)
            # self.ngd = helpers.round_coordinates(self.ngd)
            # self.ngd["SGMNT_DTE"] = self.ngd.SGMNT_DTE.dt.strftime("%Y%m%d").astype(int)

        # Load NRN data.
        if self.process_nrn:
            logger.info(f"Loading NRN data: {self.src_nrn}|layer={self.layer_nrn}.")
            self.nrn = gpd.read_file(self.src_nrn, layer=self.layer_nrn)
            logger.info("Successfully loaded NRN data.")

            # Standardize data.
<<<<<<< HEAD
            self.nrn = self.nrn.to_crs(3347)
=======
>>>>>>> 3c5d0d92d4d8c04014f32dde0fd346671b75b7cb
            self.nrn = helpers.round_coordinates(self.nrn)

    def __call__(self) -> None:
        """Executes the CRN class."""

        if self.process_ngd:
            self.fetch_ngd_deltas()
        if self.process_nrn:
            self.fetch_nrn_deltas()

    def fetch_ngd_deltas(self) -> None:
        """Identifies and retrieves NGD deltas."""

        logger.info("Fetching NGD deltas.")

        # filter ngd arcs based on date created.
        ngd_additions = set(self.ngd.loc[self.ngd["SGMNT_DTE"] >= 20210601, "geometry"].map
                            (lambda g: attrgetter("coords")(g)))

        # Create NGD additions GeoDatFrame.
        ngd_add_gdf = gpd.GeoDataFrame(geometry=list(map(LineString, ngd_additions)), crs=self.crn.crs)

        # Export NGD additions GeoDataFrame to GeoPackage
        helpers.export(ngd_add_gdf, dst=self.src, name=f"{self.source}_ngd_deltas")

    def fetch_nrn_deltas(self) -> None:
        """Identifies and retrieves NRN deltas."""

        logger.info("Fetching NRN deltas.")

        # ORIGINAL - start
        # Extract all nrn vertex coordinates.
        nrn_flag = self.nrn["nid"].map(len) == 32

        nrn_nodes = set(self.nrn.loc[nrn_flag, "geometry"].map(
            lambda x: tuple(set(attrgetter("coords")(x)))))

        # Extract all crn vertex coordinates.
        crn_flag = self.crn["segment_id"].map(len) == 32 & (self.crn["segment_type"] == 1)

        crn_nodes = set(self.crn.loc[crn_flag, "geometry"].map(
            lambda x: tuple(set(attrgetter("coords")(x)))))
        # ORIGINAL - end

        # NEW - start
        # Compile all nrn and crn road points as sets.
        nrn_nodes = set(self.nrn["geometry"].map(lambda g: attrgetter("coords")(g)).explode())
        crn_nodes = set(self.crn.loc[self.crn["segment_type" == 1], "geometry"].map(
            lambda g: attrgetter("coords")(g)).explode())
        # NEW - end

        # Configure deltas.
        additions = nrn_nodes - crn_nodes
        deletions = crn_nodes - nrn_nodes
<<<<<<< HEAD
        del_gdf = gpd.GeoDataFrame(geometry=list(map(Point, deletions)), crs=self.crn.crs)
        add_gdf = gpd.GeoDataFrame(geometry=list(map(Point, additions)), crs=self.crn.crs)

        # Create DataFrames from delta pt tuples.
        del_df = pd.DataFrame({"del_status": "delete", "geometry": del_gdf["geometry"].map(
                                      lambda g: (attrgetter("coords")(g)))})
        add_df = pd.DataFrame({"add_status": "add", "geometry": add_gdf["geometry"].map(
                                      lambda g: (attrgetter("coords")(g)))})

        # Merge NRN deltas dataframes.
        nrn_deltas = del_df.merge(add_df, on="geometry", how="outer")

        # Compile NRN delta classifications.
        nrn_deltas["status"] = -1
        nrn_deltas.loc[nrn_deltas["del_status"].isna(), "status"] = "addition"
        nrn_deltas.loc[nrn_deltas["add_status"].isna(), "status"] = "deletion"

        # Create NRN deltas GeoDataFrame.
        nrn_deltas = nrn_deltas.loc[nrn_deltas["status"] != -1].fillna("other").copy(deep=True)
        if len(nrn_deltas):
            deltas_nrn = gpd.GeoDataFrame(nrn_deltas, geometry=list(map(Point, nrn_deltas["geometry"])),
                                          crs=self.crn.crs)

        # Export delta GeoDataFrames to GeoPackages.
        helpers.export(deltas_nrn, dst=self.src, name=f"{self.source}_nrn_deltas_T")
        # helpers.export(add_gdf, dst=self.src, name=f"{self.source}_additions")
=======

        # TODO: Create and export GeoDataFrames from delta pt tuples.
>>>>>>> 3c5d0d92d4d8c04014f32dde0fd346671b75b7cb



@click.command()
@click.argument("source", type=click.Choice(helpers.load_yaml("../config.yaml")["sources"], False))
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
