import click
import fiona
import geopandas as gpd
import logging
import numpy as np
import os
import pandas as pd
import requests
import shutil
import sys
import uuid
import zipfile
from collections import Counter
from datetime import datetime
from itertools import chain
from operator import itemgetter
from scipy.spatial import cKDTree
from shapely.geometry import Point

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import helpers


# Set logger.
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)


class Stage:
    """Defines an NRN stage."""

    def __init__(self, source):
        self.stage = 2
        self.source = source.lower()

        # Configure and validate input data path.
        self.data_path = os.path.abspath("../../data/interim/{}.gpkg".format(self.source))
        if not os.path.exists(self.data_path):
            logger.exception("Input data not found: \"{}\".".format(self.data_path))
            sys.exit(1)

    def apply_domains(self):
        """Applies the field domains to each column in the target dataframes."""

        logging.info("Applying field domains to junction.")
        defaults = helpers.compile_default_values()
        dtypes = helpers.compile_dtypes()
        field = None

        try:

            for field, domains in defaults["junction"].items():

                logger.info("Target field \"{}\": Applying domain.".format(field))

                # Apply domains to dataframe.
                default = defaults["junction"][field]
                self.dframes["junction"][field] = self.dframes["junction"][field].map(
                    lambda val: default if val == "" or pd.isna(val) else val)

                # Force adjust data type.
                self.dframes["junction"][field] = self.dframes["junction"][field].astype(dtypes["junction"][field])

        except (AttributeError, KeyError, ValueError):
            logger.exception("Invalid schema definition for table: junction, field: {}.".format(field))
            sys.exit(1)

    def compile_target_attributes(self):
        """Compiles the target (distribution format) yaml file into a dictionary."""

        logger.info("Compiling target attributes yaml.")
        table = field = None

        # Load yaml.
        self.target_attributes = helpers.load_yaml(os.path.abspath("../distribution_format.yaml"))

        # Remove field length from dtype attribute.
        logger.info("Configuring target attributes.")
        try:

            for table in self.target_attributes:
                for field, vals in self.target_attributes[table]["fields"].items():
                    self.target_attributes[table]["fields"][field] = vals[0]

        except (AttributeError, KeyError, ValueError):
            logger.exception("Invalid schema definition for table: {}, field: {}.".format(table, field))
            sys.exit(1)

    def export_gpkg(self):
        """Exports the junctions dataframe as a GeoPackage layer."""

        logger.info("Exporting junctions dataframe to GeoPackage layer.")

        # Export junctions dataframe to GeoPackage layer.
        helpers.export_gpkg({"junction": self.dframes["junction"]}, self.data_path)

    def gen_attributes(self):
        """Generate the remaining attributes for the output junction dataset."""

        logger.info("Generating remaining dataset attributes.")

        def compute_connected_attribute(junction, attribute):
            """
            Computes the given attribute from connected features to the given junction dataframe.
            Currently supported attributes: 'accuracy', 'exitnbr'.
            """

            # Validate input attribute.
            if attribute not in ("accuracy", "exitnbr"):
                logger.exception("Unsupported attribute provided: {}.".format(attribute))
                sys.exit(1)

            # Compile default field value.
            default = helpers.compile_default_values()["junction"][attribute]

            # Concatenate ferryseg and roadseg, if possible.
            if "ferryseg" in self.dframes:
                df = gpd.GeoDataFrame(pd.concat(itemgetter("ferryseg", "roadseg")(self.dframes), ignore_index=False,
                                                sort=False))
            else:
                df = self.dframes["roadseg"].copy(deep=True)

            # Generate kdtree.
            tree = cKDTree(np.concatenate([np.array(geom.coords) for geom in df["geometry"]]))

            # Compile indexes of segments at 0 meters distance from each junction. These represent connected segments.
            connected_idx = junction["geometry"].map(lambda geom: list(chain(*tree.query_ball_point(geom.coords, r=0))))

            # Construct a uuid series aligned to the series of segment points.
            pts_uuid = np.concatenate([[uuid] * count for uuid, count in
                                       df["geometry"].map(lambda geom: len(geom.coords)).iteritems()])

            # Retrieve the uuid associated with the connected indexes.
            connected_uuid = connected_idx.map(lambda index: itemgetter(*index)(pts_uuid))

            # Compile the attribute for all segment uuids.
            attribute_uuid = df[attribute].to_dict()

            # Convert associated uuids to attributes.
            # Return a series of the attribute default if an unsupported attribute was specified.

            # Attribute: accuracy.
            if attribute == "accuracy":
                connected_attribute = connected_uuid.map(
                    lambda uuid: max(itemgetter(*uuid)(attribute_uuid)) if isinstance(uuid, tuple) else
                    itemgetter(uuid)(attribute_uuid))

            # Attribute: exitnbr.
            if attribute == "exitnbr":
                connected_attribute = connected_uuid.map(
                    lambda uuid: tuple(set(itemgetter(*uuid)(attribute_uuid))) if isinstance(uuid, tuple) else
                    (itemgetter(uuid)(attribute_uuid),))

                # Concatenate, sort, and remove invalid attribute tuples.
                connected_attribute = connected_attribute.map(
                    lambda vals: ", ".join(sorted([str(val) for val in vals if val != default and not pd.isna(val)])))

            # Populate empty results with default.
            connected_attribute = connected_attribute.map(lambda val: val if len(str(val)) else default)

            return connected_attribute.copy(deep=True)

        # Set remaining attributes, where possible.
        self.dframes["junction"]["acqtech"] = "Computed"
        self.dframes["junction"]["metacover"] = "Complete"
        self.dframes["junction"]["credate"] = datetime.today().strftime("%Y%m%d")
        self.dframes["junction"]["datasetnam"] = self.dframes["roadseg"]["datasetnam"][0]
        self.dframes["junction"]["accuracy"] = compute_connected_attribute(self.dframes["junction"], "accuracy")
        self.dframes["junction"]["provider"] = "Federal"
        self.dframes["junction"]["exitnbr"] = compute_connected_attribute(self.dframes["junction"], "exitnbr")

    def gen_junctions(self):
        """Generates a junction GeoDataFrame for all junctypes: Dead End, Ferry, Intersection, and NatProvTer."""

        logger.info("Generating junctions.")

        # Concatenate ferryseg and roadseg, if possible.
        if "ferryseg" in self.dframes:
            df = gpd.GeoDataFrame(pd.concat(itemgetter("ferryseg", "roadseg")(self.dframes),
                                            ignore_index=False, sort=False))
        else:
            df = self.dframes["roadseg"].copy(deep=True)

        # Compile uuid groups for all endpoints.
        logger.info("Grouping uuids by shared endpoints.")

        # TEST
        pts_uuid = np.concatenate([[uuid] * count for uuid, count in df["geometry"].map(lambda geom: len(geom.coords)).iteritems()])
        pts_x, pts_y = np.concatenate([np.array(geom.coords) for geom in df["geometry"]]).T
        pts_df = pd.DataFrame({"x": pts_x, "y": pts_y, "uuid": pts_uuid})

        pts_unique = pts_df[~pts_df[["x", "y"]].duplicated(keep=False)][["x", "y"]].values
        endpoints_unique = np.unique(np.concatenate([np.array(itemgetter(0, -1)(geom.coords)) for geom in df["geometry"]]), axis=0)
        deadend = np.array(list(set(map(tuple, pts_unique)).intersection(set(map(tuple, endpoints_unique)))))

        pts_dup = pts_df[(pts_df[["x", "y"]].duplicated(keep=False)) & (~pts_df.duplicated(keep="first"))]
        counts = Counter(map(tuple, pts_dup[["x", "y"]].values))
        intersection = np.array([pt for pt, count in counts.items() if count >= 3])

        ferry = np.array([])
        if "ferryseg" in self.dframes:
            ferry = np.concatenate([np.array(itemgetter(0, -1)(geom.coords)) for geom in self.dframes["ferryseg"]["geometry"]])
            deadend = np.array(list(set(map(tuple, deadend)).difference(set(map(tuple, ferry)))))
            intersection = np.array(list(set(map(tuple, intersection)).difference(set(map(tuple, ferry)))))

        merged = np.concatenate([deadend, ferry, intersection])
        natprovter_flag = list(map(lambda pt: not Point(pt).within(self.boundary), merged))
        natprovter = merged[natprovter_flag]
        deadend = np.array(list(set(map(tuple, deadend)).difference(set(map(tuple, natprovter)))))
        ferry = np.array(list(set(map(tuple, ferry)).difference(set(map(tuple, natprovter)))))
        intersection = np.array(list(set(map(tuple, intersection)).difference(set(map(tuple, natprovter)))))

        junctions = [gpd.GeoDataFrame({'junctype': junctype, "uuid": [uuid.uuid4().hex for _ in range(len(pts))]},
                                      geometry=pd.Series(map(Point, pts))) for junctype, pts in
                     {"ferry": ferry, "natprovter": natprovter, "intersection": intersection,
                      "deadend": deadend}.items() if len(pts)]

        # TEST

        # # Construct a uuid series aligned to the series of endpoints.
        # pts_uuid = df["uuid"].values.repeat(2)
        #
        # # Construct x- and y-coordinate series aligned to the series of points.
        # pts_x, pts_y = np.concatenate([np.array(itemgetter(0, -1)(geom.coords)) for geom in df["geometry"]]).T
        #
        # # Join the uuids, x-, and y-coordinates.
        # pts_df = pd.DataFrame({"x": pts_x, "y": pts_y, "uuid": pts_uuid})
        #
        # # Group uuids according to x- and y-coordinates.
        # uuids_grouped = pts_df.groupby(["x", "y"])["uuid"].apply(list)
        #
        # # Configure junctypes.
        # logger.info("Configuring junctypes.")
        # junctypes = dict()
        #
        # # junctype: NatProvTer.
        # logger.info("Configuring junctype: NatProvTer.")
        #
        # # Process: Query indexes (points) not within adm boundaries, store indexes, drop results from dataframe.
        # junctypes["NatProvTer"] = uuids_grouped[~np.vectorize(
        #     lambda coords: Point(coords).within(self.boundary))(uuids_grouped.index)].index.values
        # uuids_grouped.drop(junctypes["NatProvTer"], inplace=True)
        #
        # # junctype: Ferry.
        # logger.info("Configuring junctype: Ferry.")
        #
        # # Process: If ferryseg exists, query indexes (points) where the uuid group contains a ferryseg uuid (via set
        # # subtraction), store indexes, drop results from dataframe.
        # if "ferryseg" in self.dframes:
        #
        #     ferryseg_uuids = set(self.dframes["ferryseg"]["uuid"].values)
        #     junctypes["Ferry"] = uuids_grouped[uuids_grouped.map(
        #         lambda uuids: len(set(uuids) - ferryseg_uuids) < len(set(uuids)))].index.values
        #     uuids_grouped.drop(junctypes["Ferry"], inplace=True)
        #
        # # junctype: Dead End.
        # logger.info("Configuring junctype: Dead End.")
        #
        # # Process: Query indexes (points) with a uuid group of only 1 uuid, store indexes, no need to drop results.
        # junctypes["Dead End"] = uuids_grouped[uuids_grouped.map(len) == 1].index.values
        #
        # # junctype: Intersection.
        # logger.info("Configuring junctype: Intersection.")
        #
        # # Process: Query indexes (points) with a uuid group >= 3 unique uuids, store indexes, no need to drop results.
        # junctypes["Intersection"] = uuids_grouped[uuids_grouped.map(lambda uuids: len(set(uuids)) >= 3)].index.values
        #
        # # Compile junctypes as GeoDataFrames.
        # junctions = [gpd.GeoDataFrame({"junctype": junctype, "uuid": [uuid.uuid4().hex for _ in range(len(pts))]},
        #                               geometry=pd.Series(map(Point, pts)))
        #              for junctype, pts in junctypes.items() if len(pts)]

        # Concatenate junctions with target dataset and set uuid as index.
        self.junction = gpd.GeoDataFrame(pd.concat([self.junction, *junctions], ignore_index=True, sort=False),
                                         crs=self.dframes["roadseg"].crs)
        self.junction.index = self.junction["uuid"]

        # Store results.
        self.dframes["junction"] = self.junction.copy(deep=True)

    def gen_target_dataframe(self):
        """Creates empty junction dataframe."""

        logger.info("Creating target dataframe.")

        self.junction = gpd.GeoDataFrame().assign(**{field: pd.Series(dtype=dtype) for field, dtype in
                                                     self.target_attributes["junction"]["fields"].items()})

    def load_boundaries(self):
        """Downloads and loads the geometry of the administrative boundaries for the source province."""

        logger.info("Loading administrative boundaries.")

        # Download administrative boundaries.
        logger.info("Downloading administrative boundary file.")
        source = helpers.load_yaml("../downloads.yaml")["provincial_boundaries"]
        download_url, filename = itemgetter("url", "filename")(source)

        try:

            # Get raw content stream from download url.
            download = helpers.get_url(download_url, stream=True, timeout=30)

            # Copy download content to file.
            with open("../../data/interim/boundaries.zip", "wb") as f:
                shutil.copyfileobj(download.raw, f)

        except (requests.exceptions.RequestException, shutil.Error) as e:
            logger.exception("Unable to download administrative boundary file: \"{}\".".format(download_url))
            logger.exception(e)
            sys.exit(1)

        # Extract zipped file.
        logger.info("Extracting zipped administrative boundary file.")
        with zipfile.ZipFile("../../data/interim/boundaries.zip", "r") as zip_f:
            zip_f.extractall("../../data/interim/boundaries")

        # Transform administrative boundary file to GeoPackage layer with crs EPSG:4617.
        logger.info("Transforming administrative boundary file.")
        helpers.ogr2ogr({
            "query": "-where \"\\\"PRUID\\\"='{}'\"".format(
                {"ab": 48, "bc": 59, "mb": 46, "nb": 13, "nl": 10, "ns": 12, "nt": 61, "nu": 62, "on": 35, "pe": 11,
                 "qc": 24, "sk": 47, "yt": 60}[self.source]),
            "dest": os.path.abspath("../../data/interim/boundaries.geojson"),
            "src": os.path.abspath("../../data/interim/boundaries/{}".format(filename)),
            "options": "-t_srs EPSG:4617 -nlt MULTIPOLYGON"
        })

        # Load boundaries as a single geometry object.
        logger.info("Loading administrative boundaries' geometry.")
        self.boundary = gpd.read_file("../../data/interim/boundaries.geojson",
                                      crs=self.dframes["roadseg"].crs)["geometry"][0]

        # Remove temporary files.
        logger.info("Removing temporary administrative boundary files and directories.")
        for f in os.listdir("../../data/interim"):
            if os.path.splitext(f)[0] == "boundaries":
                path = os.path.join("../../data/interim", f)
                try:
                    os.remove(path) if os.path.isfile(path) else shutil.rmtree(path)
                except (OSError, shutil.Error) as e:
                    logger.warning("Unable to remove directory or file: \"{}\".".format(os.path.abspath(path)))
                    logger.warning(e)
                    continue

    def load_gpkg(self):
        """Loads input GeoPackage layers into dataframes."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.load_boundaries()
        self.compile_target_attributes()
        self.gen_target_dataframe()
        self.gen_junctions()
        self.gen_attributes()
        self.apply_domains()
        self.export_gpkg()


@click.command()
@click.argument("source", type=click.Choice("ab bc mb nb nl ns nt nu on pe qc sk yt parks_canada".split(), False))
def main(source):
    """Executes an NRN stage."""

    try:

        with helpers.Timer():
            stage = Stage(source)
            stage.execute()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: exiting program.")
        sys.exit(1)

if __name__ == "__main__":
    main()
