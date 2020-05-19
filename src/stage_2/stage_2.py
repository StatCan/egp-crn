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
from operator import attrgetter, itemgetter
from scipy.spatial import cKDTree
from shapely.geometry import box, Point, Polygon, MultiPolygon, GeometryCollection

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

    def divide_polygon(self, poly, threshold, count=0):
        """Divides a polygon into 2 parts until the size <= threshold or the max recursions is reached."""

        bounds = poly.bounds
        width = bounds[2] - bounds[0]
        height = bounds[3] - bounds[1]

        # Exit recursion once limits are reached.
        if max(width, height) <= threshold or count == 250:
            return [poly]

        # Conditionally split polygon by height or width.
        if height >= width:
            a = box(bounds[0], bounds[1], bounds[2], bounds[1] + (height / 2))
            b = box(bounds[0], bounds[1] + (height / 2), bounds[2], bounds[3])
        else:
            a = box(bounds[0], bounds[1], bounds[0] + (width / 2), bounds[3])
            b = box(bounds[0] + (width / 2), bounds[1], bounds[2], bounds[3])
        result = []

        # Compile split results, further recurse.
        for d in (a, b,):
            c = poly.intersection(d)
            if not isinstance(c, GeometryCollection):
                c = [c]
            for e in c:
                if isinstance(e, (Polygon, MultiPolygon)):
                    result.extend(self.divide_polygon(e, threshold, count + 1))

        if count > 0:
            return result

        # Compile final result as a single-part polygon.
        final_result = []
        for g in result:
            if isinstance(g, MultiPolygon):
                final_result.extend(g)
            else:
                final_result.append(g)

        return final_result

    def export_gpkg(self):
        """Exports the junctions dataframe as a GeoPackage layer."""

        logger.info("Exporting junctions dataframe to GeoPackage layer.")

        # Export junctions dataframe to GeoPackage layer.
        helpers.export_gpkg({"junction": self.dframes["junction"]}, self.data_path)

    def gen_attributes(self):
        """Generate the remaining attributes for the output junction dataset."""

        logger.info("Generating remaining dataset attributes.")

        def compute_connected_attributes(attributes):
            """
            Computes the given attributes from connected features to the junction dataframe.
            Currently supported attributes: 'accuracy', 'exitnbr'.
            """

            junction = self.dframes["junction"].copy(deep=True)

            # Validate input attributes.
            if not set(attributes).issubset({"accuracy", "exitnbr"}):
                logger.exception("One or more unsupported attributes provided: {}.".format(", ".join(attributes)))
                sys.exit(1)

            # Concatenate ferryseg and roadseg, if possible.
            if "ferryseg" in self.dframes:
                df = gpd.GeoDataFrame(
                    pd.concat(itemgetter("ferryseg", "roadseg")(self.dframes), ignore_index=False, sort=False))
            else:
                df = self.dframes["roadseg"].copy(deep=True)

            # Generate kdtree.
            tree = cKDTree(np.concatenate(df["geometry"].map(attrgetter("coords")).to_numpy()))

            # Compile indexes of segments at 0 meters distance from each junction. These represent connected segments.
            connected_idx = junction["geometry"].map(lambda geom: list(chain(*tree.query_ball_point(geom.coords, r=0))))

            # Construct a uuid series aligned to the series of segment points.
            pts_uuid = np.concatenate([[id] * count for id, count in
                                       df["geometry"].map(lambda geom: len(geom.coords)).iteritems()])

            # Retrieve the uuid associated with the connected indexes.
            connected_uuid = connected_idx.map(lambda index: itemgetter(*index)(pts_uuid))

            # Compile the attributes for all segment uuids.
            attributes_uuid = df[attributes].to_dict()

            # Convert associated uuids to attributes.
            # Convert invalid attribute values to the default field value.
            results = dict.fromkeys(attributes)

            for attribute in attributes:
                attribute_uuid = attributes_uuid[attribute]
                default = helpers.compile_default_values()["junction"][attribute]

                # Attribute: accuracy.
                if attribute == "accuracy":
                    connected_attribute = connected_uuid.map(
                        lambda id: max(itemgetter(*id)(attribute_uuid)) if isinstance(id, tuple) else
                        itemgetter(id)(attribute_uuid))

                # Attribute: exitnbr.
                if attribute == "exitnbr":
                    connected_attribute = connected_uuid.map(
                        lambda id: tuple(set(itemgetter(*id)(attribute_uuid))) if isinstance(id, tuple) else
                        (itemgetter(id)(attribute_uuid),))

                    # Concatenate, sort, and remove invalid attribute tuples.
                    connected_attribute = connected_attribute.map(
                        lambda vals: ", ".join(sorted([str(v) for v in vals if v != default and not pd.isna(v)])))

                # Populate empty results with default.
                connected_attribute = connected_attribute.map(lambda v: v if len(str(v)) else default)

                # Store results.
                results[attribute] = connected_attribute.copy(deep=True)

            return results

        # Set remaining attributes, where possible.
        self.dframes["junction"]["acqtech"] = "Computed"
        self.dframes["junction"]["metacover"] = "Complete"
        self.dframes["junction"]["credate"] = datetime.today().strftime("%Y%m%d")
        self.dframes["junction"]["datasetnam"] = self.dframes["roadseg"]["datasetnam"][0]
        self.dframes["junction"]["provider"] = "Federal"
        connected_attributes = compute_connected_attributes(["accuracy", "exitnbr"])
        self.dframes["junction"]["accuracy"] = connected_attributes["accuracy"]
        self.dframes["junction"]["exitnbr"] = connected_attributes["exitnbr"]

    def gen_junctions(self):
        """Generates a junction GeoDataFrame for all junctypes: Dead End, Ferry, Intersection, and NatProvTer."""

        logger.info("Generating junctions.")
        df = self.dframes["roadseg"].copy(deep=True)

        # Separate unique and non-unique points.
        logger.info("Separating unique and non-unique points.")

        # Construct a uuid series aligned to the series of endpoints.
        pts_uuid = np.concatenate([[id] * count for id, count in df["geometry"].map(
            lambda geom: len(geom.coords)).iteritems()])

        # Construct x- and y-coordinate series aligned to the series of points.
        pts_x, pts_y = np.concatenate(df["geometry"].map(attrgetter("coords")).to_numpy()).T

        # Join the uuids, x-, and y-coordinates.
        pts_df = pd.DataFrame({"x": pts_x, "y": pts_y, "uuid": pts_uuid})

        # Query unique points (all) and endpoints.
        pts_unique = pts_df[~pts_df[["x", "y"]].duplicated(keep=False)][["x", "y"]].values
        endpoints_unique = np.unique(np.concatenate(
            df["geometry"].map(lambda g: itemgetter(0, -1)(attrgetter("coords")(g))).to_numpy()), axis=0)

        # Query non-unique points (all), keep only the first duplicated point from self-loops.
        pts_dup = pts_df[(pts_df[["x", "y"]].duplicated(keep=False)) & (~pts_df.duplicated(keep="first"))]

        # Query junctypes.

        # junctype: Dead End.
        # Process: Query unique points which also exist in unique endpoints.
        logger.info("Configuring junctype: Dead End.")
        deadend = set(map(tuple, pts_unique)).intersection(set(map(tuple, endpoints_unique)))

        # junctype: Intersection.
        # Process: Query non-unique points with >= 3 instances.
        logger.info("Configuring junctype: Intersection.")
        counts = Counter(map(tuple, pts_dup[["x", "y"]].values))
        intersection = {pt for pt, count in counts.items() if count >= 3}

        # junctype: Ferry.
        # Process: Compile all unique ferryseg endpoints. Remove conflicting points from other junctypes.
        logger.info("Configuring junctype: Ferry.")

        ferry = set()
        if "ferryseg" in self.dframes:
            ferry = set(chain.from_iterable(itemgetter(0, -1)(g.coords) for g in self.dframes["ferryseg"]["geometry"]))
            deadend = deadend.difference(ferry)
            intersection = intersection.difference(ferry)

        # junctype: NatProvTer.
        # Process: Query, from compiled junctypes, points not within the administrative boundary. Remove conflicting
        # points from other junctypes.
        logger.info("Configuring junctype: NatProvTer.")

        # Split administrative boundary into smaller polygons.
        boundary_polys = gpd.GeoSeries(self.divide_polygon(self.boundary, 0.1))

        # Compile all junctions as Point geometries.
        merged_pts = gpd.GeoSeries(map(Point, chain.from_iterable([deadend, ferry, intersection])))

        # Use the junctions' spatial indexes to query points within the split boundary polygons.
        pts_sindex = merged_pts.sindex
        pts_within = boundary_polys.map(
            lambda poly: merged_pts.iloc[list(pts_sindex.intersection(poly.bounds))].within(poly))
        pts_within = pts_within.map(lambda series: series.index[series].values)
        pts_within = set(chain.from_iterable(pts_within.to_list()))

        # Invert query and compile resulting points as NatProvTer.
        natprovter = set(merged_pts[~merged_pts.index.isin(pts_within)].map(lambda pt: pt.coords[0]))

        # Remove conflicting points from other junctypes.
        deadend = deadend.difference(natprovter)
        ferry = ferry.difference(natprovter)
        intersection = intersection.difference(natprovter)

        # Load junctions into target dataset.
        logger.info("Loading junctions into target dataset.")

        # Compile junctypes as GeoDataFrames.
        junctions = [gpd.GeoDataFrame(
            {'junctype': junctype, "uuid": [uuid.uuid4().hex for _ in range(len(pts))]},
            geometry=pd.Series(map(Point, pts))) for junctype, pts in
            {"Dead End": deadend, "Ferry": ferry, "Intersection": intersection, "NatProvTer": natprovter}.items() if
            len(pts)]

        # Concatenate junctions with target dataset and set uuid as index.
        self.dframes["junction"] = gpd.GeoDataFrame(
            pd.concat([self.junction, *junctions], ignore_index=True, sort=False), crs=self.dframes["roadseg"].crs)\
            .copy(deep=True)
        self.dframes["junction"].index = self.dframes["junction"]["uuid"]

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

        self.dframes = helpers.load_gpkg(self.data_path, layers=["ferryseg", "roadseg"])

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
@click.argument("source", type=click.Choice("ab bc mb nb nl ns nt nu on pe qc sk yt".split(), False))
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
