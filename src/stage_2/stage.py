import click
import fiona
import geopandas as gpd
import logging
import numpy as np
import pandas as pd
import requests
import sys
import uuid
from collections import Counter
from datetime import datetime
from itertools import chain
from operator import attrgetter, itemgetter
from pathlib import Path
from scipy.spatial import cKDTree
from shapely.geometry import box, Point, Polygon, MultiPolygon, GeometryCollection
from typing import Dict, List, Union

filepath = Path(__file__).resolve()
sys.path.insert(1, str(filepath.parents[1]))
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

    def __init__(self, source: str) -> None:
        """
        Initializes an NRN stage.

        :param str source: abbreviation for the source province / territory.
        """

        self.stage = 2
        self.source = source.lower()
        self.boundary = None

        # Configure and validate input data path.
        self.data_path = filepath.parents[2] / f"data/interim/{self.source}.gpkg"
        if not self.data_path.exists():
            logger.exception(f"Input data not found: \"{self.data_path}\".")
            sys.exit(1)

        # Compile field defaults, dtypes, and domains.
        self.defaults = helpers.compile_default_values(lang="en")["junction"]
        self.dtypes = helpers.compile_dtypes()["junction"]
        self.domains = helpers.compile_domains(mapped_lang="en")["junction"]

        # Load data.
        self.dframes = helpers.load_gpkg(self.data_path, layers=["ferryseg", "roadseg"])

    def apply_domains(self) -> None:
        """Applies domain restrictions to each column in the target (Geo)DataFrames."""

        logging.info("Applying field domains.")
        field = None

        try:

            for field, domain in self.domains.items():

                logger.info(f"Applying domain to field: {field}.")

                # Apply domain to series.
                series = self.dframes["junction"][field].copy(deep=True)
                series = helpers.apply_domain(series, domain["lookup"], self.defaults[field])

                # Force adjust data type.
                series = series.astype(self.dtypes[field])

                # Store results to dataframe.
                self.dframes["junction"][field] = series.copy(deep=True)

        except (AttributeError, KeyError, ValueError):
            logger.exception(f"Invalid schema definition for table: junction, field: {field}.")
            sys.exit(1)

    def compile_target_attributes(self) -> None:
        """Compiles the yaml file for the target (Geo)DataFrames (distribution format) into a dictionary."""

        logger.info("Compiling target attributes yaml.")
        table = field = None

        # Load yaml.
        self.target_attributes = helpers.load_yaml(filepath.parents[1] / "distribution_format.yaml")

        # Remove field length from dtype attribute.
        logger.info("Configuring target attributes.")
        try:

            for table in self.target_attributes:
                for field, vals in self.target_attributes[table]["fields"].items():
                    self.target_attributes[table]["fields"][field] = vals[0]

        except (AttributeError, KeyError, ValueError):
            logger.exception(f"Invalid schema definition for table: {table}, field: {field}.")
            sys.exit(1)

    def divide_polygon(self, poly: Union[MultiPolygon, Polygon], threshold: Union[float, int], pts: pd.Series,
                       count: int = 0) -> List[Union[None, MultiPolygon, Polygon]]:
        """
        Recursively divides a (Multi)Polygon into 2 parts until any of the following limits are reached:
        a) both dimensions (height and width) are <= threshold.
        b) a recursion depth of 250 is reached.
        c) no more point tuples exist within the current bounds.

        :param Union[MultiPolygon, Polygon] poly: (Multi)Polygon.
        :param Union[float, int] threshold: maximum height and width of the divided Polygons.
        :param pd.Series pts: Series of coordinate tuples.
        :param int count: current recursion depth (for internal use), default 0.
        :return List[Union[None, MultiPolygon, Polygon]]: list of Polygons, extracted from the original (Multi)Polygon.
        """

        xmin, ymin, xmax, ymax = poly.bounds

        # Configure bounds dimensions.
        width = xmax - xmin
        height = ymax - ymin

        # Exit recursion once limits are reached.
        if max(width, height) <= threshold or count == 250 or not len(pts):
            if len(pts):
                return [poly]
            else:
                return [None]

        # Conditionally split polygon by height or width.
        if height >= width:
            a = box(xmin, ymin, xmax, ymin + (height / 2))
            b = box(xmin, ymin + (height / 2), xmax, ymax)
        else:
            a = box(xmin, ymin, xmin + (width / 2), ymax)
            b = box(xmin + (width / 2), ymin, xmax, ymax)
        result = []

        # Compile split results, further recurse.
        for d in (a, b,):
            c = poly.intersection(d)
            if not isinstance(c, GeometryCollection):
                c = [c]
            for e in c:
                if isinstance(e, (Polygon, MultiPolygon)):
                    pts_subset = pts[pts.map(lambda pt: (xmin <= pt[0] <= xmax) and (ymin <= pt[1] <= ymax))]
                    result.extend(self.divide_polygon(e, threshold, pts_subset, count=count + 1))

        if count > 0:
            return result

        # Compile final result as a single-part polygon.
        final_result = []
        for g in result:
            if g:
                if isinstance(g, MultiPolygon):
                    final_result.extend(g)
                else:
                    final_result.append(g)

        return final_result

    def gen_attributes(self) -> None:
        """Generate the remaining attributes for the output junction dataset."""

        logger.info("Generating remaining dataset attributes.")

        def compute_connected_attributes(attributes: List[str]) -> Dict[str, pd.Series]:
            """
            Computes the given attributes from NRN ferryseg and roadseg features connected to the junction dataset.
            Currently supported attributes: 'accuracy', 'exitnbr'.

            :param List[str] attributes: list of attribute names.
            :return Dict[str, pd.Series]: dictionary of attributes and junction-aligned Series of attribute values.
            """

            junction = self.dframes["junction"].copy(deep=True)

            # Validate input attributes.
            if not set(attributes).issubset({"accuracy", "exitnbr"}):
                logger.exception(f"One or more unsupported attributes provided: {', '.join(attributes)}.")
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
                default = self.defaults[attribute]
                connected_attribute = None

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
                        lambda vals: ", ".join(sorted([str(v) for v in vals if v not in {default, "None"} and
                                                       not pd.isna(v)])))

                # Populate empty results with default.
                connected_attribute = connected_attribute.map(lambda v: v if len(str(v)) else default)

                # Store results.
                results[attribute] = connected_attribute.copy(deep=True)

            return results

        # Set remaining attributes, where possible.
        self.dframes["junction"]["acqtech"] = "Computed"
        self.dframes["junction"]["metacover"] = "Complete"
        self.dframes["junction"]["credate"] = datetime.today().strftime("%Y%m%d")
        self.dframes["junction"]["datasetnam"] = self.dframes["roadseg"]["datasetnam"].iloc[0]
        self.dframes["junction"]["provider"] = "Federal"
        self.dframes["junction"]["revdate"] = self.defaults["revdate"]
        connected_attributes = compute_connected_attributes(["accuracy", "exitnbr"])
        self.dframes["junction"]["accuracy"] = connected_attributes["accuracy"]
        self.dframes["junction"]["exitnbr"] = connected_attributes["exitnbr"]

    def gen_junctions(self) -> None:
        """Generates a junction GeoDataFrame for all junctypes: Dead End, Ferry, Intersection, and NatProvTer."""

        logger.info("Generating junctions.")
        df = self.dframes["roadseg"].copy(deep=True)

        # Separate unique and non-unique points.
        logger.info("Separating unique and non-unique points.")

        # Construct a uuid series aligned to the series of points.
        pts_uuid = np.concatenate([[id] * count for id, count in df["geometry"].map(
            lambda geom: len(geom.coords)).iteritems()])

        # Construct x- and y-coordinate series aligned to the series of points.
        pts_x, pts_y = np.concatenate(df["geometry"].map(attrgetter("coords")).to_numpy()).T

        # Join the uuids, x- and y-coordinates.
        pts_df = pd.DataFrame({"x": pts_x, "y": pts_y, "uuid": pts_uuid})

        # Query unique points (all) and endpoints.
        pts_unique = set(map(tuple, pts_df.loc[~pts_df[["x", "y"]].duplicated(keep=False), ["x", "y"]].values))
        endpoints_unique = set(map(tuple, np.unique(np.concatenate(
            df["geometry"].map(lambda g: itemgetter(0, -1)(attrgetter("coords")(g))).to_numpy()), axis=0)))

        # Query non-unique points (all), keep only the first duplicated point from self-loops.
        pts_dup = pts_df.loc[(pts_df[["x", "y"]].duplicated(keep=False)) &
                             (~pts_df.duplicated(keep="first")), ["x", "y"]].values

        # Query junctypes.

        # junctype: Dead End.
        # Process: Query unique points which also exist in unique endpoints.
        logger.info("Configuring junctype: Dead End.")
        deadend = pts_unique.intersection(endpoints_unique)

        # junctype: Intersection.
        # Process: Query non-unique points with >= 3 instances.
        logger.info("Configuring junctype: Intersection.")
        counts = Counter(map(tuple, pts_dup))
        intersection = {pt for pt, count in counts.items() if count >= 3}

        # junctype: Ferry.
        # Process: Compile all unique ferryseg endpoints which intersect a roadseg point. Remove conflicting points
        # from other junctypes.
        logger.info("Configuring junctype: Ferry.")

        ferry = set()
        if "ferryseg" in self.dframes:
            pts_all = pts_unique.union(set(map(tuple, pts_dup)))
            ferry = set(chain.from_iterable(self.dframes["ferryseg"]["geometry"].map(
                lambda g: itemgetter(0, -1)(attrgetter("coords")(g)))))

            ferry = ferry.intersection(pts_all)
            deadend = deadend.difference(ferry)
            intersection = intersection.difference(ferry)

        # junctype: NatProvTer.
        # Process: Query, from compiled junctypes, points not within the administrative boundary. Remove conflicting
        # points from other junctypes.
        logger.info("Configuring junctype: NatProvTer.")

        # Compile all junction as coordinate tuples and Point geometries.
        pts = pd.Series(chain.from_iterable([deadend, ferry, intersection]))
        pts_geoms = gpd.GeoSeries(pts.map(Point))

        # Split administrative boundary into smaller polygons.
        boundary_polys = gpd.GeoSeries(self.divide_polygon(self.boundary["geometry"].iloc[0], 0.1, pts))

        # Use the junctions' spatial indexes to query points within the split boundary polygons.
        pts_sindex = pts_geoms.sindex
        pts_within = boundary_polys.map(
            lambda poly: pts_geoms.iloc[list(pts_sindex.intersection(poly.bounds))].within(poly))
        pts_within = pts_within.map(lambda series: series.index[series].values)
        pts_within = set(chain.from_iterable(pts_within.to_list()))

        # Invert query and compile resulting points as NatProvTer.
        natprovter = set(pts_geoms.loc[~pts_geoms.index.isin(pts_within)].map(lambda pt: pt.coords[0]))

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

    def gen_target_dataframe(self) -> None:
        """Creates empty junction GeoDataFrame."""

        logger.info("Creating target dataframe.")

        self.junction = gpd.GeoDataFrame().assign(**{field: pd.Series(dtype=dtype) for field, dtype in
                                                     self.target_attributes["junction"]["fields"].items()})

    def load_boundaries(self) -> None:
        """Downloads and compiles the administrative boundaries for the source province / territory."""

        logger.info("Loading administrative boundaries.")

        # Download administrative boundaries.
        logger.info("Downloading administrative boundary file.")
        source = helpers.load_yaml(filepath.parents[1] / "downloads.yaml")["provincial_boundaries"]
        download_url, source_crs = itemgetter("url", "crs")(source)

        try:

            # Get raw content stream from download url.
            download = helpers.get_url(download_url, stream=True, timeout=30, verify=True).content

            # Load bytes collection into geodataframe.
            with fiona.BytesCollection(download) as f:
                self.boundary = gpd.GeoDataFrame.from_features(f, crs=source_crs)

            # Filter boundaries.
            pruid = {"ab": 48, "bc": 59, "mb": 46, "nb": 13, "nl": 10, "ns": 12, "nt": 61, "nu": 62, "on": 35, "pe": 11,
                     "qc": 24, "sk": 47, "yt": 60}[self.source]
            self.boundary = self.boundary.loc[self.boundary["PRUID"] == str(pruid)]

            # Reproject boundaries to EPSG:4617.
            self.boundary = self.boundary.to_crs("EPSG:4617")

        except (fiona.errors, requests.exceptions.RequestException) as e:
            logger.exception(f"Error encountered when compiling administrative boundary file: \"{download_url}\".")
            logger.exception(e)
            sys.exit(1)

    def execute(self) -> None:
        """Executes an NRN stage."""

        self.load_boundaries()
        self.compile_target_attributes()
        self.gen_target_dataframe()
        self.gen_junctions()
        self.gen_attributes()
        self.apply_domains()
        helpers.export({"junction": self.dframes["junction"]}, self.data_path)


@click.command()
@click.argument("source", type=click.Choice("ab bc mb nb nl ns nt nu on pe qc sk yt".split(), False))
def main(source: str) -> None:
    """
    Executes an NRN stage.

    :param str source: abbreviation for the source province / territory.
    """

    try:

        with helpers.Timer():
            stage = Stage(source)
            stage.execute()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
