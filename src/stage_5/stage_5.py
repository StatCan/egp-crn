import click
import fiona
import geopandas as gpd
import logging
import numpy as np
import os
import pandas as pd
import shapely.ops
import sys
import uuid
from itertools import chain
from operator import itemgetter
from scipy.spatial import cKDTree
from shapely.geometry import LineString, MultiPoint

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import helpers


# Suppress pandas chained assignment warning.
pd.options.mode.chained_assignment = None


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
        self.stage = 5
        self.source = source.lower()

        # Configure and validate input data path.
        self.data_path = os.path.abspath("../../data/interim/{}.gpkg".format(self.source))
        if not os.path.exists(self.data_path):
            logger.exception("Input data not found: \"{}\".".format(self.data_path))
            sys.exit(1)

    def export_gpkg(self):
        """Exports the dataframes as GeoPackage layers."""

        logger.info("Exporting dataframes to GeoPackage layers.")

        # Export target dataframes to GeoPackage layers.
        helpers.export_gpkg(self.dframes, self.data_path)

    def gen_nids_roadseg(self):
        """Groups roadseg records and assigns nid values."""

        # Compile match fields (fields which must be equal across records).
        match_fields = ["namebody", "strtypre", "strtysuf", "dirprefix", "dirsuffix"]

        # Copy dataframes, keeping only necessary fields.
        roadseg = self.dframes["roadseg"][["uuid", "nid", "adrangenid", "geometry"]].copy(deep=True)
        addrange = self.dframes["addrange"][["nid", "l_offnanid", "r_offnanid"]].copy(deep=True)
        strplaname = self.dframes["strplaname"][["nid", *match_fields]].copy(deep=True)
        junction = self.dframes["junction"][["uuid", "geometry"]].copy(deep=True)

        # Compile match fields via dataframe merges.
        roadseg = roadseg.merge(
            addrange, how="left", left_on="adrangenid", right_on="nid", suffixes=("", "_addrange")).merge(
            strplaname, how="left", left_on=["l_offnanid", "r_offnanid"], right_on=["nid", "nid"],
            suffixes=("", "_strplaname"))

        # Group uuids and geometry by match fields.
        grouped = roadseg.groupby(match_fields)[["uuid", "geometry"]].agg(list)

        # Dissolve geometries.
        grouped["geometry"] = grouped["geometry"].map(lambda geoms: shapely.ops.linemerge(geoms))

        # Split multilinestrings into multiple linestring records.
        # Process: query and explode multilinestring records, then concatenate to linestring records.
        grouped_single = grouped[~grouped["geometry"].map(lambda geom: geom.type == "MultiLineString")]
        grouped_multi = grouped[grouped["geometry"].map(lambda geom: geom.type) == "MultiLineString"]

        grouped_multi_exploded = grouped_multi.explode("geometry")
        grouped = pd.concat([grouped_single, grouped_multi_exploded], axis=0, ignore_index=False, sort=False)

        # Compile associated junction indexes for each linestring.
        # Process: use cKDTree to compile indexes of coincident junctions to each linestring point, excluding endpoints.
        junction_tree = cKDTree(np.concatenate([geom.coords for geom in junction["geometry"]]))
        grouped["junction"] = grouped["geometry"].map(
            lambda geom: list(chain(*junction_tree.query_ball_point(geom.coords[1: -1], r=0))) if len(geom.coords) > 2
            else [])

        # Convert associated junction indexes to junction geometries.
        # Process: retrieve junction geometries for associated indexes and convert to multipoint.
        grouped_no_junction = grouped[~grouped["junction"].map(lambda indexes: len(indexes) > 0)]
        grouped_junction = grouped[grouped["junction"].map(lambda indexes: len(indexes) > 0)]

        junction_geometry = junction["geometry"].reset_index(drop=True).to_dict()
        grouped_junction["junction"] = grouped_junction["junction"].map(
            lambda indexes: itemgetter(*indexes)(junction_geometry)).copy(deep=True)
        grouped_junction["junction"] = grouped_junction["junction"].map(
            lambda pts: MultiPoint(pts) if isinstance(pts, tuple) else pts)

        # Split linestrings on junctions.
        grouped_junction["geometry"] = np.vectorize(
            lambda line, pts: shapely.ops.split(line, pts), otypes=[LineString])(
            grouped_junction["geometry"], grouped_junction["junction"])

        # Split multilinestrings into multiple linestring records, concatenate all split records to non-split records.
        # Process: reset indexes, explode multilinestring records from split results, then concatenate all split and
        # non-split results.
        grouped_junction.reset_index(drop=True, inplace=True)
        grouped_no_junction.reset_index(drop=True, inplace=True)

        grouped_junction_exploded = grouped_junction.explode("geometry")
        grouped = pd.concat([grouped_no_junction, grouped_junction_exploded], axis=0, ignore_index=True, sort=False)

        # Every row now represents an nid group, with all the constituent geometries dissolved.
        # Recover now-reduced uuid groupings for each group's geometry.
        # Compile geometries as coordinate sets for efficiency.

        grouped = pd.DataFrame({"uuids": grouped["uuid"], "geometry": grouped["geometry"].map(lambda g: set(g.coords))})

        # Retrieve roadseg geometries for associated uuids.
        roadseg_geometry = roadseg["geometry"].map(lambda geom: set(itemgetter(0, 1)(geom.coords))).to_dict()
        grouped["uuids_geometry"] = grouped["uuids"].map(
            lambda uuids: list(map(set, itemgetter(*uuids)(roadseg_geometry))))

        # Filter associated uuids by spatial query: contains.
        # Process: subtract the coordinates in the dissolved group geometry from the uuid geometry (set subtraction is
        # quicker than shapely contains).
        grouped_query = pd.Series(np.vectorize(lambda g1, g2: [g1, g2])(grouped["geometry"], grouped["uuids_geometry"]))
        grouped_query = grouped_query.map(lambda row: list(map(lambda uuid_geom: len(uuid_geom - row[0]) == 0, row[1])))
        # TODO: use itertools.compress to filter the uuids.
        # TODO: the results show some groups having no matches because the dissolved geometry set lost some coordinates from the dissolve operation. These need to be recovered.

    def load_gpkg(self):
        """Loads input GeoPackage layers into dataframes."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.gen_nids_roadseg()
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
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)

if __name__ == "__main__":
    main()
