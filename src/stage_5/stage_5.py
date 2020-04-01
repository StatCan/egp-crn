import click
import fiona
import geopandas as gpd
import json
import logging
import numpy as np
import os
import pandas as pd
import pathlib
import shapely.ops
import shutil
import sys
import time
import urllib.request
import uuid
import zipfile
from itertools import chain, compress
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

        # Compile match fields (fields which must be equal across records).
        self.match_fields = ["namebody", "strtypre", "strtysuf", "dirprefix", "dirsuffix"]

        # Define dictionary for tracking dataset differences (change logs) between the previous and current vintage.
        self.change_logs = dict.fromkeys(self.dframes)

    def export_gpkg(self):
        """Exports the dataframes as GeoPackage layers."""

        logger.info("Exporting dataframes to GeoPackage layers.")

        # Export target dataframes to GeoPackage layers.
        helpers.export_gpkg(self.dframes, self.data_path)

    def export_change_logs(self):
        """Exports the dataset differences as logs."""

        change_logs_dir = os.path.abspath("../../data/processed/{}_change_logs".format(self.source))
        logger.info("Writing change logs to: \"{}\".".format(change_logs_dir))

        # Create change logs directory.
        pathlib.Path(change_logs_dir).mkdir(exist_ok=True)

        # Iterate tables and change types.
        for table in self.change_logs:
            for change, log in self.change_logs[table].items():

                # Configure log path.
                log_path = os.path.join(change_logs_dir, "{}_{}_{}.log".format(self.source, table, change))

                # Write log.
                with helpers.TempHandlerSwap(logger, log_path):
                    logger.info(log)

    def get_previous_vintage(self):
        """Downloads previous NRN vintage and loads data into dataframes."""

        logger.info("Retrieving previous NRN vintage.")
        source = helpers.load_yaml("../downloads.yaml")["previous_nrn_vintage"]

        # Configuring url for previous NRN vintage.
        logger.info("Configuring url for previous NRN vintage.")
        metadata_url = source["metadata_url"].replace("<id>", source["ids"][self.source])
        metadata = None

        attempt = 1
        max_attempts = 10
        while attempt <= max_attempts:

            try:

                # Open metadata url.
                metadata = urllib.request.urlopen(metadata_url)

            except (TimeoutError, urllib.error.URLError) as e:

                if attempt == max_attempts:
                    logger.exception("Unable to open NRN metadata url: \"{}\".".format(metadata_url))
                    logger.exception(e)
                    logger.warning("Maximum attempts reached. Exiting program.")
                    sys.exit(1)
                else:
                    logger.warning("Attempt {} of {} failed. Retrying.".format(attempt, max_attempts))
                    attempt += 1
                    time.sleep(5)
                    continue

        # Extract url from metadata.
        metadata = json.loads(metadata.read())
        download_url = metadata["result"]["resources"][0]["url"]

        # Download previous NRN vintage.
        logger.info("Downloading previous NRN vintage.")

        try:
            urllib.request.urlretrieve(download_url, "../../data/interim/previous_nrn.zip")
        except (TimeoutError, urllib.error.URLError) as e:
            logger.exception("Unable to download previous NRN vintage: \"{}\".".format(download_url))
            logger.exception(e)
            sys.exit(1)

        # Extract zipped data.
        logger.info("Extracting zipped data for previous NRN vintage.")

        gpkg_path = [f for f in zipfile.ZipFile("../../data/interim/previous_nrn.zip", "r").namelist() if
                     f.endswith(".gpkg")][0]

        with zipfile.ZipFile("../../data/interim/previous_nrn.zip", "r") as zip:
            zip.extract(gpkg_path, "../../data/interim/previous_nrn")

        # Load previous NRN vintage into dataframes.
        logger.info("Loading previous NRN vintage into dataframes.")

        self.dframes_old = helpers.load_gpkg(os.path.join("../../data/interim/previous_nrn", gpkg_path), find=True)

        # Force lowercase field names.
        for name, dframe in self.dframes_old.items():
            dframe.columns = map(str.lower, dframe.columns)
            self.dframes_old = dframe.copy(deep=True)

        # Remove temporary files.
        logger.info("Removing temporary previous NRN vintage files and directories.")

        for f in os.listdir("../../data/interim"):
            if os.path.splitext(f)[0] == "previous_nrn":
                path = os.path.join("../../data/interim", f)
                try:
                    os.remove(path) if os.path.isfile(path) else shutil.rmtree(path)
                except OSError as e:
                    logger.warning("Unable to remove directory: \"{}\".".format(os.path.abspath(path)))
                    logger.warning("OSError: {}.".format(e))
                    continue

    def load_gpkg(self):
        """Loads input GeoPackage layers into dataframes."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

    def roadseg_gen_full(self):
        """
        Generate the full representation of roadseg with all required fields for both the current and previous vintage.
        """

        logger.info("Generating full roadseg representation.")

        # roadseg

        # Copy and filter dataframes.
        roadseg = self.dframes["roadseg"][["uuid", "nid", "adrangenid", "geometry"]].copy(deep=True)
        addrange = self.dframes["addrange"][["nid", "l_offnanid", "r_offnanid"]].copy(deep=True)
        strplaname = self.dframes["strplaname"][["nid", *self.match_fields]].copy(deep=True)

        # Merge dataframes to assemble full roadseg representation.
        self.roadseg = roadseg.merge(
            addrange, how="left", left_on="adrangenid", right_on="nid", suffixes=("", "_addrange")).merge(
            strplaname, how="left", left_on=["l_offnanid", "r_offnanid"], right_on=["nid", "nid"],
            suffixes=("", "_strplaname"))

        self.roadseg.index = self.roadseg["uuid"]

        # roadseg - previous vintage

        # Copy and filter dataframes.
        roadseg = self.dframes_old["roadseg"][["nid", "adrangenid", "geometry"]].copy(deep=True)
        addrange = self.dframes_old["addrange"][["nid", "l_offnanid", "r_offnanid"]].copy(deep=True)
        strplaname = self.dframes_old["strplaname"][["nid", *self.match_fields]].copy(deep=True)

        # Merge dataframes to assemble full roadseg representation.
        self.roadseg_old = roadseg.merge(
            addrange, how="left", left_on="adrangenid", right_on="nid", suffixes=("", "_addrange")).merge(
            strplaname, how="left", left_on=["l_offnanid", "r_offnanid"], right_on=["nid", "nid"],
            suffixes=("", "_strplaname"))

    def roadseg_gen_nids(self):
        """Groups roadseg records and assigns nid values."""

        logger.info("Generating nids for table: roadseg.")

        # Copy and filter dataframes.
        roadseg = self.roadseg[["nid", "geometry"]].copy(deep=True)
        junction = self.dframes["junction"][["uuid", "geometry"]].copy(deep=True)

        # Group uuids and geometry by match fields.
        grouped = roadseg.groupby(self.match_fields)[["uuid", "geometry"]].agg(list)

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

        # Every row now represents an nid group.
        # Attributes:
        # 1) geometry: represents the dissolved geometry of the now-reduced group.
        # 2) uuids: the original attribute grouping of uuids.
        # The uuid group now needs to be reduced to match the now-reduced geometry.

        grouped = pd.DataFrame({"uuids": grouped["uuid"], "geometry": grouped["geometry"].map(lambda g: set(g.coords))})

        # Retrieve roadseg geometries for associated uuids.
        roadseg_geometry = roadseg["geometry"].map(lambda geom: set(itemgetter(0, 1)(geom.coords))).to_dict()
        grouped["uuids_geometry"] = grouped["uuids"].map(lambda uuids: itemgetter(*uuids)(roadseg_geometry))
        grouped["uuids_geometry"] = grouped["uuids_geometry"].map(lambda g: g if isinstance(g, tuple) else (g,))

        # Filter associated uuids by coordinate set subtraction.
        # Process: subtract the coordinate sets in the dissolved group geometry from the uuid geometry.
        grouped_query = pd.Series(np.vectorize(lambda g1, g2: [g1, g2])(grouped["geometry"], grouped["uuids_geometry"]))
        grouped_query = grouped_query.map(lambda row: list(map(lambda uuid_geom: len(uuid_geom - row[0]) == 0, row[1])))

        # Handle exceptions 1.
        # Identify results without uuid matches. These represents lines which backtrack onto themselves.
        # These records can be removed from the groupings as their junction-based split was in error.
        grouped_no_matches = grouped_query[grouped_query.map(lambda matches: not any(matches))]
        grouped.drop(grouped_no_matches.index, axis=0, inplace=True)
        grouped_query.drop(grouped_no_matches.index, axis=0, inplace=True)
        grouped.reset_index(drop=True, inplace=True)
        grouped_query.reset_index(drop=True, inplace=True)

        # Update grouped uuids to now-reduced list.
        grouped_query_d = grouped_query.to_dict()
        grouped = pd.Series([list(compress(uuids, grouped_query_d[index]))
                             for index, uuids in grouped["uuids"].iteritems()])

        # Assign nid to groups and explode grouped uuids.
        nid_groups = pd.DataFrame({"uuid": grouped, "nid": [uuid.uuid4().hex for _ in range(len(grouped))]})
        nid_groups = nid_groups.explode("uuid")

        # Handle exceptions 2.
        # Identify duplicated uuids. These represent dissolved groupings of two segments forming a loop, where one
        # segment is composed of only 2 points. Therefore, all coordinates in the 2 point segment will be found in the
        # other segment in the dissolved group, creating a duplicate match when filtering associated uuids.
        # Remove duplicate uuids which have also been assigned a non-unique nid.
        duplicated_uuids = nid_groups["uuid"].duplicated(keep=False)
        duplicated_nids = nid_groups["nid"].duplicated(keep=False)
        nid_groups = nid_groups[~(duplicated_uuids & duplicated_nids)]

        # Assign nids to roadseg.
        nid_groups.index = nid_groups["uuid"]
        roadseg["nid"] = nid_groups["nid"]

        # Store results.
        self.dframes["roadseg"] = roadseg.copy(deep=True)
        self.roadseg["nid"] = roadseg["nid"].copy(deep=True)

    def roadseg_recover_classify_nids(self):
        """
        1) Recovers roadseg nids from the previous NRN vintage.
        2) Generates 4 nid classification text files: added, retired, modified, confirmed."""

        logger.info("Recovering old nids and classifying all nids for table: roadseg.")

        # Copy and filter dataframes.
        roadseg = self.roadseg[["nid", "geometry"]].copy(deep=True)
        roadseg_old = self.roadseg_old[["nid", "geometry"]].copy(deep=True)

        # Group by nid.
        roadseg_grouped = roadseg.groupby("nid")["geometry"].apply(list)
        roadseg_old_grouped = roadseg_old.groupby("nid")["geometry"].apply(list)

        # Dissolve grouped geometries.
        roadseg_grouped = roadseg_grouped.map(lambda geoms: shapely.ops.linemerge(geoms))
        roadseg_old_grouped = roadseg_old_grouped.map(lambda geoms: shapely.ops.linemerge(geoms))

        # Convert series to geodataframes.
        # Restore nid index as column.
        roadseg_grouped = gpd.GeoDataFrame(roadseg_grouped.reset_index(drop=False))
        roadseg_old_grouped = gpd.GeoDataFrame(roadseg_old_grouped.reset_index(drop=False))

        # Merge current and old dataframes on geometry.
        merge = pd.merge(roadseg_old_grouped, roadseg_grouped, how="outer", on="geometry", suffixes=("_old", ""),
                         indicator=True)

        # Classify nid groups as: added, retired, modified, confirmed.
        classified_nids = {
            "added": merge[merge["_merge"] == "right_only"]["nid"].to_list(),
            "retired": merge[merge["_merge"] == "left_only"]["nid_old"].to_list(),
            "modified": None,
            "confirmed": merge[merge["_merge"] == "both"]
        }

        # Recover old nids for confirmed and modified nid groups via uuid index.
        # Merge uuids onto recovery dataframe.
        recovery = classified_nids["confirmed"].merge(roadseg[["nid", "uuid"]], how="left", on="nid")\
            .drop_duplicates(subset="nid", keep="first")
        recovery.index = recovery["uuid"]

        # Recovery old nids. Store results.
        self.roadseg.loc[self.roadseg["nid"].isin(recovery["nid"]), "nid"] = recovery["nid_old"]
        self.dframes["roadseg"]["nid"] = self.roadseg["nid"].copy(deep=True)

        # Separate modified from confirmed nid groups.
        # Restore match fields.
        roadseg_confirmed_new = classified_nids["confirmed"]\
            .merge(roadseg[["nid", *self.match_fields]], how="left", on="nid").drop_duplicates(keep="first")
        roadseg_confirmed_old = classified_nids["confirmed"]\
            .merge(roadseg_old[["nid", *self.match_fields]], how="left", left_on="nid_old", right_on="nid")\
            .drop_duplicates(keep="first")

        # Compare match fields to separate modified nid groups.
        # Update modified and confirmed nid classifications.
        flags = (roadseg_confirmed_new[self.match_fields] == roadseg_confirmed_old[self.match_fields]).all(axis=1)
        classified_nids["modified"] = classified_nids["confirmed"][flags.values].to_list()
        classified_nids["confirmed"] = classified_nids["confirmed"][~flags.values].to_list()

        # Store nid classifications as change logs.
        self.change_logs["roadseg"] = {
            change: "\n".join(map(str, ["Records listed by nid:", *nids])) if len(nids) else "No records." for
            change, nids in classified_nids.items()}

    def roadseg_update_linkages(self):
        """
        Updates the nid linkages of roadseg:
        1) blkpassage.roadnid
        2) tollpoint.roadnid
        """

        logger.info("Updating nid linkages for table: roadseg.")

        # Check table existence.
        tables = [table for table in ("blkpassage", "tollpoint") if table in self.dframes]

        if tables:

            # Filter and copy roadseg.
            roadseg = self.dframes["roadseg"][["nid", "geometry"]].copy(deep=True)

            # Generate roadseg kdtree.
            roadseg_tree = cKDTree(np.concatenate([np.array(geom.coords) for geom in roadseg["geometry"]]))

            # Compile an index-lookup table for each coordinate associated with each roadseg record.
            roadseg_pt_indexes = np.concatenate([[index] * count for index, count in
                                                 roadseg["geometry"].map(lambda geom: len(geom.coords)).iteritems()])
            roadseg_lookup = pd.Series(roadseg_pt_indexes, index=range(0, roadseg_tree.n)).to_dict()

        # Iterate dataframes, if available.
        for table in tables:
            if table in self.dframes:

                # Copy and filter dataframe.
                df = self.dframes[table][["roadnid", "geometry"]].copy(deep=True)

                # Identify maximum roadseg length.
                max_len = roadseg.length.max()

                # Compile indexes of all roadseg features within max_len distance.
                indexes = df["geometry"].map(lambda geom: roadseg_tree.query_ball_point(geom, r=max_len))

                # Retrieve associated record indexes for each point index.
                indexes = indexes.map(lambda idxs: set(itemgetter(*idxs)(roadseg_lookup)))

                # # Compile index of nearest neighbor from roadseg.
                # indexes = df["geometry"].map(lambda geom: roadseg_tree.query(geom, k=1)[1])
                #
                # # Retrieve the nid associated with the roadseg index.
                # # Store results.
                # self.dframes[table]["roadnid"] = indexes.map(lambda index: itemgetter(index)(roadseg_pts_nid))\
                #     .copy(deep=True)

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.get_previous_vintage()
        self.roadseg_gen_full()
        self.roadseg_gen_nids()
        self.roadseg_recover_classify_nids()
        self.roadseg_update_linkages()
        self.export_change_logs()
        # self.export_gpkg()


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
