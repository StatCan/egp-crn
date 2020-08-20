import click
import geopandas as gpd
import logging
import math
import networkx as nx
import numpy as np
import os
import pandas as pd
import pathlib
import string
import sys
import uuid
from itertools import chain, compress
from operator import attrgetter, itemgetter
from scipy.spatial import cKDTree
from shapely.geometry import LineString, MultiPoint, Point
from shapely.ops import linemerge, split

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
        self.stage = 3
        self.source = source.lower()

        # Configure and validate input data path.
        self.data_path = os.path.abspath("../../data/interim/{}.gpkg".format(self.source))
        if not os.path.exists(self.data_path):
            logger.exception("Input data not found: \"{}\".".format(self.data_path))
            sys.exit(1)

        # Compile match fields (fields which must be equal across records).
        self.match_fields = ["r_stname_c"]

        # Define change logs dictionary.
        self.change_logs = dict()

        # Load default field values.
        self.defaults = helpers.compile_default_values()["roadseg"]

    def export_change_logs(self):
        """Exports the dataset differences as logs - based on nids."""

        change_logs_dir = os.path.abspath("../../data/processed/{0}/{0}_change_logs".format(self.source))
        logger.info("Writing change logs to: \"{}\".".format(change_logs_dir))

        # Create change logs directory.
        pathlib.Path(change_logs_dir).mkdir(parents=True, exist_ok=True)

        # Iterate tables and change types.
        for table in self.change_logs:
            for change, log in self.change_logs[table].items():

                # Configure log path.
                log_path = os.path.join(change_logs_dir, "{}_{}_{}.log".format(self.source, table, change))

                # Write log.
                with helpers.TempHandlerSwap(logger, log_path):
                    logger.info(log)

    def export_gpkg(self):
        """Exports the dataframes as GeoPackage layers."""

        logger.info("Exporting dataframes to GeoPackage layers.")

        # Export target dataframes to GeoPackage layers.
        helpers.export_gpkg(self.dframes, self.data_path)

    def gen_and_recover_structids(self):
        """Recovers structids from the previous NRN vintage or generates new ones."""

        logger.info("Generating structids for table: roadseg.")

        # Copy and filter dataframes.
        roadseg = self.dframes["roadseg"][["uuid", "structid", "structtype", "geometry"]].copy(deep=True)
        roadseg_old = self.dframes_old["roadseg"][["structid", "structtype", "geometry"]].copy(deep=True)

        # Overwrite any pre-existing structid.
        roadseg["structid"] = [uuid.uuid4().hex for _ in range(len(roadseg))]
        roadseg.loc[roadseg["structtype"] == "None", "structid"] = "None"

        # Subset dataframes to valid structures.
        # Further subset previous vintage to records with valid IDs.
        roadseg = roadseg[roadseg["structtype"] != "None"]
        roadseg_old = roadseg_old[self.get_valid_ids(roadseg_old["structid"])]

        if len(roadseg):

            # Group contiguous structures.
            # Process: compile network x subgraphs, assign a structid to each list of subgraph uuids.
            subgraphs = nx.connected_component_subgraphs(
                helpers.gdf_to_nx(roadseg, keep_attributes=True, endpoints_only=True))
            structids = dict()

            for subgraph in subgraphs:
                structids[uuid.uuid4().hex] = list(set(nx.get_edge_attributes(subgraph, "uuid").values()))

            # Explode uuid groups and invert series-index such that the uuid is the index.
            structids = pd.Series(structids).explode()
            structids = pd.Series(structids.index.values, index=structids)

            # Assign structids to dataframe.
            roadseg.loc[structids.index, "structid"] = structids

            # Recovery old structids.
            logger.info("Recovering old structids for table: roadseg.")

            # Group by structid.
            roadseg_grouped = helpers.groupby_to_list(roadseg, "structid", "geometry")
            roadseg_old_grouped = helpers.groupby_to_list(roadseg_old, "structid", "geometry")

            # Dissolve grouped geometries.
            roadseg_grouped = roadseg_grouped.map(lambda geoms: geoms[0] if len(geoms) == 1 else linemerge(geoms))
            roadseg_old_grouped = roadseg_old_grouped.map(
                lambda geoms: geoms[0] if len(geoms) == 1 else linemerge(geoms))

            # Convert series to geodataframes.
            # Restore structid index as column.
            roadseg_grouped = gpd.GeoDataFrame({"structid": roadseg_grouped.index,
                                                "geometry": roadseg_grouped.reset_index(drop=True)})
            roadseg_old_grouped = gpd.GeoDataFrame({"structid": roadseg_old_grouped,
                                                    "geometry": roadseg_old_grouped.reset_index(drop=True)})

            # Merge current and old dataframes on geometry.
            merge = pd.merge(roadseg_old_grouped, roadseg_grouped, how="outer", on="geometry", suffixes=("_old", ""),
                             indicator=True)

            # Recover old structids via uuid index.
            # Merge uuids onto recovery dataframe.
            recovery = merge[merge["_merge"] == "both"].merge(roadseg[["structid", "uuid"]], how="left", on="structid")\
                .drop_duplicates(subset="structid", keep="first")
            recovery.index = recovery["uuid"]

            # Filter invalid structids from old data.
            recovery = recovery[self.get_valid_ids(recovery["structid_old"])]

            # Recover old structids.
            if len(recovery):
                roadseg.loc[recovery.index, "structid"] = recovery["structid_old"]

            # Store results.
            self.roadseg.loc[roadseg.index, "structid"] = roadseg["structid"].copy(deep=True)
            self.dframes["roadseg"].loc[self.roadseg.index, "structid"] = self.roadseg["structid"].copy(deep=True)

    def get_valid_ids(self, series):
        """
        Validates a series of IDs based on the following conditions:
        1) ID must be non-null.
        2) ID must be 32 digits.
        3) ID must be hexadecimal.
        Returns flags.
        """

        hexdigits = set(string.hexdigits)

        # Filter records.
        flags = ~((series.isna()) |
                  (series.map(lambda val: len(str(val)) != 32)) |
                  (series.map(lambda val: not set(str(val)).issubset(hexdigits))))

        return flags

    def load_gpkg(self):
        """Loads input GeoPackage layers into dataframes."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

        logger.info("Loading Geopackage layers - previous vintage.")

        self.dframes_old = helpers.load_gpkg("../../data/interim/{}_old.gpkg".format(self.source), find=True)

    def recover_and_classify_nids(self):
        """
        For all spatial datasets, excluding roadseg:
        1) Recovers nids from the previous NRN vintage or generates new ones.
        2) Generates 4 nid classification log files: added, retired, modified, confirmed.
        """

        # Iterate datasets.
        for table in ("blkpassage", "ferryseg", "junction", "tollpoint"):

            # Check dataset existence.
            if table in self.dframes:

                logger.info("Generating nids for table: {}.".format(table))

                # Assign nids to current vintage.
                self.dframes[table]["nid"] = [uuid.uuid4().hex for _ in range(len(self.dframes[table]))]

                # Recover old nids, if old dataset is available.
                # Classify nids.
                if table in self.dframes_old:

                    logger.info("Recovering old nids and classifying all nids for table: {}.".format(table))

                    # Copy and filter dataframes.
                    df = self.dframes[table][["nid", "uuid", "geometry"]].copy(deep=True)
                    df_old = self.dframes_old[table][["nid", "geometry"]].copy(deep=True)

                    # Merge current and old dataframes on geometry.
                    merge = pd.merge(df_old, df, how="outer", on="geometry", suffixes=("_old", ""), indicator=True)

                    # Classify nid groups as: added, retired, modified, confirmed.
                    classified_nids = {
                        "added": merge[merge["_merge"] == "right_only"]["nid"].to_list(),
                        "retired": merge[merge["_merge"] == "left_only"]["nid_old"].to_list(),
                        "modified": list(),
                        "confirmed": merge[merge["_merge"] == "both"]
                    }

                    # Recover old nids for confirmed and modified nid groups via uuid index.
                    # Merge uuids onto recovery dataframe.
                    recovery = classified_nids["confirmed"].merge(df["nid"], how="left", on="nid")\
                        .drop_duplicates(subset="nid", keep="first")
                    recovery.index = recovery["uuid"]

                    # Filter invalid nids from old data.
                    recovery = recovery[self.get_valid_ids(recovery["nid_old"])]

                    # Recover old nids.
                    if len(recovery):
                        df.loc[recovery.index, "nid"] = recovery["nid_old"]

                    # Store results.
                    self.dframes[table].loc[df.index, "nid"] = df["nid"].copy(deep=True)

                    # Update confirmed nid classification.
                    classified_nids["confirmed"] = classified_nids["confirmed"]["nid"].to_list()

                # Classify nids.
                else:

                    logger.info("Classifying all nids for table: {}. No old nid recovery required.".format(table))

                    classified_nids = {
                        "added": self.dframes[table]["nid"].to_list(),
                        "retired": list(),
                        "modified": list(),
                        "confirmed": list()
                    }

                # Store nid classifications as change logs.
                self.change_logs[table] = {
                    change: "\n".join(map(str, ["Records listed by nid:", *nids])) if len(nids) else "No records." for
                    change, nids in classified_nids.items()
                }

    def roadseg_gen_full(self):
        """
        Generate the full representation of roadseg with all required fields for both the current and previous vintage.
        """

        logger.info("Generating full roadseg representation.")

        # Copy and filter dataframe - current vintage.
        self.roadseg = self.dframes["roadseg"][["uuid", "nid", "geometry", *self.match_fields]].copy(deep=True)
        self.roadseg.index = self.roadseg["uuid"]

        # Copy and filter dataframe - previous vintage.
        self.roadseg_old = self.dframes_old["roadseg"][["nid", "geometry", *self.match_fields]].copy(deep=True)

    def roadseg_gen_nids(self):
        """Groups roadseg records and assigns nid values."""

        logger.info("Generating nids for table: roadseg.")

        # Copy and filter dataframes.
        roadseg = self.roadseg[[*self.match_fields, "uuid", "nid", "geometry"]].copy(deep=True)
        junction = self.dframes["junction"][["uuid", "geometry"]].copy(deep=True)

        # Overwrite any pre-existing structid.
        roadseg["nid"] = [uuid.uuid4().hex for _ in range(len(roadseg))]

        # Subset dataframes to where at least one match field is not equal to the default value nor "None".
        default = self.defaults[self.match_fields[0]]
        roadseg = roadseg[~((roadseg[self.match_fields].eq(roadseg[self.match_fields].iloc[:, 0], axis=0).all(axis=1)) &
                            (roadseg[self.match_fields[0]].isin(["None", default])))]

        # Group uuids and geometry by match fields.
        # To reduce processing, only duplicated records are grouped.
        dups = roadseg[roadseg[self.match_fields].duplicated(keep=False)]
        dups_geom_lookup = dups["geometry"].to_dict()
        grouped = dups.groupby(self.match_fields)["uuid"].agg(list)

        # Split groups which exceed the processing threshold.
        # Note: The threshold and new size are arbitrary. Change them if required.
        threshold = 10000
        new_size = 1000
        invalid_groups = grouped[grouped.map(len) >= threshold].copy(deep=True)
        grouped.drop(invalid_groups.index, inplace=True)
        for invalid_group in invalid_groups:
            grouped = grouped.append(pd.Series([invalid_group[start_idx * new_size: (start_idx * new_size) + new_size]
                                                for start_idx in range(int(len(invalid_group) / new_size) + 1)]))

        # Compile associated geometries for each uuid group as a dataframe.
        grouped = pd.DataFrame({"uuid": grouped.values,
                                "geometry": grouped.map(lambda uuids: itemgetter(*uuids)(dups_geom_lookup)).values},
                               index=range(len(grouped)))

        # Dissolve geometries.
        grouped["geometry"] = grouped["geometry"].map(linemerge)

        # Concatenate non-grouped groups (single uuid groups) to groups.
        non_grouped = roadseg[~roadseg[self.match_fields].duplicated(keep=False)][["uuid", "geometry"]]
        non_grouped["uuid"] = non_grouped["uuid"].map(lambda uid: [uid])
        grouped = pd.concat([grouped, non_grouped], axis=0, ignore_index=True, sort=False)

        # Split multilinestrings into multiple linestring records.
        # Process: query and explode multilinestring records, then concatenate to linestring records.
        grouped_single = grouped[~grouped["geometry"].map(lambda geom: geom.type == "MultiLineString")]
        grouped_multi = grouped[grouped["geometry"].map(lambda geom: geom.type) == "MultiLineString"]

        grouped_multi_exploded = grouped_multi.explode("geometry")
        grouped = pd.concat([grouped_single, grouped_multi_exploded], axis=0, ignore_index=False, sort=False)

        # Compile coincident junctions to each linestring point, excluding endpoints.
        junction_pts = set(chain.from_iterable(junction["geometry"].map(attrgetter("coords"))))
        grouped["junction"] = grouped["geometry"].map(lambda geom: set(list(geom.coords)[1: -1]))
        grouped["junction"] = grouped["junction"].map(lambda coords: list(coords.intersection(junction_pts)))

        # Separate groups with and without coincident junctions.
        grouped_no_junction = grouped[~grouped["junction"].map(lambda indexes: len(indexes) > 0)]
        grouped_junction = grouped[grouped["junction"].map(lambda indexes: len(indexes) > 0)]

        # Convert coords to shapely points.
        grouped_junction["junction"] = grouped_junction["junction"].map(
            lambda pts: Point(pts) if len(pts) == 1 else MultiPoint(pts))

        # Split linestrings on junctions, only for groups with coincident junctions.
        grouped_junction["geometry"] = np.vectorize(lambda line, pts: split(line, pts), otypes=[LineString])(
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

        # Filter associated uuids by validating coordinate subset.
        # Process: for each coordinate set for each uuid in a group, test if the set is a subset of the
        # complete group coordinate set.
        grouped_query = pd.Series(np.vectorize(
            lambda pts_group, pts_uuids: list(map(lambda pts_uuid: pts_uuid.issubset(pts_group), pts_uuids)),
            otypes=[np.object])(
            grouped["geometry"], grouped["uuids_geometry"]))

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
        # Store results.
        nid_groups.index = nid_groups["uuid"]
        self.roadseg.loc[nid_groups.index, "nid"] = nid_groups["nid"].copy(deep=True)
        self.dframes["roadseg"].loc[self.roadseg.index, "nid"] = self.roadseg["nid"].copy(deep=True)

    def roadseg_recover_and_classify_nids(self):
        """
        1) Recovers roadseg nids from the previous NRN vintage.
        2) Generates 4 nid classification log files: added, retired, modified, confirmed.
        """

        logger.info("Recovering old nids and classifying all nids for table: roadseg.")

        # Copy and filter dataframes.
        roadseg = self.roadseg[[*self.match_fields, "nid", "uuid", "geometry"]].copy(deep=True)
        roadseg_old = self.roadseg_old[[*self.match_fields, "nid", "geometry"]].copy(deep=True)

        # Group by nid.
        roadseg_grouped = helpers.groupby_to_list(roadseg, "nid", "geometry")
        roadseg_old_grouped = helpers.groupby_to_list(roadseg_old, "nid", "geometry")

        # Dissolve grouped geometries.
        roadseg_grouped = roadseg_grouped.map(lambda geoms: geoms[0] if len(geoms) == 1 else linemerge(geoms))
        roadseg_old_grouped = roadseg_old_grouped.map(lambda geoms: geoms[0] if len(geoms) == 1 else linemerge(geoms))

        # Convert series to geodataframes.
        # Restore nid index as column.
        roadseg_grouped = gpd.GeoDataFrame({"nid": roadseg_grouped.index,
                                            "geometry": roadseg_grouped.reset_index(drop=True)})
        roadseg_old_grouped = gpd.GeoDataFrame({"nid": roadseg_old_grouped,
                                                "geometry": roadseg_old_grouped.reset_index(drop=True)})

        # Merge current and old dataframes on geometry.
        merge = pd.merge(roadseg_old_grouped, roadseg_grouped, how="outer", on="geometry", suffixes=("_old", ""),
                         indicator=True)

        # Classify nid groups as: added, retired, modified, confirmed.
        classified_nids = {
            "added": merge[merge["_merge"] == "right_only"]["nid"].to_list(),
            "retired": merge[merge["_merge"] == "left_only"]["nid_old"].to_list(),
            "modified": list(),
            "confirmed": merge[merge["_merge"] == "both"]
        }

        # Recover old nids for confirmed and modified nid groups via uuid index.
        # Merge uuids onto recovery dataframe.
        recovery = classified_nids["confirmed"].merge(roadseg[["nid", "uuid"]], how="left", on="nid")\
            .drop_duplicates(subset="nid", keep="first")
        recovery.index = recovery["uuid"]

        # Filter invalid nids from old data.
        recovery = recovery[self.get_valid_ids(recovery["nid_old"])]

        # Recover old nids.
        if len(recovery):
            self.roadseg.loc[recovery.index, "nid"] = recovery["nid_old"]

        # Store results.
        self.dframes["roadseg"].loc[self.roadseg.index, "nid"] = self.roadseg["nid"].copy(deep=True)

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
        classified_nids["modified"] = classified_nids["confirmed"][flags.values]["nid"].to_list()
        classified_nids["confirmed"] = classified_nids["confirmed"][~flags.values]["nid"].to_list()

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

            # Identify maximum roadseg node distance (length between adjacent nodes on a line).
            max_len = np.vectorize(lambda geom: max(map(
                lambda pts: math.hypot(pts[0][0] - pts[1][0], pts[0][1] - pts[1][1]),
                zip(geom.coords[:-1], geom.coords[1:]))))\
                (roadseg["geometry"]).max()

            # Generate roadseg kdtree.
            roadseg_tree = cKDTree(np.concatenate(roadseg["geometry"].map(attrgetter("coords")).to_numpy()))

            # Compile an index-lookup dict for each coordinate associated with each roadseg record.
            roadseg_pt_indexes = np.concatenate([[index] * count for index, count in
                                                 roadseg["geometry"].map(lambda geom: len(geom.coords)).iteritems()])
            roadseg_lookup = pd.Series(roadseg_pt_indexes, index=range(0, roadseg_tree.n)).to_dict()

            # Compile an index-lookup dict for each full roadseg geometry and nid record.
            roadseg_geometry_lookup = roadseg["geometry"].to_dict()
            roadseg_nid_lookup = roadseg["nid"].to_dict()

        # Iterate dataframes, if available.
        for table in tables:

            # Copy and filter dataframe.
            df = self.dframes[table][["roadnid", "geometry"]].copy(deep=True)

            # Compile indexes of all roadseg points within max_len distance.
            roadseg_pt_indexes = df["geometry"].map(lambda geom: roadseg_tree.query_ball_point(geom, r=max_len))

            # Retrieve associated record indexes for each point index.
            df["roadseg_idxs"] = roadseg_pt_indexes.map(lambda idxs: list(set(itemgetter(*idxs)(roadseg_lookup))))

            # Retrieve associated record geometries for each record index.
            df["roadsegs"] = df["roadseg_idxs"].map(lambda idxs: itemgetter(*idxs)(roadseg_geometry_lookup))
            df["roadsegs"] = df["roadsegs"].map(lambda vals: vals if isinstance(vals, tuple) else (vals,))

            # Retrieve the local roadsegs index of the nearest roadsegs geometry.
            df["nearest_local_idx"] = np.vectorize(
                lambda pt, roads: min(enumerate(map(lambda road: road.distance(pt), roads)), key=itemgetter(1))[0])\
                (df["geometry"], df["roadsegs"])

            # Retrieve the associated roadseg record index from the local index.
            df["nearest_roadseg_idx"] = np.vectorize(
                lambda local_idx, roadseg_idxs: itemgetter(local_idx)(roadseg_idxs))\
                (df["nearest_local_idx"], df["roadseg_idxs"])

            # Retrieve the nid associated with the roadseg index.
            # Store results.
            self.dframes[table]["roadnid"] = df["nearest_roadseg_idx"].map(
                lambda roadseg_idx: itemgetter(roadseg_idx)(roadseg_nid_lookup)).copy(deep=True)

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.roadseg_gen_full()
        self.roadseg_gen_nids()
        self.roadseg_recover_and_classify_nids()
        self.roadseg_update_linkages()
        self.recover_and_classify_nids()
        self.gen_and_recover_structids()
        self.export_change_logs()
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
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)

if __name__ == "__main__":
    main()
