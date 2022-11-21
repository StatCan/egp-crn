import click
import fiona
import geopandas as gpd
import logging
import math
import pandas as pd
import sys
from copy import deepcopy
from itertools import chain, compress, tee
from operator import attrgetter, itemgetter
from pathlib import Path
from shapely.geometry import MultiPoint, Point
from typing import List, Tuple

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


def ordered_pairs(coords: Tuple[tuple, ...]) -> List[Tuple[tuple, tuple]]:
    """
    Creates an ordered sequence of adjacent coordinate pairs, sorted.

    \b
    :param Tuple[tuple, ...] coords: tuple of coordinate tuples.
    :return List[Tuple[tuple, tuple]]: ordered sequence of coordinate pair tuples.
    """

    coords_1, coords_2 = tee(coords)
    next(coords_2, None)

    return sorted(zip(coords_1, coords_2))


class CRNTopologyValidation:
    """Defines the CRN topology validation class."""

    def __init__(self, source: str) -> None:
        """
        Initializes the CRN class.

        \b
        :param str source: code for the source region (working area).
        """

        self.source = source
        self.layer = f"crn_{source}"
        self.id = "segment_id"
        self.src = Path(filepath.parents[2] / "data/crn.gpkg")
        self.dst = Path(filepath.parents[2] / "data/crn.gpkg")
        self.flag_new_gpkg = False
        self.errors = dict()
        self.export = {
            f"{self.source}_cluster_tolerance": None
        }

        # Configure src / dst paths and layer name.
        if self.dst.exists():
            if self.layer not in set(fiona.listlayers(self.dst)):
                self.src = helpers.load_yaml("../config.yaml")["filepaths"]["crn"]
        else:
            helpers.create_gpkg(self.dst)
            self.flag_new_gpkg = True
            self.src = helpers.load_yaml("../config.yaml")["filepaths"]["crn"]

        # Load source data.
        logger.info(f"Loading source data: {self.src}|layer={self.layer}.")
        self.crn = gpd.read_file(self.src, layer=self.layer)
        logger.info("Successfully loaded source data.")

        # Standardize data.
        self.crn = helpers.standardize(self.crn)

        # Create subset dataframe of exclusively roads.
        self.crn_roads = self.crn.loc[self.crn["segment_type"] == 1].copy(deep=True)

        # Generate reusable geometry variables.
        self._gen_reusable_variables()

        logger.info("Configuring validations.")

        # Define validation.
        # Note: List validations in order if execution order matters.
        self.validations = {
            303: self.connectivity_segmentation,
            101: self.construction_singlepart,
            102: self.construction_min_length,
            103: self.construction_simple,
            104: self.construction_cluster_tolerance,
            201: self.duplication_duplicated,
            202: self.duplication_overlap,
            301: self.connectivity_node_intersection,
            302: self.connectivity_min_distance
        }

        # Define validation thresholds.
        self._min_len = 3
        self._min_dist = 5
        self._min_cluster_dist = 0.01

    def __call__(self) -> None:
        """Executes the CRN class."""

        self._validate()
        self._write_errors()

        # Export required datasets.
        if not self.flag_new_gpkg:
            helpers.delete_layers(dst=self.dst, layers=self.export.keys())
        for layer, df in {self.layer: self.crn, **self.export}.items():
            if isinstance(df, pd.DataFrame):
                helpers.export(df, dst=self.dst, name=layer)

    def _gen_reusable_variables(self) -> None:
        """Generates computationally intensive, reusable geometry attributes."""

        logger.info("Generating reusable geometry attributes.")

        # Generate computationally intensive geometry attributes as new columns.
        self.crn_roads["pts_tuple"] = self.crn_roads["geometry"].map(attrgetter("coords")).map(tuple)
        self.crn_roads["pt_start"] = self.crn_roads["pts_tuple"].map(itemgetter(0))
        self.crn_roads["pt_end"] = self.crn_roads["pts_tuple"].map(itemgetter(-1))
        self.crn_roads["pts_ordered_pairs"] = self.crn_roads["pts_tuple"].map(ordered_pairs)

        # Generate computationally intensive lookups.
        pts = self.crn_roads["pts_tuple"].explode()
        pts_df = pd.DataFrame({"pt": pts.values, self.id: pts.index})
        self.pts_id_lookup = pts_df.groupby(by="pt", axis=0, as_index=True)[self.id].agg(set).to_dict()
        self.idx_id_lookup = dict(zip(range(len(self.crn_roads)), self.crn_roads.index))

    def _validate(self) -> None:
        """Executes validations against the CRN dataset."""

        logger.info("Applying validations.")

        try:

            # Iterate validations.
            for code, func in self.validations.items():
                logger.info(f"Applying validation {code}: \"{func.__name__}\".")

                # Execute validation and store results.
                self.errors[code] = deepcopy(func())

        except (KeyError, SyntaxError, ValueError) as e:
            logger.exception(f"Unable to apply validations.")
            logger.exception(e)
            sys.exit(1)

    def _write_errors(self) -> None:
        """Write error flags returned by validations to DataFrame columns."""

        logger.info(f"Writing error flags to dataset \"{self.layer}\".")

        # Quantify errors.
        identifiers = list(chain.from_iterable(self.errors.values()))
        total_records = len(identifiers)
        total_unique_records = len(set(identifiers))

        # Iterate and write errors to DataFrame.
        for code, vals in sorted(self.errors.items()):
            if len(vals):
                self.crn[f"v{code}"] = self.crn[self.id].isin(vals).astype(int)

        logger.info(f"Total records flagged by validations: {total_records:,d}.")
        logger.info(f"Total unique records flagged by validations: {total_unique_records:,d}.")

    def connectivity_min_distance(self) -> set:
        """
        Validation: Arcs must be >= 5 meters from each other, excluding connected arcs (i.e. no dangles).

        \b
        :return set: set containing identifiers of erroneous records.
        """

        errors = set()

        # Compile all non-duplicated nodes (dead ends) as a DataFrame.
        pts = self.crn_roads["pt_start"].append(self.crn_roads["pt_end"])
        deadends = pts.loc[~pts.duplicated(keep=False)]
        deadends = pd.DataFrame({"pt": deadends.values, self.id: deadends.index})

        # Generate simplified node buffers with distance tolerance.
        deadends["buffer"] = deadends["pt"].map(lambda pt: Point(pt).buffer(self._min_dist, resolution=5))

        # Query arcs which intersect each dead end buffer.
        deadends["intersects"] = deadends["buffer"].map(
            lambda buffer: set(self.crn_roads.sindex.query(buffer, predicate="intersects")))

        # Flag dead ends which have buffers with one or more intersecting arcs.
        deadends = deadends.loc[deadends["intersects"].map(len) > 1]
        if len(deadends):

            # Aggregate deadends to their source features.
            # Note: source features will exist twice if both nodes are deadends; these results will be aggregated.
            deadends_agg = deadends.groupby(by=self.id, axis=0, as_index=True)["intersects"].agg(tuple)\
                .map(chain.from_iterable).map(set).to_dict()
            deadends["intersects"] = deadends[self.id].map(deadends_agg)
            deadends.drop_duplicates(subset=self.id, inplace=True)

            # Compile identifiers corresponding to each 'intersects' index.
            deadends["intersects"] = deadends["intersects"].map(lambda idxs: set(itemgetter(*idxs)(self.idx_id_lookup)))

            # Compile identifiers containing either of the source geometry nodes.
            deadends["connected"] = deadends[self.id].map(
                lambda identifier: set(chain.from_iterable(
                    itemgetter(node)(self.pts_id_lookup) for node in itemgetter(0, -1)(
                        itemgetter(identifier)(self.crn_roads["pts_tuple"]))
                )))

            # Subtract identifiers of connected features from buffer-intersecting features.
            deadends["disconnected"] = deadends["intersects"] - deadends["connected"]

            # Filter to those results with disconnected arcs.
            flag = deadends["disconnected"].map(len) > 0
            if sum(flag):

                # Remove duplicated results.
                deadends = deadends.loc[flag]
                deadends["ids"] = deadends[[self.id, "disconnected"]].apply(
                    lambda row: tuple({row[0], *row[1]}), axis=1)
                deadends.drop_duplicates(subset="ids", keep="first", inplace=True)

                # Compile errors.
                errors.update(chain.from_iterable(deadends["ids"]))

        return errors

    def connectivity_node_intersection(self) -> set:
        """
        Validates: Arcs must only connect at endpoints (nodes).

        \b
        :return set: set containing identifiers of erroneous records.
        """

        errors = set()

        # Compile nodes.
        nodes = set(self.crn_roads["pt_start"].append(self.crn_roads["pt_end"]))

        # Compile interior vertices (non-nodes).
        # Note: only arcs with > 2 vertices are used.
        non_nodes = set(self.crn_roads.loc[self.crn_roads["pts_tuple"].map(len) > 2, "pts_tuple"]
                        .map(lambda pts: set(pts[1:-1])).explode())

        # Compile invalid vertices.
        invalid_pts = nodes.intersection(non_nodes)

        # Filter invalid vertices to those with multiple connected features.
        invalid_pts = set(compress(invalid_pts,
                                   map(lambda pt: len(itemgetter(pt)(self.pts_id_lookup)) > 1, invalid_pts)))
        if len(invalid_pts):

            # Filter arcs to those with an invalid vertex.
            invalid_ids = set(chain.from_iterable(map(lambda pt: itemgetter(pt)(self.pts_id_lookup), invalid_pts)))
            crn_roads = self.crn_roads.loc[self.crn_roads.index.isin(invalid_ids)]

            # Flag invalid arcs where the invalid vertex is a non-node.
            flag = crn_roads["pts_tuple"].map(lambda pts: len(set(pts[1:-1]).intersection(invalid_pts))) > 0
            if sum(flag):

                # Compile errors.
                errors.update(set(crn_roads.loc[flag].index))

        return errors

    def connectivity_segmentation(self) -> set:
        """
        Validates: Arcs must not cross (i.e. must be segmented at each intersection).

        \b
        :return set: set containing identifiers of erroneous records.
        """

        errors = set()

        # Query arcs which cross each arc.
        crosses = self.crn_roads["geometry"].map(lambda g: set(self.crn_roads.sindex.query(g, predicate="crosses")))

        # Flag arcs which have one or more crossing arcs.
        flag = crosses.map(len) > 0
        if sum(flag):

            # Compile errors.
            errors.update(set(self.crn_roads.loc[flag].index))

        return errors

    def construction_cluster_tolerance(self) -> set:
        """
        Validates: Arcs must have >= 1x10-2 (0.01) meters distance between adjacent vertices (cluster tolerance).

        \b
        :return set: set containing identifiers of erroneous records.
        """

        errors = set()

        # Filter arcs to those with > 2 vertices.
        crn_roads = self.crn_roads.loc[self.crn_roads["pts_tuple"].map(len) > 2]
        if len(crn_roads):

            # Explode arc coordinate pairs and calculate distances.
            coord_pairs = crn_roads["pts_ordered_pairs"].explode()
            coord_dist = coord_pairs.map(lambda pair: math.dist(*pair))

            # Flag pairs with distances that are too small.
            flag = coord_dist < self._min_cluster_dist
            if sum(flag):

                # Export invalid pairs as MultiPoint geometries.
                pts = coord_pairs.loc[flag].map(MultiPoint)
                pts_df = gpd.GeoDataFrame({self.id: pts.index.values}, geometry=[*pts], crs=self.crn_roads.crs)
                self.export[f"{self.source}_cluster_tolerance"] = pts_df.copy(deep=True)

                # Compile errors.
                errors.update(set(coord_pairs.loc[flag].index))

        return errors

    def construction_min_length(self) -> set:
        """
        Validates: Arcs must be >= 3 meters in length, except structures (e.g. Bridges).

        \b
        :return set: set containing identifiers of erroneous records.
        """

        errors = set()

        # Flag arcs which are too short.
        flag = self.crn_roads.length < self._min_len
        if sum(flag):
            
            # Flag isolated structures (structures not connected to another structure).
            
            # Compile structures.
            structures = self.crn_roads.loc[~self.crn_roads["structure_type"].isin({"Unknown", "None"})]
            
            # Compile duplicated structure nodes.
            structure_nodes = pd.Series(structures["pt_start"].append(structures["pt_end"]))
            structure_nodes_dups = set(structure_nodes.loc[structure_nodes.duplicated(keep=False)])
            
            # Flag isolated structures.
            isolated_structure_index = set(structures.loc[~((structures["pt_start"].isin(structure_nodes_dups)) |
                                                            (structures["pt_end"].isin(structure_nodes_dups)))].index)
            isolated_structure_flag = self.crn_roads.index.isin(isolated_structure_index)
            
            # Modify flag to exclude isolated structures.
            flag = (flag & (~isolated_structure_flag))
            if sum(flag):

                # Compile errors.
                errors.update(set(self.crn_roads.loc[flag].index))

        return errors

    def construction_simple(self) -> set:
        """
        Validates: Arcs must be simple (i.e. must not self-overlap, self-cross, nor touch their interior).

        \b
        :return set: set containing identifiers of erroneous records.
        """

        errors = set()

        # Flag complex (non-simple) geometries.
        flag = ~self.crn_roads.is_simple
        if sum(flag):

            # Compile errors.
            errors.update(set(self.crn_roads.loc[flag].index))

        return errors

    def construction_singlepart(self) -> set:
        """
        Validates: Arcs must be single part (i.e. 'LineString').

        \b
        :return set: set containing identifiers of erroneous records.
        """

        errors = set()

        # Flag non-LineStrings.
        flag = self.crn_roads.geom_type != "LineString"
        if sum(flag):

            # Compile errors.
            errors.update(set(self.crn_roads.loc[flag].index))

        return errors

    def duplication_duplicated(self) -> set:
        """
        Validates: Arcs must not be duplicated.

        \b
        :return set: set containing identifiers of erroneous records.
        """

        errors = set()

        # Filter arcs to those with duplicated lengths.
        crn_roads = self.crn_roads.loc[self.crn_roads.length.duplicated(keep=False)]
        if len(crn_roads):

            # Filter arcs to those with duplicated nodes.
            crn_roads = crn_roads.loc[
                crn_roads[["pt_start", "pt_end"]].agg(set, axis=1).map(tuple).duplicated(keep=False)]

            # Flag duplicated geometries.
            dups = crn_roads.loc[crn_roads["geometry"].map(
                lambda g1: crn_roads["geometry"].map(lambda g2: g1.equals(g2)).sum() > 1)]
            if len(dups):

                # Compile errors.
                errors.update(set(dups.index))

        return errors

    def duplication_overlap(self) -> set:
        """
        Validates: Arcs must not overlap (i.e. contain duplicated adjacent vertices).

        \b
        :return set: set containing identifiers of erroneous records.
        """

        errors = set()

        # Query arcs which overlap each arc.
        overlaps = self.crn_roads["geometry"].map(lambda g: set(self.crn_roads.sindex.query(g, predicate="overlaps")))

        # Flag arcs which have one or more overlapping arcs.
        flag = overlaps.map(len) > 0

        # Compile errors.
        if sum(flag):
            errors.update(set(overlaps.loc[flag].index))

        return errors


@click.command()
@click.argument("source", type=click.Choice(helpers.load_yaml("../config.yaml")["sources"], False))
def main(source: str) -> None:
    """
    Instantiates and executes the CRN class.

    \b
    :param str source: code for the source region (working area).
    """

    try:

        with helpers.Timer():
            crn = CRNTopologyValidation(source)
            crn()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
