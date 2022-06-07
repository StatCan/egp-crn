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
from shapely.geometry import Point
from shapely.ops import polygonize, unary_union
from tabulate import tabulate

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


class CRNMeshblockValidation:
    """Defines the CRN meshblock validation class."""

    def __init__(self, source: str) -> None:
        """
        Initializes the CRN class.

        :param str source: abbreviation for the source province / territory.
        """

        self.source = source
        self.layer = f"nrn_bo_{source}"
        self.id = "segment_id"
        self.bo_id = "ngd_uid"
        self.src = Path(filepath.parents[2] / "data/egp_data.gpkg")
        self.src_restore = Path(filepath.parents[2] / "data/nrn_bo_restore.gpkg")
        self.errors = dict()
        self.export = dict()

        self.meshblock_ = None
        self._meshblock_input = None
        self.meshblock_progress = {k: 0 for k in ("Valid", "Invalid", "Excluded")}
        self._crn_bos_nodes = None
        self._crn_roads_nodes_lookup = dict()
        self._crn_bos_nodes_lookup = dict()
        self._deadends = set()

        # BO integration flag.
        self._integrated = None

        # Configure source path and layer name.
        for src in (self.src, self.src_restore):
            if src.exists():
                if self.layer not in set(fiona.listlayers(src)):
                    logger.exception(f"Layer \"{self.layer}\" not found within source: \"{src}\".")
                    sys.exit(1)
            else:
                logger.exception(f"Source not found: \"{src}\".")
                sys.exit(1)

        # Load source data.
        logger.info(f"Loading source data: {self.src}|layer={self.layer}.")
        self.crn = gpd.read_file(self.src, layer=self.layer)
        logger.info("Successfully loaded source data.")

        # Load source restoration data.
        logger.info(f"Loading source restoration data: {self.src_restore}|layer={self.layer}.")
        self.crn_restore = gpd.read_file(self.src_restore, layer=self.layer)
        logger.info("Successfully loaded source restoration data.")

        # Standardize data and snap nodes.
        self.crn = helpers.standardize(self.crn)
        self.crn = helpers.snap_nodes(self.crn)

        # Separate crn bos and roads.
        self.crn_roads = self.crn.loc[(self.crn["segment_type"] == 1) |
                                      (self.crn["segment_type"].isna())].copy(deep=True)
        self.crn_bos = self.crn.loc[self.crn["segment_type"] == 3].copy(deep=True)

        logger.info("Configuring validations.")

        # Define validation.
        # Note: List validations in order if execution order matters.
        self.validations = {
            100: self.connectivity,
            101: self.connectivity_crn_proximity,
            102: self.connectivity_bo_missing,
            200: self.meshblock,
            201: self.meshblock_representation_deadend,
            202: self.meshblock_representation_non_deadend
        }

        # Define thresholds.
        self._bo_road_prox = 5

    def __call__(self) -> None:
        """Executes the CRN class."""

        self._validate()
        self._write_errors()

        # Export required datasets.
        for layer, df in {self.layer: self.crn, **self.export}.items():
            helpers.export(df, dst=self.src, name=layer)

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
            logger.exception("Unable to apply validations.")
            logger.exception(e)
            sys.exit(1)

    def _write_errors(self) -> None:
        """Write error flags returned by validations to DataFrame columns."""

        logger.info(f"Writing error flags to dataset: \"{self.layer}\".")

        # Quantify errors.
        identifiers = list(chain.from_iterable(self.errors.values()))
        total_records = len(identifiers)
        total_unique_records = len(set(identifiers))

        # Iterate and write errors to DataFrame.
        for code, vals in sorted(self.errors.items()):

            self.crn[f"v{code}"] = self.crn[self.id].isin(vals).astype(int)

        logger.info(f"Total records flagged by validations: {total_records:,d}.")
        logger.info(f"Total unique records flagged by validations: {total_unique_records:,d}.")

        # Meshblock creation progress tracker.

        # Populate and log progress tracker with total meshblock input, excluded, and flagged record counts.
        self.meshblock_progress["Valid"] = len(self._meshblock_input) - self.meshblock_progress["Invalid"]
        self.meshblock_progress["Excluded"] = len(self.crn) - len(self._meshblock_input)

        # Log progress.
        table = tabulate([[k, f"{v:,}"] for k, v in self.meshblock_progress.items()],
                         headers=["Meshblock Input Arcs", "Count"], tablefmt="rst", colalign=("left", "right"))
        logger.info("\n" + table)

    def connectivity(self) -> set:
        """
        Validation: All BOs must have nodal connections to other arcs.
        Note: This method exists to generate the dependant variables for various connectivity validations and is not
        intended to produce error logs itself.

        :return set: placeholder set based on standard validations. For this method, it will be empty.
        """

        errors = set()

        # Extract nodes.
        self.crn_roads["nodes"] = self.crn_roads["geometry"].map(
            lambda g: tuple(set(itemgetter(0, -1)(attrgetter("coords")(g)))))
        self.crn_bos["nodes"] = self.crn_bos["geometry"].map(
            lambda g: tuple(set(itemgetter(0, -1)(attrgetter("coords")(g)))))

        # Explode nodes collections and group identifiers based on shared nodes.
        crn_roads_nodes_exp = self.crn_roads["nodes"].explode()
        self._crn_roads_nodes_lookup = dict(helpers.groupby_to_list(
            pd.DataFrame({"node": crn_roads_nodes_exp.values, self.id: crn_roads_nodes_exp.index}),
            group_field="node", list_field=self.id).map(tuple))

        crn_bos_nodes_exp = self.crn_bos["nodes"].explode()
        self._crn_bos_nodes_lookup = dict(helpers.groupby_to_list(
            pd.DataFrame({"node": crn_bos_nodes_exp.values, self.id: crn_bos_nodes_exp.index}),
            group_field="node", list_field=self.id).map(tuple))

        # Explode BO node collections to allow for individual node validation.
        self._crn_bos_nodes = self.crn_bos["nodes"].explode().copy(deep=True)

        # Flag BO nodes connected to an crn road node.
        self._integrated = self._crn_bos_nodes.map(lambda node: node in self._crn_roads_nodes_lookup)

        return errors

    def connectivity_bo_missing(self) -> set:
        """
        Validates: Untouchable BO identifier is missing.

        :return set: set containing identifiers of erroneous records.
        """

        errors = set()

        # Compile untouchable BO identifiers.
        untouchable_ids = set(self.crn_restore.loc[self.crn_restore["boundary"] == 1, self.bo_id])

        # Compile missing untouchable BO identifiers.
        missing_ids = untouchable_ids - set(self.crn[self.bo_id])

        # Compile error logs.
        if len(missing_ids):
            errors.update(missing_ids)

            # Export missing BOs for reference.
            bos_df = self.crn_restore.loc[self.crn_restore[self.bo_id].isin(missing_ids)]
            self.export[f"{self.layer}_bo_missing"] = bos_df.copy(deep=True)

        return errors

    def connectivity_crn_proximity(self) -> set:
        """
        Validates: Unintegrated BO node is <= 5 meters from a CRN road (entire arc).

        :return set: set containing identifiers of erroneous records.
        """

        errors = set()

        # Compile unintegrated BO nodes.
        unintegrated_bo_nodes = pd.Series(tuple(set(self._crn_bos_nodes.loc[~self._integrated])))

        # Generate simplified node buffers with distance tolerance.
        node_buffers = unintegrated_bo_nodes.map(lambda node: Point(node).buffer(self._bo_road_prox, resolution=5))

        # Query crn roads which intersect each node buffer.
        node_intersects = node_buffers.map(
            lambda buffer: set(self.crn_roads.sindex.query(buffer, predicate="intersects")))

        # Filter unintegrated bo nodes to those with buffers with one or more intersecting crn roads.
        unintegrated_bo_nodes = unintegrated_bo_nodes.loc[node_intersects.map(len) >= 1]
        if len(unintegrated_bo_nodes):

            # Compile identifiers of arcs for resulting BO nodes.
            vals = set(chain.from_iterable(unintegrated_bo_nodes.map(self._crn_bos_nodes_lookup).values))

            # Compile error logs.
            errors.update(vals)

        return errors

    def meshblock(self) -> set:
        """
        Validates: Generate meshblock from LineStrings.
        Note: This method exists to generate the dependant variables for various meshblock validations and is not
        intended to produce error logs itself.

        :return set: placeholder set based on standard validations. For this method, it will be empty.
        """

        errors = set()

        # Compile indexes of deadend arcs.
        nodes = self.crn["geometry"].map(lambda g: itemgetter(0, -1)(attrgetter("coords")(g))).explode()
        _deadends = nodes.loc[~nodes.duplicated(keep=False)]
        self._deadends = set(_deadends.index)

        # Export deadends for reference.
        if len(_deadends):

            pts_df = gpd.GeoDataFrame(geometry=list(map(Point, set(_deadends))), crs=self.crn.crs)
            self.errors[f"{self.layer}_deadends"] = pts_df.copy(deep=True)

        # Configure meshblock input (all non-deadend and non-ferry arcs).
        self._meshblock_input = self.crn.loc[(~self.crn.index.isin(self._deadends)) &
                                             (self.crn["segment_type"] != 2)].copy(deep=True)

        # Generate meshblock.
        self.meshblock_ = gpd.GeoDataFrame(
            geometry=list(polygonize(unary_union(self._meshblock_input["geometry"].to_list()))),
            crs=self._meshblock_input.crs)
        self.export[f"{self.layer}_meshblock"] = self.meshblock_.copy(deep=True)

        return errors

    def meshblock_representation_deadend(self) -> set:
        """
        All deadend arcs (excluding ferries) must be completely within 1 meshblock polygon.

        :return set: set containing identifiers of erroneous records.
        """

        errors = set()

        # Query meshblock polygons which contain each deadend arc.
        within = self.crn.loc[(self.crn.index.isin(self._deadends)) & (self.crn["segment_type"] != 2), "geometry"]\
            .map(lambda g: set(self.meshblock_.sindex.query(g, predicate="within")))

        # Flag arcs which are not completely within one polygon.
        flag = within.map(len) != 1

        # Compile error logs.
        if sum(flag):
            errors.update(set(within.loc[flag].index))

            # Update invalid count for progress tracker.
            self.meshblock_progress["Invalid"] += sum(flag)

        return errors

    def meshblock_representation_non_deadend(self) -> set:
        """
        Validates: All non-deadend arcs (excluding ferries) must form a meshblock polygon.

        :return set: set containing identifiers of erroneous records.
        """

        errors = set()

        # Extract boundary LineStrings from meshblock Polygons.
        meshblock_boundaries = self.meshblock_.boundary

        # Query meshblock polygons which cover each arc.
        covered_by = self.crn_bos.loc[self.crn_bos["bo_new"] != 1, "geometry"].map(
            lambda g: set(meshblock_boundaries.sindex.query(g, predicate="covered_by")))

        # Flag arcs which do not form a polygon.
        flag = covered_by.map(len) == 0

        # Compile error logs.
        if sum(flag):
            errors.update(set(covered_by.loc[flag].index))

            # Update invalid count for progress tracker.
            self.meshblock_progress["Invalid"] += sum(flag)

        return errors


@click.command()
@click.argument("source", type=click.Choice(helpers.load_yaml("../config.yaml")["sources"], False))
def main(source: str) -> None:
    """
    Instantiates and executes the CRN class.

    :param str source: abbreviation for the source province / territory.
    """

    try:

        with helpers.Timer():
            crn = CRNMeshblockValidation(source)
            crn()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
