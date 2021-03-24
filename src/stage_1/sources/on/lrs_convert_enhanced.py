import click
import fiona
import geopandas as gpd
import logging
import pandas as pd
import sys
from collections import Counter
from itertools import accumulate, chain
from operator import attrgetter, itemgetter
from pathlib import Path
from shapely.geometry import LineString, MultiLineString, Point
from typing import List, Union

filepath = Path(__file__).resolve()
sys.path.insert(1, str(filepath.parents[3]))
import helpers


# Set logger.
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)


class LRS:
    """Class to convert ORN data from Linear Reference System (LRS) to GeoPackage."""

    def __init__(self, src: Union[Path, str], dst: Union[Path, str]) -> None:
        """
        Initializes the LRS conversion class.

        :param Union[Path, str] src: source path.
        :param Union[Path, str] dst: destination path.
        """

        self.nrn_datasets = dict()
        self.src_datasets = dict()
        self.base_dataset = "orn_road_net_element"
        self.geometry_dataset = "orn_road_net_element"
        self.event_measurement_fields = {"from": "from_measure", "to": "to_measure"}
        self.point_datasets = {"orn_blocked_passage", "orn_toll_point"}
        self.point_event_measurement_field = "at_measure"

        # Dataset import specifications.
        self.schema = {
            "orn_address_info": {
                "fields": ["orn_road_net_element_id", "from_measure", "to_measure", "first_house_number",
                           "last_house_number", "house_number_structure", "street_side", "effective_datetime"],
                "query": None,
                "output_fields": ["first_house_number", "last_house_number", "house_number_structure",
                                  "effective_datetime"]
            },
            "orn_blocked_passage": {
                "fields": ["orn_road_net_element_id", "at_measure", "blocked_passage_type", "agency_name",
                           "effective_datetime"],
                "query": None,
                "output_fields": ["blocked_passage_type", "agency_name", "effective_datetime", "geometry"]
            },
            "orn_jurisdiction": {
                "fields": ["orn_road_net_element_id", "from_measure", "to_measure", "street_side", "jurisdiction",
                           "effective_datetime"],
                "query": "street_side != 'Left'",
                "output_fields": ["jurisdiction", "effective_datetime"]
            },
            "orn_number_of_lanes": {
                "fields": ["orn_road_net_element_id", "from_measure", "to_measure", "number_of_lanes",
                           "effective_datetime"],
                "query": None,
                "output_fields": ["number_of_lanes", "effective_datetime"]
            },
            "orn_official_street_name": {
                "fields": ["orn_road_net_element_id", "from_measure", "to_measure", "full_street_name",
                           "effective_datetime"],
                "query": None,
                "output_fields": ["full_street_name", "effective_datetime"]
            },
            "orn_road_class": {
                "fields": ["orn_road_net_element_id", "from_measure", "to_measure", "road_class", "effective_datetime"],
                "query": None,
                "output_fields": ["road_class", "effective_datetime"]
            },
            "orn_road_net_element": {
                "fields": ["ogf_id", "road_absolute_accuracy", "direction_of_traffic_flow", "exit_number",
                           "road_element_type", "acquisition_technique", "creation_date", "revision_date", "geometry"],
                "query": "road_element_type != 'VIRTUAL ROAD'",
                "output_fields": ["road_absolute_accuracy", "direction_of_traffic_flow", "exit_number",
                                  "road_element_type", "acquisition_technique", "creation_date", "revision_date",
                                  "geometry"]
            },
            "orn_road_net_element_source": {
                "fields": ["orn_road_net_element_id", "from_measure", "to_measure", "agency_name",
                           "effective_datetime"],
                "query": None,
                "output_fields": ["agency_name", "effective_datetime"]
            },
            "orn_road_surface": {
                "fields": ["orn_road_net_element_id", "from_measure", "to_measure", "pavement_status", "surface_type",
                           "effective_datetime"],
                "query": None,
                "output_fields": ["pavement_status", "surface_type", "effective_datetime"]
            },
            "orn_route_name": {
                "fields": ["orn_road_net_element_id", "from_measure", "to_measure", "route_name_english",
                           "route_name_french", "effective_datetime"],
                "query": None,
                "output_fields": ["route_name_english", "route_name_french", "effective_datetime"]
            },
            "orn_route_number": {
                "fields": ["orn_road_net_element_id", "from_measure", "to_measure", "route_number",
                           "effective_datetime"],
                "query": None,
                "output_fields": ["route_number", "effective_datetime"]
            },
            "orn_speed_limit": {
                "fields": ["orn_road_net_element_id", "from_measure", "to_measure", "speed_limit",
                           "effective_datetime"],
                "query": None,
                "output_fields": ["speed_limit", "effective_datetime"]
            },
            "orn_street_name_parsed": {
                "fields": ["full_street_name", "directional_prefix", "street_type_prefix", "street_name_body",
                           "street_type_suffix", "directional_suffix", "effective_datetime"],
                "query": None,
                "output_fields": ["directional_prefix", "street_type_prefix", "street_name_body", "street_type_suffix",
                                  "directional_suffix", "effective_datetime"]
            },
            "orn_structure": {
                "fields": ["orn_road_net_element_id", "from_measure", "to_measure", "structure_type",
                           "structure_name_english", "structure_name_french", "effective_datetime"],
                "query": None,
                "output_fields": ["structure_type", "structure_name_english", "structure_name_french",
                                  "effective_datetime"]
            },
            "orn_toll_point": {
                "fields": ["orn_road_net_element_id", "at_measure", "toll_point_type", "agency_name",
                           "effective_datetime"],
                "query": None,
                "output_fields": ["toll_point_type", "agency_name", "effective_datetime", "geometry"]
            }
        }

        # Connections between datasets to the main (base) dataset.
        self.structure = {
            "base": self.base_dataset,
            "connections": {
                "orn_road_net_element_id": ["orn_address_info", "orn_blocked_passage", "orn_jurisdiction",
                                            "orn_number_of_lanes", "orn_official_street_name", "orn_road_class",
                                            "orn_road_net_element_source", "orn_road_surface", "orn_route_name",
                                            "orn_route_number", "orn_speed_limit", "orn_structure", "orn_toll_point"]
            }
        }

        # Connections between non-main (base) datasets.
        self.structure_non_base = {
            "orn_official_street_name": {
                "stname_c": ["orn_street_name_parsed"]
            }
        }

        # Input dataset columns to be renamed upon import.
        self.rename = {
            "acquisition_technique": "acqtech",
            "agency_name": "provider",
            "blocked_passage_type": "blkpassty",
            "creation_date": "credate",
            "direction_of_traffic_flow": "trafficdir",
            "directional_prefix": "dirprefix",
            "directional_suffix": "dirsuffix",
            "effective_datetime": "revdate",
            "exit_number": "exitnbr",
            "first_house_number": "hnumf",
            "full_street_name": "stname_c",
            "house_number_structure": "hnumstr",
            "jurisdiction": "roadjuris",
            "last_house_number": "hnuml",
            "number_of_lanes": "nbrlanes",
            "ogf_id": "orn_road_net_element_id",
            "pavement_status": "pavstatus",
            "revision_date": "revdate",
            "road_absolute_accuracy": "accuracy",
            "road_class": "roadclass",
            "route_name_english": "rtenameen",
            "route_name_french": "rtenamefr",
            "route_number": "rtnumber",
            "speed_limit": "speed",
            "street_name_body": "namebody",
            "street_type_prefix": "strtypre",
            "street_type_suffix": "strtysuf",
            "structure_name_english": "strunameen",
            "structure_name_french": "strunamefr",
            "structure_type": "structtype",
            "surface_type": "pavsurf",
            "toll_point_type": "tollpttype"
        }

        # Define composite datasets (datasets to be split into multiple datasets).
        self.composite_datasets = {
            "orn_address_info": {
                    "successive_queries": False,
                    "new_datasets": [
                        {
                            "query": "street_side != 'Right'",
                            "dataset_name": "orn_address_info_left",
                            "rename_fields": {"hnumf": "l_hnumf", "hnuml": "l_hnuml", "hnumstr": "l_hnumstr"},
                            "output_fields": ["l_hnumf", "l_hnuml", "l_hnumstr", "effective_datetime"]
                        },
                        {
                            "query": "street_side != 'Left'",
                            "dataset_name": "orn_address_info_right",
                            "rename_fields": {"hnumf": "r_hnumf", "hnuml": "r_hnuml", "hnumstr": "r_hnumstr"},
                            "output_fields": ["r_hnumf", "r_hnuml", "r_hnumstr", "effective_datetime"]
                        }
                    ]
            },
            "orn_route_name": {
                "successive_queries": True,
                "new_datasets": [
                    {
                        "query": "(~orn_road_net_element_id.duplicated(keep=False)) or "
                                 "(~orn_road_net_element_id.duplicated(keep='first'))",
                        "dataset_name": "orn_route_name_1",
                        "rename_fields": {"rtenameen": "rtename1en"},
                        "output_fields": ["rtename1en", "effective_datetime"]
                    },
                    {
                        "query": "(~orn_road_net_element_id.duplicated(keep=False)) or "
                                 "(~orn_road_net_element_id.duplicated(keep='first'))",
                        "dataset_name": "orn_route_name_2",
                        "rename_fields": {"rtenameen": "rtename2en"},
                        "output_fields": ["rtename2en", "effective_datetime"]
                    },
                    {
                        "query": "(~orn_road_net_element_id.duplicated(keep=False)) or "
                                 "(~orn_road_net_element_id.duplicated(keep='first'))",
                        "dataset_name": "orn_route_name_3",
                        "rename_fields": {"rtenameen": "rtename3en"},
                        "output_fields": ["rtename3en", "effective_datetime"]
                    },
                    {
                        "query": "(~orn_road_net_element_id.duplicated(keep=False)) or "
                                 "(~orn_road_net_element_id.duplicated(keep='first'))",
                        "dataset_name": "orn_route_name_4",
                        "rename_fields": {"rtenameen": "rtename4en"},
                        "output_fields": ["rtename4en", "effective_datetime"]
                    }
                ]
            },
            "orn_route_number": {
                "successive_queries": True,
                "new_datasets": [
                    {
                        "query": "(~orn_road_net_element_id.duplicated(keep=False)) or "
                                 "(~orn_road_net_element_id.duplicated(keep='first'))",
                        "dataset_name": "orn_route_number_1",
                        "rename_fields": {"rtnumber": "rtnumber1"},
                        "output_fields": ["rtnumber1", "effective_datetime"]
                    },
                    {
                        "query": "(~orn_road_net_element_id.duplicated(keep=False)) or "
                                 "(~orn_road_net_element_id.duplicated(keep='first'))",
                        "dataset_name": "orn_route_number_2",
                        "rename_fields": {"rtnumber": "rtnumber2"},
                        "output_fields": ["rtnumber2", "effective_datetime"]
                    },
                    {
                        "query": "(~orn_road_net_element_id.duplicated(keep=False)) or "
                                 "(~orn_road_net_element_id.duplicated(keep='first'))",
                        "dataset_name": "orn_route_number_3",
                        "rename_fields": {"rtnumber": "rtnumber3"},
                        "output_fields": ["rtnumber3", "effective_datetime"]
                    },
                    {
                        "query": "(~orn_road_net_element_id.duplicated(keep=False)) or "
                                 "(~orn_road_net_element_id.duplicated(keep='first'))",
                        "dataset_name": "orn_route_number_4",
                        "rename_fields": {"rtnumber": "rtnumber4"},
                        "output_fields": ["rtnumber4", "effective_datetime"]
                    },
                    {
                        "query": "(~orn_road_net_element_id.duplicated(keep=False)) or "
                                 "(~orn_road_net_element_id.duplicated(keep='first'))",
                        "dataset_name": "orn_route_number_5",
                        "rename_fields": {"rtnumber": "rtnumber5"},
                        "output_fields": ["rtnumber5", "effective_datetime"]
                    }
                ]
            }
        }

        # Validate src.
        self.src = Path(src).resolve()
        if self.src.suffix != ".gdb":
            logger.exception(f"Invalid src input: {src}. Must be a File GeoDatabase.")
            sys.exit(1)

        # Validate dst.
        self.dst = Path(dst).resolve()
        if self.dst.suffix != ".gpkg":
            logger.exception(f"Invalid dst input: {dst}. Must be a GeoPackage.")
            sys.exit(1)
        if self.dst.exists():
            logger.exception(f"Invalid dst input: {dst}. File already exists.")

    def assemble_network_attribution(self) -> None:
        """Assembles all required attributes from the source datasets to the segmented road network."""

        def fetch_attr_index(df_sub: pd.DataFrame, con_id: Union[int, str], interval: pd.Interval) -> \
                Union[None, int]:
            """
            Fetches the DataFrame records which matche the connection ID and overlap the breakpoint interval.

            :param pd.DataFrame df_sub: DataFrame.
            :param Union[int, str] con_id: connection ID between df_sub and the base dataset.
            :param pd.Interval interval: Interval representing the breakpoints of an attribute.
            :return Union[None, int]: None or the index of the first DataFrame record which matches the connection ID
                and overlaps the breakpoint interval.
            """

            df_filter = df_sub.loc[df_sub[con_id_field] == con_id]
            indexes = df_filter.loc[df_filter["interval"].map(lambda intv: intv.overlaps(interval))].index
            return indexes[0] if len(indexes) else None

        # Assemble attributes from source datasets.
        logger.info(f"Assembling attributes from source datasets.")

        base = self.nrn_datasets["roadseg"].copy(deep=True)

        # Convert breakpoints to pandas intervals.
        base["interval"] = base["breakpts"].map(lambda vals: pd.Interval(*vals))

        # Iterate source datasets that are connected to the base dataset and have columns to be keep on output.
        for con_id_field, names in self.structure["connections"].items():
            for name in [n for n in names if self.schema[n]["output_fields"]]:

                logger.info(f"Assembling attributes from dataset: {name}.")

                df = self.src_datasets[name].copy(deep=True)

                # Compile required attributes with updated names. Add a suffix to columns already in base dataset.
                # Note: Underscore suffixes are applied to conflicting field names. For certain fields, such as dates,
                # it may be useful to keep multiple instances.
                cols_keep = list()
                for col in self.schema[name]["output_fields"]:
                    col = self.rename[col]
                    while col in base.columns:
                        col += "_"
                        df.rename(columns={col[:-1]: col}, inplace=True)
                    cols_keep.append(col)

                    # Add new column to base dataset.
                    base[col] = None

                # Handle singular (non-segmented) matches.
                # Flag base records and filter attributes dataframe to relevant records.
                # Note: duplicated(keep='first') ensures that one-to-many matches between the base and attribute dataset
                # will still have an attribute record to link to.
                flag_base_a = base[con_id_field].isin(set(df[con_id_field])) & \
                              ~base[con_id_field].duplicated(keep=False)
                df_sub = df.loc[(df[con_id_field].isin(set(base.loc[flag_base_a, con_id_field]))) &
                                (~df[con_id_field].duplicated(keep="first")), [con_id_field, *cols_keep]]

                # Update base dataset with attributes.
                base.loc[flag_base_a, cols_keep] = base.loc[flag_base_a, [con_id_field]].merge(
                    df_sub, how="left", on=con_id_field)[cols_keep].values

                # Handle plural (segmented) matches.
                flag_base_b = False
                if "breakpts" in df.columns:

                    # Flag base records and filter attributes dataframe to relevant records.
                    flag_base_b = base[con_id_field].isin(set(df[con_id_field])) & \
                                  base[con_id_field].duplicated(keep=False)
                    df_sub = df.loc[df[con_id_field].isin(set(base.loc[flag_base_b, con_id_field])),
                                    [con_id_field, "breakpts", *cols_keep]]

                    # Convert breakpoints to pandas intervals.
                    df_sub["interval"] = df_sub["breakpts"].map(lambda vals: pd.Interval(*vals))

                    # Fetch the indexes of the attribute dataset which correspond to the base dataset.
                    args = base.loc[flag_base_b, [con_id_field, "interval"]].apply(list, axis=1)
                    idx = args.map(lambda vals: fetch_attr_index(df_sub, *vals))
                    idx = idx.loc[~idx.isna()]

                    # Update base dataset with attributes by merging the base and attribute datasets.
                    flag_base_b = base.index.isin(set(idx.index))
                    base["idx"] = None
                    base.loc[flag_base_b, "idx"] = idx
                    base.loc[flag_base_b, cols_keep] = base.loc[flag_base_b, ["idx"]].merge(
                        df_sub, how="left", left_on="idx", right_index=True)[cols_keep].values

                # Overwrite non-modified records with Nones to reverse autocasting.
                flag_base = (flag_base_a | flag_base_b)
                base.loc[~flag_base, cols_keep] = None

        # Resolve conflicting attributes.
        # Note: dates are likely the only attributes which require conflict resolution.
        logger.info(f"Resolving conflicting attributes.")

        # Iterate and compile fields with potential conflicts.
        for field, params in {
            "credate": {"func": min, "isdate": True},
            "revdate": {"func": max, "isdate": True}
        }.items():

            cols = [col for col in base.columns if col.find(field) >= 0]

            # Convert date fields to datetime objects.
            if params["isdate"]:
                for col in cols:
                    base[col] = base[col].map(pd.to_datetime).dt.strftime("%Y%m%d")

            # Apply function to conflicting columns, if required.
            if len(cols) > 1:

                logger.info(f"Resolving conflicting attributes for: {field}.")

                # Resolve conflicts into a single attribute.
                base[field] = base[cols].apply(lambda row: params["func"]([v for v in row if not pd.isna(v)]), axis=1)

        # Remove excess fields (keep all defined output fields plus geometry, drop everything else).
        cols_keep = set(map(lambda col: self.rename[col], chain.from_iterable(
            props["output_fields"] for props in self.schema.values() if props["output_fields"]))).union({"geometry"})
        base.drop(columns=set(base.columns)-cols_keep, inplace=True)

        # Store result.
        self.nrn_datasets["roadseg"] = base.copy(deep=True)

    def assemble_non_base_linkages(self) -> None:
        """Assembles dataset linkages which are not against the base dataset."""

        logger.info(f"Assembling non-base dataset linkages.")

        # Iterate non-base linkages.
        for base_name in self.structure_non_base:
            base = self.src_datasets[base_name]

            # Iterate linked datasets.
            for con_id_field, linked_name in self.structure_non_base[base_name].items():

                logger.info(f"Assembling dataset linkage: {base_name} - {linked_name}")

                # Merge datasets.
                base = base.merge(self.src_datasets[linked_name], how="left", on=con_id_field)

                # Remove linked dataset.
                del self.src_datasets[linked_name]

            # Store merged results.
            self.src_datasets[base_name] = base.copy(deep=True)

    def assemble_segmented_network(self) -> None:
        """Assembles a segmented road network from the breakpoints (event measurements) of the source datasets."""

        def merge_breakpoints_endpoints(breakpts: List[Union[float, int]], geom: Union[LineString, MultiLineString]) \
                -> List[Union[float, int]]:
            """
            Reconfigures breakpts to include the endpoints of all LineStrings.

            :param List[Union[float, int]] breakpts: sequence of breakpts (event measurements).
            :param Union[LineString, MultiLineString] geom: geometry object which represents the breakpts.
            :return List[Union[float, int]]: sequence of breakpts (event measurements), modified to include the
                endpoints of all LineStrings.
            """

            # Compile cumulative geometry lengths as endpoints.
            endpts = [0, geom.length] if isinstance(geom, LineString) else \
                list(accumulate([0, *[g.length for g in geom]]))

            # Remove breakpoints which are <= 1 unit from an endpoint or outside of the geometry length range (zero to
            # max length). Endpoints include the start and end of every individual LineString in the geometry.
            breakpts = [breakpt for breakpt in breakpts if any(
                [(endpts[i] + 1) < breakpt < (endpts[i + 1] - 1) for i in range(len(endpts) - 1)])]

            # Return appended and sorted list of breakpoint and endpoints.
            return sorted(chain(breakpts, endpts))

        def segment_geometry(breakpts: List[Union[float, int]], geom: LineString) -> LineString:
            """
            Segments a LineString at a set of breakpoints. To increase splitting accuracy, breakpoints are snapped to
            pre-existing nodes in the geometry, where possible.

            :param List[Union[float, int]] breakpts: sequence of breakpts (event measurements).
            :param LineString geom: geometry object which represents the breakpts.
            :return LineString: LineString, segmented from the original geometry.
            """

            # Return entire geometry if breakpoints cover entire length.
            if breakpts[0] == 0 and round(breakpts[-1]) == round(geom.length):
                return geom

            # Segment geometry on breakpoints.
            else:

                # Extract coordinates (nodes) from geometry.
                nodes = list(attrgetter("coords")(geom))

                # Configure the index range for all nodes between breakpoints (using bisection search).
                # Note: due to decimal differences, the safe limits are used such that kept nodes will always include
                # one node which exists before and after the first and last breakpoints, respectively.
                low, high = 0, len(nodes)-1
                while abs(high - low) > 1:
                    mid = int(low + ((high - low) / 2))
                    if breakpts[0] > geom.project(Point(nodes[mid])):
                        low = mid
                    else:
                        high = mid
                from_idx = low

                low, high = from_idx, len(nodes)-1
                while abs(high - low) > 1:
                    mid = int(low + ((high - low) / 2))
                    if breakpts[-1] < geom.project(Point(nodes[mid])):
                        high = mid
                    else:
                        low = mid
                to_idx = high + 1

                # Compile nodes from indexes.
                nodes_keep = list(map(Point, nodes[from_idx: to_idx]))

                # Conditionally populate nodes with breakpoints if empty.
                if not len(nodes_keep):
                    nodes_keep = [geom.interpolate(breakpts[0]), geom.interpolate(breakpts[-1])]

                # Conditionally remove nodes more than 1 unit outside the breakpoint range and replace breakpoints with
                # nodes if those nodes are <= 1 unit from the breakpoint.
                else:
                    if abs(breakpts[0] - geom.project(nodes_keep[0])) > 1:
                        nodes_keep = nodes_keep[1:]
                        if abs(breakpts[0] - geom.project(nodes_keep[0])) > 1:
                            nodes_keep = [geom.interpolate(breakpts[0]), *nodes_keep]
                    if abs(breakpts[-1] - geom.project(nodes_keep[-1])) > 1:
                        nodes_keep = nodes_keep[:-1]
                        if abs(breakpts[-1] - geom.project(nodes_keep[-1])) > 1:
                            nodes_keep = [*nodes_keep, geom.interpolate(breakpts[-1])]

                return LineString(nodes_keep)

        logger.info("Assembling segmented network.")

        # Assemble base - geometry connection.
        logger.info(f"Assembling base - geometry connection: {self.base_dataset} - {self.geometry_dataset}.")

        # Assemble datasets if they are not the same.
        base = self.src_datasets[self.base_dataset].copy(deep=True)
        if self.base_dataset != self.geometry_dataset:
            base = gpd.GeoDataFrame(base.merge(self.src_datasets[self.geometry_dataset], how="left",
                                               on=self.get_con_id_field(self.geometry_dataset)))

        # Explode geometries to singlepart.
        base = helpers.explode_geometry(base)

        # Iterate datasets and assemble all event measurements for each base geometry.
        logger.info(f"Compiling all event measurements as breakpoints.")

        for name, df in self.src_datasets.items():
            if name not in {self.base_dataset, self.geometry_dataset} and {"from", "to"}.issubset(set(df.columns)):
                logger.info(f"Compiling breakpoints for dataset: {name}.")

                # Identify connection field.
                con_id_field = self.get_con_id_field(name)

                # Compile breakpoints as flattened list.
                df["breakpts"] = df[["from", "to"]].apply(list, axis=1)
                breakpts = helpers.groupby_to_list(df, con_id_field, "breakpts").map(chain.from_iterable).map(list)

                # Merge breakpoints with base dataset.
                breakpts.name = f"{name}_breakpts"
                base = base.merge(breakpts, how="left", left_on=con_id_field, right_index=True)

        # Reduce and sort breakpoints into flattened lists.
        logger.info(f"Reducing and sorting breakpoints.")

        breakpt_cols = [col for col in base.columns if col.endswith("_breakpts")]
        base["breakpts"] = base[breakpt_cols].apply(
            lambda row: chain.from_iterable(r for r in row if isinstance(r, list)), axis=1).map(set).map(sorted)

        # Remove extraneous columns.
        base.drop(columns=breakpt_cols, inplace=True)

        # Filter breakpoints which are too close together.
        logger.info(f"Filtering breakpoints which are too close together.")

        # Filter breakpoints by keeping only those which are more than 1 unit distance from the next breakpoint.
        flag = base["breakpts"].map(len) >= 2
        base.loc[flag, "breakpts"] = base.loc[flag, "breakpts"].map(
            lambda pts: [*[pt for index, pt in enumerate(pts[:-1]) if
                           abs(round(pt) - round(pts[index+1])) > 1], pts[-1]])

        # Add geometry start- and endpoints (collectively referred to as endpoints), for all constituent LineStrings.
        # Note: remove breakpoints which are within 1 unit distance from the endpoints.
        logger.info(f"Adding geometry endpoints to breakpoints.")

        args = base[["breakpts", "geometry"]].apply(list, axis=1)
        base["breakpts"] = args.map(lambda vals: merge_breakpoints_endpoints(*vals))

        # Split record geometries on breakpoints.
        logger.info(f"Splitting records on geometry breakpoints.")

        # Nest breakpoints into groups of 2.
        base["breakpts"] = base["breakpts"].map(lambda pts: [[pts[i], pts[i+1]] for i in range(len(pts)-1)])

        # Explode dataframe on breakpoints.
        # Note: must use pandas dataframe since geodataframe.explode is geometry based.
        base = gpd.GeoDataFrame(pd.DataFrame(base).explode("breakpts", ignore_index=True))

        # Extract geometry segment corresponding to breakpoints.
        # Nest geometry and breakpoints to use map.
        args = base[["breakpts", "geometry"]].apply(list, axis=1)
        base["geometry"] = args.map(lambda vals: segment_geometry(*vals))

        # Store result.
        self.nrn_datasets["roadseg"] = base.copy(deep=True)

    def clean_event_measurements(self) -> None:
        """
        Performs several cleanup operations on records based on event measurement:
        1. Simplifies event measurement field names to 'from' and 'to'.
        2. Swaps measurement order for records with invalid measurements (from >= to).
        3. Repairs gaps in event measurements along the same connected feature.
        4. Flags overlapping event measurements along the same connected feature.
        """

        logger.info("Cleaning event measurement fields.")

        # Iterate dataframes with event measurement fields.
        fields = self.event_measurement_fields

        for layer, df in self.src_datasets.items():
            if set(fields.values()).issubset(df.columns):

                logger.info(f"Cleaning event measurements for dataset: {layer}.")

                # Identify connection field.
                con_id_field = self.get_con_id_field(layer)

                # Convert measurements.
                logger.info("Converting event measurements.")

                df.rename(columns={fields["from"]: "from", fields["to"]: "to"}, inplace=True)

                # Swap measurement order for records with invalid event measurements.
                logger.info("Swapping measurement order for records with invalid event measurements.")

                flag = df["from"] > df["to"]
                df.loc[flag, ["from", "to"]] = df.loc[flag, ["to", "from"]].values
                logger.info(f"Swapped {sum(flag)} of {len(df)} records.")

                # Repair gaps in measurement ranges.
                logger.info("Repairing event measurement gaps.")

                # Iterate records with duplicated connection ids.
                update_count = 0
                dup_con_ids = set(df.loc[df[con_id_field].duplicated(keep=False), con_id_field])
                for con_id in dup_con_ids:
                    records = df.loc[df[con_id_field] == con_id]
                    from_min = records["from"].min()

                    # For any gaps (tolerance = 1 unit), reduce the 'from' measurement to the appropriate neighbouring
                    # 'to' measurement.
                    for index, from_value in records.loc[records["from"] != from_min, "from"].iteritems():
                        neighbour = records.loc[(records.index != index) & ((from_value - records["to"]).between(0, 1))]
                        if len(neighbour):

                            # Update record.
                            df.loc[index, "from"] = neighbour["to"].iloc[0]
                            update_count += 1

                logger.info(f"Repaired {update_count} event measurement gaps.")

                # Flag overlapping measurement ranges.
                logger.info("Identifying overlapping event measurement ranges.")

                # Iterate records with duplicated connection ids.
                for con_id in dup_con_ids:
                    overlap_flag = False

                    # Create intervals from event measurements.
                    intervals = df.loc[df[con_id_field] == con_id, ["from", "to"]].apply(
                        lambda row: pd.Interval(*row), axis=1).to_list()

                    # Flag connection id if overlapping intervals are detected.
                    for idx, i1 in enumerate(intervals):
                        for i2 in intervals[idx + 1:]:
                            if i1.overlaps(i2):
                                overlap_flag = True
                                break
                        if overlap_flag:
                            break

                    if overlap_flag:
                        logger.warning(f"Overlap detected: {con_id_field}={con_id}.")

                # Store results.
                self.src_datasets[layer] = df.copy(deep=True)

    def compile_source_datasets(self) -> None:
        """Loads raw source layers into (Geo)DataFrames."""

        logger.info(f"Compiling source datasets from: {self.src}.")

        # Compile layer names for lowercase lookup.
        layers_lower = {name.lower(): name for name in fiona.listlayers(self.src)}

        # Iterate LRS schema.
        for index, items in enumerate(self.schema.items()):

            layer, attr = itemgetter(0, 1)(items)

            logger.info(f"Compiling source dataset {index + 1} of {len(self.schema)}: {layer}.")

            # Load layer into dataframe, force lowercase column names.
            df = gpd.read_file(self.src, driver="OpenFileGDB", layer=layers_lower[layer]).rename(columns=str.lower)

            # Filter columns.
            df.drop(columns=df.columns.difference(attr["fields"]), inplace=True)

            # Filter records with query.
            if attr["query"]:
                count = len(df)
                df.query(attr["query"], inplace=True)
                logger.info(f"Dropped {count - len(df)} of {count} records for dataset: {layer}, based on query.")

            # Update column names to match NRN.
            df.rename(columns=self.rename, inplace=True)

            # Convert tabular dataframes.
            if "geometry" not in df.columns:
                df = pd.DataFrame(df)

            # Store results.
            self.src_datasets[layer] = df.copy(deep=True)

    def configure_geometry_epsgs(self) -> None:
        """
        Configures the official EPSG code for the UTM zone of each geometry based on the location of its centroid.
        Resulting codes are assigned to a new DataFrame column.
        Input geometries are assumed to be in a Geographic Coordinate System (using lat / lon).
        """

        logger.info(f"Configuring UTM zone EPSG codes for each geometry.")

        def latlon_to_utm_epsg(lat: float, lon: float) -> int:
            """
            Returns the official EPSG code for the detected UTM Zone of the given lat / lon.

            :param float lat: latitude.
            :param float lon: longitude.
            :return int: EPSG code.
            """

            return int(32700 - round((45 + lat) / 90, 0) * 100 + round((183 + lon) / 6, 0))

        # Configure EPSG codes for geometry dataset.
        self.src_datasets[self.geometry_dataset]["epsg"] = self.src_datasets[self.geometry_dataset]["geometry"].map(
            lambda g: latlon_to_utm_epsg(*list(map(itemgetter(0), g.centroid.xy))[::-1]))

    def configure_valid_records(self) -> None:
        """
        Filters records to only those which link to the base dataset, non-matching datasets are removed.
        Flags many-to-one linkages between the base and geometry datasets.
        """

        logger.info(f"Configuring valid records.")

        # Iterate dataframes and remove records which do not link to the base dataset.
        for name, df in {k: v for k, v in self.src_datasets.items() if k != self.base_dataset}.items():

            logger.info(f"Configuring valid records for source dataset: {name}.")

            # Identify connection field.
            con_id_field = self.get_con_id_field(name)

            # Compile valid IDs from base dataset for the identified connection field.
            valid_ids = set(self.src_datasets[self.base_dataset][con_id_field])

            # Remove records with invalid connection IDs.
            df_valid = df.loc[df[con_id_field].isin(valid_ids)]
            logger.info(f"Dropped {len(df) - len(df_valid)} of {len(df)} records for dataset: {name}, based on ID "
                        f"field: {con_id_field}.")

            # Flag many-to-one linkages between base and geometry datasets, if they are not the same.
            if (name == self.geometry_dataset) and (self.base_dataset != self.geometry_dataset):

                # Compile and flag many-to-one linkages.
                base = self.src_datasets[self.base_dataset]
                for con_id, count in Counter(base.loc[base[con_id_field].duplicated(keep=False), con_id_field]).items():
                    logger.warning(f"Many-to-one linkage identified between base ({self.base_dataset}) and geometry "
                                   f"({self.geometry_dataset}) datasets: {con_id_field}={con_id}, count={count}.")

            # Store or remove dataset.
            if len(df_valid):
                self.src_datasets[name] = df_valid.copy(deep=True)
            else:
                del self.src_datasets[name]

    def export_gpkg(self) -> None:
        """Exports the NRN datasets to a GeoPackage."""

        logger.info("Exporting datasets to GeoPackage.")

        # Conditionally explode geometries.
        for table, df in self.nrn_datasets.items():

            geom_types = set(df.geom_type)
            if geom_types.issubset({"Point", "MultiPoint"}) or geom_types.issubset({"LineString", "MultiLineString"}):
                self.nrn_datasets[table] = helpers.explode_geometry(df).copy(deep=True)

        # Export to GeoPackage.
        helpers.export(self.nrn_datasets, self.dst, merge_schemas=True)

    def get_con_id_field(self, name: str) -> str:
        """
        Fetches the connection ID field, relative to the base dataset, for the given dataset name.

        :param str name: dataset name.
        :return str: connection ID field, relative to the base dataset.
        """

        for con_field, df_names in self.structure["connections"].items():
            if name in df_names:
                return con_field

    def separate_composite_datasets(self) -> None:
        """Separates specified datasets into multiple datasets."""

        logger.info("Separating composite datasets.")

        # Iterate composite datasets.
        for composite_name in self.composite_datasets:
            composite_df = self.src_datasets[composite_name]
            con_id_field = self.get_con_id_field(composite_name)

            # Iterate composite new datasets.
            for new_dataset in self.composite_datasets[composite_name]["new_datasets"]:
                dataset_name = new_dataset["dataset_name"]

                logger.info(f"Separating records from composite dataset: \"{composite_name}\" into new dataset: "
                            f"\"{dataset_name}\".")

                # Create new dataset via dataframe query and rename specified fields.
                new_df = composite_df.query(new_dataset["query"]).rename(columns=new_dataset["rename_fields"])

                # Log new record count.
                logger.info(f"New dataset: \"{dataset_name}\" contains {len(new_df)} of the original "
                            f"{len(composite_df)} composite dataset records.")

                # Store new dataset and add to class variables: schema, structure, and rename.
                # Note: updating these class variables avoids having to implement specific logic purely to handle
                # composite new datasets.
                self.src_datasets[dataset_name] = new_df.copy(deep=True)
                self.schema[dataset_name] = {"output_fields": new_dataset["output_fields"]}
                self.structure["connections"][con_id_field].append(dataset_name)
                self.rename |= new_dataset["rename_fields"]

                # Overwrite composite dataframe with new dataframe if queries are to be applied successively.
                if self.composite_datasets[composite_name]["successive_queries"]:
                    composite_df = new_df.copy(deep=True)

            # Remove original composite dataset and remove from class variables: schema and structure.
            del self.src_datasets[composite_name]
            del self.schema[composite_name]
            self.structure["connections"][con_id_field].remove(composite_name)

    def execute(self) -> None:
        """Executes class functionality."""

        self.compile_source_datasets()
        self.assemble_non_base_linkages()
        self.separate_composite_datasets()
        self.configure_valid_records()
        self.configure_geometry_epsgs()
        self.clean_event_measurements()
        self.assemble_segmented_network()
        self.assemble_network_attribution()
        self.export_gpkg()


@click.command()
@click.argument("src", type=click.Path(exists=True))
@click.option("--dst", type=click.Path(exists=False), default=filepath.parents[4] / "data/raw/on/on.gpkg",
              show_default=True)
def main(src: Union[Path, str], dst: Union[Path, str] = filepath.parents[4] / "data/raw/on/on.gpkg") -> None:
    """
    Executes the LRS class.

    :param Union[Path, str] src: source path.
    :param Union[Path, str] dst: destination path,
        default = Path(__file__).resolve().parents[4] / 'data/raw/on/on.gpkg'.
    """

    try:

        with helpers.Timer():
            lrs = LRS(src, dst)
            lrs.execute()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
