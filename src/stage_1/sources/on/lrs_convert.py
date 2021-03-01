import click
import geopandas as gpd
import logging
import numpy as np
import pandas as pd
import shapely
import sqlite3
import sys
import uuid
from collections import Counter
from collections.abc import Sequence
from itertools import chain
from operator import attrgetter, itemgetter
from osgeo import ogr, osr
from pathlib import Path
from shapely.geometry import LineString, MultiLineString
from tqdm import tqdm
from typing import List, Union

sys.path.insert(1, str(Path(__file__).resolve().parents[3]))
import helpers


# Set logger.
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)


class ORN:
    """Class to convert Ontario ORN data from Linear Reference System (LRS) to GeoPackage."""

    def __init__(self, src: Union[Path, str], dst: Union[Path, str]) -> None:
        """
        Initializes the LRS conversion class.

        :param Union[Path, str] src: source path.
        :param Union[Path, str] dst: destination path.
        """

        self.nrn_datasets = dict()
        self.source_datasets = dict()
        self.base_dataset = "orn_road_net_element"
        self.base_query = "road_element_type != 'VIRTUAL ROAD'"
        self.base_fk = "ogf_id"
        self.source_fk = "orn_road_net_element_id"
        self.event_measurement_fields = ["from_measure", "to_measure"]
        self.point_event_measurement_field = "at_measure"
        self.irreducible_datasets = ["orn_road_net_element", "orn_blocked_passage", "orn_toll_point",
                                     "orn_street_name_parsed", "orn_route_name", "orn_route_number", "orn_structure"]
        self.parities = {"orn_address_info": "street_side",
                         "orn_jurisdiction": "street_side"}
        self.address_dataset = "orn_address_info"

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

    def assemble_nrn_datasets(self) -> None:
        """Assembles the NRN datasets from all linked datasets."""

        logger.info("Assembling NRN datasets.")

        # addrange.
        logger.info("Assembling NRN dataset: addrange.")

        # Group address parities into single records.
        addrange_l = self.source_datasets["orn_address_info"].loc[
            self.source_datasets["orn_address_info"]["street_side"] == "Left"].copy(deep=True)
        addrange_r = self.source_datasets["orn_address_info"].loc[
            self.source_datasets["orn_address_info"]["street_side"] == "Right"].copy(deep=True)
        addrange_merge = addrange_l.merge(addrange_r, how="outer", on=self.source_fk, suffixes=("_l", "_r"))

        # Create addrange.
        addrange = addrange_merge.copy(deep=True)
        addrange.reset_index(drop=True, inplace=True)
        addrange["nid"] = [uuid.uuid4().hex for _ in range(len(addrange))]

        # Change parity suffixes to prefixes.
        for parity in ("l", "r"):
            addrange.rename(columns={col: f"{parity}_{''.join(col.split(f'_{parity}')[:-1])}" for col in
                                     addrange.columns if col.endswith(f"_{parity}")}, inplace=True)

        # Resolve addrange conflicting attributes.
        addrange["revdate"] = addrange[["l_revdate", "r_revdate"]].max(axis=1)
        addrange.drop(columns=["l_revdate", "r_revdate"], inplace=True)

        # Assemble addrange linked attributes: official and alternate street names.
        addrange["l_altnanid"] = addrange.merge(self.source_datasets["orn_alternate_street_name"], how="left",
                                                on=self.source_fk)["stname_c"].fillna(value="None")
        addrange["r_altnanid"] = addrange["l_altnanid"]
        addrange[["l_offnanid", "r_offnanid"]] = addrange[["l_stname_c", "r_stname_c"]]

        # Assemble addrange linked attributes.
        for col in ("accuracy", "acqtech", "credate"):
            addrange[col] = addrange.merge(self.source_datasets[self.base_dataset], how="left", left_on=self.source_fk,
                                           right_on=self.base_fk)[col]

        # strplaname
        logger.info("Assembling NRN dataset: strplaname.")

        # Compile strplaname records from left and right official and alternate street names from addrange.
        # Exclude "None" street names.
        addrange_strplaname_links = [["l_offnanid", "l_placenam"], ["r_offnanid", "r_placenam"],
                                     ["l_altnanid", "l_placenam"], ["r_altnanid", "r_placenam"]]
        strplaname_records = {index: addrange.loc[addrange[cols[0]] != "None", [*cols, "revdate"]].rename(
            columns={cols[0]: "stname_c", cols[1]: "placename"})
            for index, cols in enumerate(addrange_strplaname_links)}

        # Create strplaname.
        strplaname = pd.concat(strplaname_records.values(), ignore_index=True, sort=False).drop_duplicates(
            subset=["stname_c", "placename"], keep="first")
        strplaname.reset_index(drop=True, inplace=True)
        strplaname["nid"] = [uuid.uuid4().hex for _ in range(len(strplaname))]

        # Convert addrange offnanids and altnanids to strplaname nids.
        logger.info("Resolving NRN dataset linkage: addrange-strplaname.")

        for cols in addrange_strplaname_links:
            addrange_filtered = addrange.loc[addrange[cols[0]] != "None"]
            addrange.loc[addrange_filtered.index, cols[0]] = addrange_filtered.merge(
                strplaname[["stname_c", "placename", "nid"]], how="left", left_on=cols,
                right_on=["stname_c", "placename"])["nid_y"].values

        # Assemble strplaname linked attributes: parsed street name.
        strplaname = strplaname.merge(self.source_datasets["orn_street_name_parsed"], how="left", on="stname_c")

        # Assemble strplaname linked attributes.
        for col in ("accuracy", "acqtech", "credate"):
            attr_concat = pd.concat([addrange[[col, nid_col]].rename(columns={nid_col: "nid"}) for nid_col in
                                     ["l_offnanid", "r_offnanid", "l_altnanid", "r_altnanid"]], ignore_index=True)
            attr_grouped = helpers.groupby_to_list(attr_concat, "nid", col)

            # Resolve conflicting attributes.
            attr = attr_grouped.map(lambda vals:
                                    max(vals) if col == "accuracy"
                                    else Counter(vals).most_common()[0][0] if col == "acqtech"
                                    else min(vals))

            # Assign attributes.
            strplaname[col] = strplaname.merge(pd.DataFrame({"nid": attr.index, col: attr}), how="left", on="nid")[col]

        # Resolve strplaname conflicting attributes.
        strplaname["revdate"] = strplaname[["revdate_x", "revdate_y"]].max(axis=1)
        strplaname.drop(columns=["revdate_x", "revdate_y", "stname_c"], inplace=True)

        # roadseg
        logger.info("Assembling NRN dataset: roadseg.")

        # Create roadseg.
        roadseg = self.source_datasets[self.base_dataset].query("road_element_type == 'ROAD ELEMENT'").copy(deep=True)
        roadseg.reset_index(drop=True, inplace=True)
        roadseg["nid"] = [uuid.uuid4().hex for _ in range(len(roadseg))]

        # Cast geometry.
        roadseg["geometry"] = roadseg["geometry"].map(lambda g: g if isinstance(g, LineString) else g[0])

        # Assemble roadseg linked attributes.
        linkages = [
            {"df": addrange,
             "cols_from": ["l_stname_c", "r_stname_c", "l_placenam", "r_placenam"],
             "cols_to": ["l_stname_c", "r_stname_c", "l_placenam", "r_placenam"], "na": "Unknown"},
            {"df": addrange, "cols_from": ["nid"], "cols_to": ["adrangenid"], "na": "None"},
            {"df": addrange, "cols_from": ["l_hnumf", "r_hnumf", "l_hnuml", "r_hnuml"],
             "cols_to": ["l_hnumf", "r_hnumf", "l_hnuml", "r_hnuml"], "na": 0},
            {"df": self.source_datasets["orn_jurisdiction"], "cols_from": ["roadjuris"], "cols_to": ["roadjuris"],
             "na": "Unknown"},
            {"df": self.source_datasets["orn_number_of_lanes"], "cols_from": ["nbrlanes"], "cols_to": ["nbrlanes"],
             "na": "Unknown"},
            {"df": self.source_datasets["orn_road_class"], "cols_from": ["roadclass"], "cols_to": ["roadclass"],
             "na": "Unknown"},
            {"df": self.source_datasets["orn_road_surface"], "cols_from": ["pavstatus", "surf"],
             "cols_to": ["pavstatus", "pavsurf"], "na": {"pavstatus": "Unknown", "pavsurf": "None"}},
            {"df": self.source_datasets["orn_road_surface"], "cols_from": ["surf"], "cols_to": ["unpavsurf"],
             "na": "None"},
            {"df": self.source_datasets["orn_speed_limit"], "cols_from": ["speed"], "cols_to": ["speed"],
             "na": "Unknown"}
        ]

        # Iterate linkages.
        for linkage in linkages:
            df = linkage["df"].copy(deep=True)
            cols_from, cols_to, na = itemgetter("cols_from", "cols_to", "na")(linkage)

            # Apply linkages.
            roadseg[cols_to] = roadseg[[self.base_fk]].merge(df.rename(columns=dict(zip(cols_from, cols_to))),
                                                             how="left", left_on=self.base_fk, right_on=self.source_fk
                                                             )[cols_to].fillna(value=na)

        # Resolve conflicting attributes: pavsurf and unpavsurf.
        roadseg.loc[roadseg["pavstatus"] == "Paved", "unpavsurf"] = "None"
        roadseg.loc[roadseg["pavstatus"] == "Unpaved", "pavsurf"] = "None"

        # Configure linked route names and numbers.
        roadseg = self.configure_route_attributes(roadseg)

        # Configure linked structures.
        roadseg = self.configure_structures(roadseg)

        logger.info("Assembling NRN dataset: roadseg (continued).")

        # Resolve roadseg conflicting attributes: revdate.
        roadseg = self.resolve_revdate(roadseg, [roadseg, addrange, "orn_jurisdiction", "orn_number_of_lanes",
                                                 "orn_road_class", "orn_road_surface", "orn_speed_limit",
                                                 "orn_route_name", "orn_route_number"])

        # Remove invalid roadseg-addrange-strplaname linkages.
        # Note: could not do earlier b/c attributes were required from addrange.
        logger.info("Resolving NRN dataset linkage: roadseg-addrange-strplaname.")

        addrange_invalid_nids = set(
            addrange.loc[addrange[["l_hnumf", "r_hnumf", "l_hnuml", "r_hnuml"]].sum(axis=1) == 0, "nid"])
        addrange_valid_nanids = set(np.concatenate(
            addrange.loc[~addrange["nid"].isin(addrange_invalid_nids),
                         ["l_offnanid", "r_offnanid", "l_altnanid", "r_altnanid"]].values))

        roadseg.loc[roadseg["adrangenid"].isin(addrange_invalid_nids), "adrangenid"] = "None"
        strplaname = strplaname.loc[strplaname["nid"].isin(addrange_valid_nanids)]
        addrange = addrange.loc[~addrange["nid"].isin(addrange_invalid_nids)]

        # ferryseg
        logger.info("Assembling NRN dataset: ferryseg.")

        # Create ferryseg.
        ferryseg = self.source_datasets[self.base_dataset].query(
            "road_element_type == 'FERRY CONNECTION'").copy(deep=True)
        ferryseg.reset_index(drop=True, inplace=True)
        ferryseg["nid"] = [uuid.uuid4().hex for _ in range(len(ferryseg))]

        # Cast geometry.
        ferryseg["geometry"] = ferryseg["geometry"].map(lambda g: g if isinstance(g, LineString) else g[0])

        # Configure linked route names and numbers.
        ferryseg = self.configure_route_attributes(ferryseg)

        # Resolve roadseg conflicting attributes: revdate.
        ferryseg = self.resolve_revdate(ferryseg, [ferryseg, "orn_route_name", "orn_route_number"])

        # blkpassage
        logger.info("Assembling NRN dataset: blkpassage.")

        # Create blkpassage.
        blkpassage = self.assemble_point_dataset("orn_blocked_passage", roadseg)

        # tollpoint
        logger.info("Assembling NRN dataset: tollpoint.")

        # Create tollpoint.
        tollpoint = self.assemble_point_dataset("orn_toll_point", roadseg)

        # Store final datasets.
        logger.info("Storing finalized NRN datasets.")

        for name, df in {"addrange": addrange, "blkpassage": blkpassage, "ferryseg": ferryseg, "roadseg": roadseg,
                         "strplaname": strplaname, "tollpoint": tollpoint}.items():

            df = df.copy(deep=True)

            # Adjust datetime columns.
            for col in ("credate", "revdate"):
                df.loc[df.index, col] = df[col].map(lambda dt: dt.replace("-", "").split("T")[0])

            # Convert None values to 'None' for object columns.
            for col, dtype in df.dtypes.items():
                if col != "geometry" and dtype.kind == "O":
                    df.loc[df[col].isna(), col] = "None"

            self.nrn_datasets[name] = df.copy(deep=True)

    def assemble_point_dataset(self, source_name: str, linked_df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Assembles an NRN Point dataset for the given source name by merging it with the base dataset. Currently
        supported: blkpassage, tollpoint.

        :param str source_name: dataset name.
        :param gpd.GeoDataFrame linked_df: Point GeoDataFrame.
        :return gpd.GeoDataFrame: Point GeoDataFrame merged with the base dataset.
        """

        # Create dataset.
        df = self.source_datasets[source_name].copy(deep=True)
        df["nid"] = [uuid.uuid4().hex for _ in range(len(df))]

        # Create geometry column.
        # Process: interpolate the event measurement along the associated LineString from the base dataset.
        df["geometry"] = df.merge(linked_df[[self.base_fk, "geometry"]], how="left", left_on=self.source_fk,
                                  right_on=self.base_fk)["geometry"]
        df["geometry"] = df["geometry"].map(lambda g: g[0] if isinstance(g, MultiLineString) else g)
        df["geometry"] = df[[self.point_event_measurement_field, "geometry"]].apply(
            lambda row: row[1].interpolate(row[0]), axis=1)
        df = gpd.GeoDataFrame(df, crs=linked_df.crs)

        # Assemble linked attributes.
        for col_from, col_to in {"accuracy": "accuracy", "acqtech": "acqtech", "nid": "roadnid", "credate": "credate",
                                 "revdate": "revdate_y"}.items():
            df[col_to] = df[[self.source_fk]].merge(linked_df.rename(columns={col_from: col_to}), how="left",
                                                    left_on=self.source_fk, right_on=self.base_fk)[col_to]

        # Resolve conflicting attributes.
        df["revdate"] = df[["revdate", "revdate_y"]].max(axis=1)
        df.drop(columns=[self.point_event_measurement_field, "revdate_y"], inplace=True)

        return df.copy(deep=True)

    def compile_source_datasets(self) -> None:
        """Loads raw source layers into (Geo)DataFrames."""

        logger.info(f"Compiling source datasets from: {self.src}.")

        schema = {
            "orn_address_info": [
                "orn_road_net_element_id", "from_measure", "to_measure", "first_house_number", "last_house_number",
                "house_number_structure", "street_side", "full_street_name", "standard_municipality",
                "effective_datetime"
            ],
            "orn_alternate_street_name": [
                "orn_road_net_element_id", "from_measure", "to_measure", "full_street_name", "effective_datetime"
            ],
            "orn_blocked_passage": [
                "orn_road_net_element_id", "at_measure", "blocked_passage_type", "effective_datetime"
            ],
            "orn_jurisdiction": [
                "orn_road_net_element_id", "from_measure", "to_measure", "street_side", "jurisdiction",
                "effective_datetime"
            ],
            "orn_number_of_lanes": [
                "orn_road_net_element_id", "from_measure", "to_measure", "number_of_lanes", "effective_datetime"
            ],
            "orn_road_class": [
                "orn_road_net_element_id", "from_measure", "to_measure", "road_class", "effective_datetime"
            ],
            "orn_road_net_element": [
                "ogf_id", "road_absolute_accuracy", "direction_of_traffic_flow", "exit_number", "road_element_type",
                "acquisition_technique", "creation_date", "effective_datetime", "geometry"
            ],
            "orn_road_surface": [
                "orn_road_net_element_id", "from_measure", "to_measure", "pavement_status", "surface_type",
                "effective_datetime"
            ],
            "orn_route_name": [
                "orn_road_net_element_id", "from_measure", "to_measure", "route_name_english", "route_name_french",
                "effective_datetime"
            ],
            "orn_route_number": [
                "orn_road_net_element_id", "from_measure", "to_measure", "route_number", "effective_datetime"
            ],
            "orn_speed_limit": [
                "orn_road_net_element_id", "from_measure", "to_measure", "speed_limit", "effective_datetime"
            ],
            "orn_street_name_parsed": [
                "full_street_name", "directional_prefix", "street_type_prefix", "street_name_body",
                "street_type_suffix", "directional_suffix", "effective_datetime"
            ],
            "orn_structure": [
                "orn_road_net_element_id", "from_measure", "to_measure", "structure_type", "structure_name_english",
                "structure_name_french", "effective_datetime"
            ],
            "orn_toll_point": [
                "orn_road_net_element_id", "at_measure", "toll_point_type", "effective_datetime"
            ]
        }

        rename = {
            "acquisition_technique":     "acqtech",
            "blocked_passage_type":      "blkpassty",
            "creation_date":             "credate",
            "direction_of_traffic_flow": "trafficdir",
            "directional_prefix":        "dirprefix",
            "directional_suffix":        "dirsuffix",
            "effective_datetime":        "revdate",
            "exit_number":               "exitnbr",
            "first_house_number":        "hnumf",
            "full_street_name":          "stname_c",
            "house_number_structure":    "hnumstr",
            "jurisdiction":              "roadjuris",
            "last_house_number":         "hnuml",
            "number_of_lanes":           "nbrlanes",
            "pavement_status":           "pavstatus",
            "road_absolute_accuracy":    "accuracy",
            "road_class":                "roadclass",
            "route_name_english":        "rtenameen",
            "route_name_french":         "rtenamefr",
            "route_number":              "rtnumber",
            "speed_limit":               "speed",
            "standard_municipality":     "placenam",
            "street_name_body":          "namebody",
            "street_type_prefix":        "strtypre",
            "street_type_suffix":        "strtysuf",
            "structure_name_english":    "strunameen",
            "structure_name_french":     "strunamefr",
            "structure_type":            "structtype",
            "surface_type":              "surf",
            "toll_point_type":           "tollpttype"
        }

        # Iterate ORN schema.
        for index, items in enumerate(schema.items()):

            layer, cols = items

            logger.info(f"Compiling source layer {index + 1} of {len(schema)}: {layer}.")

            # Load layer into dataframe, force lowercase column names.
            df = gpd.read_file(self.src, driver="OpenFileGDB", layer=layer).rename(columns=str.lower)

            # Filter columns.
            df.drop(columns=df.columns.difference(cols), inplace=True)

            # Update column names to match NRN.
            df.rename(columns=rename, inplace=True)

            # Convert tabular dataframes.
            if "geometry" not in df.columns:
                df = pd.DataFrame(df)

            # Store results.
            self.source_datasets[layer] = df.copy(deep=True)

    def configure_route_attributes(self, df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Configures route name and number attribution for the given dataset.

        :param gpd.GeoDataFrame df: GeoDataFrame.
        :return gpd.GeoDataFrame: GeoDataFrame with route name and number attribution.
        """

        logger.info("Configuring route attributes.")

        for route_params in [
            {"df": self.source_datasets["orn_route_name"], "col_from": "rtenameen",
             "cols_to": ["rtename1en", "rtename2en", "rtename3en", "rtename4en"], "na": "None"},
            {"df": self.source_datasets["orn_route_name"], "col_from": "rtenamefr",
             "cols_to": ["rtename1fr", "rtename2fr", "rtename3fr", "rtename4fr"], "na": "None"},
            {"df": self.source_datasets["orn_route_number"], "col_from": "rtnumber",
             "cols_to": ["rtnumber1", "rtnumber2", "rtnumber3", "rtnumber4", "rtnumber5"], "na": "None"}
        ]:

            routes_df = route_params["df"].copy(deep=True)
            col_from, cols_to, na = itemgetter("col_from", "cols_to", "na")(route_params)

            # Filter to valid and unique records.
            routes_df = routes_df.loc[~((routes_df[col_from].isna()) | (routes_df[col_from] == na) |
                                        (routes_df[[self.source_fk, col_from]].duplicated(keep="first")))]

            if len(routes_df):

                # Configure attributes: compute and nest event lengths with attribute values, group nested events by ID,
                # sort attribute values by event lengths, unpack only attribute values.
                routes_df["event"] = routes_df[[*self.event_measurement_fields, col_from]].apply(
                    lambda row: [abs(row[0] - row[1]), row[-1]], axis=1)
                routes_df_grouped = helpers.groupby_to_list(routes_df, self.source_fk, "event")
                routes_df_filtered = routes_df_grouped.map(
                    lambda row: list(map(itemgetter(-1), sorted(row, key=itemgetter(0)))))

                # Iterate and populate target columns with nested attribute values at the given index.
                for index, col in enumerate(cols_to):
                    routes_subset = routes_df_filtered.loc[routes_df_filtered.map(len) > index].map(itemgetter(index))
                    routes_subset_df = pd.DataFrame({self.source_fk: routes_subset.index, "value": routes_subset})
                    df[col] = df.merge(routes_subset_df, how="left", left_on=self.base_fk,
                                       right_on=self.source_fk)["value"].fillna(value=na)

            else:
                for col in cols_to:
                    df[col] = na

        return df.copy(deep=True)

    def configure_structures(self, df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Configures structure attribution for the given dataset.

        :param gpd.GeoDataFrame df: GeoDataFrame.
        :return gpd.GeoDataFrame: GeoDataFrame with structure attribution.
        """

        logger.info("Configuring structures.")

        def update_geoms(geom: LineString, ranges: List[tuple, ...]) -> Union[None, List[LineString, ...]]:
            """
            Splits the LineString into smaller LineStrings by removing the given ranges (event measurements) from the
            geometry.

            :param LineString geom: LineString.
            :param List[tuple, ...] ranges: nested list of event measurements.
            :return Union[None, List[LineString, ...]]: None or a list of LineStrings, segmented from the original
                LineString.
            """

            # Compute new geometry ranges.
            new_ranges = [(0, ranges[0][0]), (ranges[-1][1], geom.length)]
            if len(ranges) > 1:
                values = list(chain.from_iterable(ranges))[1: -1]
                pairs = list(zip(values[0::2], values[1::2]))
                new_ranges.extend(pairs)
            new_ranges = [new_range for new_range in new_ranges if
                          new_range[0] != new_range[1] and new_range[0] < geom.length]

            # Update geometry from ranges.
            if len(new_ranges):
                return [shapely.ops.substring(geom, *new_range) for new_range in new_ranges]
            else:
                return None

        # Compile structure linkages.
        structures = self.source_datasets["orn_structure"].copy(deep=True)

        # Compile records with linked structures.
        df_structs = df.loc[df[self.base_fk].isin(set(structures[self.source_fk]))].copy(deep=True)

        # Convert geometries to meter-based projection.
        df_structs = helpers.reproject_gdf(df_structs, df_structs.crs.to_epsg(), 3348)

        # Merge full geometries onto structures.
        structures = structures.merge(df_structs, how="left", left_on=self.source_fk, right_on=self.base_fk)

        # Subset geometries to structure events, filter invalid results.
        structures["geometry"] = structures[["geometry", *self.event_measurement_fields]].apply(
            lambda row: shapely.ops.substring(*row), axis=1)
        structures = structures.loc[structures["geometry"].map(lambda g: isinstance(g, LineString))]

        # Remove structure geometries from original geometries.
        # Process: group structure event measurements and subtract the ranges from the base geometry.

        # Group and sort structure events.
        structures["range"] = structures[self.event_measurement_fields].apply(lambda vals: tuple(sorted(vals)), axis=1)
        structures_grouped = helpers.groupby_to_list(structures, self.source_fk, "range")
        structures_grouped = pd.DataFrame({self.source_fk: structures_grouped.index,
                                           "structure_ranges": structures_grouped})
        df_structs = df_structs.merge(structures_grouped, how="right", left_on=self.base_fk, right_on=self.source_fk)
        df_structs["structure_ranges"] = df_structs["structure_ranges"].map(sorted)

        # Generate new base geometries, filter invalid results.
        df_structs["new_geometries"] = df_structs[["geometry", "structure_ranges"]].apply(
            lambda row: update_geoms(*row), axis=1)
        df_structs = df_structs.loc[~df_structs["new_geometries"].isna()]

        # Explode nested geometries, update geometry column.
        df_structs = gpd.GeoDataFrame(pd.DataFrame(df_structs).explode("new_geometries"), crs=3348)
        df_structs["geometry"] = df_structs["new_geometries"]

        # Create new dataframe with updated records.

        # Reproject structure and base geometries to original crs.
        structures = helpers.reproject_gdf(gpd.GeoDataFrame(structures, crs=3348), 3348, df.crs.to_epsg())
        df_structs = helpers.reproject_gdf(df_structs, 3348, df.crs.to_epsg())

        # Standardize fields and append non-structure records and structure base records.
        df_non_structs = df.loc[~df[self.base_fk].isin(set(structures[self.source_fk]))].copy(deep=True)
        df_structs.drop(columns=set(df_structs)-set(df_non_structs), inplace=True)
        new_df = df_structs.append(df_non_structs, ignore_index=True)
        for new_col in {"structtype", "strunameen", "strunamefr"}:
            new_df[new_col] = "None"

        # Standardize fields for structure records and append records to new dataframe.
        structures["revdate"] = structures[["revdate_x", "revdate_y"]].max(axis=1)
        structures.drop(columns=set(structures)-set(new_df)-{"structtype", "strunameen", "strunamefr"}, inplace=True)
        new_df = new_df.append(structures, ignore_index=True)

        return new_df.copy(deep=True)

    def configure_valid_records(self) -> None:
        """Configures and keeps only records which link to valid records from the base dataset."""

        logger.info(f"Configuring valid records.")

        # Filter base dataset to valid records.
        logger.info(f"Configuring valid records for base dataset: {self.base_dataset}.")

        count = len(self.source_datasets[self.base_dataset])
        self.source_datasets[self.base_dataset].query(self.base_query, inplace=True)
        logger.info(f"Dropped {count - len(self.source_datasets[self.base_dataset])} of {count} records for base "
                    f"dataset: {self.base_dataset}.")

        # Compile base dataset foreign keys.
        base_fkeys = set(self.source_datasets[self.base_dataset][self.base_fk])

        # Iterate dataframes and remove records which do not link to the base dataset.
        for name, df in self.source_datasets.items():
            if self.source_fk in df.columns:

                logger.info(f"Configuring valid records for source dataset: {name}.")

                df_valid = df.loc[df[self.source_fk].isin(base_fkeys)]
                logger.info(f"Dropped {len(df) - len(df_valid)} of {len(df)} records for dataset: {name}.")

                # Store or delete dataset.
                if len(df_valid):
                    self.source_datasets[name] = df_valid.copy(deep=True)
                else:
                    del self.source_datasets[name]

    def export_gpkg(self) -> None:
        """Exports the NRN datasets to a GeoPackage."""

        logger.info(f"Exporting datasets to GeoPackage: {self.dst}.")

        try:

            logger.info(f"Creating data source: {self.dst}.")

            # Create GeoPackage.
            driver = ogr.GetDriverByName("GPKG")
            gpkg = driver.CreateDataSource(str(self.dst))

            # Iterate dataframes.
            for name, df in self.nrn_datasets.items():

                logger.info(f"Layer {name}: creating layer.")

                # Configure layer shape type and spatial reference.
                if isinstance(df, gpd.GeoDataFrame):

                    srs = osr.SpatialReference()
                    srs.ImportFromEPSG(df.crs.to_epsg())

                    if len(df.geom_type.unique()) > 1:
                        raise ValueError(f"Multiple geometry types detected for dataframe {name}: "
                                         f"{', '.join(map(str, df.geom_type.unique()))}.")
                    elif df.geom_type[0] in {"Point", "MultiPoint", "LineString", "MultiLineString"}:
                        shape_type = attrgetter(f"wkb{df.geom_type[0]}")(ogr)
                    else:
                        raise ValueError(f"Invalid geometry type(s) for dataframe {name}: "
                                         f"{', '.join(map(str, df.geom_type.unique()))}.")
                else:
                    shape_type = ogr.wkbNone
                    srs = None

                # Create layer.
                layer = gpkg.CreateLayer(name=name, srs=srs, geom_type=shape_type, options=["OVERWRITE=YES"])

                logger.info(f"Layer {name}: configuring schema.")

                # Configure layer schema (field definitions).
                ogr_field_map = {"f": ogr.OFTReal, "i": ogr.OFTInteger, "O": ogr.OFTString}

                for field_name, dtype in df.dtypes.items():
                    if field_name != "geometry":
                        field_defn = ogr.FieldDefn(field_name, ogr_field_map[dtype.kind])
                        layer.CreateField(field_defn)

                # Write layer.
                layer.StartTransaction()

                for feat in tqdm(df.itertuples(index=False), total=len(df),
                                 desc=f"Writing to file={self.dst.name}, layer={name}",
                                 bar_format="{desc}: |{bar}| {percentage:3.0f}% {r_bar}"):

                    # Instantiate feature.
                    feature = ogr.Feature(layer.GetLayerDefn())

                    # Set feature properties.
                    properties = feat._asdict()
                    for prop in set(properties) - {"geometry"}:
                        field_index = feature.GetFieldIndex(prop)
                        feature.SetField(field_index, properties[prop])

                    # Set feature geometry, if required.
                    if srs:
                        geom = ogr.CreateGeometryFromWkb(properties["geometry"].wkb)
                        feature.SetGeometry(geom)

                    # Create feature.
                    layer.CreateFeature(feature)

                    # Clear pointer for next iteration.
                    feature = None

                layer.CommitTransaction()

        except (Exception, KeyError, ValueError, sqlite3.Error) as e:
            logger.exception(f"Error raised when writing to GeoPackage: {self.dst}.")
            logger.exception(e)
            sys.exit(1)

    def map_unrecognized_values(self) -> None:
        """Maps unrecognized attribute values to presumed NRN equivalents."""

        logger.info(f"Mapping unrecognized field values.")

        # Define value mapping.
        value_map = {
            "blkpassty":
                {
                    "Permanent": "Permanently Fixed"
                },
            "dirprefix":
                {
                    "North East": "Northeast",
                    "North West": "Northwest",
                    "South East": "Southeast",
                    "South West": "Southwest",
                },
            "dirsuffix":
                {
                    "North East": "Northeast",
                    "North West": "Northwest",
                    "South East": "Southeast",
                    "South West": "Southwest",
                },
            "roadclass":
                {
                    "Alleyway / Laneway": "Alleyway / Lane",
                    "Service": "Service Lane"
                },
            "strtypre":
                {
                    "Bypass": "By-pass",
                    "Concession Road": "Concession",
                    "Corner": "Corners",
                    "County Road": "Road",
                    "Crossroad": "Crossroads",
                    "Cul De Sac": "Cul-de-sac",
                    "Fire Route": "Route",
                    "Garden": "Gardens",
                    "Height": "Heights",
                    "Hills": "Hill",
                    "Isle": "Island",
                    "Lanes": "Lane",
                    "Pointe": "Point",
                    "Regional Road": "Road"
                },
            "strtysuf":
                {
                    "Bypass": "By-pass",
                    "Concession Road": "Concession",
                    "Corner": "Corners",
                    "County Road": "Road",
                    "Crossroad": "Crossroads",
                    "Cul De Sac": "Cul-de-sac",
                    "Fire Route": "Route",
                    "Garden": "Gardens",
                    "Height": "Heights",
                    "Hills": "Hill",
                    "Isle": "Island",
                    "Lanes": "Lane",
                    "Pointe": "Point",
                    "Regional Road": "Road"
                },
            "tollpttype":
                {
                    "Physical": "Physical Toll Booth",
                    "Virtual": "Virtual Toll Booth",
                },
            "trafficdir":
                {
                    "Both": "Both directions",
                    "Negative": "Opposite direction",
                    "Positive": "Same direction"
                }
        }

        # Iterate dataframes and columns.
        for name, df in self.nrn_datasets.items():
            for col in set(df.columns).intersection(set(value_map)):

                # Iterate and update mapped values.
                for val_from, val_to in value_map[col].items():
                    self.nrn_datasets[name].loc[
                        self.nrn_datasets[name][col].map(str.lower) == val_from.lower(), col] = val_to

    def resolve_revdate(self, base_df: Union[gpd.GeoDataFrame, pd.DataFrame],
                        linked_dfs: Sequence[Union[gpd.GeoDataFrame, pd.DataFrame, str], ...]) -> \
            Union[gpd.GeoDataFrame, pd.DataFrame]:
        """
        Updates the revdate attribute of the given base dataset to the maximum from all linked datasets.

        :param Union[gpd.GeoDataFrame, pd.DataFrame] base_df: (Geo)DataFrame of which the revdate attribute will be
            updated.
        :param Sequence[Union[gpd.GeoDataFrame, pd.DataFrame, str], ...] linked_dfs: (Geo)DataFrames and / or names of
            datasets linked to the base dataset and with a revdate attribute.
        :return Union[gpd.GeoDataFrame, pd.DataFrame]: (Geo)DataFrame with a modified revdate attribute.
        """

        # Compile linked dataframes.
        dfs = list()
        for linked_df in linked_dfs:
            if isinstance(linked_df, str):
                dfs.append(self.source_datasets[linked_df].copy(deep=True))
            else:
                dfs.append(linked_df.copy(deep=True))

        # Concatenate all dataframes.
        dfs_concat = pd.concat([df.rename(columns={self.base_fk: self.source_fk})[[self.source_fk, "revdate"]]
                                for df in dfs], ignore_index=True)

        # Group by identifier, configure and assign maximum value.
        base_df.index = base_df[self.base_fk]
        base_df["revdate"] = helpers.groupby_to_list(dfs_concat, self.source_fk, "revdate").map(max)
        base_df.reset_index(drop=True, inplace=True)

        return base_df.copy(deep=True)

    def resolve_unsplit_parities(self) -> None:
        """
        For paritized attributes, duplicates records where the parity field = 'Both' into 'Left' and 'Right'. This
        makes it easier to reduce LRS attributes.
        """

        logger.info("Resolving unsplit parities.")

        # Iterate paritized datasets and fields.
        for table, field in self.parities.items():

            logger.info(f"Resolving unsplit parities for dataset: {table}, field: {field}.")

            df = self.source_datasets[table].copy(deep=True)

            # Copy unsplit records and update as "Right".
            right_side = df.loc[df[field] == "Both"].copy(deep=True)
            right_side.loc[right_side.index, field] = "Right"

            # Update original records as "Left".
            df.loc[df[field] == "Both", field] = "Left"

            # Concatenate right-side attributes to original dataframe.
            df = pd.concat([df, right_side], ignore_index=True)

            # Store results.
            if "geometry" in df.columns:
                self.source_datasets[table] = gpd.GeoDataFrame(df.copy(deep=True))
            else:
                self.source_datasets[table] = df.copy(deep=True)

    def reduce_events(self) -> None:
        """
        Reduces many-to-one base dataset events to the event with the longest measurement.
        Exception: address dataset will keep the longest event for both "Left" and "Right" paritized instances.
        """

        def configure_address_structure(structures: List[str, ...]) -> str:
            """
            Configures the address structure given an iterable of structure values.

            :param List[str, ...] structures: list of structure values.
            :return str: structure value.
            """

            structures = set(structures)

            if len(structures) == 1:
                return list(structures)[0]
            elif "Unknown" in structures:
                return "Unknown"
            elif "Irregular" in structures:
                return "Irregular"
            elif "Mixed" in structures or {"Even", "Odd"}.issubset(structures):
                return "Mixed"
            else:
                return "Unknown"

        logger.info("Reducing events.")

        # Iterate datasets, excluding the base and irreducible datasets.
        for name, df in self.source_datasets.items():
            if name not in {self.base_dataset, *self.irreducible_datasets}:

                logger.info(f"Reducing events for dataset: {name}.")

                # Calculate event lengths.
                df["event_length"] = np.abs(df[self.event_measurement_fields[0]] - df[self.event_measurement_fields[1]])

                # Handle paritized address fields.
                if name == self.address_dataset:

                    logger.info("Address dataset detected. Reducing events by parity.")
                    dfs = list()

                    for parity in ("Left", "Right"):

                        logger.info(f"Reducing events for parity: {parity}.")

                        # Get parity records.
                        records = df.loc[df[self.parities[name]] == parity].copy(deep=True)

                        # Configure updated address attributes.
                        logger.info("Configuring updated address attributes.")

                        address_attributes = {
                            "hnumf": helpers.groupby_to_list(records, self.source_fk, "hnumf").map(min),
                            "hnuml": helpers.groupby_to_list(records, self.source_fk, "hnuml").map(max),
                            "hnumstr": helpers.groupby_to_list(
                                records, self.source_fk, "hnumstr").map(configure_address_structure),
                            "revdate": helpers.groupby_to_list(records, self.source_fk, "revdate").map(max)
                        }

                        # Drop duplicate events, keeping the maximum event_length.
                        records = records.sort_values("event_length").drop_duplicates(self.source_fk, keep="last")

                        # Update address attributes.
                        records.index = records[self.source_fk]
                        for attribute, series in address_attributes.items():

                            logger.info(f"Updating address attribute: {attribute}.")

                            # Update attribute.
                            records[attribute].update(series)

                        records.reset_index(drop=True, inplace=True)

                        dfs.append(records)

                    # Concatenate records.
                    df = pd.concat(dfs, ignore_index=True)

                else:

                    # Drop duplicate events, keeping the maximum event_length.
                    df = df.sort_values("event_length").drop_duplicates(self.source_fk, keep="last")

                # Drop event measurement fields.
                df.drop(columns=["event_length", *self.event_measurement_fields], inplace=True)

                # Log changes.
                logger.info(f"Dropped {len(self.source_datasets[name]) - len(df)} of {len(self.source_datasets[name])} "
                            f"records for dataset: {name}.")

                # Store results.
                if "geometry" in df.columns:
                    self.source_datasets[name] = gpd.GeoDataFrame(df.copy(deep=True))
                else:
                    self.source_datasets[name] = df.copy(deep=True)

    def execute(self) -> None:
        """Executes class functionality."""

        self.compile_source_datasets()
        self.configure_valid_records()
        self.resolve_unsplit_parities()
        self.reduce_events()
        self.assemble_nrn_datasets()
        self.map_unrecognized_values()
        self.export_gpkg()


@click.command()
@click.argument("src", type=click.Path(exists=True))
@click.option("--dst", type=click.Path(exists=False),
              default=Path(__file__).resolve().parents[4] / "data/raw/on/orn.gpkg", show_default=True)
def main(src: Union[Path, str],
         dst: Union[Path, str] = Path(__file__).resolve().parents[4] / "data/raw/on/orn.gpkg") -> None:
    """
    Executes the ORN class.

    :param Union[Path, str] src: source path.
    :param Union[Path, str] dst: destination path,
        default = Path(__file__).resolve().parents[4] / 'data/raw/on/orn.gpkg'.
    """

    try:

        with helpers.Timer():
            orn = ORN(src, dst)
            orn.execute()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
