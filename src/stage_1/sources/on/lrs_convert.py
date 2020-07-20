import click
import geopandas as gpd
import logging
import numpy as np
import os
import pandas as pd
import sys
import uuid
from operator import itemgetter
from src import helpers


# Set logger.
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)


class ORN:
    """Class to convert Ontario ORN data from Linear Reference System (LRS) to GeoPackage."""

    def __init__(self, src, dst):
        self.nrn_datasets = dict()
        self.source_datasets = dict()
        self.base_dataset = "orn_road_net_element"
        self.base_query = "road_element_type != 'VIRTUAL ROAD'"
        self.base_fk = "ogf_id"
        self.source_fk = "orn_road_net_element_id"
        self.event_measurement_fields = ["from_measure", "to_measure"]
        self.irreducible_datasets = ["orn_road_net_element", "orn_blocked_passage", "orn_toll_point",
                                     "orn_street_name_parsed", "orn_route_name", "orn_route_number"]
        self.parities = {"orn_address_info": "street_side",
                         "orn_jurisdiction": "street_side"}
        self.address_dataset = "orn_address_info"

        # Validate src.
        self.src = os.path.abspath(src)
        if os.path.splitext(self.src)[-1] != ".gdb":
            logger.exception(f"Invalid src input: {src}. Must be a File GeoDatabase.")
            sys.exit(1)

        # Validate dst.
        self.dst = os.path.abspath(dst)
        if os.path.splitext(self.dst)[-1] != ".gpkg":
            logger.exception(f"Invalid dst input: {dst}. Must be a GeoPackage.")
            sys.exit(1)
        if os.path.exists(self.dst):
            logger.exception(f"Invalid dst input: {dst}. File already exists.")

    def assemble_nrn_datasets(self):
        """Assembles the NRN datasets from all linked datasets."""

        logger.info("Assembling NRN datasets.")

        # addrange.
        logger.info("Assembling NRN dataset: addrange.")

        # Group address parities into single records.
        addrange_l = self.source_datasets["orn_address_info"][
            self.source_datasets["orn_address_info"]["street_side"] == "Left"].copy(deep=True)
        addrange_r = self.source_datasets["orn_address_info"][
            self.source_datasets["orn_address_info"]["street_side"] == "Right"].copy(deep=True)
        addrange_merge = addrange_l.merge(addrange_r, how="outer", on=self.source_fk, suffixes=("_l", "_r"))

        # Create addrange.
        addrange = addrange_merge.copy(deep=True)
        addrange.reset_index(drop=True, inplace=True)
        addrange["nid"] = [uuid.uuid4().hex for _ in range(len(addrange))]

        # Resolve conflicting attributes.
        addrange["effective_datetime"] = addrange[["effective_datetime_l", "effective_datetime_r"]].max(axis=1)
        addrange.drop(columns=["effective_datetime_l", "effective_datetime_r"], inplace=True)

        # Configure official and alternate street names fields.
        addrange["l_altnanid"] = addrange.merge(self.source_datasets["orn_alternate_street_name"], how="left",
                                                on=self.source_fk)["full_street_name"].fillna(value="None")
        addrange["r_altnanid"] = addrange["l_altnanid"]
        addrange[["l_offnanid", "r_offnanid"]] = addrange[["full_street_name_l", "full_street_name_r"]]

        # strplaname
        logger.info("Assembling NRN dataset: strplaname.")

        # Compile strplaname records from left and right official and alternate street names from addrange.
        # Exclude "None" street names.
        addrange_strplaname_links = [["l_offnanid", "standard_municipality_l"],
                                     ["r_offnanid", "standard_municipality_r"],
                                     ["l_altnanid", "standard_municipality_l"],
                                     ["r_altnanid", "standard_municipality_r"]]
        strplaname_records = {index: addrange[addrange[cols[0]] != "None"][[*cols, "effective_datetime"]].rename(
            columns={cols[0]: "full_street_name", cols[1]: "placename"})
            for index, cols in enumerate(addrange_strplaname_links)}

        # Create strplaname.
        strplaname = pd.concat(strplaname_records.values(), ignore_index=True, sort=False).drop_duplicates(
            subset=["full_street_name", "placename"], keep="first")
        strplaname.reset_index(drop=True, inplace=True)
        strplaname["nid"] = [uuid.uuid4().hex for _ in range(len(strplaname))]

        # Assemble strplaname linked attributes.
        strplaname = strplaname.merge(self.source_datasets["orn_street_name_parsed"], how="left", on="full_street_name")

        # Convert addrange offnanids and altnanids to strplaname nids.
        logger.info("Assembling NRN dataset linkage: addrange-strplaname.")

        for cols in addrange_strplaname_links:
            addrange_filtered = addrange[addrange[cols[0]] != "None"]
            addrange.loc[addrange_filtered.index, cols[0]] = addrange_filtered.merge(
                strplaname[["full_street_name", "placename", "nid"]], how="left", left_on=cols,
                right_on=["full_street_name", "placename"])["nid_y"].values

        # Resolve strplaname conflicting attributes.
        strplaname["effective_datetime"] = strplaname[["effective_datetime_x", "effective_datetime_y"]].max(axis=1)
        strplaname.drop(columns=["effective_datetime_x", "effective_datetime_y", "full_street_name"], inplace=True)

        # roadseg
        logger.info("Assembling NRN dataset: roadseg.")

        # Create roadseg.
        roadseg = self.source_datasets[self.base_dataset].query("road_element_type == 'ROAD ELEMENT'").copy(deep=True)
        roadseg.reset_index(drop=True, inplace=True)
        roadseg["nid"] = [uuid.uuid4().hex for _ in range(len(roadseg))]

        # Assemble roadseg linked attributes.
        linkages = [
            {"df": addrange,
             "cols_from": ["full_street_name_l", "full_street_name_r", "standard_municipality_l",
                           "standard_municipality_r"],
             "cols_to": ["l_stname_c", "r_stname_c", "l_placenam", "r_placenam"], "na": "Unknown"},
            {"df": addrange, "cols_from": ["nid"], "cols_to": ["adrangenid"], "na": "None"},
            {"df": self.source_datasets["orn_jurisdiction"], "cols_from": ["jurisdiction"], "cols_to": ["roadjuris"],
             "na": "Unknown"},
            {"df": self.source_datasets["orn_number_of_lanes"], "cols_from": ["number_of_lanes"],
             "cols_to": ["nbrlanes"], "na": "Unknown"},
            {"df": self.source_datasets["orn_road_class"], "cols_from": ["road_class"], "cols_to": ["roadclass"],
             "na": "Unknown"},
            {"df": self.source_datasets["orn_road_surface"], "cols_from": ["pavement_status", "surface_type"],
             "cols_to": ["pavstatus", "pavsurf"], "na": {"pavstatus": "Unknown", "pavsurf": "None"}},
            {"df": self.source_datasets["orn_road_surface"], "cols_from": ["surface_type"], "cols_to": ["unpavsurf"],
             "na": "None"},
            {"df": self.source_datasets["orn_speed_limit"], "cols_from": ["speed_limit"], "cols_to": ["speed"],
             "na": "Unknown"},
            {"df": self.source_datasets["orn_structure"],
             "cols_from": ["structure_type", "structure_name_english", "structure_name_french"],
             "cols_to": ["structtype", "strunameen", "strunamefr"], "na": "None"},
        ]

        # Iterate linkages.
        for linkage in linkages:
            df = linkage["df"].copy(deep=True)
            cols_from, cols_to, na = itemgetter("cols_from", "cols_to", "na")(linkage)

            # Apply linkages.
            roadseg[cols_to] = roadseg[[self.base_fk]].merge(df.rename(columns=dict(zip(cols_from, cols_to))),
                                                             how="left", left_on=self.base_fk, right_on=self.source_fk
                                                             )[cols_to].fillna(value=na)

        # Configure linked route names and numbers.
        roadseg = self.configure_route_attributes(roadseg)

        # Resolve roadseg conflicting attributes: effective datetime.
        roadseg = self.resolve_effective_datetime(
            roadseg, [roadseg, addrange, "orn_jurisdiction", "orn_number_of_lanes", "orn_road_class",
                      "orn_road_surface", "orn_speed_limit", "orn_structure", "orn_route_name", "orn_route_number"])

        # ferryseg
        logger.info("Assembling NRN dataset: ferryseg.")

        # Create ferryseg.
        ferryseg = self.source_datasets[self.base_dataset].query(
            "road_element_type == 'FERRY CONNECTION'").copy(deep=True)
        ferryseg.reset_index(drop=True, inplace=True)
        ferryseg["nid"] = [uuid.uuid4().hex for _ in range(len(ferryseg))]

        # Configure linked route names and numbers.
        ferryseg = self.configure_route_attributes(ferryseg)

        # Resolve roadseg conflicting attributes: effective datetime.
        ferryseg = self.resolve_effective_datetime(ferryseg, [ferryseg, "orn_route_name", "orn_route_number"])

        # blkpassage
        logger.info("Assembling NRN dataset: blkpassage.")

        # Create blkpassage.
        blkpassage = self.source_datasets["orn_blocked_passage"].copy(deep=True)
        blkpassage["nid"] = [uuid.uuid4().hex for _ in range(len(blkpassage))]
        # TODO

        # tollpoint
        logger.info("Assembling NRN dataset: tollpoint.")

        # Create tollpoint.
        tollpoint = self.source_datasets["orn_toll_point"].copy(deep=True)
        tollpoint["nid"] = [uuid.uuid4().hex for _ in range(len(tollpoint))]
        # TODO

        # Store final datasets.
        for name, df in {"addrange": addrange, "blkpassage": blkpassage, "ferryseg": ferryseg, "roadseg": roadseg,
                         "strplaname": strplaname, "tollpoint": tollpoint}.items():
            self.nrn_datasets[name] = df.copy(deep=True)

    def compile_source_datasets(self):
        """Loads source layers into (Geo)DataFrames."""

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

        # Iterate ORN schema.
        for index, items in enumerate(schema.items()):

            layer, cols = items

            logger.info(f"Compiling source layer {index + 1} of {len(schema)}: {layer}.")

            # Load layer into dataframe, force lowercase column names.
            df = gpd.read_file(self.src, driver="OpenFileGDB", layer=layer).rename(columns=str.lower)

            # Filter columns.
            df.drop(columns=df.columns.difference(cols), inplace=True)

            # Convert tabular dataframes.
            if "geometry" not in df.columns:
                df = pd.DataFrame(df)

            # Store results.
            self.source_datasets[layer] = df.copy(deep=True)

    def configure_route_attributes(self, df):
        """Configures the route name and number attributes for the given DataFrame."""

        for route_params in [
            {"df": self.source_datasets["orn_route_name"], "col_from": "route_name_english",
             "cols_to": ["rtename1en", "rtename2en", "rtename3en", "rtename4en"], "na": "None"},
            {"df": self.source_datasets["orn_route_name"], "col_from": "route_name_french",
             "cols_to": ["rtename1fr", "rtename2fr", "rtename3fr", "rtename4fr"], "na": "None"},
            {"df": self.source_datasets["orn_route_number"], "col_from": "route_number",
             "cols_to": ["rtnumber1", "rtnumber2", "rtnumber3", "rtnumber4", "rtnumber5"], "na": "None"}
        ]:

            routes_df = route_params["df"].copy(deep=True)
            col_from, cols_to, na = itemgetter("col_from", "cols_to", "na")(route_params)

            # Filter to valid and unique records.
            routes_df = routes_df[~((routes_df[col_from].isna()) | (routes_df[col_from] == na) |
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
                    routes_subset = routes_df_filtered[routes_df_filtered.map(len) > index].map(itemgetter(index))
                    routes_subset_df = pd.DataFrame({self.source_fk: routes_subset.index, "value": routes_subset})
                    df[col] = df.merge(routes_subset_df, how="left", left_on=self.base_fk,
                                       right_on=self.source_fk)["value"].fillna(value=na)

            else:
                for col in cols_to:
                    df[col] = na

        return df.copy(deep=True)

    def configure_valid_records(self):
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

                df_valid = df[df[self.source_fk].isin(base_fkeys)]
                logger.info(f"Dropped {len(df) - len(df_valid)} of {len(df)} records for dataset: {name}.")

                # Store or delete dataset.
                if len(df_valid):
                    self.source_datasets[name] = df_valid.copy(deep=True)
                else:
                    del self.source_datasets[name]

    def resolve_effective_datetime(self, main_df, linked_dfs):
        """
        Updates the effective_datetime for the given DataFrame from the maximum of all linked DataFrames, for each
        identifier.
        """

        # Compile linked dataframes.
        dfs = list()
        for linked_df in linked_dfs:
            if isinstance(linked_df, str):
                dfs.append(self.source_datasets[linked_df].copy(deep=True))
            else:
                dfs.append(linked_df.copy(deep=True))

        # Concatenate all dataframes.
        dfs_concat = pd.concat([df.rename(columns={self.base_fk: self.source_fk})[[
            self.source_fk, "effective_datetime"]] for df in dfs], ignore_index=True)

        # Group by identifier, configure and assign maximum value.
        main_df.index = main_df[self.base_fk]
        main_df["effective_datetime"] = helpers.groupby_to_list(
            dfs_concat, self.source_fk, "effective_datetime").map(max)
        main_df.reset_index(drop=True, inplace=True)

        return main_df.copy(deep=True)

    def resolve_unsplit_parities(self):
        """
        For paritized attributes, duplicates records where the parity field = 'Both' into 'Left' and 'Right'. This
        makes it easier to reduce lrs attributes.
        """

        logger.info("Resolving unsplit parities.")

        # Iterate paritized datasets and fields.
        for table, field in self.parities.items():

            logger.info(f"Resolving unsplit parities for dataset: {table}, field: {field}.")

            df = self.source_datasets[table].copy(deep=True)

            # Copy unsplit records and update as "Right".
            right_side = df[df[field] == "Both"].copy(deep=True)
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

    def reduce_events(self):
        """
        Reduces many-to-one base dataset events to the event with the longest measurement.
        Exception: address dataset will keep the longest event for both "Left" and "Right" paritized instances.
        """

        def configure_address_structure(structures):
            """Configures the address structure given an iterable of structures."""

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
                        records = df[df[self.parities[name]] == parity].copy(deep=True)

                        # Configure updated address attributes.
                        logger.info("Configuring updated address attributes.")

                        address_attributes = {
                            "first_house_number": helpers.groupby_to_list(
                                records, self.source_fk, "first_house_number").map(min),
                            "last_house_number": helpers.groupby_to_list(
                                records, self.source_fk, "last_house_number").map(max),
                            "house_number_structure": helpers.groupby_to_list(
                                records, self.source_fk, "house_number_structure").map(configure_address_structure),
                            "effective_datetime": helpers.groupby_to_list(
                                records, self.source_fk, "effective_datetime").map(max)
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

    def execute(self):
        """Executes class functionality."""

        self.compile_source_datasets()
        self.configure_valid_records()
        self.resolve_unsplit_parities()
        self.reduce_events()
        self.assemble_nrn_datasets()


@click.command()
@click.argument("src", type=click.Path(exists=True))
@click.argument("dst", type=click.Path(exists=False))
def main(src, dst):
    """Executes the ORN class."""

    try:

        with helpers.Timer():
            orn = ORN(src, dst)
            orn.execute()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
