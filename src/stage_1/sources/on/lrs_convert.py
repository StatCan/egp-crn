import click
import geopandas as gpd
import logging
import numpy as np
import os
import pandas as pd
import sys
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
        self.irreducible_datasets = ["orn_road_net_element", "orn_blocked_passage", "orn_toll_point"]
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

    def assemble_base_dataset(self):
        """Assembles the base dataset attributes from all linked datasets and resolves any conflicting attributes."""

        # TODO

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
            "orn_structure": [
                "orn_road_net_element_id", "from_measure", "to_measure", "structure_type", "structure_name_english",
                "structure_name_french", "effective_datetime"
            ],
            "orn_toll_point": [
                "orn_road_net_element_id", "at_measure", "toll_point_type", "effective_datetime"
            ],
            "orn_jurisdiction": [
                "orn_road_net_element_id", "from_measure", "to_measure", "street_side", "jurisdiction",
                "effective_datetime"
            ],
            "orn_number_of_lanes": [
                "orn_road_net_element_id", "from_measure", "to_measure", "number_of_lanes", "effective_datetime"
            ],
            "orn_official_street_name": [
                "orn_road_net_element_id", "from_measure", "to_measure", "full_street_name", "effective_datetime"
            ],
            "orn_road_class": [
                "orn_road_net_element_id", "from_measure", "to_measure", "road_class", "effective_datetime"
            ],
            "orn_road_net_element": [
                "ogf_id", "road_absolute_accuracy", "direction_of_traffic_flow", "exit_number", "road_element_type",
                "acquisition_technique", "creation_date", "effective_datetime", "geometry"
            ],
            "orn_street_name_parsed": [
                "full_street_name", "directional_prefix", "street_type_prefix", "street_name_body",
                "street_type_suffix", "directional_suffix", "effective_datetime"
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

    def configure_secondary_linkages(self):
        """Joins datasets which do not directly link to the base dataset."""

        logger.info("Configuring secondary dataset linkages.")

        # Define linkages
        linkages = [
            {"left": "orn_official_street_name", "right": "orn_street_name_parsed", "left_on": "full_street_name",
             "right_on": "full_street_name"},
        ]

        # Apply linkages.
        for linkage in linkages:

            logger.info(f"Configuring linkage: {linkage['left_on']} - {linkage['right_on']}.")

            # Merge dataframes.
            self.source_datasets[linkage["left"]] = self.source_datasets[linkage["left"]].merge(
                self.source_datasets[linkage["right"]], how="left", left_on=linkage["left_on"],
                right_on=linkage["right_on"], suffixes=("", "_merge"))

            # Delete right dataframe.
            del self.source_datasets[linkage["right"]]

    def configure_valid_records(self):
        """Configures and keeps only records which link to valid records from the base dataset."""

        logger.info(f"Configuring valid records.")

        # Filter base dataset to valid records.
        logger.info(f"Configuring valid records for base dataset: {self.base_dataset}.")
        self.source_datasets[self.base_dataset].query(self.base_query, inplace=True)

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
        Exception: paritized fields keep the longest event for both "Left" and "Right" instances.
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

                # Handle paritized fields.
                if name in self.parities:

                    dfs = list()
                    for parity in ("Left", "Right"):

                        logger.info(f"Paritized dataset detected. Reducing dataset for parity: {parity}.")

                        # Get parity records.
                        records = df[df[self.parities[name]] == parity].copy(deep=True)

                        # Handle address ranges.
                        address_attributes = dict()
                        if name == self.address_dataset:

                            logger.info("Address dataset detected. Configuring updated address attributes.")

                            # Configure updated address attributes.
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

                        # Update address attributes, if possible.
                        if len(address_attributes):

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
        self.configure_secondary_linkages()
        self.configure_valid_records()
        self.resolve_unsplit_parities()
        self.reduce_events()
        self.assemble_base_dataset()


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
