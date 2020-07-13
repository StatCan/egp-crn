import click
import geopandas as gpd
import logging
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
        """
        Configures and keeps only valid records for each source dataset.
        1) Removes records which do not link to the base dataset.
        2) Filters many-to-one relationships to the record with the longest event along the base dataset.
        """

        # TODO

    def execute(self):
        """Executes class functionality."""

        self.compile_source_datasets()
        self.configure_secondary_linkages()
        self.configure_valid_records()
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
