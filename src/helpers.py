import datetime
import fiona
import geopandas as gpd
import logging
import os
import shutil
import sqlite3
import sys
import time
import yaml


logger = logging.getLogger()


class Timer:
    """Tracks stage runtime."""

    def __init__(self):
        self.start_time = None

    def __enter__(self):
        logger.info("Started.")
        self.start_time = time.time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        total_seconds = time.time() - self.start_time
        delta = datetime.timedelta(seconds=total_seconds)
        logger.info("Finished. Time elapsed: {}.".format(delta))


def export_gpkg(dataframes, output_path, empty_gpkg_path=os.path.abspath("../../data/empty.gpkg")):
    """Receives a dictionary of pandas dataframes and exports them as geopackage layers."""

    # Create gpkg from template if it doesn't already exist.
    if not os.path.exists(output_path):
        shutil.copyfile(empty_gpkg_path, output_path)

    # Export target dataframes to GeoPackage layers.
    try:
        for name, gdf in dataframes.items():

            logger.info("Writing to GeoPackage {}, layer={}.".format(output_path, name))

            # Spatial data.
            if "geometry" in dir(gdf):
                # Open GeoPackage.
                with fiona.open(output_path, "w", layer=name, driver="GPKG", crs=gdf.crs,
                                schema=gpd.io.file.infer_schema(gdf)) as gpkg:

                    # Write to GeoPackage.
                    gpkg.writerecords(gdf.iterfeatures())

            # Tabular data.
            else:
                # Create sqlite connection.
                con = sqlite3.connect(output_path)

                # Write to GeoPackage.
                gdf.to_sql(name, con)

                # Insert record into gpkg_contents metadata table.
                con.cursor().execute("insert into 'gpkg_contents' ('table_name', 'data_type') values "
                                     "('{}', 'attributes');".format(name))

                # Commit and close db connection.
                con.commit()
                con.close()

            logger.info("Successfully exported layer.")

    except (ValueError, fiona.errors.FionaValueError):
        logger.exception("ValueError raised when writing GeoPackage layer.")
        sys.exit(1)


def load_yaml(path):
    """Loads and returns a yaml file."""

    with open(path, "r", encoding="utf8") as f:

        try:
            return yaml.safe_load(f)
        except (ValueError, yaml.YAMLError):
            logger.exception("Unable to load yaml file: {}.".format(path))
