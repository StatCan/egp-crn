import copy
import fiona
import geopandas as gpd
import logging
import os
import sqlite3
import sys
import yaml


logger = logging.getLogger()


def export_gpkg(dataframes, gpkg_path):
    """Receives a dictionary of pandas dataframes and exports them as geopackage layers."""

    # Create gpkg from template if it doesn't already exist.
    if not os.path.exists(gpkg_path):
        copy(os.path.abspath("../data/empty.gpkg"), gpkg_path)

    # Export target dataframes to GeoPackage layers.
    try:
        for name, gdf in dataframes.items():

            logger.info("Writing to GeoPackage {}, layer={}.".format(gpkg_path, name))

            # Spatial data.
            if "geometry" in dir(gdf):
                # Open GeoPackage.
                with fiona.open(gpkg_path, "w", layer=name, driver="GPKG", crs=gdf.crs,
                                schema=gpd.io.file.infer_schema(gdf)) as gpkg:

                    # Write to GeoPackage.
                    gpkg.writerecords(gdf.iterfeatures())

            # Tabular data.
            else:
                # Create sqlite connection.
                con = sqlite3.connect(gpkg_path)

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
