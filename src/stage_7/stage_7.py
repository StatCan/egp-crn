import click
import fiona
import geopandas as gpd
import logging
import numpy as np
import os
import pandas as pd
import pathlib
import shutil
import sys
import zipfile
from itertools import chain
from multiprocessing.pool import ThreadPool
from operator import itemgetter
from osgeo import ogr
from scipy.spatial import Delaunay
from shapely.geometry import Polygon

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

    def __init__(self, source, major_version, minor_version):
        self.stage = 7
        self.source = source.lower()
        self.major_version = major_version
        self.minor_version = minor_version

        # Configure and validate input data path.
        self.data_path = os.path.abspath("../../data/interim/{}.gpkg".format(self.source))
        if not os.path.exists(self.data_path):
            logger.exception("Input data not found: \"{}\".".format(self.data_path))
            sys.exit(1)

        # Configure and validate output data path.
        self.output_path = os.path.abspath("../../data/processed/{}".format(self.source))
        if os.path.exists(self.output_path) and len(os.listdir(self.output_path)) != 0:
            logger.exception("Output namespace already occupied: \"{}\".".format(self.output_path))
            sys.exit(1)

        # Compile output formats.
        self.formats = [os.path.splitext(f)[0] for f in os.listdir("distribution_formats/en")]

    def compile_french_domain_mapping(self):
        """Compiles French field domains mapped to their English equivalents for all dataframes."""

        logger.info("Compiling French field domain mapping.")
        defaults_en = helpers.compile_default_values(lang="en")
        defaults_fr = helpers.compile_default_values(lang="fr")
        distribution_format = helpers.load_yaml(os.path.abspath("../distribution_format.yaml"))
        self.domains_map = dict()

        for suffix in ("en", "fr"):

            # Load yaml.
            domains_yaml = helpers.load_yaml(os.path.abspath("../field_domains_{}.yaml".format(suffix)))

            # Compile domain values.
            # Iterate tables.
            for table in distribution_format:
                # Register table.
                if table not in self.domains_map.keys():
                    self.domains_map[table] = dict()

                # Iterate fields and values.
                for field, vals in domains_yaml["tables"][table].items():
                    # Register field.
                    if field not in self.domains_map[table].keys():
                        self.domains_map[table][field] = list()

                    try:

                        # Configure reference domain.
                        while isinstance(vals, str):
                            table_ref, field_ref = vals.split(";") if vals.find(";") > 0 else [table, vals]
                            vals = domains_yaml["tables"][table_ref][field_ref]

                        # Configure mapping as dict. Format: {English: French}.
                        if vals:

                            vals = vals.values() if isinstance(vals, dict) else vals

                            if suffix == "en":
                                self.domains_map[table][field] = vals
                            else:
                                # Compile mapping.
                                self.domains_map[table][field] = dict(zip(self.domains_map[table][field], vals))

                                # Add default field value.
                                self.domains_map[table][field][defaults_en[table][field]] = defaults_fr[table][field]

                        else:
                            del self.domains_map[table][field]

                    except (AttributeError, KeyError, ValueError):
                        logger.exception("Unable to configure field mapping for English-French domains.")

    def define_kml_bboxes(self, table="roadseg"):
        """
        Defines a set of bounding box extents to divide the kml-bound input GeoDataFrame.
        This is required due to the low feature / size limitations of kml.
        """

        logger.info("Defining KML bounding boxes.")
        df = self.dframes["kml"]["en"][table]

        # Retrieve total bbox.
        total_bbox = df["geometry"].total_bounds

        # Calculate bboxes.
        bbox_size = 0.1
        bboxes = list()

        x0, x1 = total_bbox[0], total_bbox[2]
        while x0 < x1:

            y0, y1 = total_bbox[1], total_bbox[3]
            while y0 < y1:

                # Convert bbox to shapely polygon.
                coords = [x0, y0, x0 + bbox_size, y0 + bbox_size]
                bbox = Polygon([(itemgetter(*indexes)(coords)) for indexes in ((0, 1), (0, 3), (2, 3), (2, 1), (0, 1))])
                bboxes.append(bbox)

                y0 += bbox_size

            x0 += bbox_size

        # Filter invalid bboxes.

        # Create GeoDataFrame from bboxes.
        bboxes_df = gpd.GeoDataFrame(geometry=bboxes)

        # Create Delaunay tesselation from bboxes.
        delaunay = Delaunay(np.concatenate([np.array(geom.exterior.coords) for geom in bboxes]))

        # Compile input feature points.
        feature_pts = np.concatenate([np.array(geom.coords) for geom in df["geometry"]])

        # Find simplices containing feature points.
        indexes = list(set(delaunay.find_simplex(feature_pts)))
        valid_simplices_idx = itemgetter(indexes)(delaunay.simplices)

        # Convert simplex indexes to coordinates.
        valid_simplices_pts = list(map(lambda indexes: itemgetter(*indexes)(delaunay.points), valid_simplices_idx))
        valid_simplices_pts_all = set(map(tuple, chain.from_iterable(valid_simplices_pts)))

        # Identify valid bboxes.
        # Process: Subtract valid simplices coordinates from bbox coordinates.
        valid_bboxes = bboxes_df[bboxes_df["geometry"].map(
            lambda bbox: len(set(bbox.exterior.coords) - valid_simplices_pts_all) == 0)]

        # Identify validity of uncertain bboxes (boundary bboxes which may or may not contain a simplex).
        # Process: Subtract bbox coordinates from valid simplices coordinates, keep bboxes where at least one simplex
        # has 0 remaining coordinates (i.e. the simplex is completely within the bbox).

        # Create GeoDataFrame from simplices.
        valid_simplices_df = pd.DataFrame({"coords": [set(map(tuple, simplex)) for simplex in valid_simplices_pts]})

        # Compile uncertain bboxes.
        bboxes_uncertain = bboxes_df.loc[bboxes_df["geometry"].map(
            lambda bbox: len(set(bbox.exterior.coords) - valid_simplices_pts_all) == 1)]
        bboxes_uncertain_values = bboxes_uncertain["geometry"].map(lambda bbox: set(bbox.exterior.coords)).values

        # Identify valid bboxes.
        valid_bboxes_uncertain = bboxes_uncertain[np.vectorize(
            lambda bbox: valid_simplices_df["coords"].map(
                lambda simplex: len(simplex - bbox) == 0).any())(bboxes_uncertain_values)]

        # Append all valid bboxes.
        valid_bboxes_all = valid_bboxes.append(valid_bboxes_uncertain, ignore_index=True)

        # Store only bbox coords.
        self.bboxes = valid_bboxes_all["geometry"].exterior.map(
            lambda geom: "{} {} {} {}".format(*chain.from_iterable(itemgetter(0, 2)(geom.coords)))).to_list()

    def export_data(self):
        """Exports and packages all data."""

        logger.info("Exporting data.")

        # Iterate formats and languages.
        for frmt in self.dframes:
            for lang in self.dframes[frmt]:

                # Retrieve export specifications.
                export_specs = helpers.load_yaml("distribution_formats/{}/{}.yaml".format(lang, frmt))
                driver_long_name = ogr.GetDriverByName(export_specs["data"]["driver"]).GetMetadata()["DMD_LONGNAME"]

                logger.info("Exporting format: \"{}\", language: \"{}\".".format(driver_long_name, lang))

                # Configure and format export paths and table names.
                export_dir = os.path.join(self.output_path, self.format_path(export_specs["data"]["dir"]))
                export_file = self.format_path(export_specs["data"]["file"]) if export_specs["data"]["file"] else None
                export_tables = {table: self.format_path(export_specs["conform"][table]["name"]) for table in
                                 self.dframes[frmt][lang]}

                # Generate directory structure.
                logger.info("Generating directory structure: \"{}\".".format(export_dir))
                pathlib.Path(export_dir).mkdir(parents=True, exist_ok=True)

                # Export data to temporary file.
                temp_path = os.path.join(os.path.dirname(self.data_path), "{}_temp.gpkg".format(self.source))
                logger.info("Exporting temporary GeoPackage: \"{}\".".format(temp_path))
                helpers.export_gpkg(self.dframes[frmt][lang], temp_path)

                # Export data.
                logger.info("Transforming data format from GeoPackage to {}.".format(driver_long_name))

                # Iterate tables.
                for table in export_tables:

                    # Configure ogr2ogr inputs.
                    kwargs = {
                        "driver": "-f \"{}\"".format(export_specs["data"]["driver"]),
                        "append": "-append",
                        "dest": "\"{}\""
                            .format(os.path.join(export_dir, export_file if export_file else export_tables[table])),
                        "src": "\"{}\"".format(temp_path),
                        "src_layer": table,
                        "nln": "-nln {}".format(export_tables[table]) if export_file else ""
                    }

                    # Iterate bboxes if format=kml.
                    if frmt == "kml":

                        total = len(self.bboxes)
                        tasks = list()

                        # Configure ogr2ogr tasks.
                        for index, bbox in enumerate(self.bboxes):
                            index += 1

                            # Configure logging message.
                            log = "Transforming table: \"{}\" ({} of {}).".format(table, index, total)

                            # Configure ogr2ogr parameters.

                            # Add modified output name to ogr2ogr parameters.
                            if index == 1:
                                kwargs["dest"] = "{}_1{}".format(*os.path.splitext(kwargs["dest"]))
                            else:
                                name, ext = os.path.splitext(kwargs["dest"])
                                kwargs["dest"] = "{2}{3}{0}{1}".format(index, ext, *name.rpartition("_")[:-1])

                            # Add bbox to ogr2ogr parameters.
                            kwargs["clipsrc"] = "-clipsrc {}".format(bbox)

                            # Store task.
                            tasks.append((kwargs.copy(), log))

                        # Execute tasks.
                        with ThreadPool(4) as tpool:
                            tpool.starmap(helpers.ogr2ogr, tasks)
                        tpool.join()

                    else:

                        logger.info("Transforming table: \"{}\".".format(table))

                        # Run ogr2ogr subprocess.
                        helpers.ogr2ogr(kwargs)

                # Delete temporary file.
                logger.info("Deleting temporary GeoPackage: \"{}\".".format(temp_path))
                if os.path.exists(temp_path):
                    driver = ogr.GetDriverByName("GPKG")
                    driver.DeleteDataSource(temp_path)
                    del driver

    def format_path(self, path):
        upper = True if os.path.basename(path)[0].isupper() else False

        for key in ("source", "major_version", "minor_version"):
            val = str(eval("self.{}".format(key)))
            val = val.upper() if upper else val.lower()
            path = path.replace("<{}>".format(key), val)

        return path

    def gen_french_dataframes(self):
        """
        Generate French equivalents of all dataframes.
        Note: Only the data values areupdated, not the column names.
        """

        logger.info("Generating French dataframes.")

        # Reconfigure dataframes dict to hold English and French data.
        dframes = {"en": dict(), "fr": dict()}
        for lang in ("en", "fr"):
            for table, df in self.dframes.items():
                dframes[lang][table] = df.copy(deep=True)

        # Apply data mapping.
        defaults_en = helpers.compile_default_values(lang="en")
        defaults_fr = helpers.compile_default_values(lang="fr")
        table, field = None, None

        try:

            # Iterate dataframes.
            for table, df in dframes["fr"].items():

                logger.info("Applying French data mapping to \"{}\".".format(table))

                # Iterate fields.
                for field in defaults_en[table]:

                    logger.info("Target field: \"{}\".".format(field))

                    # Apply both field domains and defaults mapping.
                    if field in self.domains_map[table]:
                        df[field] = df[field].map(self.domains_map[table][field])

                    # Apply only field defaults mapping.
                    else:
                        df.loc[df[field] == defaults_en[table][field], field] = defaults_fr[table][field]

                # Store resulting dataframe.
                dframes["fr"][table] = df

            # Store results.
            self.dframes = dframes

        except (AttributeError, KeyError, ValueError):
            logger.exception("Unable to apply French data mapping for table: {}, field: {}.".format(table, field))
            sys.exit(1)

    def gen_output_schemas(self):
        """Generate the output schema required for each dataframe and each output format."""

        logger.info("Generating output schemas.")
        frmt, lang, table = None, None, None

        # Reconfigure dataframes dict to hold all formats and languages.
        dframes = {frmt: {"en": dict(), "fr": dict()} for frmt in self.formats}

        try:

            # Iterate formats.
            for frmt in dframes:
                # Iterate languages.
                for lang in dframes[frmt]:

                    # Retrieve schemas.
                    schemas = helpers.load_yaml("distribution_formats/{}/{}.yaml".format(lang, frmt))["conform"]

                    # Iterate tables.
                    for table in [t for t in schemas if t in self.dframes[lang]]:

                        logger.info("Generating output schema for format: \"{}\", language: \"{}\", table: \"{}\"."
                                    .format(frmt, lang, table))

                        # Conform dataframe to output schema.

                        # Retrieve dataframe.
                        df = self.dframes[lang][table].copy(deep=True)

                        # Drop non-required columns.
                        drop_columns = df.columns.difference([*schemas[table]["fields"], "geometry"])
                        df.drop(drop_columns, axis=1, inplace=True)

                        # Conform column names.
                        df.columns = map(lambda col: "geometry" if col == "geometry" else schemas[table]["fields"][col],
                                         df.columns)

                        # Store results.
                        dframes[frmt][lang][table] = df

            # Store result.
            self.dframes = dframes

        except (AttributeError, KeyError, ValueError):
            logger.exception("Unable to apply output schema for format: {}, language: {}, table: {}."
                             .format(frmt, lang, table))
            sys.exit(1)

    def load_gpkg(self):
        """Loads input GeoPackage layers into dataframes."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

    def zip_data(self):
        """Compresses all exported data directories to .zip format."""

        logger.info("Apply .zip compression to output data directories.")

        # Iterate output directories.
        root = os.path.abspath("../../data/processed/{}".format(self.source))
        for data_dir in os.listdir(root):

            data_dir = os.path.join(root, data_dir)

            # Walk directory and zip contents.
            logger.info("Applying .zip compression to directory \"{}\".".format(data_dir))

            try:

                with zipfile.ZipFile("{}.zip".format(data_dir), "w") as zip:
                    for dir, subdirs, files in os.walk(data_dir):
                        for file in files:

                            # Configure path.
                            path = os.path.join(dir, file)

                            # Configure new relative path inside .zip file.
                            arcname = os.path.join(os.path.basename(data_dir), os.path.relpath(path, data_dir))

                            # Write to .zip file.
                            zip.write(path, arcname)

            except (zipfile.BadZipFile, zipfile.LargeZipFile) as e:
                logger.exception("Unable to compress directory.")
                logger.exception("zipfile error: {}".format(e))
                sys.exit(1)

            # Remove original directory.
            logger.info("Removing original directory: \"{}\".".format(data_dir))

            try:

                shutil.rmtree(data_dir)

            except (OSError, shutil.Error) as e:
                logger.exception("Unable to remove directory.")
                logger.exception("shutil error: {}".format(e))
                sys.exit(1)

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.compile_french_domain_mapping()
        self.gen_french_dataframes()
        self.gen_output_schemas()
        self.define_kml_bboxes()
        self.export_data()
        self.zip_data()


@click.command()
@click.argument("source", type=click.Choice("ab bc mb nb nl ns nt nu on pe qc sk yt parks_canada".split(), False))
@click.argument("major_version")
@click.argument("minor_version")
def main(source, major_version, minor_version):
    """Executes an NRN stage."""

    try:

        with helpers.Timer():
            stage = Stage(source, major_version, minor_version)
            stage.execute()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)

if __name__ == "__main__":
    main()
