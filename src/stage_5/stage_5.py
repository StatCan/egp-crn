import click
import json
import logging
import multiprocessing
import numpy as np
import os
import pandas as pd
import pathlib
import re
import shutil
import sys
import zipfile
from copy import deepcopy
from datetime import datetime
from operator import itemgetter
from osgeo import ogr
from queue import Queue
from threading import Thread

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

    def __init__(self, source):
        self.stage = 5
        self.source = source.lower()
        self.major_version = None
        self.minor_version = None

        # Configure and validate input data path.
        self.data_path = os.path.abspath("../../data/interim/{}.gpkg".format(self.source))
        if not os.path.exists(self.data_path):
            logger.exception("Input data not found: \"{}\".".format(self.data_path))
            sys.exit(1)

        # Configure and validate output data path. Only one directory source_change_logs can pre-exist in out dir.
        self.output_path = os.path.abspath("../../data/processed/{}".format(self.source))
        if os.path.exists(self.output_path):
            if not set(os.listdir(self.output_path)).issubset(
                    {"{}_change_logs".format(self.source), "{}_change_logs".format(self.source)}):
                logger.exception("Output namespace already occupied: \"{}\".".format(self.output_path))
                sys.exit(1)

        # Compile output formats.
        self.formats = [os.path.splitext(f)[0] for f in os.listdir("distribution_formats/en")]

    def configure_release_version(self):
        """Configures the major and minor release versions for the current NRN vintage."""

        logger.info("Configuring NRN release version.")
        source = helpers.load_yaml("../downloads.yaml")["previous_nrn_vintage"]

        # Retrieve metadata for previous NRN vintage.
        logger.info("Retrieving metadata for previous NRN vintage.")
        metadata_url = source["metadata_url"].replace("<id>", source["ids"][self.source])

        # Get metadata from url.
        metadata = helpers.get_url(metadata_url, timeout=30)

        # Extract release year and version numbers from metadata.
        metadata = json.loads(metadata.content)
        release_year = int(metadata["result"]["metadata_created"][:4])
        self.major_version, self.minor_version = list(
            map(int, re.findall(r"\d+", metadata["result"]["resources"][0]["url"])[-2:]))

        # Conditionally set major and minor version numbers.
        if release_year == datetime.now().year:
            self.minor_version += 1
        else:
            self.major_version += 1
            self.minor_version = 0

    def define_kml_groups(self):
        """
        Defines groups by which to segregate the kml-bound input GeoDataFrame.
        This is required due to the low feature and size limitations of kml.
        """

        logger.info("Defining KML groups.")
        self.kml_groups = dict()
        placenames, placenames_exceeded = None, None

        # Iterate languages.
        for lang in ("en", "fr"):

            logger.info("Defining KML groups for language: \"{}\".".format(lang))

            # Determine language-specific field names.
            l_placenam, r_placenam = itemgetter("l_placenam", "r_placenam")(
                helpers.load_yaml("distribution_formats/{}/kml.yaml".format(lang))["conform"]["roadseg"]["fields"])

            # Retrieve source dataframe.
            df = self.dframes["kml"][lang]["roadseg"].copy(deep=True)

            # Compile placenames.
            if placenames is None:
                placenames = sorted(set(np.append(df[l_placenam].unique(), df[r_placenam].unique())))
                placenames = pd.Series(placenames)

            # Generate placenames dataframe.
            # names: Conform placenames to valid file names.
            # queries: Configure ogr2ogr -where query.
            placenames_df = pd.DataFrame({
                "names": map(lambda name: re.sub("[\W_]+", "_", name), placenames),
                "queries": placenames.map(lambda name: "-where \"\\\"{0}\\\"='{2}' or \\\"{1}\\\"='{2}'\""
                                          .format(l_placenam, r_placenam, name.replace("'", "''")))
            })

            # Identify placenames exceeding feature limit.
            if placenames_exceeded is None:
                logger.info("Identifying placenames with excessive feature totals.")

                limit = 1000
                flags = np.vectorize(
                    lambda name: len(df[(df[l_placenam] == name) | (df[r_placenam] == name)]) > limit)(
                    placenames_df["names"])
                placenames_exceeded = placenames_df[flags]["names"]

            # Remove exceeding placenames from placenames dataframe.
            placenames_df = placenames_df[~placenames_df["names"].isin(placenames_exceeded)]

            # Add rowid field to simulate SQLite column.
            df["ROWID"] = range(1, len(df) + 1)

            # Compile limit-sized rowid ranges for each exceeding placename.
            for placename in placenames_exceeded:

                logger.info("Separating features for limit-exceeding placename: \"{}\".".format(placename))

                # Compile rowids for placename.
                rowids = df[(df[l_placenam] == placename) | (df[r_placenam] == placename)]["ROWID"].values

                # Compile feature index bounds based on feature limit.
                bounds = list(itemgetter([i*limit for i in range(0, int(len(rowids) / limit) +
                                                                 (0 if (len(rowids) % limit == 0) else 1))])(rowids))
                bounds.append(rowids[-1] + 1)

                # Configure placename sql statements for each feature bounds.
                sql_statements = list(map(
                    lambda vals: "(ROWID >= {0} and ROWID < {1}) and ({2} = '{4}' or {3} = '{4}')"
                        .format(vals[1], bounds[vals[0] + 1], l_placenam, r_placenam, placename.replace("'", "''")),
                    enumerate(bounds[:-1])
                ))

                # Add sql statements to placenames dataframe.
                rows = pd.DataFrame({
                    "names": map(lambda i: "{}_{}".format(placename, i), range(1, len(sql_statements) + 1)),
                    "queries": map("-sql \"select * from roadseg where {}\" -dialect SQLITE".format, sql_statements)
                })

                # Append new rows to placenames dataframe.
                placenames_df = placenames_df.append(rows).reset_index(drop=True)

            # Store results.
            self.kml_groups[lang] = placenames_df

    def export_data(self):
        """Exports and packages all data."""

        logger.info("Exporting output data.")

        # Configure and store tasks.
        tasks_mp = [(frmt, lang) for frmt in self.dframes for lang in self.dframes[frmt]]

        # Execute tasks in process pool.
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
        pool.map_async(self.multiprocess_export_data, tasks_mp).get()

    def export_temp_data(self):
        """
        Exports temporary data as GeoPackages.
        Temporary file is required since ogr2ogr, which is used for data transformation, is file based.
        """

        # Export temporary files.
        logger.info("Exporting temporary GeoPackages.")

        # Configure and store tasks.
        tasks_mp = list()
        for frmt in self.dframes:
            for lang in self.dframes[frmt]:
                temp_path = os.path.abspath("../../data/interim/{}_{}_{}_temp.gpkg".format(self.source, frmt, lang))
                tasks_mp.append((self.dframes[frmt][lang], temp_path))

        # Execute tasks in process pool.
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
        pool.map_async(self.multiprocess_export_gpkg, tasks_mp).get()

    def format_path(self, path):
        """Formats a path with class variables: source, major_version, minor_version."""

        upper = True if os.path.basename(path)[0].isupper() else False

        for key in ("source", "major_version", "minor_version"):
            val = str(eval("self.{}".format(key)))
            val = val.upper() if upper else val.lower()
            path = path.replace("<{}>".format(key), val)

        return path

    def gen_french_dataframes(self):
        """
        Generate French equivalents of all dataframes.
        Note: Only the data values are updated, not the column names.
        """

        logger.info("Generating French dataframes.")

        # Reconfigure dataframes dict to hold English and French data.
        dframes = {
            "en": {table: df.copy(deep=True) for table, df in self.dframes.items()},
            "fr": {table: df.copy(deep=True) for table, df in self.dframes.items()}
        }
        self.dframes = deepcopy(dframes)

        # Compile defaults and domains.
        defaults_en = helpers.compile_default_values(lang="en")
        defaults_fr = helpers.compile_default_values(lang="fr")
        domains = helpers.compile_domains(mapped_lang="fr")

        # Map field domains to French equivalents.
        table = None
        field = None

        try:

            # Iterate dataframes and fields.
            for table, df in dframes["fr"].items():
                for field, domain in domains[table].items():

                    logger.info(f"Applying domain to table: {table}, field: {field}.")

                    # Apply domain to series.
                    series = df[field].copy(deep=True)
                    series = helpers.apply_domain(series, domain["lookup"], defaults_fr[table][field])

                    # Convert defaults to French equivalents.
                    series.loc[series == defaults_en[table][field]] = defaults_fr[table][field]

                    # Store results to dataframe.
                    self.dframes["fr"][table][field] = series.copy(deep=True)

        except (AttributeError, KeyError, ValueError):
            logger.exception(f"Unable to apply French domain mapping for table: {table}, field: {field}.")
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

    def multiprocess_export_data(self, task_mp):
        """Exports and packages data via multiprocessing."""

        # Retrieve export format and language from multiprocessing task.
        frmt, lang = task_mp

        # Retrieve export specifications.
        export_specs = helpers.load_yaml("distribution_formats/{}/{}.yaml".format(lang, frmt))
        driver_long_name = ogr.GetDriverByName(export_specs["data"]["driver"]).GetMetadata()["DMD_LONGNAME"]

        logger.info("Exporting format: \"{}\", language: \"{}\".".format(driver_long_name, lang))

        # Configure temporary data path.
        temp_path = os.path.abspath("../../data/interim/{}_{}_{}_temp.gpkg".format(self.source, frmt, lang))

        # Configure and format export paths and table names.
        export_dir = os.path.join(self.output_path, self.format_path(export_specs["data"]["dir"]))
        export_file = self.format_path(export_specs["data"]["file"]) if export_specs["data"]["file"] else None
        export_tables = {table: self.format_path(export_specs["conform"][table]["name"]) for table in
                         self.dframes[frmt][lang]}

        # Generate directory structure.
        logger.info("Generating directory structure: \"{}\".".format(export_dir))
        pathlib.Path(export_dir).mkdir(parents=True, exist_ok=True)

        # Export data.
        logger.info("Transforming data format from GeoPackage to {}.".format(driver_long_name))

        # Iterate tables.
        for table in export_tables:

            # Configure ogr2ogr inputs.
            kwargs = {
                "driver": "-f \"{}\"".format(export_specs["data"]["driver"]),
                "append": "-append",
                "pre_args": "",
                "dest": "\"{}\"".format(os.path.join(export_dir, export_file if export_file else export_tables[table])),
                "src": "\"{}\"".format(temp_path),
                "src_layer": table,
                "nln": "-nln {}".format(export_tables[table]) if export_file else ""
            }

            # Iterate kml groups.
            if frmt == "kml":

                # Remove ogr2ogr src layer parameter since kml exporting uses -sql.
                # This is purely to avoid an ogr2ogr warning.
                if "src_layer" in kwargs:
                    del kwargs["src_layer"]

                # Compile kml groups and initialize tasks queue.
                kml_groups = self.kml_groups[lang]
                total = len(kml_groups)
                tasks_threading = Queue(maxsize=0)

                # Configure ogr2ogr tasks.
                for index, task in kml_groups.iterrows():
                    index += 1

                    # Configure logging message.
                    log = "Transforming table: \"{}\" ({} of {}: \"{}\").".format(table, index, total, task[0])

                    # Add kml group name and query to ogr2ogr parameters.
                    path, ext = os.path.splitext(kwargs["dest"])
                    kwargs["dest"] = os.path.join(os.path.dirname(path), task[0]) + ext
                    kwargs["pre_args"] = task[1]

                    # Store task.
                    tasks_threading.put((kwargs.copy(), log))

                # Execute tasks.
                for t in range(multiprocessing.cpu_count()):
                    worker = Thread(target=self.thread_ogr2ogr, args=(tasks_threading,))
                    worker.setDaemon(True)
                    worker.start()
                tasks_threading.join()

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

    def multiprocess_export_gpkg(self, task):
        """Calls helpers.export_gpkg via multiprocessing."""

        helpers.export_gpkg(*task)

    def thread_ogr2ogr(self, tasks):
        """Calls helpers.ogr2ogr via threads."""

        while not tasks.empty():

            task = tasks.get()
            helpers.ogr2ogr(*task)
            tasks.task_done()

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

                with zipfile.ZipFile("{}.zip".format(data_dir), "w") as zip_f:
                    for dir, subdirs, files in os.walk(data_dir):
                        for file in files:

                            # Configure path.
                            path = os.path.join(dir, file)

                            # Configure new relative path inside .zip file.
                            arcname = os.path.join(os.path.basename(data_dir), os.path.relpath(path, data_dir))

                            # Write to .zip file.
                            zip_f.write(path, arcname)

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
        self.configure_release_version()
        self.gen_french_dataframes()
        self.gen_output_schemas()
        self.define_kml_groups()
        self.export_temp_data()
        self.export_data()
        self.zip_data()


@click.command()
@click.argument("source", type=click.Choice("ab bc mb nb nl ns nt nu on pe qc sk yt".split(), False))
def main(source):
    """Executes an NRN stage."""

    try:

        with helpers.Timer():
            stage = Stage(source)
            stage.execute()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)

if __name__ == "__main__":
    main()
