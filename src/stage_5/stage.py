import click
import logging
import os
import pandas as pd
import pathlib
import re
import shutil
import sys
import zipfile
from collections import Counter
from copy import deepcopy
from datetime import datetime
from operator import itemgetter
from osgeo import ogr
from tqdm import tqdm

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

    def __init__(self, source: str, remove: bool = False) -> None:
        """
        Initializes an NRN stage.

        :param str source: abbreviation for the source province / territory.
        :param bool remove: removes pre-existing files within the data/processed directory for the specified source,
            excluding change logs, default False.
        """

        self.stage = 5
        self.source = source.lower()
        self.remove = remove
        self.major_version = None
        self.minor_version = None

        # Configure and validate input data path.
        self.data_path = os.path.abspath(f"../../data/interim/{self.source}.gpkg")
        if not os.path.exists(self.data_path):
            logger.exception(f"Input data not found: {self.data_path}.")
            sys.exit(1)

        # Configure output path.
        self.output_path = os.path.abspath(f"../../data/processed/{self.source}")

        # Conditionally clear output namespace.
        namespace = set(map(lambda f: os.path.join(self.output_path, f),
                            set(os.listdir(self.output_path)) - {f"{self.source}_change_logs"}))

        if len(namespace):
            logger.warning("Output namespace already occupied.")

            if self.remove:
                logger.warning("Parameter remove=True: Removing conflicting files.")

                for f in namespace:
                    logger.info(f"Removing conflicting file: \"{f}\".")

                    try:
                        if os.path.isdir(f):
                            shutil.rmtree(f)
                        else:
                            os.remove(f)
                    except OSError as e:
                        logger.exception(f"Unable to remove file: \"{f}\".")
                        logger.exception(e)
                        sys.exit(1)

            else:
                logger.exception(
                    "Parameter remove=False: Unable to proceed while output namespace is occupied. Set "
                    "remove=True (-r) or manually clear the output namespace.")
                sys.exit(1)

        # Compile output formats.
        self.formats = [os.path.splitext(f)[0] for f in os.listdir("distribution_formats/en")]

        # Configure field defaults and domains.
        self.defaults = {lang: helpers.compile_default_values(lang=lang) for lang in ("en", "fr")}
        self.domains = helpers.compile_domains(mapped_lang="fr")

    def configure_release_version(self) -> None:
        """Configures the major and minor release versions for the current NRN vintage."""

        logger.info("Configuring NRN release version.")

        # Iterate release notes to extract the version number and release year for current source.
        release_year = None
        release_notes = os.path.abspath("../../docs/release_notes.rst")

        try:

            for line in open(release_notes, "r"):
                if line.find(self.source.upper()) >= 0:
                    specs = [val for val in line.split(" ") if val != ""]
                    self.major_version, self.minor_version = list(map(int, specs[2].split(".")))
                    release_year = int(specs[3][:4])
                    break

        except (IndexError, ValueError) as e:
            logger.exception(f"Unable to extract version number and / or release date from \"{release_notes}\".")
            logger.exception(e)
            sys.exit(1)

        # Note: can't use 'not any' logic since 0 is an acceptable minor version value.
        if any(val is None for val in [self.major_version, self.minor_version, release_year]):
            logger.exception(f"Unable to extract version number and / or release date from \"{release_notes}\".")
            sys.exit(1)

        # Conditionally set major and minor version numbers.
        if release_year == datetime.now().year:
            self.minor_version += 1
        else:
            self.major_version += 1
            self.minor_version = 0

    def define_kml_groups(self) -> None:
        """
        Defines groups by which to segregate the kml-bound input GeoDataFrame.
        This is required due to the low feature and size limitations of kml.
        """

        logger.info("Defining KML groups.")
        self.kml_groups = dict()
        placenames, placenames_exceeded = None, None
        kml_limit = 250

        # Iterate languages.
        for lang in ("en", "fr"):

            logger.info(f"Defining KML groups for language: {lang}.")

            # Determine language-specific field names.
            l_placenam, r_placenam = itemgetter("l_placenam", "r_placenam")(
                helpers.load_yaml(f"distribution_formats/{lang}/kml.yaml")["conform"]["roadseg"]["fields"])

            # Retrieve source dataframe.
            df = self.dframes["kml"][lang]["roadseg"].copy(deep=True)

            # Compile placenames.
            if placenames is None:

                # Compile sorted placenames.
                placenames = pd.concat([df[l_placenam], df[df[l_placenam] != df[r_placenam]][r_placenam]],
                                       ignore_index=True).sort_values(ascending=True)

                # Flag limit-exceeding and non-limit-exceeding placenames.
                placenames_exceeded = {name for name, count in Counter(placenames).items() if count > kml_limit}
                placenames = pd.Series(sorted(set(placenames.unique()) - placenames_exceeded))

                # Sanitize placenames for sql syntax.
                placenames = placenames.map(lambda name: name.replace("'", "''"))
                placenames_exceeded = set(map(lambda name: name.replace("'", "''"), placenames_exceeded))

            # Swap English-French default placename.
            else:
                default_add = self.defaults[lang]["roadseg"]["l_placenam"]
                default_rm = self.defaults["en" if lang == "fr" else "fr"]["roadseg"]["l_placenam"]
                if default_rm in placenames:
                    placenames = {*placenames - {default_rm}, default_add}
                else:
                    placenames_exceeded = {*placenames_exceeded - {default_rm}, default_add}

            # Generate dataframe with export parameters.
            # names: Conform placenames to valid file names.
            # queries: Configure ogr2ogr -where query.
            placenames_df = pd.DataFrame({
                "names": map(lambda name: re.sub(r"[\W_]+", "_", name), placenames),
                "queries": placenames.map(
                    lambda name: f"-where \"\\\"{l_placenam}\\\"='{name}' or \\\"{r_placenam}\\\"='{name}'\"")
            })

            # Add rowid field to simulate SQLite column.
            df["ROWID"] = range(1, len(df) + 1)

            # Compile rowid ranges of size=kml_limit for each limit-exceeding placename.
            for placename in sorted(placenames_exceeded):

                logger.info(f"Separating features for limit-exceeding placename: {placename}.")

                # Compile rowids for placename.
                rowids = df[(df[l_placenam] == placename) | (df[r_placenam] == placename)]["ROWID"].values

                # Split rowids into kml_limit-sized chunks and configure sql queries.
                sql_queries = list()
                for i in range(0, len(rowids), kml_limit):
                    ids = ','.join(map(str, rowids[i: i + kml_limit]))
                    sql_queries.append(f"-sql \"select * from roadseg where ROWID in ({ids})\" -dialect SQLITE")

                # Generate dataframe with export parameters.
                # names: Conform placenames to valid file names and add chunk id as suffix.
                # queries: Configure ogr2ogr -sql query.
                placename_valid = re.sub(r"[\W_]+", "_", placename)
                placenames_exceeded_df = pd.DataFrame({
                    "names": map(lambda i: f"{placename_valid}_{i}", range(1, len(sql_queries) + 1)),
                    "queries": sql_queries
                })

                # Append dataframe to full placenames dataframe.
                placenames_df = placenames_df.append(placenames_exceeded_df).reset_index(drop=True)

            # Store results.
            self.kml_groups[lang] = placenames_df

    def export_data(self) -> None:
        """Exports and packages all data."""

        logger.info("Exporting output data.")

        # Iterate export formats and languages.
        for frmt in self.dframes:
            for lang in self.dframes[frmt]:

                logger.info(f"Format: {frmt}, language: {lang}; configuring export parameters.")

                # Retrieve export specifications.
                export_specs = helpers.load_yaml(f"distribution_formats/{lang}/{frmt}.yaml")

                # Configure temporary data path.
                temp_path = os.path.abspath(f"../../data/interim/{self.source}_{frmt}_{lang}_temp.gpkg")

                # Configure and format export paths and table names.
                export_dir = os.path.join(self.output_path, self.format_path(export_specs["data"]["dir"]))
                export_file = self.format_path(export_specs["data"]["file"]) if export_specs["data"]["file"] else None
                export_tables = {table: self.format_path(export_specs["conform"][table]["name"]) for table in
                                 self.dframes[frmt][lang]}

                # Generate directory structure.
                logger.info(f"Format: {frmt}, language: {lang}; generating directory structure.")
                pathlib.Path(export_dir).mkdir(parents=True, exist_ok=True)

                # Iterate tables.
                for table in export_tables:

                    logger.info(f"Format: {frmt}, language: {lang}, table: {table}; configuring ogr2ogr parameters.")

                    # Configure ogr2ogr inputs.
                    kwargs = {
                        "driver": f"-f \"{export_specs['data']['driver']}\"",
                        "append": "-append",
                        "pre_args": "",
                        "dest": f"\"{os.path.join(export_dir, export_file if export_file else export_tables[table])}\"",
                        "src": f"\"{temp_path}\"",
                        "src_layer": table,
                        "nln": f"-nln {export_tables[table]}" if export_file else ""
                    }

                    # Handle kml.
                    if frmt == "kml":

                        # Remove ogr2ogr src layer parameter since kml exporting uses -sql.
                        # This is purely to avoid an ogr2ogr warning.
                        if "src_layer" in kwargs:
                            del kwargs["src_layer"]

                        # Configure kml path properties.
                        kml_groups = self.kml_groups[lang]
                        kml_path, kml_ext = os.path.splitext(kwargs["dest"])

                        # Iterate kml groups.
                        for kml_group in tqdm(kml_groups.itertuples(index=False), total=len(kml_groups),
                                              desc=f"Format: {frmt}, language: {lang}, table: {table}; generating "
                                                   f"output"):

                            # Add kml group name and query to ogr2ogr parameters.
                            name, query = itemgetter("names", "queries")(kml_group._asdict())
                            kwargs["dest"] = f"{os.path.join(os.path.dirname(kml_path), name)}{kml_ext}"
                            kwargs["pre_args"] = query

                            # Run ogr2ogr subprocess.
                            helpers.ogr2ogr(kwargs)

                    else:

                        logger.info(f"Format: {frmt}, language: {lang}, table: {table}; generating output: "
                                    f"{kwargs['dest']}.")

                        # Run ogr2ogr subprocess.
                        helpers.ogr2ogr(kwargs)

                # Delete temporary file.
                logger.info(f"Format: {frmt}, language: {lang}; deleting temporary GeoPackage.")
                if os.path.exists(temp_path):
                    driver = ogr.GetDriverByName("GPKG")
                    driver.DeleteDataSource(temp_path)
                    del driver

    def export_temp_data(self) -> None:
        """
        Exports temporary data as GeoPackages.
        Temporary file is required since ogr2ogr (which is used for data transformation) is file based.
        """

        # Export temporary files.
        logger.info("Exporting temporary GeoPackages.")

        # Iterate formats and languages.
        for frmt in self.dframes:
            for lang in self.dframes[frmt]:

                # Configure paths.
                temp_path = os.path.abspath(f"../../data/interim/{self.source}_{frmt}_{lang}_temp.gpkg")
                export_schemas_path = os.path.abspath(f"distribution_formats/{lang}/{frmt}.yaml")

                # Export to GeoPackage.
                helpers.export_gpkg(self.dframes[frmt][lang], temp_path, export_schemas_path)

    def format_path(self, path: str) -> str:
        """
        Formats a path with class variables: source, major_version, minor_version.

        :param str path: string path requiring formatting.
        :return str: formatted path.
        """

        upper = True if os.path.basename(path)[0].isupper() else False

        for key in ("source", "major_version", "minor_version"):
            val = str(eval(f"self.{key}"))
            val = val.upper() if upper else val.lower()
            path = path.replace(f"<{key}>", val)

        return path

    def gen_french_dataframes(self) -> None:
        """
        Generate French equivalents of all NRN datasets.
        Note: Only the data values are updated, not the column names.
        """

        logger.info("Generating French dataframes.")

        # Reconfigure dataframes dict to hold English and French data.
        dframes = {
            "en": {table: df.copy(deep=True) for table, df in self.dframes.items()},
            "fr": {table: df.copy(deep=True) for table, df in self.dframes.items()}
        }
        self.dframes = deepcopy(dframes)

        # Apply French translations to field values.
        table = None
        field = None

        try:

            # Iterate dataframes and fields.
            for table, df in dframes["fr"].items():
                for field in set(df.columns) - {"uuid", "geometry"}:

                    logger.info(f"Applying French translations for table: {table}, field: {field}.")

                    series = df[field].copy(deep=True)

                    # Translate domain values.
                    if field in self.domains[table]:
                        series = helpers.apply_domain(series, self.domains[table][field]["lookup"],
                                                      self.defaults["fr"][table][field])

                    # Translate default values and Nones.
                    series.loc[series == self.defaults["en"][table][field]] = self.defaults["fr"][table][field]
                    series.loc[series == "None"] = "Aucun"

                    # Store results to dataframe.
                    self.dframes["fr"][table][field] = series.copy(deep=True)

        except (AttributeError, KeyError, ValueError):
            logger.exception(f"Unable to apply French translations for table: {table}, field: {field}.")
            sys.exit(1)

    def gen_output_schemas(self) -> None:
        """Generate the output schema required for each NRN dataset and each output format."""

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
                    schemas = helpers.load_yaml(f"distribution_formats/{lang}/{frmt}.yaml")["conform"]

                    # Iterate tables.
                    for table in [t for t in schemas if t in self.dframes[lang]]:

                        logger.info(f"Generating output schema for format: {frmt}, language: {lang}, table: {table}.")

                        # Conform dataframe to output schema.

                        # Retrieve dataframe.
                        df = self.dframes[lang][table].copy(deep=True)

                        # Drop non-required columns.
                        drop_columns = df.columns.difference([*schemas[table]["fields"], "geometry"])
                        df.drop(columns=drop_columns, inplace=True)

                        # Map column names.
                        df.rename(columns=schemas[table]["fields"], inplace=True)

                        # Store results.
                        dframes[frmt][lang][table] = df

            # Store result.
            self.dframes = dframes

        except (AttributeError, KeyError, ValueError):
            logger.exception(f"Unable to apply output schema for format: {frmt}, language: {lang}, table: {table}.")
            sys.exit(1)

    def load_gpkg(self) -> None:
        """Loads input GeoPackage layers into (Geo)DataFrames."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

    def zip_data(self) -> None:
        """Compresses all exported data directories into .zip files."""

        logger.info("Apply compression and zip to output data directories.")

        # Iterate output directories.
        root = os.path.abspath(f"../../data/processed/{self.source}")
        for data_dir in os.listdir(root):

            data_dir = os.path.join(root, data_dir)

            # Walk directory, compress, and zip contents.
            logger.info(f"Applying compression and writing .zip from directory {data_dir}.")

            try:

                with zipfile.ZipFile(f"{data_dir}.zip", "w") as zip_f:
                    for dir, subdirs, files in os.walk(data_dir):
                        for file in files:

                            # Configure path.
                            path = os.path.join(dir, file)

                            # Configure new relative path inside .zip file.
                            arcname = os.path.join(os.path.basename(data_dir), os.path.relpath(path, data_dir))

                            # Write to and compress .zip file.
                            zip_f.write(path, arcname=arcname, compress_type=zipfile.ZIP_DEFLATED)

            except (zipfile.BadZipFile, zipfile.LargeZipFile) as e:
                logger.exception("Unable to compress directory.")
                logger.exception(e)
                sys.exit(1)

            # Remove original directory.
            logger.info(f"Removing original directory: {data_dir}.")

            try:

                shutil.rmtree(data_dir)

            except (OSError, shutil.Error) as e:
                logger.exception("Unable to remove directory.")
                logger.exception(e)
                sys.exit(1)

    def execute(self) -> None:
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
@click.option("--remove / --no-remove", "-r", default=False, show_default=True,
              help="Remove pre-existing files within the data/processed directory for the specified source, excluding "
                   "change logs.")
def main(source: str, remove: bool = False) -> None:
    """
    Executes an NRN stage.

    :param str source: abbreviation for the source province / territory.
    :param bool remove: removes pre-existing files within the data/processed directory for the specified source,
        excluding change logs, default False.
    """

    try:

        with helpers.Timer():
            stage = Stage(source, remove)
            stage.execute()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
