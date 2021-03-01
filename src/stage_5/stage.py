import click
import logging
import pandas as pd
import re
import sys
import zipfile
from collections import Counter
from copy import deepcopy
from datetime import datetime
from operator import itemgetter
from osgeo import ogr
from pathlib import Path
from tqdm.auto import trange
from typing import Union

sys.path.insert(1, str(Path(__file__).resolve().parents[1]))
import helpers


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
        self.data_path = Path(__file__).resolve().parents[2] / f"data/interim/{self.source}.gpkg"
        if not self.data_path.exists():
            logger.exception(f"Input data not found: {self.data_path}.")
            sys.exit(1)

        # Configure output path.
        self.output_path = Path(__file__).resolve().parents[2] / f"data/processed/{self.source}"

        # Conditionally clear output namespace.
        namespace = list(filter(lambda f: f.stem != f"{self.source}_change_logs", self.output_path.glob("*")))

        if len(namespace):
            logger.warning("Output namespace already occupied.")

            if self.remove:
                logger.warning("Parameter remove=True: Removing conflicting files.")

                for f in namespace:
                    logger.info(f"Removing conflicting file: \"{f}\".")

                    if f.is_file():
                        f.unlink()
                    else:
                        helpers.rm_tree(f)

            else:
                logger.exception("Parameter remove=False: Unable to proceed while output namespace is occupied. Set "
                                 "remove=True (-r) or manually clear the output namespace.")
                sys.exit(1)

        # Compile output formats.
        self.formats = [f.stem for f in (Path(__file__).resolve().parent / "distribution_formats/en").glob("*")]

        # Configure field defaults and domains.
        self.defaults = {lang: helpers.compile_default_values(lang=lang) for lang in ("en", "fr")}
        self.domains = helpers.compile_domains(mapped_lang="fr")

        # Define custom progress bar format.
        # Note: the only change from default is moving the percentage to the right end of the progress bar.
        self.bar_format = "{desc}: |{bar}| {percentage:3.0f}% {r_bar}"

    def configure_release_version(self) -> None:
        """Configures the major and minor release versions for the current NRN vintage."""

        logger.info("Configuring NRN release version.")

        # Iterate release notes to extract the version number and release year for current source.
        release_year = None
        release_notes = Path(__file__).resolve().parents[2] / "docs/release_notes.rst"

        try:

            headers = ("Code", "Edition", "Release Date")
            headers_rng = None

            for line in open(release_notes, "r"):

                # Identify index range for data columns.
                if all(line.find(header) >= 0 for header in headers):
                    headers_rng = {header: (line.find(header), line.find(header) + len(header)) for header in headers}

                # Identify data values for source.
                if headers_rng:
                    if line[headers_rng["Code"][0]: headers_rng["Code"][1]].strip(" ") == self.source.upper():
                        version = line[headers_rng["Edition"][0]: headers_rng["Edition"][1]].split(".")
                        self.major_version, self.minor_version = list(map(int, version))
                        release_year = int(line[headers_rng["Release Date"][0]: headers_rng["Release Date"][1]][:4])
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

        logger.info(f"Configured NRN release version: {self.major_version}.{self.minor_version}")

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
                placenames = pd.concat([df[l_placenam], df.loc[df[l_placenam] != df[r_placenam], r_placenam]],
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
                rowids = df.loc[(df[l_placenam] == placename) | (df[r_placenam] == placename), "ROWID"].values

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

        # Configure export progress bar.
        file_count = 0
        for frmt in self.dframes:
            for lang in self.dframes[frmt]:
                if frmt == "kml":
                    file_count += len(self.kml_groups[lang])
                else:
                    file_count += len(self.dframes[frmt][lang])
        export_progress = trange(file_count, desc="Exporting data", bar_format=self.bar_format)

        # Iterate export formats and languages.
        for frmt in self.dframes:
            for lang in self.dframes[frmt]:

                # Retrieve export specifications.
                export_specs = helpers.load_yaml(f"distribution_formats/{lang}/{frmt}.yaml")

                # Configure temporary data path.
                temp_path = Path(__file__).resolve().parents[2] / f"data/interim/{self.source}_{frmt}_{lang}_temp.gpkg"

                # Configure and format export paths and table names.
                export_dir = self.output_path / self.format_path(export_specs["data"]["dir"])
                export_file = self.format_path(export_specs["data"]["file"]) if export_specs["data"]["file"] else None
                export_tables = {table: self.format_path(export_specs["conform"][table]["name"]) for table in
                                 self.dframes[frmt][lang]}

                # Configure ogr2ogr inputs.
                kwargs = {
                    "driver": f"-f \"{export_specs['data']['driver']}\"",
                    "append": "-append",
                    "pre_args": "",
                    "dest": "",
                    "src": f"\"{temp_path}\"",
                    "src_layer": "",
                    "nln": ""
                }

                # Generate directory structure.
                Path(export_dir).mkdir(parents=True, exist_ok=True)

                # Iterate tables.
                for table in export_tables:

                    # Modify table-specific ogr2ogr inputs.
                    kwargs["dest"] = export_dir / (export_file if export_file else export_tables[table])
                    kwargs["src_layer"] = table
                    kwargs["nln"] = f"-nln {export_tables[table]}" if export_file else ""

                    # Handle kml.
                    if frmt == "kml":

                        # Remove ogr2ogr src layer parameter since kml exporting uses -sql.
                        # This is purely to avoid an ogr2ogr warning.
                        if "src_layer" in kwargs:
                            del kwargs["src_layer"]

                        # Configure kml path properties.
                        kml_groups = self.kml_groups[lang]
                        kml_dir = kwargs["dest"].parent
                        kml_ext = f".{kwargs['dest'].suffix}"

                        # Iterate kml groups.
                        for kml_group in kml_groups.itertuples(index=False):

                            # Add kml group name and query to ogr2ogr parameters.
                            name, query = itemgetter("names", "queries")(kml_group._asdict())
                            kwargs["dest"] = kml_dir / (name + kml_ext)
                            kwargs["pre_args"] = query

                            # Run ogr2ogr subprocess.
                            export_progress.set_description_str(f"Exporting file={kwargs['dest'].name}")
                            helpers.ogr2ogr(kwargs)
                            export_progress.update(1)

                    else:

                        # Run ogr2ogr subprocess.
                        export_progress.set_description_str(
                            f"Exporting file={kwargs['dest'].name}, layer={kwargs['src_layer']}")
                        helpers.ogr2ogr(kwargs)
                        export_progress.update(1)

                # Delete temporary file.
                if temp_path.exists():
                    driver = ogr.GetDriverByName("GPKG")
                    driver.DeleteDataSource(str(temp_path))
                    del driver

        # Close progress bar.
        export_progress.close()

    def export_temp_data(self) -> None:
        """
        Exports temporary data as GeoPackages.
        Temporary file is required since ogr2ogr (which is used for data transformation) is file based.
        """

        # Export temporary files.
        logger.info("Exporting temporary GeoPackages.")

        # Configure export progress bar.
        file_count = 0
        for frmt in self.dframes:
            for lang in self.dframes[frmt]:
                file_count += len(self.dframes[frmt][lang])
        export_progress = trange(file_count, desc="Exporting temporary data", bar_format=self.bar_format)

        # Iterate formats and languages.
        for frmt in self.dframes:
            for lang in self.dframes[frmt]:

                # Configure paths.
                temp_path = Path(__file__).resolve().parents[2] / f"data/interim/{self.source}_{frmt}_{lang}_temp.gpkg"
                export_schemas_path = Path(__file__).resolve().parent / f"distribution_formats/{lang}/{frmt}.yaml"

                # Iterate datasets and export to GeoPackage.
                for table, df in self.dframes[frmt][lang].items():

                    export_progress.set_description_str(f"Exporting temporary file={temp_path.name}, layer={table}")
                    helpers.export_gpkg({table: df}, temp_path, export_schemas_path, suppress_logs=True,
                                        nested_pbar=True)
                    export_progress.update(1)

        # Close progress bar.
        export_progress.close()

    def format_path(self, path: Union[Path, str]) -> Path:
        """
        Formats a path with class variables: source, major_version, minor_version.

        :param Union[Path, str] path: path requiring formatting.
        :return Path: formatted path.
        """

        # Construct replacement dictionary.
        lookup = {k: str(v).upper() for k, v in (("<source>", self.source),
                                                 ("<major_version>", self.major_version),
                                                 ("<minor_version>", self.minor_version))}

        # Replace path keywords with variables.
        path = re.sub(string=str(path),
                      pattern=f"({'|'.join(lookup.keys())})",
                      repl=lambda match: lookup[match.string[match.start(): match.end()]])

        return Path(path)

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
        """Compresses and zips all export data directories."""

        logger.info("Applying compression and zipping output data directories.")

        # Configure root directory.
        root = Path(__file__).resolve().parents[2] / f"data/processed/{self.source}"

        # Configure zip progress bar.
        file_count = 0
        for data_dir in filter(lambda f: f.name != f"{self.source}_change_logs.zip", root.glob("*")):
            file_count += len(list(filter(Path.is_file, data_dir.rglob("*"))))
        zip_progress = trange(file_count, desc="Compressing data", bar_format=self.bar_format)

        # Iterate output directories. Ignore change logs if already zipped.
        for data_dir in filter(lambda f: f.name != f"{self.source}_change_logs.zip", root.glob("*")):

            try:

                # Recursively iterate directory files, compress, and zip contents.
                with zipfile.ZipFile(f"{data_dir}.zip", "w") as zip_f:
                    for file in filter(Path.is_file, data_dir.rglob("*")):

                        zip_progress.set_description_str(f"Compressing file={file.name}")

                        # Configure new relative path inside .zip file.
                        arcname = data_dir.stem / file.relative_to(data_dir)

                        # Write to and compress .zip file.
                        zip_f.write(file, arcname=arcname, compress_type=zipfile.ZIP_DEFLATED)
                        zip_progress.update(1)

            except (zipfile.BadZipFile, zipfile.LargeZipFile) as e:
                logger.exception("Unable to compress directory.")
                logger.exception(e)
                sys.exit(1)

            # Remove original directory.
            helpers.rm_tree(data_dir)

        # Close progress bar.
        zip_progress.close()

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
