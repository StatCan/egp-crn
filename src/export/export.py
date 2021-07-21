import click
import jinja2
import logging
import numpy as np
import pandas as pd
import re
import sys
import yaml
import zipfile
from collections import Counter
from datetime import datetime
from operator import attrgetter, itemgetter
from pathlib import Path
from sqlalchemy import create_engine
from tqdm import tqdm
from tqdm.auto import trange
from typing import Union

filepath = Path(__file__).resolve()
sys.path.insert(1, str(filepath.parents[1]))
import helpers


# Set logger.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)


class Stage:
    """Defines an NRN stage."""

    def __init__(self, source: str, url: str, remove: bool) -> None:
        """
        Initializes an NRN stage.

        :param str source: abbreviation for the source province / territory.
        :param str url: PostgreSQL database connection URL.
        :param bool remove: removes pre-existing files within the data/processed directory for the specified source,
            excluding change logs, default False.
        """

        self.stage = filepath.stem
        self.source = source.lower()
        self.url = url
        self.remove = remove
        self.major_version = None
        self.minor_version = None
        self.dframes = dict()

        # Configure output path.
        self.output_path = filepath.parents[2] / f"data/processed/{self.source}"

        # Clear output namespace.
        namespace = list(self.output_path.glob("*"))

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

        # Configure field defaults and domains.
        self.defaults = {lang: helpers.compile_default_values(lang=lang) for lang in ("en", "fr")}
        self.domains = helpers.compile_domains(mapped_lang="fr")

        # Configure export formats.
        distribution_formats_path = filepath.parent / "distribution_formats"
        self.formats = [f.stem for f in (distribution_formats_path / "en").glob("*")]
        self.distribution_formats = {
            "en": {frmt: helpers.load_yaml(distribution_formats_path / f"en/{frmt}.yaml") for frmt in self.formats},
            "fr": {frmt: helpers.load_yaml(distribution_formats_path / f"fr/{frmt}.yaml") for frmt in self.formats}
        }

        # Define custom progress bar format.
        # Note: the only change from default is moving the percentage to the right end of the progress bar.
        self.bar_format = "{desc}: |{bar}| {percentage:3.0f}% {r_bar}"

    def configure_release_version(self) -> None:
        """Configures the major and minor release versions for the current NRN vintage."""

        logger.info("Configuring NRN release version.")

        # Extract the version number and release year for current source from the release notes.
        release_year = None
        release_notes_path = filepath.parent / "distribution_docs/release_notes.yaml"
        release_notes = helpers.load_yaml(release_notes_path)

        try:

            # Extract previous release version and date.
            version, release_date = itemgetter("edition", "release_date")(release_notes[self.source])

            # Standardize raw variables.
            self.major_version, self.minor_version = map(int, str(version).split("."))
            release_year = int(str(release_date)[:4])

            # Configure new release version.
            if release_year == datetime.now().year:
                self.minor_version += 1
            else:
                self.major_version += 1
                self.minor_version = 0

        except (IndexError, ValueError) as e:
            logger.exception(f"Unable to extract version number and / or release date from \"{release_notes}\".")
            logger.exception(e)
            sys.exit(1)

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

            # Retrieve source dataframe.
            df = self.dframes[lang]["roadseg"].copy(deep=True)

            # Compile placenames.
            if placenames is None:

                # Compile placenames.
                placenames = pd.Series([*df["l_placenam"], *df.loc[df["l_placenam"] != df["r_placenam"], "r_placenam"]])

                # Flag limit-exceeding placenames.
                placenames_exceeded = {name for name, count in Counter(placenames).items() if count > kml_limit}

                # Compile unique placename values.
                placenames = set(placenames)

            # Swap English-French default placename value, only if the value is present.
            else:
                default_add = self.defaults["fr"]["roadseg"]["l_placenam"]
                default_rm = self.defaults["en"]["roadseg"]["l_placenam"]
                if default_rm in placenames:
                    placenames = {*placenames - {default_rm}, default_add}
                if default_rm in placenames_exceeded:
                    placenames_exceeded = {*placenames_exceeded - {default_rm}, default_add}

            # Compile export parameters.
            # name: placename as a valid file name.
            # query: pandas query for all records with the placename.
            names = list()
            queries = list()

            # Iterate placenames and compile export parameters.
            for placename in sorted(placenames):

                if placename in placenames_exceeded:

                    # Compile indexes of placename records.
                    indexes = df.query(f"l_placenam==\"{placename}\" or r_placenam==\"{placename}\"").index
                    for index, indexes_range in enumerate(np.array_split(indexes, (len(indexes) // kml_limit) + 1)):
                        # Configure export parameters.
                        names.append(re.sub(r"[\W_]+", "_", f"{placename}_{index}"))
                        queries.append(f"index.isin({list(indexes_range)})")

                else:

                    # Configure export parameters.
                    names.append(re.sub(r"[\W_]+", "_", placename))
                    queries.append(f"l_placenam==\"{placename}\" or r_placenam==\"{placename}\"")

            # Store results.
            self.kml_groups[lang] = pd.DataFrame({"name": names, "query": queries})

    def export_data(self) -> None:
        """Exports and packages all data."""

        logger.info("Exporting output data.")

        # Configure export progress bar.
        file_count = 0
        for lang, dfs in self.dframes.items():
            for frmt in self.formats:
                count = len(set(dfs).intersection(set(self.distribution_formats[lang][frmt]["conform"])))
                file_count += (len(self.kml_groups[lang]) * count) if frmt == "kml" else count
        export_progress = trange(file_count, desc="Exporting data", bar_format=self.bar_format)

        # Iterate export formats and languages.
        for lang, dfs in self.dframes.items():
            for frmt in self.formats:

                # Retrieve export specifications.
                export_specs = self.distribution_formats[lang][frmt]

                # Filter required dataframes.
                dframes = {name: df.copy(deep=True) for name, df in dfs.items() if name in export_specs["conform"]}

                # Configure export directory.
                export_dir, export_file = itemgetter("dir", "file")(export_specs["data"])
                export_dir = self.output_path / self.format_path(export_dir) / self.format_path(export_file)

                # Configure mapped layer names.
                nln_map = {table: self.format_path(export_specs["conform"][table]["name"]) for table in dframes}

                # Configure export kwargs.
                kwargs = {
                    "driver": {"gml": "GML", "gpkg": "GPKG", "kml": "KML", "shp": "ESRI Shapefile"}[frmt],
                    "type_schemas": helpers.load_yaml(filepath.parents[1] / "distribution_format.yaml"),
                    "export_schemas": export_specs,
                    "nln_map": nln_map,
                    "keep_uuid": False,
                    "outer_pbar": export_progress,
                    "epsg": 4617,
                    "geom_type": {table: df.geom_type.iloc[0] for table, df in dframes.items() if "geometry" in
                                  df.columns}
                }

                # Configure KML.
                if frmt == "kml":

                    # Configure export names.
                    self.kml_groups[lang]["name"] = self.kml_groups[lang]["name"].map(
                        lambda name: str(export_dir).replace("<name>", name))

                    # Iterate export datasets.
                    for table, df in dframes.items():

                        # Map dataframe queries (more efficient than iteratively querying).
                        self.kml_groups[lang]["df"] = self.kml_groups[lang]["query"].map(
                            lambda query: df.query(query).copy(deep=True))

                        # Iterate KML groups.
                        for kml_group in self.kml_groups[lang].itertuples(index=False):
                            # Export data.
                            kml_name, kml_df = attrgetter("name", "df")(kml_group)
                            helpers.export({table: kml_df}, kml_name, **kwargs)

                # Configure non-KML.
                else:
                    # Export data.
                    helpers.export(dframes, export_dir, **kwargs)

        # Close progress bar.
        export_progress.close()

    def extract_data(self) -> None:
        """Extracts NRN database records for the source into (Geo)DataFrames."""

    def format_path(self, path: Union[Path, str, None]) -> Union[Path, str]:
        """
        Formats a path with class variables: source, major_version, minor_version.

        :param Union[Path, str, None] path: path requiring formatting.
        :return Union[Path, str]: formatted path or empty str.
        """

        if not path:
            return ""

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
        self.dframes = {
            "en": {table: df.copy(deep=True) for table, df in self.dframes.items()},
            "fr": {table: df.copy(deep=True) for table, df in self.dframes.items()}
        }

        # Iterate dataframes and fields.
        for table, df in self.dframes["fr"].items():
            fields = set(df.columns) - {"uuid", "geometry"}
            for field in tqdm(fields, total=len(fields), desc=f"Applying French translations for table: {table}.",
                              bar_format=self.bar_format):

                try:

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

    def transform_data(self) -> None:
        """Transforms datasets from the NRN database schema into the NRN distribution format."""

    def update_distribution_docs(self) -> None:
        """
        Writes updated documentation to data/processed for:
            - completion rates
            - release notes
        """

        def write_documents(data: dict, filename: str) -> None:
            """
            Updates a document template with a dictionary and exports:
                1) an rst file representing the updated template.
                2) a yaml file containing the updated dictionary.

            :param dict data: dictionary of values used to populate the document template.
            :param str filename: basename of a document in ../distribution_docs to be updated.
            """

            # Configure source and destination paths.
            src = filepath.parent / f"distribution_docs/{filename}.rst"
            dst = self.output_path / filename

            try:

                # Load document as jinja template.
                with open(src, "r") as doc:
                    template = jinja2.Template(doc.read())

                # Update template.
                updated_doc = template.render(data)

            except (jinja2.TemplateError, jinja2.TemplateAssertionError, jinja2.UndefinedError) as e:
                logger.exception(f"Unable to render updated Jinja2.Template for: {src}.")
                logger.exception(e)
                sys.exit(1)

            # Export updated document.
            try:

                # Write rst.
                with open(dst.with_suffix(".rst"), "w") as doc:
                    doc.write(updated_doc)

                # Write yaml.
                with open(dst.with_suffix(".yaml"), "w") as doc:
                    yaml.dump(data, doc)

            except (ValueError, yaml.YAMLError) as e:
                logger.exception(f"Unable to write document: {dst}.")
                logger.exception(e)
                sys.exit(1)

        # Update release notes.
        logger.info(f"Updating documentation: release notes.")

        # Compile previous data.
        data = helpers.load_yaml(filepath.parent / "distribution_docs/release_notes.yaml")

        # Update release notes - edition, release date, validity date.
        data[self.source]["edition"] = float(f"{self.major_version}.{self.minor_version}")
        data[self.source]["release_date"] = datetime.now().strftime("%Y-%m")
        data[self.source]["validity_date"] = datetime.now().strftime("%Y-%m")

        # Update release notes - number of kilometers.
        # Note: EPSG:3348 used to get geometry lengths in meters.
        kms = int(round(self.dframes["en"]["roadseg"].to_crs("EPSG:3348").length.sum() / 1000, 0))
        data[self.source]["number_of_kilometers"] = f"{kms:,d}"

        # Write updated documents.
        write_documents(data, "release_notes")

        # Update completion rates.
        logger.info(f"Updating documentation: completion rates.")

        # Compile previous data.
        data = helpers.load_yaml(filepath.parent / "distribution_docs/completion_rates.yaml")

        # Update completion rates.

        # Iterate dataframe and column names.
        for table, df in self.dframes["en"].items():
            for col in data[table]:

                # Configure column completion rate.
                # Note: Values between 0 and 1 are rounded to 1, values between 99 and 100 are rounded to 99.
                completion_rate = (len(df.loc[~df[col].isin({"Unknown", -1})]) / len(df)) * 100
                if 0 < completion_rate < 1:
                    completion_rate = 1
                if 99 < completion_rate < 100:
                    completion_rate = 99

                # Update column value for source.
                data[table][col][self.source] = int(completion_rate)

                # Update column average.
                vals = itemgetter(*set(data[table][col]) - {"avg"})(data[table][col])
                data[table][col]["avg"] = int(round(sum(map(int, vals)) / len(vals), 0))

        # Write updated documents.
        write_documents(data, "completion_rates")

    def zip_data(self) -> None:
        """Compresses and zips all export data directories."""

        logger.info("Applying compression and zipping output data directories.")

        # Configure root directory.
        root = filepath.parents[2] / f"data/processed/{self.source}"

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

        self.extract_data() #TODO
        self.transform_data() #TODO
        self.configure_release_version()
        self.gen_french_dataframes()
        self.define_kml_groups()
        self.export_data()
        self.zip_data()
        self.update_distribution_docs()


@click.command()
@click.argument("source", type=click.Choice("ab bc mb nb nl ns nt nu on pe qc sk yt".split(), False))
@click.option("--url", "-u", default="postgresql://postgres:postgres@localhost:5433/nrn", show_default=True,
              help="PostgreSQL database connection URL.")
@click.option("--remove / --no-remove", "-r", default=False, show_default=True,
              help="Remove pre-existing files within the data/processed directory for the specified source, excluding "
                   "change logs.")
def main(source: str, url: str = "postgresql://postgres:postgres@localhost:5433/nrn", remove: bool = False) -> None:
    """
    Executes an NRN stage.

    :param str source: abbreviation for the source province / territory.
    :param str url: PostgreSQL database connection url.
    :param bool remove: removes pre-existing files within the data/processed directory for the specified source,
        default False.
    """

    try:

        with helpers.Timer():
            stage = Stage(source, url, remove)
            stage.execute()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
