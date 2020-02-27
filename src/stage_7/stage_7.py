import click
import logging
import os
import pandas as pd
import pathlib
import subprocess
import sys
from osgeo import ogr

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
        if os.path.exists(self.output_path):
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

    def export_data(self):
        """Exports and packages all data."""

        logger.info("Exporting data.")

        def format_path(path):
            upper = True if os.path.basename(path)[0].isupper() else False

            for key in ("source", "major_version", "minor_version"):
                val = str(eval("self.{}".format(key)))
                val = val.upper() if upper else val.lower()
                path = path.replace("<{}>".format(key), val)

            return path

        # Iterate formats and languages.
        for frmt in self.dframes:
            for lang in self.dframes[frmt]:

                # Retrieve export specifications.
                export_specs = helpers.load_yaml("distribution_formats/{}/{}.yaml".format(lang, frmt))
                driver_long_name = ogr.GetDriverByName(export_specs["data"]["driver"]).GetMetadata()["DMD_LONGNAME"]

                logger.info("Exporting format: \"{}\", language: \"{}\".".format(driver_long_name, lang))

                # Configure and format export paths and table names.
                export_dir = format_path(export_specs["data"]["dir"])
                export_file = format_path(export_specs["data"]["file"]) if export_specs["data"]["file"] else None
                export_tables = {table: format_path(export_specs["conform"][table]["name"]) for table in
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
                try:

                    # Iterable tables.
                    for table in export_tables:

                        # Define ogr2ogr command.
                        args = "ogr2ogr -f \"{}\" -append \"{}\" \"{}\" {} {}".format(
                            export_specs["data"]["driver"],
                            os.path.join(export_dir, export_file if export_file else export_tables[table]),
                            temp_path,
                            table,
                            "-nln {}".format(export_tables[table]) if export_file else ""
                        )

                        # Run subprocess.
                        subprocess.run(args, shell=True, check=True)

                except subprocess.CalledProcessError as e:
                    logger.exception("Unable to transform data source.")
                    logger.exception("ogr2ogr error: {}".format(e))
                    sys.exit(1)

                # Delete temporary file.
                logger.info("Deleting temporary GeoPackage: \"{}\".".format(temp_path))
                if os.path.exists(temp_path):
                    driver = ogr.GetDriverByName("GPKG")
                    driver.DeleteDataSource(temp_path)
                    del driver

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

        # TODO

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.compile_french_domain_mapping()
        self.gen_french_dataframes()
        self.gen_output_schemas()
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
