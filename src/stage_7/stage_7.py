import click
import logging
import os
import pandas as pd
import sys

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
        self.stage = 7
        self.source = source.lower()

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

    def export_gpkg(self):
        """Exports the dataframes as GeoPackage layers."""

        logger.info("Exporting dataframes to GeoPackage layers.")
#        helpers.export_gpkg(self.dframes, self.data_path)

    def gen_french_dataframes(self):
        """
        Generate French equivalents of all dataframes.
        Note: Only the data values areupdated, not the column names.
        """

        logger.info("Generating French dataframes.")

        # Reconfigure dataframes dict to hold English and French data.
        dframes = {name: {"en": df.copy(deep=True), "fr": None} for name, df in self.dframes.items()}

        # Apply data mapping.
        defaults_en = helpers.compile_default_values(lang="en")
        defaults_fr = helpers.compile_default_values(lang="fr")
        table, field = None, None

        try:

            # Iterate dataframes.
            for table, df in self.dframes.items():

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
                dframes[table]["fr"] = df

        except (AttributeError, KeyError, ValueError):
            logger.exception("Unable to apply French data mapping for table: {}, field: {}.".format(table, field))
            sys.exit(1)

        # Store results.
        self.dframes = dframes

    def load_gpkg(self):
        """Loads input GeoPackage layers into dataframes."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.compile_french_domain_mapping()
        self.gen_french_dataframes()
        # TODO: write format exporting function. self.export_gpkg is commented out since it may not be used.
#        self.export_gpkg()


@click.command()
@click.argument("source", type=click.Choice("ab bc mb nb nl ns nt nu on pe qc sk yt parks_canada".split(), False))
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
