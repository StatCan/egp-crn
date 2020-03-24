import click
import logging
import os
import pandas as pd
import sys
from copy import deepcopy
from operator import itemgetter

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import validation_functions
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
        self.stage = 4
        self.source = source.lower()
        self.dframes_modified = list()

        # Configure and validate input data path.
        self.data_path = os.path.abspath("../../data/interim/{}.gpkg".format(self.source))
        if not os.path.exists(self.data_path):
            logger.exception("Input data not found: \"{}\".".format(self.data_path))
            sys.exit(1)

        # Compile default field values.
        self.defaults = helpers.compile_default_values()

    def export_gpkg(self):
        """Exports the dataframes as GeoPackage layers."""

        logger.info("Exporting dataframes to GeoPackage layers.")

        # Export target dataframes to GeoPackage layers.
        if len(self.dframes_modified):
            export_dframes = {name: self.dframes[name] for name in set(self.dframes_modified)}
            helpers.export_gpkg(export_dframes, self.data_path)
        else:
            logger.info("Export not required, no dataframe modifications detected.")

    def gen_flag_variables(self):
        """Generates variables required for storing and logging error and modification flags for records."""

        logger.info("Generating flag variables.")

        # Create flag dictionary entry for each gpkg dataframe.
        self.flags = {name: {"modifications": dict(), "errors": dict()} for name in [*self.dframes.keys(), "multiple"]}

        # Load flag messages yaml.
        self.flag_messages_yaml = helpers.load_yaml(os.path.abspath("flag_messages.yaml"))

    def load_gpkg(self):
        """Loads input GeoPackage layers into dataframes."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

    def log_messages(self):
        """Logs any errors and modification messages flagged by the attribute validations."""

        logger.info("Writing modification and error logs.")

        log_path = os.path.abspath("../../data/interim/{}_stage_{}.log".format(self.source, self.stage))
        with helpers.TempHandlerSwap(logger, log_path):

            # Iterate dataframe flags.
            for name in self.flags:
                for flag_typ in self.flags[name]:

                    # Iterate non-empty flag tables (validations).
                    for validation in [val for val, data in self.flags[name][flag_typ].items() if data is not None]:

                        # Retrieve flags.
                        flags = self.flags[name][flag_typ][validation]

                        # Log error messages, iteratively if multiple error codes stored in dictionary.
                        if isinstance(flags, dict):
                            for code, code_flags in [[k, v] for k, v in flags.items() if len(v)]:

                                # Log messages.
                                vals = "\n".join(map(str, code_flags))
                                logger.info(self.flag_messages_yaml[validation][flag_typ][code].format(name, vals))

                        else:
                            if len(flags):

                                # Log messages.
                                vals = "\n".join(map(str, flags))
                                logger.info(self.flag_messages_yaml[validation][flag_typ][1].format(name, vals))

    def validations(self):
        """Applies a set of validations to one or more dataframes."""

        logger.info("Applying validations.")

        try:

            # Define functions and parameters.
            # TODO: move all unique_attr_functions into the following func params list.
            funcs = {
                "strip_whitespace": {"tables": self.dframes.keys(), "iterate": True, "args": ()},
                "title_route_text": {"tables": ["roadseg", "ferryseg"], "iterate": True, "args": ()},
                "validate_dates": {"tables": self.dframes.keys(), "iterate": True, "args": ()},
                "validate_exitnbr_conflict": {"tables": ["roadseg"], "iterate": True, "args": ()},
                "validate_exitnbr_roadclass": {"tables": ["roadseg"], "iterate": True, "args": ()},
                "validate_ids": {"tables": self.dframes.keys(), "iterate": True, "args": ()},
                "validate_nbrlanes": {"tables": ["roadseg"], "iterate": True, "args": ()},
                "validate_pavement": {"tables": ["roadseg"], "iterate": True, "args": ()},
                "validate_roadclass_rtnumber1": {"tables": ["ferryseg", "roadseg"], "iterate": True, "args": ()},
                "validate_roadclass_self_intersection": {"tables": ["roadseg"], "iterate": True, "args": ()},
                "validate_roadclass_structtype": {"tables": ["roadseg"], "iterate": True, "args": ()},
                "validate_route_contiguity": {"tables": ["roadseg", "ferryseg"], "iterate": False, "args": ()},
                "validate_speed": {"tables": ["roadseg"], "iterate": True, "args": ()}
            }

            # Iterate functions and table names.
            for func, params in funcs.items():
                for table in params["tables"]:
                    logger.info("Applying validation: {}. Target dataframe: {}.".format(func.replace("_", " "), table))

                    # Validate required dataframes and compile function args.
                    if params["iterate"]:
                        if table not in self.dframes:
                            logger.warning("Missing required layer: {}. Skipping validation.".format(table))
                            continue
                        args = (self.dframes[table].copy(deep=True), *params["args"])

                    else:
                        missing = [name for name in params["tables"] if name not in self.dframes]
                        if any(missing):
                            logger.warning("Missing required layer(s): {}. Skipping validation."
                                           .format(", ".join(map("\"{}\"".format, missing))))
                            break
                        args = (*map(deepcopy, itemgetter(*params["tables"])(self.dframes)), *params["args"])

                    # Call function.
                    results = eval("validation_functions.{}(*args)".format(func))

                    # Store results.
                    self.flags[table]["errors"][func] = results["errors"]
                    self.flags[table]["modifications"][func] = results["modifications"]
                    if "modified_dframes" in results:
                        if params["iterate"]:
                            self.dframes[table] = results["modified_dframes"]
                            self.dframes_modified.append(table)
                        else:
                            for mod_index, mod_table in enumerate(params["tables"]):
                                if results["modified_dframes"][mod_index] is not None:
                                    self.dframes[mod_table] = results["modified_dframes"][mod_index]
                                    self.dframes_modified.append(mod_table)

                    # Break iteration for non-iterative function.
                    if not params["iterate"]:
                        break

        except (KeyError, SyntaxError, ValueError):
            logger.exception("Unable to apply validation.")
            sys.exit(1)

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.gen_flag_variables()
        self.validations()
        self.log_messages()
        self.export_gpkg()


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
