import click
import logging
import os
import pandas as pd
import sys
from collections import defaultdict
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
        self.error_logs = defaultdict(dict)
        self.dframes_modified = list()

        # Configure and validate input data path.
        self.data_path = os.path.abspath("../../data/interim/{}.gpkg".format(self.source))
        if not os.path.exists(self.data_path):
            logger.exception("Input data not found: \"{}\".".format(self.data_path))
            sys.exit(1)

        # Compile default field values.
        self.defaults = helpers.compile_default_values()

        # Load validation messages yaml.
        self.validation_messages_yaml = helpers.load_yaml(os.path.abspath("validation_messages.yaml"))

    def classify_tables(self):
        """Groups table names by geometry type."""

        self.df_lines = ("ferryseg", "roadseg")
        self.df_points = ("blkpassage", "junction", "tollpoint")

    def export_gpkg(self):
        """Exports the dataframes as GeoPackage layers."""

        logger.info("Exporting dataframes to GeoPackage layers.")

        # Export target dataframes to GeoPackage layers.
        if len(self.dframes_modified):
            export_dframes = {name: self.dframes[name] for name in set(self.dframes_modified)}
            helpers.export_gpkg(export_dframes, self.data_path)
        else:
            logger.info("Export not required, no dataframe modifications detected.")

    def load_gpkg(self):
        """Loads input GeoPackage layers into dataframes."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

    def log_errors(self):
        """Templates and outputs error logs returned by validation functions."""

        logger.info("Writing error logs.")

        log_path = os.path.abspath("../../data/interim/{}_validation_errors.log".format(self.source))
        with helpers.TempHandlerSwap(logger, log_path):

            # Iterate datasets and validations containing error logs.
            for table in self.error_logs:
                for validation, logs in self.error_logs[table].items():

                    # Validate error logs are not empty.
                    if logs is not None and len(logs):

                        # Iterate error codes.
                        if isinstance(logs, dict):
                            for code, code_logs in [[k, v] for k, v in logs.items() if len(v)]:

                                # Template and log errors.
                                vals = "\n".join(map(str, code_logs))
                                logger.warning(self.validation_messages_yaml[validation][code].format(table, vals))

                        else:

                            # Template and log errors.
                            vals = "\n".join(map(str, logs))
                            logger.warning(self.validation_messages_yaml[validation][1].format(table, vals))

    def validations(self):
        """Applies a set of validations to one or more dataframes."""

        logger.info("Applying validations.")

        try:

            # Define functions and parameters.
            # Note: List functions in order if execution order matters.
            funcs = {
                "strip_whitespace": {"tables": self.dframes.keys(), "iterate": True, "args": ()},
                "title_route_text": {"tables": ["roadseg", "ferryseg"], "iterate": True, "args": ()},
                "identify_duplicate_lines": {"tables": self.df_lines, "iterate": True, "args": ()},
                "identify_duplicate_points": {"tables": self.df_points, "iterate": True, "args": ()},
                "identify_isolated_lines":
                    {"tables": ["roadseg", "junction"], "iterate": False,
                     "args": (self.dframes["ferryseg"].copy(deep=True) if "ferryseg" in self.dframes else None,)},
                "validate_dates": {"tables": self.dframes.keys(), "iterate": True, "args": ()},
                "validate_deadend_disjoint_proximity":
                    {"tables": ["junction", "roadseg"], "iterate": False, "args": ()},
                "validate_exitnbr_conflict": {"tables": ["roadseg"], "iterate": True, "args": ()},
                "validate_exitnbr_roadclass": {"tables": ["roadseg"], "iterate": True, "args": ()},
                "validate_ferry_road_connectivity":
                    {"tables": ["ferryseg", "roadseg", "junction"], "iterate": False, "args": ()},
                "validate_ids": {"tables": self.dframes.keys(), "iterate": True, "args": ()},
                "validate_line_endpoint_clustering": {"tables": self.df_lines, "iterate": True, "args": ()},
                "validate_line_length": {"tables": self.df_lines, "iterate": True, "args": ()},
                "validate_line_merging_angle": {"tables": self.df_lines, "iterate": True, "args": ()},
                "validate_line_proximity": {"tables": self.df_lines, "iterate": True, "args": ()},
                "validate_nbrlanes": {"tables": ["roadseg"], "iterate": True, "args": ()},
                "validate_nid_linkages":
                    {"tables": self.dframes.keys(), "iterate": True, "args": (self.dframes,)},
                "validate_pavement": {"tables": ["roadseg"], "iterate": True, "args": ()},
                "validate_point_proximity": {"tables": self.df_points, "iterate": True, "args": ()},
                "validate_road_structures": {"tables": ["roadseg", "junction"], "iterate": False, "args": ()},
                "validate_roadclass_rtnumber1": {"tables": ["ferryseg", "roadseg"], "iterate": True, "args": ()},
                "validate_roadclass_self_intersection": {"tables": ["roadseg"], "iterate": True, "args": ()},
                "validate_roadclass_structtype": {"tables": ["roadseg"], "iterate": True, "args": ()},
                "validate_route_contiguity":
                    {"tables": ["roadseg"], "iterate": False,
                     "args": (self.dframes["ferryseg"].copy(deep=True) if "ferryseg" in self.dframes else None,)},
                "validate_speed": {"tables": ["roadseg"], "iterate": True, "args": ()}
            }

            # Iterate functions and datasets.
            for func, params in funcs.items():
                for table in params["tables"]:

                    logger.info("Applying validation \"{}\" to target dataset(s): {}."
                                .format(func.replace("_", " "), table))

                    # Validate dataset availability and configure function args.
                    if params["iterate"]:
                        if table not in self.dframes:
                            logger.warning("Skipping validation for missing dataset: {}.".format(table))
                            continue
                        args = (self.dframes[table].copy(deep=True), *params["args"])

                    else:
                        missing = set(params["tables"]) - set(self.dframes)
                        if len(missing):
                            logger.warning("Skipping validation for missing dataset(s): {}.".format(", ".join(missing)))
                            break
                        if len(params["tables"]) == 1:
                            args = (*map(deepcopy, (itemgetter(*params["tables"])(self.dframes),)), *params["args"])
                        else:
                            args = (*map(deepcopy, itemgetter(*params["tables"])(self.dframes)), *params["args"])

                    # Call function.
                    results = eval("validation_functions.{}(*args)".format(func))

                    # Store results.
                    self.error_logs[table][func] = results["errors"]
                    if "modified_dframes" in results:
                        if not isinstance(results["modified_dframes"], dict):
                            results["modified_dframes"] = {table: results["modified_dframes"]}
                        self.dframes.update(results["modified_dframes"])
                        self.dframes_modified.extend(results["modified_dframes"])

                    # Break iteration for non-iterative function.
                    if not params["iterate"]:
                        break

        except (KeyError, SyntaxError, ValueError):
            logger.exception("Unable to apply validation.")
            sys.exit(1)

    def execute(self):
        """Executes an NRN stage."""

        self.load_gpkg()
        self.classify_tables()
        self.validations()
        self.log_errors()
        self.export_gpkg()


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
