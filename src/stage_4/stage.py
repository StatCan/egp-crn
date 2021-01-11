import click
import logging
import os
import pandas as pd
import sys
from collections import defaultdict
from copy import deepcopy

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import helpers
from validation_functions import Validator


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

    def __init__(self, source, remove):
        self.stage = 4
        self.source = source.lower()
        self.remove = remove
        self.errors = defaultdict(dict)
        self.validator = None

        # Configure and validate input data path.
        self.data_path = os.path.abspath("../../data/interim/{}.gpkg".format(self.source))
        if not os.path.exists(self.data_path):
            logger.exception("Input data not found: \"{}\".".format(self.data_path))
            sys.exit(1)

        # Configure output path.
        self.output_path = os.path.abspath(f"../../data/interim/{self.source}_validation_errors.log")

        # Conditionally clear output namespace.
        if os.path.exists(self.output_path):
            logger.warning("Output namespace already occupied.")

            if self.remove:
                logger.warning("Parameter remove=True: Removing conflicting files.")
                logger.info(f"Removing conflicting file: \"{self.output_path}\".")

                try:
                    os.remove(self.output_path)
                except OSError as e:
                    logger.exception(f"Unable to remove file: \"{self.output_path}\".")
                    logger.exception(e)
                    sys.exit(1)

            else:
                logger.exception(
                    "Parameter remove=False: Unable to proceed while output namespace is occupied. Set "
                    "remove=True (-r) or manually clear the output namespace.")
                sys.exit(1)

        # Load and classify data.
        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

        # Compile error codes.
        self.error_codes = {
            "duplicated_lines": 1,
            "duplicated_points": 2,
            "isolated_lines": 3,
            "dates": 4,
            "deadend_proximity": 5,
            "conflicting_exitnbrs": 6,
            "exitnbr_roadclass_relationship": 7,
            "ferry_road_connectivity": 8,
            "ids": 9,
            "line_endpoint_clustering": 10,
            "line_length": 11,
            "line_merging_angle": 12,
            "line_proximity": 13,
            "nbrlanes": 14,
            "nid_linkages": 15,
            "conflicting_pavement_status": 16,
            "point_proximity": 17,
            "structure_attributes": 18,
            "roadclass_rtnumber_relationship": 19,
            "self_intersecting_elements": 20,
            "self_intersecting_structures": 21,
            "route_contiguity": 22,
            "speed": 23,
            "encoding": 24
        }

    def log_errors(self):
        """Outputs error logs returned by validation functions."""

        logger.info("Writing error logs.")

        with helpers.TempHandlerSwap(logger, self.output_path):

            # Iterate and log errors.
            for heading, errors in sorted(self.validator.errors.items()):
                errors = "\n".join(map(str, errors))
                logger.warning(f"{heading}\n{errors}\n")

    def validations(self):
        """Applies a set of validations to one or more dataframes."""

        logger.info("Applying validations.")

        try:

            # Instantiate validator class.
            self.validator = Validator(self.dframes)

            # Define functions and parameters.
            # Note: List functions in order if execution order matters.
            funcs = {
                "conflicting_exitnbrs": {"tables": ["roadseg"], "iterate": True},
                "conflicting_pavement_status": {"tables": ["roadseg"], "iterate": True},
                "dates": {"tables": self.dframes.keys(), "iterate": True},
                "deadend_proximity": {"tables": ["junction", "roadseg"], "iterate": False},
                "duplicated_lines": {"tables": self.df_lines, "iterate": True},
                "duplicated_points": {"tables": self.df_points, "iterate": True},
                "exitnbr_roadclass_relationship": {"tables": ["roadseg"], "iterate": True},
                "ferry_road_connectivity": {"tables": ["ferryseg", "roadseg", "junction"], "iterate": False},
                "ids": {"tables": self.dframes.keys(), "iterate": True},
                "isolated_lines": {"tables": ["roadseg", "junction"], "iterate": False},
                "line_endpoint_clustering": {"tables": self.df_lines, "iterate": True},
                "line_length": {"tables": self.df_lines, "iterate": True},
                "line_merging_angle": {"tables": self.df_lines, "iterate": True},
                "line_proximity": {"tables": self.df_lines, "iterate": True},
                "nbrlanes": {"tables": ["roadseg"], "iterate": True},
                "nid_linkages": {"tables": self.dframes.keys(), "iterate": True},
                "point_proximity": {"tables": self.df_points, "iterate": True},
                "roadclass_rtnumber_relationship": {"tables": ["ferryseg", "roadseg"], "iterate": True},
                "route_contiguity": {"tables": ["roadseg"], "iterate": False},
                "self_intersecting_elements": {"tables": ["roadseg"], "iterate": True},
                "self_intersecting_structures": {"tables": ["roadseg"], "iterate": True},
                "speed": {"tables": ["roadseg"], "iterate": True},
                "structure_attributes": {"tables": ["roadseg", "junction"], "iterate": False},
                "encoding": {"tables": self.dframes.keys(), "iterate": True}
            }

            # Iterate functions and datasets.
            for func, params in funcs.items():
                for table in params["tables"]:

                    logger.info(f"Applying validation \"{func}\" to dataset(s): "
                                f"{table if params['iterate'] else ', '.join(params['tables'])}.")

                    # Validate dataset availability and configure function args.
                    if params["iterate"]:
                        if table not in self.dframes:
                            logger.warning(f"Skipping validation for missing dataset: {table}.")
                            continue
                        args = (table,)

                    else:
                        missing = set(params["tables"]) - set(self.dframes)
                        if len(missing):
                            logger.warning(f"Skipping validation for missing dataset(s): {', '.join(missing)}.")
                            break
                        else:
                            args = (*params["tables"],)

                    # Call function.
                    results = eval(f"self.validator.{func}(*args)")

                    # Iterate results.
                    for code, errors in results.items():
                        if len(errors):

                            # Generate error code + heading and store results.
                            heading = f"E{self.error_codes[func]:03}{code:02} for dataset(s): " \
                                      f"{table if params['iterate'] else ', '.join(sorted(params['tables']))}"
                            self.validator.errors[heading] = deepcopy(errors)

                    # Break iteration for non-iterative function.
                    if not params["iterate"]:
                        break

        except (KeyError, SyntaxError, ValueError) as e:
            logger.exception("Unable to apply validation.")
            logger.exception(e)
            sys.exit(1)

    def execute(self):
        """Executes an NRN stage."""

        self.validations()
        self.log_errors()


@click.command()
@click.argument("source", type=click.Choice("ab bc mb nb nl ns nt nu on pe qc sk yt".split(), False))
@click.option("--remove / --no-remove", "-r", default=False, show_default=True,
              help="Remove pre-existing validation log within the data/processed directory for the specified source.")
def main(source, remove):
    """Executes an NRN stage."""

    try:

        with helpers.Timer():
            stage = Stage(source, remove)
            stage.execute()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)

if __name__ == "__main__":
    main()
