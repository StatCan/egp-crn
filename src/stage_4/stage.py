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
        self.errors = defaultdict(dict)

        # Configure and validate input data path.
        self.data_path = os.path.abspath("../../data/interim/{}.gpkg".format(self.source))
        if not os.path.exists(self.data_path):
            logger.exception("Input data not found: \"{}\".".format(self.data_path))
            sys.exit(1)

        # Compile default field values.
        self.defaults = helpers.compile_default_values()

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

    def classify_tables(self):
        """Groups table names by geometry type."""

        self.df_lines = ("ferryseg", "roadseg")
        self.df_points = ("blkpassage", "junction", "tollpoint")

    def load_gpkg(self):
        """Loads input GeoPackage layers into dataframes."""

        logger.info("Loading Geopackage layers.")

        self.dframes = helpers.load_gpkg(self.data_path)

    def log_errors(self):
        """Outputs error logs returned by validation functions."""

        logger.info("Writing error logs.")

        log_path = os.path.abspath("../../data/interim/{}_validation_errors.log".format(self.source))
        with helpers.TempHandlerSwap(logger, log_path):

            # Iterate and log errors.
            for heading, errors in sorted(self.errors.items()):
                errors = "\n".join(map(str, errors))
                logger.warning(f"{heading}\n{errors}\n")

    def validations(self):
        """Applies a set of validations to one or more dataframes."""

        logger.info("Applying validations.")

        try:

            # Define functions and parameters.
            # Note: List functions in order if execution order matters.
            funcs = {
                "conflicting_exitnbrs": {"tables": ["roadseg"], "iterate": True, "args": ()},
                "conflicting_pavement_status": {"tables": ["roadseg"], "iterate": True, "args": ()},
                "dates": {"tables": self.dframes.keys(), "iterate": True, "args": ()},
                "deadend_proximity": {"tables": ["junction", "roadseg"], "iterate": False, "args": ()},
                "duplicated_lines": {"tables": self.df_lines, "iterate": True, "args": ()},
                "duplicated_points": {"tables": self.df_points, "iterate": True, "args": ()},
                "exitnbr_roadclass_relationship": {"tables": ["roadseg"], "iterate": True, "args": ()},
                "ferry_road_connectivity":
                    {"tables": ["ferryseg", "roadseg", "junction"], "iterate": False, "args": ()},
                "ids": {"tables": self.dframes.keys(), "iterate": True, "args": ()},
                "isolated_lines":
                    {"tables": ["roadseg", "junction"], "iterate": False,
                     "args": (self.dframes["ferryseg"].copy(deep=True) if "ferryseg" in self.dframes else None,)},
                "line_endpoint_clustering": {"tables": self.df_lines, "iterate": True, "args": ()},
                "line_length": {"tables": self.df_lines, "iterate": True, "args": ()},
                "line_merging_angle": {"tables": self.df_lines, "iterate": True, "args": ()},
                "line_proximity": {"tables": self.df_lines, "iterate": True, "args": ()},
                "nbrlanes": {"tables": ["roadseg"], "iterate": True, "args": ()},
                "nid_linkages": {"tables": self.dframes.keys(), "iterate": True, "args": (self.dframes,)},
                "point_proximity": {"tables": self.df_points, "iterate": True, "args": ()},
                "roadclass_rtnumber_relationship": {"tables": ["ferryseg", "roadseg"], "iterate": True, "args": ()},
                "route_contiguity":
                    {"tables": ["roadseg"], "iterate": False,
                     "args": (self.dframes["ferryseg"].copy(deep=True) if "ferryseg" in self.dframes else None,)},
                "self_intersecting_elements": {"tables": ["roadseg"], "iterate": True, "args": ()},
                "self_intersecting_structures": {"tables": ["roadseg"], "iterate": True, "args": ()},
                "speed": {"tables": ["roadseg"], "iterate": True, "args": ()},
                "structure_attributes": {"tables": ["roadseg", "junction"], "iterate": False, "args": ()},
                "encoding": {"tables": self.dframes.keys(), "iterate": True, "args": ()}
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
                        args = (self.dframes[table].copy(deep=True), *params["args"])

                    else:
                        missing = set(params["tables"]) - set(self.dframes)
                        if len(missing):
                            logger.warning(f"Skipping validation for missing dataset(s): {', '.join(missing)}.")
                            break
                        if len(params["tables"]) == 1:
                            args = (*map(deepcopy, (itemgetter(*params["tables"])(self.dframes),)), *params["args"])
                        else:
                            args = (*map(deepcopy, itemgetter(*params["tables"])(self.dframes)), *params["args"])

                    # Call function.
                    results = eval(f"validation_functions.{func}(*args)")

                    # Iterate results.
                    for code, errors in results.items():
                        if len(errors):

                            # Generate error code + heading and store results.
                            heading = f"E{self.error_codes[func]:03}{code:02} for dataset(s): " \
                                      f"{table if params['iterate'] else ', '.join(sorted(params['tables']))}"
                            self.errors[heading] = deepcopy(errors)

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
