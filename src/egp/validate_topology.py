import click
import fiona
import geopandas as gpd
import logging
import sys
from pathlib import Path


filepath = Path(__file__).resolve()
sys.path.insert(1, str(filepath.parents[1]))
import helpers
from validation_functions import Validator


# Set logger.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(handler)

# Create logger for validation errors.
logger_validations = logging.getLogger("validations")
logger_validations.setLevel(logging.WARNING)


class EGP_Topology_Validation:
    """Defines the EGP topology validation class."""

    def __init__(self, src: str, dst: str, remove: bool = False) -> None:
        """
        Initializes the EGP class.

        :param str src: path to the source GeoPackage containing layer `segment`.
        :param str dst: path to the directory where the validation log file will be written.
        :param bool remove: remove pre-existing validation log within the `dst` directory, default False.
        """

        self.src = Path(src).resolve()
        self.dst = Path(dst).resolve()
        self.remove = remove
        self.Validator = None
        self.validations_log = Path(self.dst / "validations.log")

        # Configure source path.
        if self.src.exists():
            if "segment" not in set(fiona.listlayers(self.src)):
                logger.exception(f"Layer \"segment\" not found within source: \"{self.src}\".")
                sys.exit(1)
        else:
            logger.exception(f"Source not found: \"{self.src}\".")
            sys.exit(1)

        # Configure destination path.
        if self.dst.exists():
            if self.validations_log.exists():
                if remove:
                    logger.info(f"Removing conflicting file: \"{self.validations_log}\".")
                else:
                    logger.exception(f"Conflicting file exists (\"{self.validations_log}\") but remove=False. Set "
                                     f"remove=True (-r) or manually clear the output namespace.")
                    sys.exit(1)
        else:
            logger.exception(f"Destination not found: \"{self.dst}\".")
            sys.exit(1)

        # Load source data.
        logger.info(f"Loading source data: {self.src}|layer=segment.")
        self.segment = gpd.read_file(self.src, layer="segment")
        logger.info("Successfully loaded source data.")

    def log_errors(self) -> None:
        """Outputs error logs returned by validation functions."""

        logger.info("Writing error logs.")

        # Add File Handler to validation logger.
        f_handler = logging.FileHandler(self.validations_log)
        f_handler.setLevel(logging.WARNING)
        f_handler.setFormatter(logger.handlers[0].formatter)
        logger_validations.addHandler(f_handler)

        # Iterate and log errors.
        for code, errors in sorted(self.Validator.errors.items()):
            errors["values"] = "\n".join(map(str, errors["values"]))
            logger_validations.warning(f"{code}\n{errors}\n")

            if errors["query"]:
                logger_validations.warning(f"{code} query: {errors['query']}\n")

    def validations(self) -> None:
        """Applies a set of validations to segments."""

        logger.info("Applying validations.")

        # Instantiate and execute validator class.
        self.Validator = Validator(self.segment)
        self.Validator.execute()

    def execute(self) -> None:
        """Executes the EGP class."""

        self.validations()
        self.log_errors()


@click.command()
@click.argument("src", type=click.Path(exists=True))
@click.argument("dst", type=click.Path(exists=True))
@click.option("--remove / --no-remove", "-r", default=False, show_default=True,
              help="Remove pre-existing validation log within the `dst` directory, default False.")
def main(src: str, dst: str, remove: bool = False) -> None:
    """
    Instantiates and executes the EGP class.

    :param str src: path to the source GeoPackage containing layer `segment`.
    :param str dst: path to the directory where the validation log file will be written.
    :param bool remove: remove pre-existing validation log within the `dst` directory, default False.
    """

    try:

        with helpers.Timer():
            egp = EGP_Topology_Validation(src, dst, remove)
            egp.execute()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
