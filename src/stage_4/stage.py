import click
import logging
import sys
from pathlib import Path

filepath = Path(__file__).resolve()
sys.path.insert(1, str(filepath.parents[1]))
import helpers
from validation_functions import Validator


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
        :param bool remove: removes pre-existing validation log within the data/processed directory for the specified
            source, default False.
        """

        self.stage = 4
        self.source = source.lower()
        self.remove = remove
        self.Validator = None

        # Configure and validate input data path.
        self.data_path = filepath.parents[2] / f"data/interim/{self.source}.gpkg"
        if not self.data_path.exists():
            logger.exception(f"Input data not found: \"{self.data_path}\".")
            sys.exit(1)

        # Configure output path.
        self.output_path = filepath.parents[2] / f"data/interim/{self.source}_validation_errors.log"

        # Conditionally clear output namespace.
        if self.output_path.exists():
            logger.warning("Output namespace already occupied.")

            if self.remove:
                logger.warning(f"Parameter remove=True: Removing conflicting file: \"{self.output_path}\".")
                self.output_path.unlink()

            else:
                logger.exception("Parameter remove=False: Unable to proceed while output namespace is occupied. Set "
                                 "remove=True (-r) or manually clear the output namespace.")
                sys.exit(1)

        # Load data.
        self.dframes = helpers.load_gpkg(self.data_path)

    def log_errors(self) -> None:
        """Outputs error logs returned by validation functions."""

        logger.info("Writing error logs.")

        with helpers.TempHandlerSwap(logger, self.output_path):

            # Iterate and log errors.
            for heading, errors in sorted(self.Validator.errors.items()):
                errors = "\n".join(map(str, errors))
                logger.warning(f"{heading}\n{errors}\n")

    def validations(self) -> None:
        """Applies a set of validations to one or more NRN datasets."""

        logger.info("Applying validations.")

        # Instantiate and execute validator class.
        self.Validator = Validator(self.dframes)
        self.Validator.execute()

    def execute(self) -> None:
        """Executes an NRN stage."""

        self.validations()
        self.log_errors()


@click.command()
@click.argument("source", type=click.Choice("ab bc mb nb nl ns nt nu on pe qc sk yt".split(), False))
@click.option("--remove / --no-remove", "-r", default=False, show_default=True,
              help="Remove pre-existing validation log within the data/processed directory for the specified source.")
def main(source: str, remove: bool = False) -> None:
    """
    Executes an NRN stage.

    :param str source: abbreviation for the source province / territory.
    :param bool remove: removes pre-existing validation log within the data/processed directory for the specified
        source, default False.
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
