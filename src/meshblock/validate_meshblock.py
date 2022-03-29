import click
import fiona
import geopandas as gpd
import logging
import re
import sys
from pathlib import Path
from tabulate import tabulate

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


class CRNMeshblockValidation:
    """Defines the CRN meshblock validation class."""

    def __init__(self, source: str, remove: bool = False, export_meshblock: bool = False) -> None:
        """
        Initializes the CRN class.

        :param str source: abbreviation for the source province / territory.
        :param bool remove: remove pre-existing output file (validations.log), default False.
        :param bool export_meshblock: export meshblock polygon layer.
        """

        self.source = source
        self.layer = f"nrn_bo_{source}"
        self.remove = remove
        self.export_meshblock = export_meshblock
        self.Validator = None
        self.src = Path(filepath.parents[2] / "data/interim/egp_data.gpkg")
        self.src_restore = Path(filepath.parents[2] / r"data/interim/nrn_bo_restore.zip")
        self.validations_log = Path(self.src.parent / "validations.log")

        # Configure source path and layer name.
        for src in (self.src, self.src_restore):
            if src.exists():
                if self.layer not in set(fiona.listlayers(fr"zip://{str(src)}" if src.suffix == ".zip" else src)):
                    logger.exception(f"Layer \"{self.layer}\" not found within source: \"{src}\".")
                    sys.exit(1)
            else:
                logger.exception(f"Source not found: \"{src}\".")
                sys.exit(1)

        # Configure destination path.
        if self.validations_log.exists():
            if remove:
                logger.info(f"Removing conflicting file: \"{self.validations_log}\".")
                self.validations_log.unlink()
            else:
                logger.exception(f"Conflicting file exists (\"{self.validations_log}\") but remove=False. Set "
                                 f"remove=True (-r) or manually clear the output namespace.")
                sys.exit(1)

        # Load source data.
        logger.info(f"Loading source data: {self.src}|layer={self.layer}.")
        self.nrn = gpd.read_file(self.src, layer=self.layer)
        logger.info("Successfully loaded source data.")

    def __call__(self) -> None:
        """Executes the CRN class."""

        self.validations()
        self.log_errors()

    def log_errors(self) -> None:
        """Outputs error logs returned by validation functions."""

        logger.info(f"Writing error logs: \"{self.validations_log}\".")

        total_records = 0
        unique_records = set()

        # Add File Handler to validation logger.
        f_handler = logging.FileHandler(self.validations_log)
        f_handler.setLevel(logging.WARNING)
        f_handler.setFormatter(logger.handlers[0].formatter)
        logger_validations.addHandler(f_handler)

        # Iterate and log errors.
        for code, errors in sorted(self.Validator.errors.items()):

            # Format and write logs.
            errors["values"] = "\n".join(map(str, errors["values"]))
            logger_validations.warning(f"{code}\n\nValues:\n{errors['values']}\n\nQuery: {errors['query']}\n")

            # Quantify invalid records.
            total_records += errors["query"].count(",") + 1
            unique_records.update(set(re.findall(pattern=r"\((.*?)\)", string=errors["query"])[0].split(",")))

        logger.info(f"Total records flagged by validations: {total_records:,d}.")
        logger.info(f"Total unique records flagged by validations: {len(unique_records):,d}.")

        # Log meshblock creation progress.
        table = tabulate([[k, f"{v:,}"] for k, v in self.Validator.meshblock_progress.items()],
                         headers=["Meshblock Input Arcs", "Count"], tablefmt="rst", colalign=("left", "right"))
        logger.info("\n" + table)

    def validations(self) -> None:
        """Applies a set of validations to BOs."""

        logger.info("Initiating validator.")

        # Instantiate and execute validator class.
        self.Validator = Validator(self.nrn, source=self.source, dst=self.src, layer=self.layer)
        self.Validator()

        # Conditionally export meshblock.
        if self.export_meshblock:
            helpers.export(self.Validator.meshblock_, dst=self.src, name=f"meshblock_{self.source}")


@click.command()
@click.argument("source", type=click.Choice(helpers.load_yaml("../config.yaml")["sources"], False))
@click.option("--remove / --no-remove", "-r", default=False, show_default=True,
              help="Remove pre-existing output file (validations.log).")
@click.option("--export-meshblock / --no-export-meshblock", "-e", default=False, show_default=True,
              help="Export meshblock polygon layer.")
def main(source: str, remove: bool = False, export_meshblock: bool = False) -> None:
    """
    Instantiates and executes the CRN class.

    :param str source: abbreviation for the source province / territory.
    :param bool remove: remove pre-existing output file (validations.log), default False.
    :param bool export_meshblock: export meshblock polygon layer.
    """

    try:

        with helpers.Timer():
            crn = CRNMeshblockValidation(source, remove, export_meshblock)
            crn()

    except KeyboardInterrupt:
        logger.exception("KeyboardInterrupt: Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
