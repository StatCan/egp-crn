import click
import click_log
import logging
import sys
from pathlib import Path


logging.basicConfig(format="%(asctime)s - %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()


class Stage:
    """Defines an NRN stage."""

    def __init__(self, prov, data):
        self.stage = 1
        self.prov = prov.lower()
        self.data = Path(data)

    def execute(self):
        """Executes an NRN stage."""

        logger.info(self.stage)
        logger.info(self.prov)
        logger.info(self.data)

@click.command()
@click_log.simple_verbosity_option(logger)
@click.argument("prov", type=click.Choice(["ab", "bc", "mb", "nb", "nl", "ns", "nt", "nu", "on", "pe", "qc", "sk",
                                           "yt"], case_sensitive=False))
@click.argument("data", type=click.Path(exists=True, file_okay=False, dir_okay=True, resolve_path=True),
                default="../../data/raw")
def main(prov, data):
    """Executes an NRN stage."""

    stage = Stage(prov, data)
    stage.execute()

if __name__ == "__main__":
    try:

        main()
        logger.info("Finished.")

    except KeyboardInterrupt:
        sys.stdout.write("KeyboardInterrupt exception: exiting program.")
