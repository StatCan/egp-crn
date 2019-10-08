import click
import fiona
import geopandas as gpd
import logging
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
import helpers


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
        self.stage = 1
        self.source = source.lower()

        # Configure raw data path.
        self.data_path = os.path.join(os.path.abspath("../../data/raw"), self.source)

        # Create temp dir.
        self.temp_dir = helpers.create_temp_directory(self.stage)

    def execute(self):
        """Executes an NRN stage."""

        return


@click.command()
@click.argument("source", type=click.Choice(["ab", "bc", "mb", "nb", "nl", "ns", "nt", "nu", "on", "pe", "qc", "sk",
                                             "yt", "parks_canada"], case_sensitive=False))
def main(source):
    """Executes an NRN stage."""

    logger.info("Started.")

    stage = Stage(source)
    stage.execute()

    logger.info("Finished.")

if __name__ == "__main__":
    try:

        main()

    except KeyboardInterrupt:
        sys.stdout.write("KeyboardInterrupt exception: exiting program.")
