import logging
import os
import sys
import tempfile
from datetime import datetime


logger = logging.getLogger()


def create_temp_directory(stage_id, dir="../../data/interim"):
    """Creates a temporary directory for the given stage execution."""

    logger.info("Creating temp directory.")

    # Configure temp dir path.
    params = {"suffix": "_{}".format(datetime.today().strftime("%Y%m%d-%H%M%S")),
              "prefix": "STAGE{}_".format(stage_id),
              "dir": os.path.abspath(dir)}

    try:
        temp_dir = tempfile.mkdtemp(**params)
    except FileExistsError:
        logger.error("Directory already exists in {}.".format(params["dir"]))

    return temp_dir
