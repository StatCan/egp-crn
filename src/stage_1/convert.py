import logging
from pathlib import Path
from .source import NRNSource


def get_sources(source_dir):
    """Generate a list of source definitions to read."""
    
    return source_dir.rglob("*.yaml")

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, filename="stage_1.log")

    # read all the source configuration
    logging.info("Looking for sources")
    src_cfgs = []
    for path in get_sources(Path("sources")):
        logging.info("Found {}".format(path))
        src_cfgs.append(NRNSource(path))
    
    logging.info("Found {} sources".format(len(src_cfgs)))
    
    for s in src_cfgs:
        logging.info("Converting {}".format(s.out_filename))
        s.convert()
