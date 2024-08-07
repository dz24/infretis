"""The functions to be used to run infretis via the terminal."""
import argparse
import asyncio

from infretis.scheduler import scheduler
from infretis.setup import setup_config, setup_internal


def infretisrun():
    """Read input and runs infretis."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i", "--input", help="Location of infretis input file", required=True
    )

    args_dict = vars(parser.parse_args())
    input_file = args_dict["input"]
    config = setup_config(input_file)
    if config is None:
        return

    md_items, state = setup_internal(config)
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(scheduler(loop, md_items, state))
    finally:
        loop.close()


def infretisinit():
    """To generate initial *toml template and other features."""
    return
