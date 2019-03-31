import argparse
import sys
import logging
import asyncio
import random
import os
from pathlib import Path
from serverdb_log_parser_multithreaded import __version__
from serverdb_log_parser_multithreaded.log_parser.log_parser import Parser
from mongoengine import *

__author__ = "Sherry Ummen"
__copyright__ = "Sherry Ummen"
__license__ = "mit"

_logger = logging.getLogger(__name__)


def parse_args(args):
    """Parse command line parameters

    Args:
      args ([str]): command line parameters as list of strings

    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace
    """
    parser = argparse.ArgumentParser(
        description="ServerDB log parser")
    parser.add_argument(
        '--version',
        action='version',
        version='serverdb_log_parser_multithreaded {ver}'.format(ver=__version__))
    parser.add_argument(
        '-v',
        '--verbose',
        dest="loglevel",
        help="set loglevel to INFO",
        action='store_const',
        const=logging.INFO)
    parser.add_argument(
        '-vv',
        '--very-verbose',
        dest="loglevel",
        help="set loglevel to DEBUG",
        action='store_const',
        const=logging.DEBUG)
    parser.add_argument(
        '-f',
        '--force',
        dest="force",
        help="Delete the database before parsing ",
        action='store_true')
    parser.add_argument(
        '-p',
        '--path',
        dest="folder_path",
        help="Folder Path to parse ", required=True)
    parser.add_argument(
        '-d',
        '--database',
        dest="database_name",
        help="Name of the database")

    return parser.parse_args(args)


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(level=loglevel, stream=sys.stdout,
                        format=logformat, datefmt="%Y-%m-%d %H:%M:%S")


async def produce(queue, folder_path: str):
    path = Path(folder_path)
    folders = [x for x in path.iterdir() if x.is_dir()]
    for folder in folders:
        username: str = os.path.basename(folder)
        for file in Path.glob(folder, 'serverdb_*.log'):
            await queue.put((username, file))
    await queue.put(None)


async def consume(queue):

    while True:
        # wait for an item from the producer
        item = await queue.get()
        # process the item
        await Parser(item[1], item[0]).parse()
        # Notify the queue that the item has been processed
        queue.task_done()
        print(f"Items remaining : {queue.qsize()}")
        if item == None:
            break


async def parse_in(folder_path: str):
    queue = asyncio.Queue()
    # run the producer and wait for completion
    await produce(queue, folder_path)

    await consume(queue)
    # wait until the consumer has processed all items
    await queue.join()


def main(args):

    args = parse_args(args)
    setup_logging(args.loglevel)

    dbname = "ServerDBLogDataPython"

    if args.database_name:
        dbname = args.database_name

    if args.force:
        from pymongo import MongoClient
        client = MongoClient()
        client.drop_database(dbname)

    connect(db=dbname)
    _logger.debug("Scripts starts here")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(parse_in(args.folder_path))
    loop.close()

    _logger.info("Script ends here")


def run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
