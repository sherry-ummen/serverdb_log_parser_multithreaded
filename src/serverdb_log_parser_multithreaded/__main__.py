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
import multiprocessing as mp
from multiprocessing import Manager
from multiprocessing import Queue
import time

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


def parse_multi_process(folder_path: str, db_name: str):
    mpl = mp.log_to_stderr()
    mpl.setLevel(logging.INFO)
    m = Manager()
    q = m.Queue()
    path = Path(folder_path)
    folders = [(x, os.path.basename(x))
               for x in path.iterdir() if x.is_dir()]
    files = []
    for folder in folders:
        for file in Path.glob(folder[0], 'serverdb_*.log'):
            q.put((str(file), folder[1], db_name))
            files.append(q)

    pool = mp.Pool(mp.cpu_count())
    result = pool.map_async(run_parser, files)

    # monitor loop
    prev_qsize = q.qsize()
    while True:
        if result.ready():
            break
        else:
            size = q.qsize()
            if prev_qsize != size:
                print(f"Remaining items to process: {size}")
            prev_qsize = size
            time.sleep(1)

    outputs = result.get()


def run_parser(queue):
    Parser(queue).parse()


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

    _logger.debug("Scripts starts here")
    start = time.time()
    parse_multi_process(args.folder_path, dbname)
    end = time.time()
    print(f"\n{'#'*20}\nTotal time taken : {end - start} secs\n{'#'*20}\n")

    _logger.info("Script ends here")


def run():
    """Entry point for console_scripts
    """
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
