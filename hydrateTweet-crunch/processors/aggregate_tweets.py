"""
Given the number of days, it aggregates in a single file tweets of the same pool of days, year and language.

The output format is json.
"""

import os
import json
import argparse
import datetime
import arrow
from dateutil import parser
from pathlib import Path

from typing import Iterable, Iterator, Mapping

from .. import file_utils as fu
from .. import dumper
from .. import custom_types
from .. import utils

from operator import itemgetter
from pprint import pprint

# print a dot each NTWEET tweets
NTWEET = 10000


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'aggregate-tweets',
        help='Aggregates in a single file tweets of the same pool of days, year and language',
    )
    parser.add_argument(
        '--type',
        type=str,
        default='week',
        choices={'week'},
        help='The number of days that will be used to aggregate tweets together [default: week]'
    )

    parser.set_defaults(func=main, which='aggregate_tweets')


def close_all_descriptors(desr_dict:dict):
    for lang in desr_dict:
        desr_dict[lang].close()
    utils.log('descriptors all closed')


def main(
        dump: Iterable[list],
        basename: str,
        args: argparse.Namespace,
        shared) -> None:
    """Main function that parses the date contained in the field 'created_at' 
       of the json and the arguments and writes the output."""

    desr_dict = {}

    output = open(os.devnull, 'wt')

    path_list = basename.split('-')

    for obj in dump:
        year = 'Err'
        month = 'Err'
        day = 'Err'
        lang = 'Err'
        if 'created_at' in obj and 'lang' in obj:
            try:
                date = arrow.get(obj['created_at'], "ddd MMM DD HH:mm:ss Z YYYY")
                start_date = date.shift(days=-int(date.weekday()))
                year = start_date.year
                month = start_date.strftime("%m")
                day = start_date.strftime("%d")
                lang = obj['lang']
            except:
                utils.log(f"Error while parsing the date {obj['created_at']}")

        if not args.dry_run:
            descriptor = f'{lang}-{year}/{month}/{day}'
            if not descriptor in desr_dict:
                file_path = f"{args.output_dir_path}/aggregate-tweets/groupby_{args.type}/{lang}"
                Path(file_path).mkdir(parents=True, exist_ok=True)

                output_filename = f"{file_path}/{path_list[0]}-{path_list[1]}-{year}-{month}-{day}.json"
                
                # Save the descriptor for that particular language
                desr_dict[descriptor] = fu.output_writer(
                    path=output_filename,
                    compression=args.output_compression,
                )

            # Retrieve the descriptor for that particular language
            output = desr_dict[descriptor]

        output.write(json.dumps(obj))
        output.write("\n")
    
    close_all_descriptors(desr_dict)
