"""
Given the number of days, it aggregates in a single file tweets of the same pool of days, year and language.

The output format is json.
"""

import os
import json
import argparse
import datetime
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
        '--n-days',
        type=int,
        required=True,
        choices=range(1, 367),
        help='The number of days that will be used to aggregate tweets together'
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

    if not args.dry_run:
        stats_path = f"{args.output_dir_path}/aggregate-tweets/stats"
        Path(stats_path).mkdir(parents=True, exist_ok=True)
        varname = ('{basename}.{func}'
                   .format(basename=basename,
                           func='aggregate-tweets'
                           )
                   )
        
        stats_filename = f"{stats_path}/{varname}.stats.xml"

        stats_output = fu.output_writer(
            path=stats_filename,
            compression=args.output_compression,
            mode='wt'
        )

    path_list = basename.split('-')

    for obj in dump:
        year = 'Err'
        month = 'Err'
        day = 'Err'
        lang = 'Err'
        if 'created_at' in obj and 'lang' in obj:
            try:
                date_obj = parser.parse(obj['created_at'])
                year = date_obj.strftime("%Y")
                pool = int(date_obj.strftime("%j"))//args.n_days
                start_day = pool*args.n_days
                if start_day < 10:
                    start_day = f'0{start_day}'
                else:
                    start_day = str(start_day)
                start_date = datetime.datetime.strptime(start_day, "%j")
                month = start_date.strftime("%d")
                day = start_date.strftime("%m")
                lang = obj['lang']
            except:
                utils.log(f"Error while parsing the date {obj['created_at']}")

        if not args.dry_run:
            if not obj['lang'] in desr_dict:
                file_path = f"{args.output_dir_path}/aggregate-tweets/{lang}/{year}"
                Path(file_path).mkdir(parents=True, exist_ok=True)

                output_filename = f"{file_path}/{path_list[0]}-{path_list[1]}-{year}-{month}-{day}.json"
                
                # Save the descriptor for that particular language
                desr_dict[obj['lang']] = fu.output_writer(
                    path=output_filename,
                    compression=args.output_compression,
                )

            # Retrieve the descriptor for that particular language
            output = desr_dict[obj['lang']]

        output.write(json.dumps(obj))
        output.write("\n")
    
    close_all_descriptors(desr_dict)
