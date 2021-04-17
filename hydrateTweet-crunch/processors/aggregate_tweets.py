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

# templates
stats_template = '''
<stats>
    <performance>
        <start_time>${stats['performance']['start_time'] | x}</start_time>
        <end_time>${stats['performance']['end_time'] | x}</end_time>
        <input>
            <objects>${stats['performance']['input']['objects'] | x}</objects>
        </input>
    </performance>
</stats>
'''


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
    stats = {
        'performance': {
            'start_time': None,
            'end_time': None,
            'input': {
                'objects': 0
            },
        },
    }

    desr_dict = {}
    stats['performance']['start_time'] = datetime.datetime.utcnow()

    output = open(os.devnull, 'wt')
    stats_output = open(os.devnull, 'wt')

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
        stats['performance']['input']['objects'] += 1
        year = 'Err'
        pool = 'Err'
        lang = 'Err'
        if 'created_at' in obj and 'lang' in obj:
            try:
                date_obj = parser.parse(obj['created_at'])
                year = date_obj.strftime("%Y")
                pool = int(date_obj.strftime("%j"))//args.n_days
                lang = obj['lang']
            except:
                utils.log(f"Error while parsing the date {obj['created_at']}")

        if not args.dry_run:
            if not obj['lang'] in desr_dict:
                file_path = f"{args.output_dir_path}/aggregate-tweets/{lang}/{year}"
                Path(file_path).mkdir(parents=True, exist_ok=True)

                output_filename = f"{file_path}/{path_list[0]}-{path_list[1]}-{pool}.json"
                
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

    stats['performance']['end_time'] = datetime.datetime.utcnow()
    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )
    
    stats_output.close()
