"""
Analyse a json file of users and keep track of the number of times a location is declared.

The output format is csv.
"""

import os
import csv
import re
import argparse
import datetime
import math
from pathlib import Path

from typing import Iterable, Iterator, Mapping, Counter

from .. import file_utils as fu
from .. import dumper
from .. import custom_types
from .. import utils

from operator import itemgetter
from pprint import pprint

# print a dot each NTWEET tweets
NUSERS = 10000

# templates
stats_template = '''
<stats>
    <performance>
        <start_time>${stats['performance']['start_time'] | x}</start_time>
        <end_time>${stats['performance']['end_time'] | x}</end_time>
        <input>
            <locations>${stats['performance']['input']['locations'] | x}</locations>
            <users>${stats['performance']['input']['users'] | x}</users>
        </input>
    </performance>
</stats>
'''


def process_lines(
        dump: Iterable[list],
        stats: Mapping,
        locations_dict:dict,
        args:argparse.Namespace) -> str:
    """Assign each revision to the snapshot or snapshots to which they
       belong.
    """

    for raw_obj in dump:
        location = raw_obj['location']
        clean_loc = re.sub(r"\s+", "", location).lower()

        if not clean_loc in locations_dict:
            locations_dict[clean_loc] = {'location': location, 'occurrences': 1}
            stats['performance']['input']['locations'] += 1
        else:
            locations_dict[clean_loc]['occurrences'] += 1

        stats['performance']['input']['users'] += 1
        nobjs = stats['performance']['input']['users']
        if (nobjs-1) % NUSERS == 0:
            utils.dot()


def get_locations(
    stats: Mapping,
    locations_dict: dict,
    args: argparse.Namespace):
    if '' in locations_dict:
        del locations_dict['']
    sorted_locations = [v for _, v in sorted(locations_dict.items(), key=lambda item: item[1]['occurrences'], reverse=True)]
    for location in sorted_locations:
        yield {
            'location': location['location'], 
            'occurrences': location['occurrences']
            }


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'calc-locations-frequency',
        help='Analyse a json file of users and keep track of the number of times a location is declared.',
    )

    parser.set_defaults(func=main, which='calc_locations_frequency')


def main(
        dump: Iterable[list],
        basename: str,
        args: argparse.Namespace,
        shared) -> None:
    """Main function that parses the arguments and writes the output."""
    stats = {
        'performance': {
            'start_time': None,
            'end_time': None,
            'input': {
                'locations': 0,
                'users': 0
            },
        },
    }

    locations_dict:dict = {}

    stats['performance']['start_time'] = datetime.datetime.utcnow()

    output = open(os.devnull, 'wt')
    stats_output = open(os.devnull, 'wt')

    # process the dump
    process_lines(
        dump,
        stats=stats,
        locations_dict=locations_dict,
        args=args
    )
    
    res = get_locations(
        stats=stats,
        locations_dict=locations_dict,
        args=args
    )

    path_list = re.split('-|\.', basename)
    lang = path_list[0]

    if not args.dry_run:
        stats_path = f"{args.output_dir_path}/locations-frequency/stats/{lang}"
        Path(stats_path).mkdir(parents=True, exist_ok=True)
        varname = ('{basename}.{func}'
                   .format(basename=basename,
                           func='calc-locations-frequency'
                           )
                   )
        stats_filename = f"{stats_path}/{varname}.stats.xml"

        stats_output = fu.output_writer(
            path=stats_filename,
            compression=args.output_compression,
            mode='wt'
        )

        if not lang is None:
            file_path = f"{args.output_dir_path}/locations-frequency"
            Path(file_path).mkdir(parents=True, exist_ok=True)

            output_filename = f"{file_path}/{lang}-users-locations.csv"

            output = fu.output_writer(
                path=output_filename,
                compression=args.output_compression,
                mode='wt'
            )
    
    fieldnames = ['location', 'occurrences']

    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for line in res:
        writer.writerow(line)
    output.close()

    stats['performance']['end_time'] = datetime.datetime.utcnow()
    
    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )
    
    stats_output.close()