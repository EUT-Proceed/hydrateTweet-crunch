"""
Geocode a csv file of locations.

The output format is json.
"""

import os
import csv
import json
import re
import argparse
import datetime
import math
from pathlib import Path
import geopy

from typing import Iterable, Iterator, Mapping, Counter

from .. import file_utils as fu
from .. import dumper
from .. import custom_types
from .. import utils

from operator import itemgetter
from pprint import pprint

# print a dot each NTWEET tweets
NLOCATIONS = 10000

# templates
stats_template = '''
<stats>
    <performance>
        <start_time>${stats['performance']['start_time'] | x}</start_time>
        <end_time>${stats['performance']['end_time'] | x}</end_time>
        <input>
            <locations>${stats['performance']['input']['locations'] | x}</locations>
        </input>
    </performance>
</stats>
'''


def process_lines(
        dump: Iterable[list],
        stats: Mapping,
        geocoder,
        args:argparse.Namespace) -> geopy.location.Location:
    """Assign each revision to the snapshot or snapshots to which they
       belong.
    """
    count = 0
    csv_reader = csv.DictReader(dump)
    for line in csv_reader:
        location = line['location']
        res = geocoder.geocode(location, addressdetails=True)
        count += 1

        if res:
            stats['performance']['input']['locations'] += 1
            nobjs = stats['performance']['input']['locations']
            if (nobjs-1) % NLOCATIONS == 0:
                utils.dot()
            yield res.raw
        
        if count > args.requests:
            utils.log(f'Reached max number of requests ({args.request})')
            break


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'geocode',
        help='Geocode a csv file of locations.',
    )
    parser.add_argument(
        '--requests',
        type=int,
        required=False,
        default=100000,
        help='the number of requests to be executed per file [default: 100000].',
    )
    parser.add_argument(
        '--user-agent',
        type=str,
        required=False,
        default='covid19_emotional_impact_research',
        help='the name of the user agent for the nominatim endpoint [default: covid19_emotional_impact_research].',
    )

    parser.set_defaults(func=main, which='geocode')


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
                'locations': 0
            },
        },
    }

    geocoder = geopy.geocoders.Nominatim(user_agent=args.user_agent)

    stats['performance']['start_time'] = datetime.datetime.utcnow()

    output = open(os.devnull, 'wt')
    stats_output = open(os.devnull, 'wt')

    # process the dump
    res = process_lines(
        dump,
        stats=stats,
        geocoder=geocoder,
        args=args
    )

    path_list = re.split('-|\.', basename)
    lang = path_list[0]

    if not args.dry_run:
        stats_path = f"{args.output_dir_path}/geocode/stats/{lang}"
        Path(stats_path).mkdir(parents=True, exist_ok=True)
        varname = ('{basename}.{func}'
                   .format(basename=basename,
                           func='geocode'
                           )
                   )
        stats_filename = f"{stats_path}/{varname}.stats.xml"

        stats_output = fu.output_writer(
            path=stats_filename,
            compression=args.output_compression,
            mode='wt'
        )

        if not lang is None:
            file_path = f"{args.output_dir_path}/geocode"
            Path(file_path).mkdir(parents=True, exist_ok=True)

            output_filename = f"{file_path}/{lang}-locations-geocode.json"

            output = fu.output_writer(
                path=output_filename,
                compression=args.output_compression,
                mode='wt'
            )

    for obj in res:
        output.write(json.dumps(obj))
        output.write("\n")
        
    output.close()

    stats['performance']['end_time'] = datetime.datetime.utcnow()
    
    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )
    
    stats_output.close()