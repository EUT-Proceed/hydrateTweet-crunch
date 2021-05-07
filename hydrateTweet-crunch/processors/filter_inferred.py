"""
Filter the .csv file obtained from infer_user processor, keeping only the inferred 
users which match specific characteristics.

The output format is csv.
"""

import os
import csv
import re
import argparse
import datetime
import math
from pathlib import Path

from typing import Iterable, Iterator, Mapping, Counter, TextIO

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
            <male>${stats['performance']['input']['male'] | x}</male>
            <female>${stats['performance']['input']['female'] | x}</female>
            <org>${stats['performance']['input']['org'] | x}</org>
            <total>${stats['performance']['input']['total'] | x}</total>
        </input>
    </performance>
</stats>
'''


def process_lines(
        dump: TextIO,
        stats: Mapping,
        args:argparse.Namespace) -> str:
    """Assign each revision to the snapshot or snapshots to which they
       belong.
    """
    csv_reader = csv.DictReader(dump)
    for inferred_user in csv_reader:
        stats['performance']['input']['total'] += 1
        nobjs = stats['performance']['input']['total']
        if (nobjs-1) % NTWEET == 0:
            utils.dot()
            
        if inferred_user['org'] == 'True' and float(inferred_user['org_acc']) >= args.org_acc:
            inferred_user['category'] = 'org'
        elif float(inferred_user['gender_acc']) >= args.gender_acc:
            inferred_user['category'] = inferred_user['gender']

        if 'category' in inferred_user:
            stats['performance']['input'][inferred_user['category']] += 1
            yield inferred_user


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'filter-inferred',
        help='Filter users obtained from m3inference based on specific parameters.',
    )
    parser.add_argument(
        '--gender-acc',
        type=float,
        required=False,
        default=0.95,
        help='The minimum gender accuracy who a user should (at least) have in order to be considered [default: 0.95].',
    )
    parser.add_argument(
        '--org-acc',
        type=float,
        required=False,
        default=0.95,
        help='The minimum organization accuracy which an organization should (at least) have in order to be considered [default: 0.95].',
    )

    parser.set_defaults(func=main, which='filter_inferred')


def main(
        dump: TextIO,
        basename: str,
        args: argparse.Namespace,
        shared) -> None:
    """Main function that parses the arguments and writes the output."""
    stats = {
        'performance': {
            'start_time': None,
            'end_time': None,
            'input': {
                'male': 0,
                'female': 0,
                'org': 0,
                'total': 0
            },
        },
    }

    fieldnames = [
        "id_str",
        "screen_name",
        "name",
        "tweets",
        "days_tweeted",
        "location",
        "category",
        "gender",
        "gender_acc",
        "age",
        "age_acc",
        "age_>=40_acc",
        "age_30-39_acc",
        "age_19-29_acc",
        "age_<=18_acc",
        "org",
        "org_acc"
    ]

    stats_dict:dict = {}
    for fieldname in fieldnames:
        stats_dict[fieldname] = 0

    stats['performance']['start_time'] = datetime.datetime.utcnow()

    output = open(os.devnull, 'wt')
    stats_output = open(os.devnull, 'wt')

    # process the dump
    res = process_lines(
        dump,
        stats=stats,
        args=args
    )

    path_list = re.split('-|\.', basename)
    lang = path_list[0]
    addHeader = False

    if not args.dry_run:
        stats_path = f"{args.output_dir_path}/filter-inferred/stats/"
        Path(stats_path).mkdir(parents=True, exist_ok=True)
        varname = ('{basename}.{func}'
                   .format(basename=basename,
                           func='filter-inferred'
                           )
                   )
        stats_filename = f"{stats_path}/{varname}.stats.xml"

        stats_output = fu.output_writer(
            path=stats_filename,
            compression=args.output_compression,
            mode='wt'
        )

        file_path = f"{args.output_dir_path}/filter-inferred"
        Path(file_path).mkdir(parents=True, exist_ok=True)

        output_filename = f"{file_path}/{lang}-inferred-users.csv"

        #The header of the .csv will be added only if the file doesn't exist
        if not args.output_compression:
            if not Path(output_filename).exists():
                addHeader = True
        else:
            if not Path(f"{output_filename}.{args.output_compression}").exists():
                addHeader = True

        output = fu.output_writer(
            path=output_filename,
            compression=args.output_compression
        )

    writer = csv.DictWriter(output, fieldnames=fieldnames)
    if addHeader:
        writer.writeheader()
    for valid_user in res:
        writer.writerow(valid_user)
    output.close()

    stats['performance']['end_time'] = datetime.datetime.utcnow()
    
    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )
    
    stats_output.close()