"""
Extract specific field from tweets and sort them by language.

The output format is json.
"""

import os
import json
import argparse
import datetime
from pathlib import Path

from typing import Iterable, Iterator, Mapping

from .. import file_utils as fu
from .. import dumper
from .. import types
from .. import utils

from operator import itemgetter
from pprint import pprint

# print a dot each NPRINTREVISION revisions
NPRINTREVISION = 10000

# templates
stats_template = '''
<stats>
    <performance>
        <start_time>${stats['performance']['start_time'] | x}</start_time>
        <end_time>${stats['performance']['end_time'] | x}</end_time>
        <input>
            <uniques>${stats['performance']['input']['unique'] | x}</uniques>
            <retweets>${stats['performance']['input']['retweet'] | x}</retweets>
        </input>
    </performance>
</stats>
'''


def process_lines(
        dump: Iterable[list],
        stats: Mapping) -> Iterator[list]:
    """Assign each revision to the snapshot or snapshots to which they
       belong.
    """

    for raw_obj in dump:
        if not 'retweeted_status' in raw_obj:
            obj = types.cast_json(raw_obj)

            stats['performance']['input']['unique'] += 1
            
            nobjs = stats['performance']['input']['unique']
            if (nobjs-1) % NPRINTREVISION == 0:
                utils.dot()

            yield obj
        else:
            stats['performance']['input']['retweet'] += 1


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'sort-lang',
        help='Sorts tweets by language and day, also removes useless fields.',
    )

    parser.set_defaults(func=main)


def main(
        dump: Iterable[list],
        basename: str,
        args: argparse.Namespace,
        desr_dict: dict) -> None:
    """Main function that parses the arguments and writes the output."""
    stats = {
        'performance': {
            'start_time': None,
            'end_time': None,
            'input': {
                'unique': 0,
                'retweet': 0
            },
        },
    }
    stats['performance']['start_time'] = datetime.datetime.utcnow()

    output = open(os.devnull, 'wt')
    stats_output = open(os.devnull, 'wt')

    # process the dump
    #! ask cristian if it's better to remove the function call
    res = process_lines(
        dump,
        stats=stats,
    )

    #TODO... based on the information returned from res configure the output
    if not args.dry_run:
        stats_path = f"{args.output_dir_path}/sort-lang/stats"
        Path(stats_path).mkdir(parents=True, exist_ok=True)
        varname = ('{basename}.{func}'
                   .format(basename=basename,
                           func='sort-lang'
                           )
                   )
        
        #! stats output: it is constant over the same file
        stats_filename = f"{stats_path}/{varname}.stats.xml"

        stats_output = fu.output_writer(
            path=stats_filename,
            compression=args.output_compression,
        )

    path_list = basename.split('-')

    for obj in res:
        if not args.dry_run:
            if not obj['lang'] in desr_dict['descriptors']:
                # utils.log(f"opening a descriptor for {obj['lang']}")
                file_path = f"{args.output_dir_path}/sort-lang/{obj['lang']}/{path_list[3]}-{path_list[4]}"
                Path(file_path).mkdir(parents=True, exist_ok=True)

                output_filename = f"{file_path}/{path_list[0]}-{path_list[1]}-{path_list[3]}-{path_list[4]}-{path_list[5]}.json"
                
                #Save the descriptor for that particular language
                desr_dict['descriptors'][obj['lang']] = fu.output_writer(
                    path=output_filename,
                    compression=args.output_compression,
                )

            #retrieve the descriptor for that particular language
            output = desr_dict['descriptors'][obj['lang']]

        output.write(json.dumps(obj))
        output.write("\n")

    stats['performance']['end_time'] = datetime.datetime.utcnow()
    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )
