"""
Analyse the text from tweets and generate a file which contains statistics w.r.t emotions.

The output format is csv.
"""

import os
import csv
import re
import argparse
import datetime
from pathlib import Path

from typing import Iterable, Iterator, Mapping, Counter

from .. import file_utils as fu
from .. import dumper
from .. import custom_types
from .. import utils
from ..emotion_lexicon import initEmotionLexicon, countEmotionsOfText, Emotions, getEmotionName

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
            <users>${stats['performance']['input']['users'] | x}</users>
            <tweets>${stats['performance']['input']['tweets'] | x}</tweets>
        </input>
    </performance>
</stats>
'''

stats_template_finalize = '''
<stats>
    <performance>
        <start_time>${stats['performance']['start_time'] | x}</start_time>
        <end_time>${stats['performance']['end_time'] | x}</end_time>
        <input>
            <users>${stats['performance']['input']['users'] | x}</users>
            <languages>${stats['performance']['input']['languages'] | x}</languages>
        </input>
    </performance>
</stats>
'''

fieldnames = [
    "id",
    "screen_name",
    "tweets",
    "days_tweeted",
    "location",
    "followers_count",
    "statuses_count",
    "last_tweet"
]


def process_lines(
        dump: Iterable[list],
        stats: Mapping,
        shared: dict,
        date:datetime
        ) -> None:
    """Assign each revision to the snapshot or snapshots to which they
       belong.
    """
    lang = None
    for raw_obj in dump:
        # extract the language first: if the language is not in shared, add it
        lang = raw_obj['lang']
        if not lang in shared:
            shared[lang] = {}
        lang_dict = shared[lang]

        # check if the user_id is in the language dict
        user_id = str(raw_obj['user']['id'])
        if not user_id in lang_dict:
            stats['performance']['input']['users'] += 1
            lang_dict[user_id] = init_user(raw_obj, date)
        user = lang_dict[user_id]

        user['tweets'] += 1
        stats['performance']['input']['tweets'] += 1
        if user['last_tweet'] < date:
            user['days_tweeted'] += 1
            user['last_tweet'] = date

        nobjs = stats['performance']['input']['tweets']
        if (nobjs-1) % NTWEET == 0:
            utils.dot()
    return lang


def init_user(raw_obj, date:datetime) -> dict:
    user = raw_obj['user']
    return {
        "id": int(user['id']),
        "screen_name": user['screen_name'],
        "tweets": 0,
        "days_tweeted": 1,
        "location": user["location"],
        "followers_count": int(user["followers_count"]),
        "statuses_count": int(user["statuses_count"]),
        "last_tweet": date
    }


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'analyse-users',
        help='Analyse users from tweets and generate a file which contains info about every user and their number of tweets.',
    )

    parser.set_defaults(func=main, finalize=write_users)


def main(
        dump: Iterable[list],
        basename: str,
        args: argparse.Namespace,
        shared: dict) -> None:
    """Main function that parses the arguments and writes the output."""
    stats = {
        'performance': {
            'start_time': None,
            'end_time': None,
            'input': {
                'users': 0,
                'tweets': 0
            },
        },
    }
    
    stats['performance']['start_time'] = datetime.datetime.utcnow()

    # extract the date from the name
    path_list = re.split('-|\.', basename)
    date:datetime = datetime.datetime(int(path_list[2]), int(path_list[3]), int(path_list[4]))
    
    # process the dump
    lang = process_lines(
        dump,
        stats=stats,
        shared=shared,
        date=date
    )

    stats_output = open(os.devnull, 'wt')

    if not args.dry_run:
        stats_path = f"{args.output_dir_path}/analyse-users/stats/{lang}"
        Path(stats_path).mkdir(parents=True, exist_ok=True)
        varname = ('{basename}.{func}'
                   .format(basename=basename,
                           func='analyse-users'
                           )
                   )
        stats_filename = f"{stats_path}/{varname}.stats.xml"

        stats_output = fu.output_writer(
            path=stats_filename,
            compression=args.output_compression,
            mode='wt'
        )

    stats['performance']['end_time'] = datetime.datetime.utcnow()
    
    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )
    
    stats_output.close()

def write_users(
        args: argparse.Namespace,
        shared) -> None:
    
    stats = {
        'performance': {
            'start_time': None,
            'end_time': None,
            'input': {
                'users': 0,
                'languages': 0
            },
        },
    }
    
    stats['performance']['start_time'] = datetime.datetime.utcnow()
    
    stats_output = open(os.devnull, 'wt')
    if not args.dry_run:
        stats_path = f"{args.output_dir_path}/analyse-users/stats"
        Path(stats_path).mkdir(parents=True, exist_ok=True)
        varname = ('{basename}.{func}'
                .format(basename='coronavirus-tweets',
                        func='analyse-users-finalize'
                        )
                )
        stats_filename = f"{stats_path}/{varname}.stats.xml"

        stats_output = fu.output_writer(
            path=stats_filename,
            compression=args.output_compression,
            mode='wt'
        )

    for lang in shared:
        stats['performance']['input']['languages'] += 1
        output = open(os.devnull, 'wt')

        if not args.dry_run:
            file_path = f"{args.output_dir_path}/analyse-users"
            Path(file_path).mkdir(parents=True, exist_ok=True)

            output_filename = f"{file_path}/{lang}-users.csv"

            output = fu.output_writer(
                path=output_filename,
                compression=args.output_compression,
            )
        
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for user_id in shared[lang]:
            stats['performance']['input']['users'] += 1
            user = shared[lang][user_id]
            writer.writerow(user)
        
        output.close

    stats['performance']['end_time'] = datetime.datetime.utcnow()

    with stats_output:
        dumper.render_template(
            stats_template_finalize,
            stats_output,
            stats=stats,
        )
    
    stats_output.close()

