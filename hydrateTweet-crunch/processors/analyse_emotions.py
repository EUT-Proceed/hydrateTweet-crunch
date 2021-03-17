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


def process_lines(
        dump: Iterable[list],
        stats: Mapping,
        users_dict:dict,
        stats_dict:dict,
        args:argparse.Namespace) -> str:
    """Assign each revision to the snapshot or snapshots to which they
       belong.
    """

    first = next(dump)
    lang = first['lang']
    if initEmotionLexicon(lang=lang):
        process_tweet(
            first,
            stats=stats,
            stats_dict=stats_dict,
            users_dict=users_dict,
            args=args
        )
        for raw_obj in dump:
            process_tweet(
                raw_obj,
                stats=stats,
                stats_dict=stats_dict,
                users_dict=users_dict,
                args=args
            )
        return lang
    else:
        return None


def process_tweet(
    tweet: dict,
    stats: Mapping,
    users_dict:dict,
    stats_dict:dict,
    args: argparse.Namespace):
    """Analyze a tweet based on the specifics
    """

    full_text = tweet['full_text']
    user_id = str(tweet['user']['id'])
    if  not user_id in users_dict:
        if not args.per_tweet:
            users_dict[user_id] = new_emotions_dict()
            stats_dict['total'] += 1
        else:
            users_dict[user_id] = 0
        stats['performance']['input']['users'] += 1

    if args.per_tweet:
        emotions = new_emotions_dict()
        stats_dict['total'] += 1
    else:
        emotions = users_dict[user_id]

    for emotion in countEmotionsOfText(full_text):
        emotion_name = getEmotionName(emotion)
        if emotion_name in emotions and emotions[emotion_name] == 0:
            stats_dict[emotion_name] += 1
            emotions[emotion_name] = 1
    
    stats['performance']['input']['tweets'] += 1
    nobjs = stats['performance']['input']['tweets']
    if (nobjs-1) % NTWEET == 0:
        utils.dot()


def new_emotions_dict() -> dict:
    emotions = {
            "positive":0, 
            "negative":0, 
            "anger":0, 
            "anticipation":0, 
            "disgust":0, 
            "fear":0, 
            "joy":0, 
            "sadness":0, 
            "surprise":0, 
            "trust":0
        }
    return emotions


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'analyse-emotions',
        help='Analyse the text from tweets and generate a file which contains statistics w.r.t emotions.',
    )
    parser.add_argument(
        '--per-tweet', '-t',
        action='store_true',
        help="Consider each tweet indipendently",
    )

    parser.set_defaults(func=main)

def calculate_emotions_percentage(
        stats_dict:dict,
        args:argparse.Namespace
    ):
    for emotion in Emotions:
        emotion_name = getEmotionName(emotion)
        if emotion_name in stats_dict:
            if stats_dict['total'] > 0:
                stats_dict[f'{emotion_name}_percentage'] = stats_dict[emotion_name]/stats_dict['total']

def main(
        dump: Iterable[list],
        basename: str,
        args: argparse.Namespace) -> None:
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
    users_dict:dict = {}
    fieldnames = [
        "date",
        "positive_percentage", 
        "negative_percentage", 
        "anger_percentage", 
        "anticipation_percentage", 
        "disgust_percentage", 
        "fear_percentage", 
        "joy_percentage", 
        "sadness_percentage", 
        "surprise_percentage", 
        "trust_percentage", 
        "total", 
        "positive", 
        "negative", 
        "anger", 
        "anticipation", 
        "disgust", 
        "fear", 
        "joy", 
        "sadness", 
        "surprise", 
        "trust"
    ]

    stats_dict:dict = {}
    for fieldname in fieldnames:
        stats_dict[fieldname] = 0


    stats['performance']['start_time'] = datetime.datetime.utcnow()

    output = open(os.devnull, 'wt')
    stats_output = open(os.devnull, 'wt')
    addHeader = False

    # process the dump
    lang = process_lines(
        dump,
        stats=stats,
        users_dict=users_dict,
        stats_dict=stats_dict,
        args=args
    )

    # calculate the percentages
    calculate_emotions_percentage(
        stats_dict=stats_dict,
        args=args
    )

    path_list = re.split('-|\.', basename)
    stats_dict['date'] = f"{path_list[3]}/{path_list[4]}/{path_list[2]}"

    if not args.dry_run:
        stats_path = f"{args.output_dir_path}/analyse-emotions/stats"
        Path(stats_path).mkdir(parents=True, exist_ok=True)
        varname = ('{basename}.{func}'
                   .format(basename=basename,
                           func='analyse-emotions'
                           )
                   )
        if args.per_tweet:
            stats_filename = f"{stats_path}/{varname}-per-tweet.stats.xml"
        else:
            stats_filename = f"{stats_path}/{varname}.stats.xml"

        stats_output = fu.output_writer(
            path=stats_filename,
            compression=args.output_compression,
        )

        if not lang is None:
            file_path = f"{args.output_dir_path}/analyse-emotions"
            Path(file_path).mkdir(parents=True, exist_ok=True)

            if args.per_tweet:
                output_filename = f"{file_path}/{lang}-{path_list[0]}-{path_list[1]}-per-tweet.csv"
            else:
                output_filename = f"{file_path}/{lang}-{path_list[0]}-{path_list[1]}.csv"

            if not Path(output_filename).exists() and not Path(f"{output_filename}.{args.output_compression}").exists():
                #The header of the .csv will be added only if the file doesn't exist
                addHeader = True

            output = fu.output_writer(
                path=output_filename,
                compression=args.output_compression,
            )

    writer = csv.DictWriter(output, fieldnames=fieldnames)
    if addHeader:
        writer.writeheader()
    writer.writerow(stats_dict)
    output.close

    stats['performance']['end_time'] = datetime.datetime.utcnow()
    
    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )
    
    stats_output.close()