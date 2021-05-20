"""
Analyse the text of the tweets and keep track of the number of times a word is used (w.r.t. the emotion it belongs to).

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
from ..emotion_lexicon import initEmotionLexicon, Emotions, getEmotionName, tokenize, getEmotionsOfWord

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
            <words>${stats['performance']['input']['words'] | x}</words>
            <tweets>${stats['performance']['input']['tweets'] | x}</tweets>
        </input>
    </performance>
</stats>
'''

RELEVANT_EMOTIONS = ["positive", "negative", "anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]

def process_lines(
        dump: Iterable[list],
        stats: Mapping,
        words_dict:dict,
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
            words_dict=words_dict,
            args=args
        )
        for raw_obj in dump:
            process_tweet(
                raw_obj,
                stats=stats,
                words_dict=words_dict,
                args=args
            )
        return lang
    else:
        return None


def process_tweet(
    tweet: dict,
    stats: Mapping,
    words_dict: dict,
    args: argparse.Namespace):
    """Analyze the words in a tweet and save their occurrences w.r.t. their emotion
    """

    full_text = tweet['full_text']

    for word in tokenize(full_text):
        emotions = getEmotionsOfWord(word)

        # Check if the list is empty
        if emotions:
            stats['performance']['input']['words'] += 1
            words_dict['total'] += 1

        for emotion in emotions:
            emotion = getEmotionName(emotion)
            if emotion in RELEVANT_EMOTIONS:
                if not word in words_dict[emotion]:
                    words_dict[emotion][word] = 1
                else:
                    words_dict[emotion][word] += 1

    stats['performance']['input']['tweets'] += 1
    nobjs = stats['performance']['input']['tweets']
    if (nobjs-1) % NTWEET == 0:
        utils.dot()

def get_first_n_words(
    stats: Mapping,
    words_dict: dict,
    args: argparse.Namespace):
    for emotion, words in words_dict.items():
        if emotion in RELEVANT_EMOTIONS:
            sorted_words = [k for k, _ in sorted(words.items(), key=lambda item: item[1], reverse=True)]
            n_words = args.n_words if len(sorted_words) >= args.n_words else len(sorted_words)
            for i in range(n_words):
                word = sorted_words[i]
                yield {'word': word, 'occurrences': words[word], 'emotion': emotion}
            

def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'calc-words-frequency',
        help='Analyse the text of the tweets and keep track of the number of times a word is used (w.r.t. the emotion it belongs to)',
    )
    parser.add_argument(
        '--n-words',
        type=int,
        required=False,
        default=30,
        help='The number of words per emotion that will be saved on the output file [default: 30].',
    )

    parser.set_defaults(func=main, which='calc_words_frequency')

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
                'words': 0,
                'tweets': 0
            },
        },
    }

    if args.n_words <= 0:
        utils.log('the parameter --n-words cannot be lower than 1, exiting...')
        exit(1)

    words_dict:dict = {}

    for emotion in RELEVANT_EMOTIONS:
        words_dict[emotion] = {}

    words_dict['total'] = 0

    stats['performance']['start_time'] = datetime.datetime.utcnow()

    output = open(os.devnull, 'wt')
    stats_output = open(os.devnull, 'wt')
    addHeader = False

    # process the dump
    lang = process_lines(
        dump,
        stats=stats,
        words_dict=words_dict,
        args=args
    )
    
    res = get_first_n_words(
        stats=stats,
        words_dict=words_dict,
        args=args
    )

    path_list = re.split('-|\.', basename)
    date = f"{path_list[3]}-{path_list[4]}-{path_list[2]}"

    if not args.dry_run:
        stats_path = f"{args.output_dir_path}/words-frequency/stats/{lang}"
        Path(stats_path).mkdir(parents=True, exist_ok=True)
        varname = ('{basename}.{func}'
                   .format(basename=basename,
                           func='calc-words-frequency'
                           )
                   )
        stats_filename = f"{stats_path}/{varname}.stats.xml"

        stats_output = fu.output_writer(
            path=stats_filename,
            compression=args.output_compression,
            mode='wt'
        )

        if not lang is None:
            file_path = f"{args.output_dir_path}/words-frequency"
            Path(file_path).mkdir(parents=True, exist_ok=True)

            output_filename = f"{file_path}/{lang}-{path_list[0]}-{path_list[1]}-{date}-top-{args.n_words}-words.csv"

            output = fu.output_writer(
                path=output_filename,
                compression=args.output_compression,
                mode='wt'
            )
    
    fieldnames = ['word', 'occurrences', 'emotion']

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