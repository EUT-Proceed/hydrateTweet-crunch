"""
Analyse the text of the tweets and keep track of the number of times a word is used (w.r.t. the liwc category it belongs to).

The output format is csv.
"""

import os
import csv
import re
import argparse
import datetime
import math
import liwc
from pathlib import Path

from typing import Iterable, Iterator, Mapping, Counter

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
            <words>${stats['performance']['input']['words'] | x}</words>
            <tweets>${stats['performance']['input']['tweets'] | x}</tweets>
        </input>
    </performance>
</stats>
'''

# add others if needed
liwc_dicts = {
    'it': 'hydrateTweet-crunch/assets/Italian_LIWC2007_Dictionary.dic',
    'en': 'hydrateTweet-crunch/assets/English_LIWC2015_Dictionary.dic',
    'es': 'hydrateTweet-crunch/assets/Spanish_LIWC2007_Dictionary.dic',
}

def get_valid_users(args: argparse.Namespace):
    if os.path.exists(args.users_file):
        try:
            json_reader = fu.open_jsonobjects_file(args.users_file)
            valid_users = set()
            for user in json_reader:
                valid_users.add(user["id_str"])
            return valid_users
        except:
            try:
                csv_reader = csv.DictReader(fu.open_csv_file(args.users_file))
                valid_users = {}
                for user in csv_reader:
                    valid_users.add(user["id_str"])
                return valid_users
            except:
                utils.log('The file is neither a .json nor a .csv')

    return None

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

    valid_users=None

    if args.users_file:
        utils.log('Specified a set of users to filter the tweet')
        valid_users = get_valid_users(args)
        if not valid_users:
            utils.log('The file of valid users could not be found\n')
            return None

    if lang in liwc_dicts:
        parse, category_names = liwc.load_token_parser(liwc_dicts[lang])

        for category in category_names:
            words_dict[category] = {}

        words_dict['words'] = 0
        words_dict['tweets'] = 0

        process_tweet(
            first,
            parse=parse,
            stats=stats,
            words_dict=words_dict,
            valid_users=valid_users,
            args=args
        )
        for raw_obj in dump:
            process_tweet(
                raw_obj,
                parse=parse,
                stats=stats,
                words_dict=words_dict,
                valid_users=valid_users,
                args=args
            )
        return lang
    else:
        return None


def process_tweet(
    tweet: dict,
    parse,
    stats: Mapping,
    words_dict: dict,
    valid_users,
    args: argparse.Namespace):
    """Analyze the words in a tweet and save their occurrences w.r.t. their emotion
    """

    full_text = tweet['full_text']
    user_id = str(tweet['user']['id'])

    if args.users_file and not user_id in valid_users:
        return

    for word in re.split(r'\s+|\.|,|\!|\?|:|;|\(|\)|#|-', full_text.lower()):
        for i, liwc_category in enumerate( parse(word)):
            if i == 0:
                stats['performance']['input']['words'] += 1
                words_dict['words'] += 1
            if not word in words_dict[liwc_category]:
                words_dict[liwc_category][word] = 1
            else:
                words_dict[liwc_category][word] += 1
                

    words_dict['tweets'] += 1
    stats['performance']['input']['tweets'] += 1
    nobjs = stats['performance']['input']['tweets']
    if (nobjs-1) % NTWEET == 0:
        utils.dot()


def get_first_n_words(
    stats: Mapping,
    words_dict: dict,
    args: argparse.Namespace):
    for category, words in words_dict.items():
        if isinstance(words, dict):
            sorted_words = [k for k, _ in sorted(words.items(), key=lambda item: item[1], reverse=True)]
            n_words = args.n_words if len(sorted_words) >= args.n_words else len(sorted_words)
            for i in range(n_words):
                word = sorted_words[i]
                yield {
                    'word': word, 
                    'occurrences': words[word], 
                    'occ/words': words[word]/words_dict['words'], 
                    'occ/tweets': words[word]/words_dict['tweets'], 
                    'category': category
                    }


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'calc-liwc-words-frequency',
        help='Analyse the text of the tweets and keep track of the number of times a word is used (w.r.t. the liwc category it belongs to)',
    )
    parser.add_argument(
        '--n-words',
        type=int,
        required=False,
        default=30,
        help='The number of words per category that will be saved on the output file [default: 30].',
    )
    parser.add_argument(
        '--users-file',
        type=Path,
        required=False,
        default=None,
        help='Optional file containing the users whose tweet will be considered in the process.',
    )

    parser.set_defaults(func=main, which='calc_liwc_words_frequency')


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

    stats['performance']['start_time'] = datetime.datetime.utcnow()

    output = open(os.devnull, 'wt')
    stats_output = open(os.devnull, 'wt')

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
    date = f"{path_list[2]}-{path_list[3]}-{path_list[4]}"

    if not args.dry_run:
        stats_path = f"{args.output_dir_path}/liwc-words-frequency/stats/{lang}"
        Path(stats_path).mkdir(parents=True, exist_ok=True)
        varname = ('{basename}.{func}'
                   .format(basename=basename,
                           func='calc-liwc-words-frequency'
                           )
                   )
        stats_filename = f"{stats_path}/{varname}.stats.xml"

        stats_output = fu.output_writer(
            path=stats_filename,
            compression=args.output_compression,
            mode='wt'
        )

        if not lang is None:
            file_path = f"{args.output_dir_path}/liwc-words-frequency"
            Path(file_path).mkdir(parents=True, exist_ok=True)

            output_filename = f"{file_path}/{lang}-{path_list[0]}-{path_list[1]}-{date}-top-{args.n_words}-words.csv"

            output = fu.output_writer(
                path=output_filename,
                compression=args.output_compression,
                mode='wt'
            )
        fieldnames = ['word', 'occurrences', 'occ/words', 'occ/tweets', 'category']

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