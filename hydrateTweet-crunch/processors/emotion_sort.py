"""
Analyse tweets and sort them based on the liwc categories or emotions contained in it.

The output format is json.
"""

import os
import csv
import json
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
from ..emotion_lexicon import initEmotionLexicon, countEmotionsOfText, Emotions, getEmotionName

from operator import itemgetter
from pprint import pprint

# print a dot each NTWEET tweets
NTWEET = 10000

RELEVANT_EMOTIONS = ["positive", "negative", "anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]

# templates
stats_template = '''
<stats>
    <performance>
        <start_time>${stats['performance']['start_time'] | x}</start_time>
        <end_time>${stats['performance']['end_time'] | x}</end_time>
        <input>
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
        valid_users = set()
        try:
            json_reader = fu.open_jsonobjects_file(args.users_file)
            for user in json_reader:
                valid_users.add(user["id_str"])
            return valid_users
        except:
            try:
                csv_reader = csv.DictReader(fu.open_csv_file(args.users_file))
                for user in csv_reader:
                    valid_users.add(user["id_str"])
                return valid_users
            except:
                utils.log('The file is neither a .json nor a .csv')
    return None

def process_lines(
        dump: Iterable[list],
        stats: Mapping,
        tweets_dict:dict,
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

    if args.lexicon == 'liwc' and lang in liwc_dicts:
        parse, category_names = liwc.load_token_parser(liwc_dicts[lang])
    elif args.lexicon == 'emolex' and initEmotionLexicon(lang=lang):
        ...
    else:
        return None

    process_tweet(
        first,
        parse=parse,
        stats=stats,
        tweets_dict=tweets_dict,
        valid_users=valid_users,
        args=args
    )
    for raw_obj in dump:
        process_tweet(
            raw_obj,
            parse=parse,
            stats=stats,
            tweets_dict=tweets_dict,
            valid_users=valid_users,
            args=args
        )
    return lang


def process_tweet(
    tweet: dict,
    parse,
    stats: Mapping,
    tweets_dict: dict,
    valid_users,
    args: argparse.Namespace):
    """Analyze the words in a tweet and classify them
    """

    full_text = tweet['full_text']
    user_id = str(tweet['user']['id'])
    tweet_id = str(tweet['id'])

    if args.users_file and not user_id in valid_users:
        return

    if args.lexicon == 'liwc':
        for token in re.split(r'\s+|\.|,|\!|\?|:|;|\(|\)|#|-', full_text.lower()):
            for liwc_category in parse(token):
                if not liwc_category in tweets_dict:
                    tweets_dict[liwc_category] = {}
                category_dict = tweets_dict[liwc_category]
                if not tweet_id in category_dict:
                    category_dict[tweet_id] = {'score': 0, 'full_text': full_text}
                category_dict[tweet_id]['score'] += 1
    elif args.lexicon == 'emolex':
        for emotion, score in countEmotionsOfText(full_text).items():
            emotion_name = getEmotionName(emotion)
            if emotion_name in RELEVANT_EMOTIONS:
                if not emotion_name in tweets_dict:
                    tweets_dict[emotion_name] = {}
                emotion_dict = tweets_dict[emotion_name]
                if not tweet_id in emotion_dict:
                    emotion_dict[tweet_id] = {'score': score, 'full_text': full_text}

    stats['performance']['input']['tweets'] += 1
    nobjs = stats['performance']['input']['tweets']
    if (nobjs-1) % NTWEET == 0:
        utils.dot()


def sort_tweets_per_score(tweets_dict: dict):
    for category, tweets in tweets_dict.items():
        if isinstance(tweets, dict):
            tweets_dict[category] = {k: v for k, v in sorted(tweets.items(), key=lambda item: item[1]['score'], reverse=True)}


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'sort-emotion',
        help='Sorts tweets by liwc categories or emotions while keeping only the relevant fields.',
    )
    parser.add_argument(
        '--lexicon',
        type=str,
        choices={'emolex', 'liwc'},
        required=False,
        default='liwc',
        help='The lexicon used for the emotion detection [default: liwc].',
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
                'tweets': 0
            },
        },
    }

    stats['performance']['start_time'] = datetime.datetime.utcnow()

    output = open(os.devnull, 'wt')
    stats_output = open(os.devnull, 'wt')

    tweets_dict = {}

    # process the dump
    lang = process_lines(
        dump,
        stats=stats,
        tweets_dict=tweets_dict,
        args=args
    )
    
    sort_tweets_per_score(tweets_dict=tweets_dict)

    path_list = re.split('-|\.', basename)
    date = f"{path_list[2]}-{path_list[3]}-{path_list[4]}"

    if not args.dry_run:
        stats_path = f"{args.output_dir_path}/sort-emotion/{lang}/stats"
        Path(stats_path).mkdir(parents=True, exist_ok=True)
        varname = ('{basename}.{func}'
                   .format(basename=basename,
                           func='sort-emotion'
                           )
                   )
        stats_filename = f"{stats_path}/{varname}.stats.xml"

        stats_output = fu.output_writer(
            path=stats_filename,
            compression=args.output_compression,
            mode='wt'
        )

        if not lang is None:
            file_path = f"{args.output_dir_path}/sort-emotion/{lang}"
            Path(file_path).mkdir(parents=True, exist_ok=True)

            output_filename = f"{lang}-{path_list[0]}-{path_list[1]}-{date}.csv"
        
        for category, tweets in tweets_dict.items():
            print(tweets)
            print()
            if isinstance(tweets, dict):
                full_path = f'{file_path}/{category}/{output_filename}'
                Path(f'{file_path}/{category}').mkdir(parents=True, exist_ok=True)
                output = fu.output_writer(
                    path=full_path,
                    compression=args.output_compression,
                    mode='wt'
                )
                for tweet in tweets.values():
                    output.write(json.dumps(tweet))
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