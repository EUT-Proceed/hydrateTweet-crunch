"""
Analyse the text from tweets and generate a file which contains statistics w.r.t emotions for the location type specified.

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
from ..emotion_lexicon import initEmotionLexicon, countEmotionsOfText, Emotions, getEmotionName
from .analyse_emotions import get_main_fieldnames, new_emotions_dict, calculate_emotions

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

RELEVANT_EMOTIONS = ["positive", "negative", "anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]

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
        valid_users = get_valid_users(args, lang)
        if not valid_users:
            utils.log('The file of valid users could not be found\n')
            return None

        process_tweet(
            first,
            stats=stats,
            stats_dict=stats_dict,
            users_dict=users_dict,
            valid_users=valid_users,
            args=args
        )
        for raw_obj in dump:
            process_tweet(
                raw_obj,
                stats=stats,
                stats_dict=stats_dict,
                users_dict=users_dict,
                valid_users=valid_users,
                args=args
            )
        return lang
    else:
        return None


def get_valid_users(args: argparse.Namespace,
                    lang: str):
    for compression in ['', '.gz', '.7z', '.bz2']:
        json_file = f"{args.output_dir_path}/map-users-to-location/{args.location_type}/{lang}-geocoded-users-per-{args.location_type}.json{compression}"
        if os.path.exists(json_file):
            json_reader = fu.open_jsonobjects_file(json_file)
            valid_users = {}
            for user in json_reader:
                valid_users[user["id_str"]] = user["location"]
            return valid_users
    return None


def process_tweet(
    tweet: dict,
    stats: Mapping,
    users_dict:dict,
    stats_dict:dict,
    valid_users,
    args: argparse.Namespace):
    """Analyze a tweet based on the specifics
    """

    full_text = tweet['full_text']
    user_id = str(tweet['user']['id'])
    if not user_id in valid_users:
        return
    
    location = valid_users[user_id]

    if not location in stats_dict:
        stats_dict[location] = {}
        fieldnames = get_main_fieldnames(args)
        for fieldname in fieldnames:
            stats_dict[location][fieldname] = 0
    
    location_stats = stats_dict[location] 
        
    if not user_id in users_dict:
        users_dict[user_id] = new_emotions_dict()
        location_stats['total'] += 1
        stats['performance']['input']['users'] += 1

    emotions = users_dict[user_id]

    for emotion in countEmotionsOfText(full_text):
        emotion_name = getEmotionName(emotion)
        if emotion_name in emotions and emotions[emotion_name] == 0:
            location_stats[f'{emotion_name}_count'] += 1
            emotions[emotion_name] = 1
    
    stats['performance']['input']['tweets'] += 1
    nobjs = stats['performance']['input']['tweets']
    if (nobjs-1) % NTWEET == 0:
        utils.dot()


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'analyse-locations-emotions',
        help='Analyse the text from tweets and generate a file which contains statistics w.r.t emotions for the location type specified',
    )
    parser.add_argument(
        '--location-type',
        type=str,
        default='state',
        choices={'state', 'state_district', 'country', 'country_code', 'county', 'town', 'municipality', 'postcode', 'village'},
        help='The type of the locations that will be used to map the user into the right file [default: state]'
    )

    parser.set_defaults(func=main, which='analyse_locations_emotions', filter_users=None, )


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
                'users': 0,
                'tweets': 0
            },
        },
    }

    users_dict:dict = {}
    stats_dict:dict = {}

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

    if not args.dry_run:
        stats_path = f"{args.output_dir_path}/analyse-locations-emotions/{args.location_type}/{lang}/stats"
        Path(stats_path).mkdir(parents=True, exist_ok=True)
        varname = ('{basename}.{func}'
                   .format(basename=basename,
                           func='analyse-locations-emotions'
                           )
                   )
        
        stats_filename = f"{stats_path}/{varname}.stats.xml"

        stats_output = fu.output_writer(
            path=stats_filename,
            compression=args.output_compression,
            mode='wt'
        )
    
    path_list = re.split('-|\.', basename)
    
    for location, location_stats in stats_dict.items():

        location_stats['date'] = f"{path_list[2]}/{path_list[3]}/{path_list[4]}"

        # calculate emotions
        calculate_emotions(
            stats_dict=location_stats,
            args=args
        )
        
        if not args.dry_run:        
            if not lang is None:
                file_path = f"{args.output_dir_path}/analyse-locations-emotions/{args.location_type}/{lang}"
                Path(file_path).mkdir(parents=True, exist_ok=True)

                # create the file base name
                clean_location = re.sub(r"\s+|/", "_", location)
                output_filename = f"{file_path}/{lang}-{path_list[0]}-{path_list[1]}-{clean_location}.csv"

                #The header of the .csv will be added only if the file doesn't exist
                if not args.output_compression:
                    if not Path(output_filename).exists():
                        addHeader = True
                else:
                    if not Path(f"{output_filename}.{args.output_compression}").exists():
                        addHeader = True

                output = fu.output_writer(
                    path=output_filename,
                    compression=args.output_compression,
                )

        writer = csv.DictWriter(output, fieldnames=get_main_fieldnames(args))
        if addHeader:
            writer.writeheader()
        writer.writerow(location_stats)
        output.close()

    stats['performance']['end_time'] = datetime.datetime.utcnow()
    
    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )
    
    stats_output.close()