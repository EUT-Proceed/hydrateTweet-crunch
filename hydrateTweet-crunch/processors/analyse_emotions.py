"""
Analyse the text from tweets and generate a file which contains statistics w.r.t emotions.

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
            <lines>${stats['performance']['input']['lines'] | x}</lines>
        </input>
    </performance>
    <results>
        <positive_mean>${stats['results']['positive_mean'] | x}<positive_mean>
        <negative_mean>${stats['results']['negative_mean'] | x}<negative_mean>
        <anger_mean>${stats['results']['anger_mean'] | x}<anger_mean>
        <anticipation_mean>${stats['results']['anticipation_mean'] | x}<anticipation_mean>
        <disgust_mean>${stats['results']['disgust_mean'] | x}<disgust_mean>
        <fear_mean>${stats['results']['fear_mean'] | x}<fear_mean>
        <joy_mean>${stats['results']['joy_mean'] | x}<joy_mean>
        <sadness_mean>${stats['results']['sadness_mean'] | x}<sadness_mean>
        <surprise_mean>${stats['results']['surprise_mean'] | x}<surprise_mean>
        <trust_mean>${stats['results']['trust_mean'] | x}<trust_mean>
        <positive_stdv>${stats['results']['positive_stdv'] | x}<positive_stdv>
        <negative_stdv>${stats['results']['negative_stdv'] | x}<negative_stdv>
        <anger_stdv>${stats['results']['anger_stdv'] | x}<anger_stdv>
        <anticipation_stdv>${stats['results']['anticipation_stdv'] | x}<anticipation_stdv>
        <disgust_stdv>${stats['results']['disgust_stdv'] | x}<disgust_stdv>
        <fear_stdv>${stats['results']['fear_stdv'] | x}<fear_stdv>
        <joy_stdv>${stats['results']['joy_stdv'] | x}<joy_stdv>
        <sadness_stdv>${stats['results']['sadness_stdv'] | x}<sadness_stdv>
        <surprise_stdv>${stats['results']['surprise_stdv'] | x}<surprise_stdv>
        <trust_stdv>${stats['results']['trust_stdv'] | x}<trust_stdv>
    <results>
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
    if not user_id in users_dict:
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
            stats_dict[f'{emotion_name}_count'] += 1
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
        help='Consider each tweet indipendently',
    )
    parser.add_argument(
        '--standardize', '-s',
        action='store_true',
        help='Standardize the results obtained using mean and standard deviation'
    )

    parser.set_defaults(func=main, which='analyse_emotions')

def calculate_emotions(
        stats_dict:dict,
        args:argparse.Namespace
    ):
    for emotion in Emotions:
        emotion_name = getEmotionName(emotion)
        if emotion_name in stats_dict and stats_dict['total'] > 0:
            stats_dict[emotion_name] = stats_dict[f'{emotion_name}_count']/stats_dict['total']

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
    fieldnames = [
        "date",
        "positive", 
        "negative", 
        "anger", 
        "anticipation", 
        "disgust", 
        "fear", 
        "joy", 
        "sadness", 
        "surprise", 
        "trust", 
        "total", 
        "positive_count", 
        "negative_count", 
        "anger_count", 
        "anticipation_count", 
        "disgust_count", 
        "fear_count", 
        "joy_count", 
        "sadness_count", 
        "surprise_count", 
        "trust_count"
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


    # calculate emotions
    calculate_emotions(
        stats_dict=stats_dict,
        args=args
    )

    path_list = re.split('-|\.', basename)
    stats_dict['date'] = f"{path_list[3]}/{path_list[4]}/{path_list[2]}"

    if not args.dry_run:
        stats_path = f"{args.output_dir_path}/analyse-emotions/stats/{lang}"
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
            mode='wt'
        )

        if not lang is None:
            file_path = f"{args.output_dir_path}/analyse-emotions"
            Path(file_path).mkdir(parents=True, exist_ok=True)

            if args.per_tweet:
                output_filename = f"{file_path}/{lang}-{path_list[0]}-{path_list[1]}-per-tweet.csv"
            else:
                output_filename = f"{file_path}/{lang}-{path_list[0]}-{path_list[1]}.csv"

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

            if args.standardize:
                if not output_filename in shared:
                    if args.output_compression:
                        output_filename = '.'.join([output_filename, args.output_compression])
                    shared[output_filename] = new_emotions_dict()

    writer = csv.DictWriter(output, fieldnames=fieldnames)
    if addHeader:
        writer.writeheader()
    writer.writerow(stats_dict)
    output.close()

    stats['performance']['end_time'] = datetime.datetime.utcnow()
    
    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )
    
    stats_output.close()

def standardize(
        args: argparse.Namespace,
        shared) -> None:
    
    fieldnames = [
        "date",
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

    # For each file analyzed before
    for input_file_path in shared:

        stats = {
            'performance': {
                'start_time': None,
                'end_time': None,
                'input': {
                    'lines': 0
                },
            },
            'results':{
                'positive_mean': 0,
                'negative_mean': 0, 
                'anger_mean': 0, 
                'anticipation_mean': 0, 
                'disgust_mean': 0, 
                'fear_mean': 0, 
                'joy_mean': 0, 
                'sadness_mean': 0, 
                'surprise_mean': 0, 
                'trust_mean': 0,
                'positive_stdv': 0,
                'negative_stdv': 0, 
                'anger_stdv': 0, 
                'anticipation_stdv': 0, 
                'disgust_stdv': 0, 
                'fear_stdv': 0, 
                'joy_stdv': 0, 
                'sadness_stdv': 0, 
                'surprise_stdv': 0, 
                'trust_stdv': 0
            }
        }

        stats['performance']['start_time'] = datetime.datetime.utcnow()

        utils.log(f"Calculating mean and standard deviation for {input_file_path}...")
        basename = Path(input_file_path).stem
        if not args.output_compression is None:
            # Remove the .csv.gz
            basename = Path(basename).stem

        stats_dict = shared[input_file_path]

        #init days, mean and stdv
        stats_dict["days"] = 0
        for emotion in Emotions:
            emotion_name = getEmotionName(emotion)
            if emotion_name in stats_dict:
                stats_dict[f"{emotion_name}_mean"] = 0
                stats_dict[f"{emotion_name}_stdv"] = 0

        # Calculate mean for every emotions
        calculate_means(stats_dict, input_file_path, stats)

        # Calculate standard deviation for every emotions
        calculate_stdvs(stats_dict, input_file_path, stats)

        utils.log(f"Writing standardized values for {input_file_path}...")

        output = open(os.devnull, 'wt')
        stats_output = open(os.devnull, 'wt')
        if not args.dry_run:
            stats_path = f"{args.output_dir_path}/standardize/stats"
            Path(stats_path).mkdir(parents=True, exist_ok=True)
            varname = ('{basename}.{func}'
                    .format(basename=basename,
                            func='standardize'
                            )
                    )
            stats_filename = f"{stats_path}/{varname}.stats.xml"

            stats_output = fu.output_writer(
                path=stats_filename,
                compression=args.output_compression,
                mode='wt'
            )

            file_path = f"{args.output_dir_path}/standardize"
            Path(file_path).mkdir(parents=True, exist_ok=True)
            output_filename = f"{file_path}/{basename}-standardized.csv"

            output = fu.output_writer(
                path=output_filename,
                compression=args.output_compression,
                mode='wt'
            )

        writer = csv.DictWriter(output, fieldnames=fieldnames)
        # Write the .csv header
        writer.writeheader()

        csv_file = fu.open_csv_file(input_file_path)
        csv_reader = csv.DictReader(csv_file)
        for line in csv_reader:
            stats['performance']['input']['lines'] += 1
            csv_row = new_emotions_dict()
            csv_row["date"] = line["date"]
            for emotion in Emotions:
                emotion_name = getEmotionName(emotion)
                if emotion_name in stats_dict:
                    emotion_value = float(line[emotion_name])
                    mean = stats_dict[f"{emotion_name}_mean"]
                    stdv = stats_dict[f"{emotion_name}_stdv"]
                    try:
                        csv_row[emotion_name] = (emotion_value - mean) / stdv
                    except:
                        csv_row[emotion_name] = 0
            writer.writerow(csv_row)

        output.close()
        csv_file.close()

        stats['performance']['end_time'] = datetime.datetime.utcnow()
        with stats_output:
            dumper.render_template(
                stats_template_finalize,
                stats_output,
                stats=stats,
            )
    
        stats_output.close()

def calculate_means(
    stats_dict:dict,
    file_path:str,
    stats:dict) -> None:
    csv_file = fu.open_csv_file(file_path)
    csv_reader = csv.DictReader(csv_file)
    for line in csv_reader:
        stats_dict["days"] += 1
        for emotion in Emotions:
            emotion_name = getEmotionName(emotion)
            if emotion_name in stats_dict:
                stats_dict[f"{emotion_name}_mean"] += float(line[emotion_name])
    
    for emotion in Emotions:
            emotion_name = getEmotionName(emotion)
            if emotion_name in stats_dict:
                stats_dict[f"{emotion_name}_mean"] /= stats_dict["days"]
                stats['results'][f"{emotion_name}_mean"] = stats_dict[f"{emotion_name}_mean"]
    
    csv_file.close()

def calculate_stdvs(
        stats_dict:dict,
        file_path:str,
        stats:dict) -> None:
    csv_file = fu.open_csv_file(file_path)
    csv_reader = csv.DictReader(csv_file)
    for line in csv_reader:
        for emotion in Emotions:
            emotion_name = getEmotionName(emotion)
            if emotion_name in stats_dict:
                mean = stats_dict[f"{emotion_name}_mean"]
                emotion_value = float(line[emotion_name])
                stats_dict[f"{emotion_name}_stdv"] += pow(emotion_value - mean, 2)

    for emotion in Emotions:
        emotion_name = getEmotionName(emotion)
        if emotion_name in stats_dict:
            stats['results'][f"{emotion_name}_stdv"] = stats_dict[f"{emotion_name}_stdv"] = math.sqrt(stats_dict[f"{emotion_name}_stdv"] / stats_dict["days"])

    csv_file.close()