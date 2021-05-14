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

stats_template_finalize_per_category = '''
<stats>
    <performance>
        <start_time>${stats['performance']['start_time'] | x}</start_time>
        <end_time>${stats['performance']['end_time'] | x}</end_time>
        <input>
            <lines>${stats['performance']['input']['lines'] | x}</lines>
        </input>
    </performance>
    <results>
        <male_positive_mean>${stats['results']['male_positive_mean'] | x}<male_positive_mean>
        <male_negative_mean>${stats['results']['male_negative_mean'] | x}<male_negative_mean>
        <male_anger_mean>${stats['results']['male_anger_mean'] | x}<male_anger_mean>
        <male_anticipation_mean>${stats['results']['male_anticipation_mean'] | x}<male_anticipation_mean>
        <male_disgust_mean>${stats['results']['male_disgust_mean'] | x}<male_disgust_mean>
        <male_fear_mean>${stats['results']['male_fear_mean'] | x}<male_fear_mean>
        <male_joy_mean>${stats['results']['male_joy_mean'] | x}<male_joy_mean>
        <male_sadness_mean>${stats['results']['male_sadness_mean'] | x}<male_sadness_mean>
        <male_surprise_mean>${stats['results']['male_surprise_mean'] | x}<male_surprise_mean>
        <male_trust_mean>${stats['results']['male_trust_mean'] | x}<male_trust_mean>
        <female_positive_mean>${stats['results']['female_positive_mean'] | x}<female_positive_mean>
        <female_negative_mean>${stats['results']['female_negative_mean'] | x}<female_negative_mean>
        <female_anger_mean>${stats['results']['female_anger_mean'] | x}<female_anger_mean>
        <female_anticipation_mean>${stats['results']['female_anticipation_mean'] | x}<female_anticipation_mean>
        <female_disgust_mean>${stats['results']['female_disgust_mean'] | x}<female_disgust_mean>
        <female_fear_mean>${stats['results']['female_fear_mean'] | x}<female_fear_mean>
        <female_joy_mean>${stats['results']['female_joy_mean'] | x}<female_joy_mean>
        <female_sadness_mean>${stats['results']['female_sadness_mean'] | x}<female_sadness_mean>
        <female_surprise_mean>${stats['results']['female_surprise_mean'] | x}<female_surprise_mean>
        <female_trust_mean>${stats['results']['female_trust_mean'] | x}<female_trust_mean>
        <org_positive_mean>${stats['results']['org_positive_mean'] | x}<org_positive_mean>
        <org_negative_mean>${stats['results']['org_negative_mean'] | x}<org_negative_mean>
        <org_anger_mean>${stats['results']['org_anger_mean'] | x}<org_anger_mean>
        <org_anticipation_mean>${stats['results']['org_anticipation_mean'] | x}<org_anticipation_mean>
        <org_disgust_mean>${stats['results']['org_disgust_mean'] | x}<org_disgust_mean>
        <org_fear_mean>${stats['results']['org_fear_mean'] | x}<org_fear_mean>
        <org_joy_mean>${stats['results']['org_joy_mean'] | x}<org_joy_mean>
        <org_sadness_mean>${stats['results']['org_sadness_mean'] | x}<org_sadness_mean>
        <org_surprise_mean>${stats['results']['org_surprise_mean'] | x}<org_surprise_mean>
        <org_trust_mean>${stats['results']['org_trust_mean'] | x}<org_trust_mean>
        <male_positive_stdv>${stats['results']['male_positive_stdv'] | x}<male_positive_stdv>
        <male_negative_stdv>${stats['results']['male_negative_stdv'] | x}<male_negative_stdv>
        <male_anger_stdv>${stats['results']['male_anger_stdv'] | x}<male_anger_stdv>
        <male_anticipation_stdv>${stats['results']['male_anticipation_stdv'] | x}<male_anticipation_stdv>
        <male_disgust_stdv>${stats['results']['male_disgust_stdv'] | x}<male_disgust_stdv>
        <male_fear_stdv>${stats['results']['male_fear_stdv'] | x}<male_fear_stdv>
        <male_joy_stdv>${stats['results']['male_joy_stdv'] | x}<male_joy_stdv>
        <male_sadness_stdv>${stats['results']['male_sadness_stdv'] | x}<male_sadness_stdv>
        <male_surprise_stdv>${stats['results']['male_surprise_stdv'] | x}<male_surprise_stdv>
        <male_trust_stdv>${stats['results']['male_trust_stdv'] | x}<male_trust_stdv>
        <female_positive_stdv>${stats['results']['female_positive_stdv'] | x}<female_positive_stdv>
        <female_negative_stdv>${stats['results']['female_negative_stdv'] | x}<female_negative_stdv>
        <female_anger_stdv>${stats['results']['female_anger_stdv'] | x}<female_anger_stdv>
        <female_anticipation_stdv>${stats['results']['female_anticipation_stdv'] | x}<female_anticipation_stdv>
        <female_disgust_stdv>${stats['results']['female_disgust_stdv'] | x}<female_disgust_stdv>
        <female_fear_stdv>${stats['results']['female_fear_stdv'] | x}<female_fear_stdv>
        <female_joy_stdv>${stats['results']['female_joy_stdv'] | x}<female_joy_stdv>
        <female_sadness_stdv>${stats['results']['female_sadness_stdv'] | x}<female_sadness_stdv>
        <female_surprise_stdv>${stats['results']['female_surprise_stdv'] | x}<female_surprise_stdv>
        <female_trust_stdv>${stats['results']['female_trust_stdv'] | x}<female_trust_stdv>
        <org_positive_stdv>${stats['results']['org_positive_stdv'] | x}<org_positive_stdv>
        <org_negative_stdv>${stats['results']['org_negative_stdv'] | x}<org_negative_stdv>
        <org_anger_stdv>${stats['results']['org_anger_stdv'] | x}<org_anger_stdv>
        <org_anticipation_stdv>${stats['results']['org_anticipation_stdv'] | x}<org_anticipation_stdv>
        <org_disgust_stdv>${stats['results']['org_disgust_stdv'] | x}<org_disgust_stdv>
        <org_fear_stdv>${stats['results']['org_fear_stdv'] | x}<org_fear_stdv>
        <org_joy_stdv>${stats['results']['org_joy_stdv'] | x}<org_joy_stdv>
        <org_sadness_stdv>${stats['results']['org_sadness_stdv'] | x}<org_sadness_stdv>
        <org_surprise_stdv>${stats['results']['org_surprise_stdv'] | x}<org_surprise_stdv>
        <org_trust_stdv>${stats['results']['org_trust_stdv'] | x}<org_trust_stdv>
    <results>
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
        valid_users=None
        if args.per_category:
            valid_users = get_valid_users(args, lang)
            if not valid_users:
                utils.log('The file of valid users could not be found')
                return None
        process_tweet(
            first,
            stats=stats,
            stats_dict=stats_dict,
            users_dict=users_dict,
            valid_user=valid_users,
            args=args
        )
        for raw_obj in dump:
            process_tweet(
                raw_obj,
                stats=stats,
                stats_dict=stats_dict,
                users_dict=users_dict,
                valid_user=valid_users,
                args=args
            )
        return lang
    else:
        return None


def get_valid_users(args: argparse.Namespace,
                       lang: str):
    for compression in ['', '.gz', '.7z', '.bz2']:
        csv_file = f"{args.output_dir_path}/filter-inferred/{lang}-inferred-users.csv{compression}"
        if os.path.exists(csv_file):
            csv_reader = csv.DictReader(fu.open_csv_file(csv_file))
            valid_users = {}
            for inferred_user in csv_reader:
                valid_users[inferred_user["id_str"]] = inferred_user["category"]
            return valid_users
    return None


def process_tweet(
    tweet: dict,
    stats: Mapping,
    users_dict:dict,
    stats_dict:dict,
    valid_user:dict,
    args: argparse.Namespace):
    """Analyze a tweet based on the specifics
    """

    full_text = tweet['full_text']
    user_id = str(tweet['user']['id'])
    if args.per_category and not user_id in valid_user:
        return
    elif args.per_category: 
        category = f'{valid_user[user_id]}_'
    else:
        category = ''
        
    if (not user_id in users_dict) and (not args.per_tweet):
        users_dict[user_id] = new_emotions_dict()
        stats_dict[f'{category}total'] += 1
        stats['performance']['input']['users'] += 1

    if args.per_tweet:
        emotions = new_emotions_dict()
        stats_dict[f'{category}total'] += 1
    else:
        emotions = users_dict[user_id]

    for emotion in countEmotionsOfText(full_text):
        emotion_name = getEmotionName(emotion)
        if emotion_name in emotions and emotions[emotion_name] == 0:
            stats_dict[f'{category}{emotion_name}_count'] += 1
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
        '--per-category', '-c',
        choices={'over-category', 'over-total'},
        required=False,
        default=None,
        help='Calculate statistics w.r.t three main categories (male, female, org) over the specific category or over the total [default: None]',
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
        if args.per_category:
            for category in ['male', 'female', 'org']:
                emotion_category_name = f"{category}_{emotion_name}"
                if args.per_category == 'over-category':
                    divider = stats_dict[f'{category}_total']
                else: 
                    divider = stats_dict[f'male_total'] + stats_dict[f'female_total'] + stats_dict[f'org_total']

                if emotion_name in RELEVANT_EMOTIONS and stats_dict[f'{category}_total'] > 0:
                    stats_dict[emotion_category_name] = stats_dict[f'{emotion_category_name}_count']/divider
        else:
            if emotion_name in RELEVANT_EMOTIONS and stats_dict['total'] > 0:
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
    fieldnames = get_main_fieldnames(args)

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
        elif args.per_category:
            stats_filename = f"{stats_path}/{varname}-per-category.stats.xml"
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
            elif args.per_category == 'over-category':
                output_filename = f"{file_path}/{lang}-{path_list[0]}-{path_list[1]}-per-category-over-category.csv"
            elif args.per_category == 'over-total':
                output_filename = f"{file_path}/{lang}-{path_list[0]}-{path_list[1]}-per-category-over-total.csv"
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
                    shared[output_filename] = {}

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
    
    fieldnames = get_standardize_fieldnames(args)

    # For each file analyzed before
    for input_file_path in shared:

        stats = get_standardize_stats(args)

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
            if emotion_name in RELEVANT_EMOTIONS:
                if args.per_category and args.per_category == 'over-category':
                    for category in ['male', 'female', 'org']:
                        emotion_category_name = f"{category}_{emotion_name}"
                        stats_dict[f"{emotion_category_name}_mean"] = 0
                        stats_dict[f"{emotion_category_name}_stdv"] = 0
                else:
                    stats_dict[f"{emotion_name}_mean"] = 0
                    stats_dict[f"{emotion_name}_stdv"] = 0

        # Calculate mean for every emotions
        calculate_means(stats_dict, input_file_path, stats, args)

        # Calculate standard deviation for every emotions
        calculate_stdvs(stats_dict, input_file_path, stats, args)

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
            csv_row = {}
            csv_row["date"] = line["date"]
            for emotion in Emotions:
                emotion_name = getEmotionName(emotion)
                if emotion_name in RELEVANT_EMOTIONS:
                    if args.per_category:
                        for category in ['male', 'female', 'org']:
                            emotion_category_name = f"{category}_{emotion_name}"
                            emotion_value = float(line[emotion_category_name])
                            if args.per_category == 'over-category':
                                mean = stats_dict[f"{emotion_category_name}_mean"]
                                stdv = stats_dict[f"{emotion_category_name}_stdv"]
                            else:
                                mean = stats_dict[f"{emotion_name}_mean"]
                                stdv = stats_dict[f"{emotion_name}_stdv"]

                            try:
                                csv_row[emotion_category_name] = (emotion_value - mean) / stdv
                            except:
                                csv_row[emotion_category_name] = 0
                    else:
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
                stats_template_finalize if not args.per_category else stats_template_finalize_per_category,
                stats_output,
                stats=stats,
            )
    
        stats_output.close()


def calculate_means(
        stats_dict:dict,
        file_path:str,
        stats:dict,
        args:argparse.Namespace) -> None:
    csv_file = fu.open_csv_file(file_path)
    csv_reader = csv.DictReader(csv_file)
    for line in csv_reader:
        stats_dict["days"] += 1
        for emotion in Emotions:
            emotion_name = getEmotionName(emotion)
            if emotion_name in RELEVANT_EMOTIONS:
                if args.per_category:
                    for category in ['male', 'female', 'org']:
                        emotion_category_name = f"{category}_{emotion_name}"
                        if args.per_category == 'over-category':
                            stats_dict[f"{emotion_category_name}_mean"] += float(line[emotion_category_name])
                        else:
                            stats_dict[f"{emotion_name}_mean"] += float(line[emotion_category_name])
                else:
                    stats_dict[f"{emotion_name}_mean"] += float(line[emotion_name])
    
    for emotion in Emotions:
            emotion_name = getEmotionName(emotion)
            if emotion_name in RELEVANT_EMOTIONS:
                if args.per_category:
                    if args.per_category == 'over-total':
                        stats_dict[f"{emotion_name}_mean"] /= stats_dict["days"]
                    for category in ['male', 'female', 'org']:
                        emotion_category_name = f"{category}_{emotion_name}"
                        if args.per_category == 'over-category':
                            stats_dict[f"{emotion_category_name}_mean"] /= stats_dict["days"]
                            stats['results'][f"{emotion_category_name}_mean"] = stats_dict[f"{emotion_category_name}_mean"]
                        else:
                            stats['results'][f"{emotion_category_name}_mean"] = stats_dict[f"{emotion_name}_mean"]
                else:
                    stats_dict[f"{emotion_name}_mean"] /= stats_dict["days"]
                    stats['results'][f"{emotion_name}_mean"] = stats_dict[f"{emotion_name}_mean"]
    
    csv_file.close()


def calculate_stdvs(
        stats_dict:dict,
        file_path:str,
        stats:dict,
        args:argparse.Namespace) -> None:
    csv_file = fu.open_csv_file(file_path)
    csv_reader = csv.DictReader(csv_file)
    for line in csv_reader:
        for emotion in Emotions:
            emotion_name = getEmotionName(emotion)
            if emotion_name in RELEVANT_EMOTIONS:
                if args.per_category:
                    emotion_value_over_total = 0
                    for category in ['male', 'female', 'org']:
                        emotion_category_name = f"{category}_{emotion_name}"
                        emotion_value = float(line[emotion_category_name])
                        if args.per_category == 'over-category':
                            mean = stats_dict[f"{emotion_category_name}_mean"]
                            stats_dict[f"{emotion_category_name}_stdv"] += (emotion_value - mean)**2
                        else:
                            emotion_value_over_total += emotion_value
                    if args.per_category == 'over-total':
                        mean = stats_dict[f"{emotion_name}_mean"]
                        stats_dict[f"{emotion_name}_stdv"] += (emotion_value_over_total - mean)**2
                else:
                    mean = stats_dict[f"{emotion_name}_mean"]
                    emotion_value = float(line[emotion_name])
                    stats_dict[f"{emotion_name}_stdv"] += (emotion_value - mean)**2

    for emotion in Emotions:
        emotion_name = getEmotionName(emotion)
        if emotion_name in RELEVANT_EMOTIONS:
            if args.per_category:
                if args.per_category == 'over-total':
                    stats_dict[f"{emotion_name}_stdv"] = math.sqrt(stats_dict[f"{emotion_name}_stdv"] / stats_dict["days"])
                for category in ['male', 'female', 'org']:
                    emotion_category_name = f"{category}_{emotion_name}"
                    if args.per_category == 'over-category':
                        stats['results'][f"{emotion_category_name}_stdv"] = stats_dict[f"{emotion_category_name}_stdv"] = math.sqrt(stats_dict[f"{emotion_category_name}_stdv"] / stats_dict["days"])
                    else:
                        stats['results'][f"{emotion_category_name}_stdv"] = stats_dict[f"{emotion_name}_stdv"]
            else:
                stats['results'][f"{emotion_name}_stdv"] = stats_dict[f"{emotion_name}_stdv"] = math.sqrt(stats_dict[f"{emotion_name}_stdv"] / stats_dict["days"])

    csv_file.close()


def get_main_fieldnames(args: argparse.Namespace) -> Iterable[str]:
    if args.per_category:
        return [
            "date",
            "male_positive", 
            "male_negative", 
            "male_anger", 
            "male_anticipation", 
            "male_disgust", 
            "male_fear", 
            "male_joy", 
            "male_sadness", 
            "male_surprise", 
            "male_trust", 
            "male_total",
            "female_positive", 
            "female_negative", 
            "female_anger", 
            "female_anticipation", 
            "female_disgust", 
            "female_fear", 
            "female_joy", 
            "female_sadness", 
            "female_surprise", 
            "female_trust", 
            "female_total",  
            "org_positive", 
            "org_negative", 
            "org_anger", 
            "org_anticipation", 
            "org_disgust", 
            "org_fear", 
            "org_joy", 
            "org_sadness", 
            "org_surprise", 
            "org_trust", 
            "org_total",                      
            "male_positive_count", 
            "male_negative_count", 
            "male_anger_count", 
            "male_anticipation_count", 
            "male_disgust_count", 
            "male_fear_count", 
            "male_joy_count", 
            "male_sadness_count", 
            "male_surprise_count", 
            "male_trust_count",
            "female_positive_count", 
            "female_negative_count", 
            "female_anger_count", 
            "female_anticipation_count", 
            "female_disgust_count", 
            "female_fear_count", 
            "female_joy_count", 
            "female_sadness_count", 
            "female_surprise_count", 
            "female_trust_count",
            "org_positive_count", 
            "org_negative_count", 
            "org_anger_count", 
            "org_anticipation_count", 
            "org_disgust_count", 
            "org_fear_count", 
            "org_joy_count", 
            "org_sadness_count", 
            "org_surprise_count", 
            "org_trust_count"                        
        ] 
    else:
        return [
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


def get_standardize_stats(args: argparse.Namespace) -> dict:
    if args.per_category:
        return {
            'performance': {
                'start_time': None,
                'end_time': None,
                'input': {
                    'lines': 0
                },
            },
            'results':{
                'male_negative_mean': 0, 
                'male_anger_mean': 0, 
                'male_anticipation_mean': 0, 
                'male_disgust_mean': 0, 
                'male_fear_mean': 0, 
                'male_joy_mean': 0, 
                'male_sadness_mean': 0, 
                'male_surprise_mean': 0, 
                'male_trust_mean': 0,
                'male_positive_stdv': 0,
                'male_negative_stdv': 0, 
                'male_anger_stdv': 0, 
                'male_anticipation_stdv': 0, 
                'male_disgust_stdv': 0, 
                'male_fear_stdv': 0, 
                'male_joy_stdv': 0, 
                'male_sadness_stdv': 0, 
                'male_surprise_stdv': 0, 
                'male_trust_stdv': 0,
                'female_negative_mean': 0, 
                'female_anger_mean': 0, 
                'female_anticipation_mean': 0, 
                'female_disgust_mean': 0, 
                'female_fear_mean': 0, 
                'female_joy_mean': 0, 
                'female_sadness_mean': 0, 
                'female_surprise_mean': 0, 
                'female_trust_mean': 0,
                'female_positive_stdv': 0,
                'female_negative_stdv': 0, 
                'female_anger_stdv': 0, 
                'female_anticipation_stdv': 0, 
                'female_disgust_stdv': 0, 
                'female_fear_stdv': 0, 
                'female_joy_stdv': 0, 
                'female_sadness_stdv': 0, 
                'female_surprise_stdv': 0, 
                'female_trust_stdv': 0,
                'org_negative_mean': 0, 
                'org_anger_mean': 0, 
                'org_anticipation_mean': 0, 
                'org_disgust_mean': 0, 
                'org_fear_mean': 0, 
                'org_joy_mean': 0, 
                'org_sadness_mean': 0, 
                'org_surprise_mean': 0, 
                'org_trust_mean': 0,
                'org_positive_stdv': 0,
                'org_negative_stdv': 0, 
                'org_anger_stdv': 0, 
                'org_anticipation_stdv': 0, 
                'org_disgust_stdv': 0, 
                'org_fear_stdv': 0, 
                'org_joy_stdv': 0, 
                'org_sadness_stdv': 0, 
                'org_surprise_stdv': 0, 
                'org_trust_stdv': 0                
            }
        }
    else:
        return {
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


def get_standardize_fieldnames(args: argparse.Namespace) -> Iterable[str]:
    if args.per_category:
        return [
            "date",
            "male_positive", 
            "male_negative", 
            "male_anger", 
            "male_anticipation", 
            "male_disgust", 
            "male_fear", 
            "male_joy", 
            "male_sadness", 
            "male_surprise", 
            "male_trust",
            "female_positive", 
            "female_negative", 
            "female_anger", 
            "female_anticipation", 
            "female_disgust", 
            "female_fear", 
            "female_joy", 
            "female_sadness", 
            "female_surprise", 
            "female_trust",
            "org_positive", 
            "org_negative", 
            "org_anger", 
            "org_anticipation", 
            "org_disgust", 
            "org_fear", 
            "org_joy", 
            "org_sadness", 
            "org_surprise", 
            "org_trust"
        ]
    else:
        return [
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
