"""
Standardize the results from a .csv using mean and standard deviation.

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
from .analyse_emotions import calculate_means, calculate_stdvs, new_emotions_dict 

from operator import itemgetter
from pprint import pprint

# templates
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


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'standardize',
        help='Standardize the results from a .csv using mean and standard deviation',
    )

    parser.set_defaults(func=main, which='standardize')

def main(
        input_file_path: Path,
        basename: str,
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

    if not args.output_compression is None:
        # Remove the .csv.gz
        basename = Path(basename).stem

    stats_dict = new_emotions_dict()

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