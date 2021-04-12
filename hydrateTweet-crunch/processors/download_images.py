"""
Infer gender, age and if it's an organization given a user.

The output format is csv.
"""

import os
import shutil
import io
import csv
import json
import re
import argparse
import datetime
from pathlib import Path
from m3inference import M3Twitter
from m3inference.consts import TW_DEFAULT_PROFILE_IMG
from m3inference.preprocess import download_resize_img
from m3inference.m3twitter import get_extension

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
            <users>${stats['performance']['input']['users'] | x}</users>
            <to_download>${stats['performance']['input']['to_download'] | x}</to_download>
        </input>
    </performance>
</stats>
'''

fieldnames = [
    "id_str",
    "screen_name",
    "name",
    "tweets",
    "days_tweeted",
    "location",
    "gender",
    "gender_acc",
    "age",
    "age_acc",
    "org",
    "org_acc"
]


def process_lines(
        dump: io.TextIOWrapper,
        stats: Mapping,
        args:argparse.Namespace,
        cache_dir: str
        ) -> Iterator[list]:
    """It checks for each line (user) if the number of tweets is above a certain minimum and if
       it is the case, it downloads the image
    """
    
    for user in dump:
        stats['performance']['input']['users'] += 1
        # download the image only if the user reached the minimum number of tweets
        if 'tweets' in user and int(user['tweets']) >= args.min_tweets:
            img_path = user["profile_image_url_https"]
            if img_path != "" and not user['default_profile_image']:
                stats['performance']['input']['to_download'] += 1
                img_path = img_path.replace("_normal", "_400x400")
                img_file_resize = "{}/{}_224x224.{}".format(cache_dir, user["id_str"], get_extension(img_path))
                if not args.dry_run:
                    download_resize_img(img_path, img_file_resize)


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'download-images',
        help="Given a json file, it downloads the corresponding image for each lines in the format specified by m3inference. Used to download images in parallel",
    )
    parser.add_argument(
        '--min-tweets',
        type=int,
        required=False,
        default=2,
        help='The minimum number of tweets that a user should have in order to be analysed [default: 2].',
    )
    parser.add_argument(
        '--cache-dir',
        type=str,
        required=True,
        help='The name of the cache directory that will be used later on by m3inference: parameter is appended to \"twitter_cache_\".'
    )

    parser.set_defaults(func=main, which='download_images')


def main(
        dump: io.TextIOWrapper,
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
                'to_download': 0
            },
        },
    }
    
    stats['performance']['start_time'] = datetime.datetime.utcnow()
    cache_dir=f"{args.output_dir_path}/twitter_cache_{args.cache_dir}"
    Path(cache_dir).mkdir(parents=True, exist_ok=True)

    # process the dump
    process_lines(
        dump,
        stats=stats,
        args=args,
        cache_dir=cache_dir
    )
    
    stats_output = open(os.devnull, 'wt')
    if not args.dry_run:
        # extract useful info from the name
        path_list = re.split('-|\.', basename)
        lang = path_list[0]

        stats_path = f"{args.output_dir_path}/download-images/stats/{lang}"
        Path(stats_path).mkdir(parents=True, exist_ok=True)
        varname = ('{basename}-{pid}.{func}'
                   .format(basename=basename,
                           pid=os.getpid(),
                           func='download-images'
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