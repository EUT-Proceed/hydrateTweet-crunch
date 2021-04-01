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
        <start_preprocess>${stats['performance']['start_preprocess'] | x}</start_preprocess>
        <end_preprocess>${stats['performance']['end_preprocess'] | x}</end_preprocess>
        <start_infer>${stats['performance']['start_infer'] | x}</start_infer>
        <end_infer>${stats['performance']['end_infer'] | x}</end_infer>
        <input>
            <users>${stats['performance']['input']['users'] | x}</users>
            <to_infer>${stats['performance']['input']['to_infer'] | x}</to_infer>
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
        shared: dict,
        m3twitter:M3Twitter
        ) -> Iterator[list]:
    """It checks for each line (user) if the number of tweets is above a certain minimum and if
       it is the case, it uses transform_jsonl_object to get a particular dict to perform 
       inference later on
    """

    for user in dump:
        stats['performance']['input']['users'] += 1
        # trasform the object only if the user reached the minimum number of tweets
        if 'tweets' in user and int(user['tweets']) > args.min_tweets:
            stats['performance']['input']['to_infer'] += 1
            shared[user['id_str']] = init_user(user)
            yield m3twitter.transform_jsonl_object(user)

def init_user(user:dict) -> dict:
    return {
        "id_str": user["id_str"],
        "screen_name": user["screen_name"],
        "name": user["name"],
        "tweets": 0,
        "days_tweeted": 1,
        "location": user["location"],
        "gender": "",
        "gender_acc": -1,
        "age": "",
        "age_acc": -1,
        "org": False,
        "org_acc": -1
    }


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'infer-users',
        help="Given .json file of users it infers gender, age and if it's an organization using m3inference",
    )
    parser.add_argument(
        '--min-tweets',
        type=int,
        required=False,
        default=1,
        help='The minimum number of tweets that a user should have in order to be analysed [default: 1].',
    )

    parser.set_defaults(func=main, which='infer_users')


def main(
        dump: io.TextIOWrapper,
        basename: str,
        args: argparse.Namespace,
        shared: dict) -> None:
    """Main function that parses the arguments and writes the output."""
    stats = {
        'performance': {
            'start_preprocess': None,
            'end_preprocess': None,
            'start_infer': None,
            'end_infer': None,
            'input': {
                'users': 0,
                'to_infer': 0
            },
        },
    }
    
    stats['performance']['start_preprocess'] = datetime.datetime.utcnow()    
    cache_dir=f"{args.output_dir_path}/twitter_cache"
    m3twitter=M3Twitter(cache_dir=cache_dir, use_full_model=True)

    # process the dump
    res = process_lines(
        dump,
        stats=stats,
        args=args,
        shared=shared,
        m3twitter=m3twitter
    )

    m3_input_file = f"{cache_dir}/m3_input.jsonl"
    output = fu.output_writer(
        path=m3_input_file,
        compression=None,
        mode='wt'
    )

    for obj in res:
        if not os.path.exists(obj['img_path']):
            obj['img_path'] = TW_DEFAULT_PROFILE_IMG
        output.write(json.dumps(obj))
        output.write("\n")
    
    output.close()

    stats['performance']['end_preprocess'] = datetime.datetime.utcnow()

    stats_output = open(os.devnull, 'wt')
    output = open(os.devnull, 'wt')
    if not args.dry_run:
        # extract useful info from the name
        path_list = re.split('-|\.', basename)
        lang = path_list[0]

        stats_path = f"{args.output_dir_path}/infer-users/stats/{lang}"
        Path(stats_path).mkdir(parents=True, exist_ok=True)
        varname = ('{basename}.{func}'
                   .format(basename=basename,
                           func='infer-users'
                           )
                   )
        stats_filename = f"{stats_path}/{varname}.stats.xml"

        stats_output = fu.output_writer(
            path=stats_filename,
            compression=args.output_compression,
            mode='wt'
        )

        file_path = f"{args.output_dir_path}/infer-users"
        Path(file_path).mkdir(parents=True, exist_ok=True)

        output_filename = f"{file_path}/{lang}-users-inference.csv"

        output = fu.output_writer(
            path=output_filename,
            compression=args.output_compression,
            mode='wt'
        )

    stats['performance']['start_infer'] = datetime.datetime.utcnow()
    
    inferred_users = m3twitter.infer(m3_input_file)

    utils.log('Writing the results...')

    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for user in inferred_users:
        if user in shared:
            user_dict = shared[user]
            inferred_user_stats = inferred_users[user]

            inferred_gender = inferred_user_stats['gender']
            if inferred_gender['female'] >= inferred_gender['male']:
                user_dict['gender'] = 'female'
                user_dict['gender_acc'] = inferred_gender['female']
            else:
                user_dict['gender'] = 'male'
                user_dict['gender_acc'] = inferred_gender['male']

            inferred_age = inferred_user_stats['age']
            for age_index in inferred_age:
                if inferred_age[age_index] > user_dict['age_acc']:
                    user_dict['age_acc'] = inferred_age[age_index]
                    user_dict['age'] = age_index

            inferred_org = inferred_user_stats['org']
            if inferred_org['is-org'] >= inferred_org['non-org']:
                user_dict['org'] = True
                user_dict['org_acc'] = inferred_org['is-org']
            else:
                user_dict['org'] = False
                user_dict['org_acc'] = inferred_org['non-org']

            writer.writerow(user_dict)

    output.close()

    utils.log('Finished to write results')

    stats['performance']['end_infer'] = datetime.datetime.utcnow()

    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )
    
    stats_output.close()

    try:
        utils.log("Deleting cache directory")
        shutil.rmtree(cache_dir)
    except OSError as e:
        utils.log(f"Error: {e.filename} - {e.strerror}.")