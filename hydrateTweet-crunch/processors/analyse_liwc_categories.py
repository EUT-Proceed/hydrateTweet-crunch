"""
Analyse the text from tweets and generate a file which contains statistics w.r.t liwc categories.

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
            <users>${stats['performance']['input']['users'] | x}</users>
            <tweets>${stats['performance']['input']['tweets'] | x}</tweets>
        </input>
    </performance>
</stats>
'''

stats_template_finalize = '''
<stats>
    <performance>
        <start_time>${stats["performance"]["start_time"] | x}</start_time>
        <end_time>${stats["performance"]["end_time"] | x}</end_time>
        <input>
            <lines>${stats["performance"]["input"]["lines"] | x}</lines>
        </input>
    </performance>
    <results>
'''

# add others if needed
liwc_dicts = {
    'it': 'hydrateTweet-crunch/assets/Italian_LIWC2007_Dictionary.dic',
    'en': 'hydrateTweet-crunch/assets/English_LIWC2015_Dictionary.dic',
    'es': 'hydrateTweet-crunch/assets/Spanish_LIWC2007_Dictionary.dic',
}


def process_lines(
        dump: Iterable[list],
        stats: Mapping,
        users_dict:dict,
        stats_dict:dict,
        fieldnames:Iterable[list],
        args:argparse.Namespace) -> (str, Iterable[list]):
    """Assign each revision to the snapshot or snapshots to which they
       belong.
    """

    first = next(dump)
    lang = first['lang']
    if lang in liwc_dicts:
        parse, category_names = liwc.load_token_parser(liwc_dicts[lang])

        if args.filter_users == 'per-category':
            filter_fields = ['male_', 'female_', 'org_']
        elif args.filter_users == 'per-age':
            filter_fields = ['<40_', '>=40_']
        else:
            filter_fields = ['']

        for field in filter_fields:
            for category in category_names:
                fieldnames.append('{}{}'.format(field, category))
            fieldnames.append('{}total'.format(field))
            for category in category_names:
                fieldnames.append('{}{}_count'.format(field, category))

        for fieldname in fieldnames:
            stats_dict[fieldname] = 0

        valid_users=None
        if args.filter_users:
            valid_users = get_valid_users(args, lang)
            if not valid_users:
                utils.log('The file of valid users could not be found')
                return None

        process_tweet(
            first,
            parse=parse,
            category_names=category_names,
            stats_dict=stats_dict,
            users_dict=users_dict,
            valid_users=valid_users,
            stats=stats,
            args=args
        )
        for raw_obj in dump:
            process_tweet(
                raw_obj,
                parse=parse,
                category_names=category_names,
                stats_dict=stats_dict,
                users_dict=users_dict,
                valid_users=valid_users,
                stats=stats,
                args=args
            )
        return (lang, category_names)
    else:
        return None


def get_valid_users(args: argparse.Namespace,
                    lang: str):
    if args.filter_users == 'per-category':
        for compression in ['', '.gz', '.7z', '.bz2']:
            csv_file = f"{args.output_dir_path}/filter-inferred/per-category/{lang}-inferred-users.csv{compression}"
            if os.path.exists(csv_file):
                csv_reader = csv.DictReader(fu.open_csv_file(csv_file))
                valid_users = {}
                for inferred_user in csv_reader:
                    valid_users[inferred_user["id_str"]] = inferred_user["category"]
                return valid_users
    elif args.filter_users == 'per-age':
        for compression in ['', '.gz', '.7z', '.bz2']:
            csv_file = f"{args.output_dir_path}/filter-inferred/per-age/{lang}-inferred-users.csv{compression}"
            if os.path.exists(csv_file):
                csv_reader = csv.DictReader(fu.open_csv_file(csv_file))
                valid_users = {}
                for inferred_user in csv_reader:
                    valid_users[inferred_user["id_str"]] = inferred_user["age"]
                return valid_users
    elif args.filter_users == 'per-tweet-number':
        for compression in ['', '.gz', '.7z', '.bz2']:
            json_file = f"{args.output_dir_path}/analyse-users/{lang}-users.json{compression}"
            if os.path.exists(json_file):
                json_reader = fu.open_jsonobjects_file(json_file)
                valid_users = set()
                for user in json_reader:
                    valid_users.add(user["id_str"])
                return valid_users
    return None


def process_tweet(
    tweet: dict,
    parse,
    category_names:Iterable[list],
    users_dict:dict,
    stats_dict:dict,
    valid_users,
    stats: Mapping,
    args: argparse.Namespace):
    """Analyze a tweet based on the specifics
    """

    full_text = tweet['full_text']
    user_id = str(tweet['user']['id'])
    if args.filter_users and not user_id in valid_users:
        return
    elif args.filter_users == 'per-category' or args.filter_users == 'per-age':
        filter_fields = f'{valid_users[user_id]}_'
    else:
        filter_fields = ''
        
    if (not user_id in users_dict) and (not args.per_tweet):
        users_dict[user_id] = new_categories_dict(category_names)
        stats_dict[f'{filter_fields}total'] += 1
        stats['performance']['input']['users'] += 1

    if args.per_tweet:
        user_categories = new_categories_dict(category_names)
        stats_dict[f'{filter_fields}total'] += 1
    else:
        user_categories = users_dict[user_id]

    for token in re.split(r'\s+|\.|,|\!|\?|:|;|\(|\)|#|-', full_text.lower()):
        for liwc_category in parse(token):
            if liwc_category in user_categories and user_categories[liwc_category] == 0:
                stats_dict[f'{filter_fields}{liwc_category}_count'] += 1
                user_categories[liwc_category] = 1
    
    stats['performance']['input']['tweets'] += 1
    nobjs = stats['performance']['input']['tweets']
    if (nobjs-1) % NTWEET == 0:
        utils.dot()


def new_categories_dict(category_names:Iterable[list]) -> dict:
    return {category: 0 for category in category_names}


def calculate_liwc_categories(
        stats_dict:dict,
        category_names:Iterable[list],
        args:argparse.Namespace
    ):

    if args.filter_users == 'per-category':
        filter_fields = ['male_', 'female_', 'org_']
    elif args.filter_users == 'per-age':
        filter_fields = ['<40_', '>=40_']
    else:
        filter_fields = ['']

    for field in filter_fields:
        for category in category_names:
            category_stats_name = '{}{}'.format(field, category)
            if category_stats_name in stats_dict and stats_dict[f'{field}total'] > 0:
                stats_dict[category_stats_name] = stats_dict[f'{category_stats_name}_count']/stats_dict[f'{field}total']


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'analyse-liwc-categories',
        help='Analyse the text from tweets and generate a file which contains statistics w.r.t liwc categories.',
    )
    parser.add_argument(
        '--per-tweet', '-t',
        action='store_true',
        help='Consider each tweet indipendently',
    )
    parser.add_argument(
        '--filter-users', '-f',
        choices={'per-category', 'per-age', 'per-tweet-number'},
        required=False,
        default=None,
        help='Filter users per category (male, female, org), per age (>=40, <40) or based on their number of tweets over the dataset [default: None]',
    )
    parser.add_argument(
        '--standardize', '-s',
        action='store_true',
        help='Standardize the results obtained using mean and standard deviation'
    )

    parser.set_defaults(func=main, which='analyse_liwc_categories')


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
    fieldnames=['date']

    stats['performance']['start_time'] = datetime.datetime.utcnow()

    output = open(os.devnull, 'wt')
    stats_output = open(os.devnull, 'wt')
    addHeader = False

    # process the dump
    lang, category_names = process_lines(
        dump,
        stats=stats,
        users_dict=users_dict,
        stats_dict=stats_dict,
        fieldnames=fieldnames,
        args=args
    )

    # calculate emotions
    calculate_liwc_categories(
        stats_dict=stats_dict,
        category_names=category_names,
        args=args
    )

    path_list = re.split('-|\.', basename)
    stats_dict['date'] = f"{path_list[2]}/{path_list[3]}/{path_list[4]}"

    if not args.dry_run:
        stats_path = f"{args.output_dir_path}/analyse-liwc-categories/stats/{lang}"
        Path(stats_path).mkdir(parents=True, exist_ok=True)
        varname = ('{basename}.{func}'
                   .format(basename=basename,
                           func='analyse-liwc-categories'
                           )
                   )
        
        stats_filename = f"{stats_path}/{varname}"

        if args.filter_users == 'per-category':
            stats_filename = f"{stats_filename}-per-category"
        elif args.filter_users == 'per-age':
            stats_filename = f"{stats_filename}-per-age"
        elif args.filter_users == 'per-tweet-number':
            stats_filename = f"{stats_filename}-filtered"

        if args.per_tweet:
            stats_filename = f"{stats_filename}-per-tweet.stats.xml"
        else:
            stats_filename = f"{stats_filename}.stats.xml"

        stats_output = fu.output_writer(
            path=stats_filename,
            compression=args.output_compression,
            mode='wt'
        )

        if not lang is None:
            file_path = f"{args.output_dir_path}/analyse-liwc-categories"
            Path(file_path).mkdir(parents=True, exist_ok=True)

            # create the file base name
            output_filename = f"{file_path}/{lang}-{path_list[0]}-{path_list[1]}"

            if args.filter_users == 'per-category':
                output_filename = f"{output_filename}-per-category"
            elif args.filter_users == 'per-age':
                output_filename = f"{output_filename}-per-age"
            elif args.filter_users == 'per-tweet-number':
                output_filename = f"{output_filename}-filtered"

            if args.per_tweet:
                output_filename = f"{output_filename}-per-tweet.csv"
            else:
                output_filename = f"{output_filename}.csv"

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
                    shared[output_filename] = category_names

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

    # For each file analyzed before
    for input_file_path in shared:
        
        category_names = shared[input_file_path]

        stats_dict = {}
        stats_dict["days"] = 0

        stats = {
            'performance': {
                'start_time': None,
                'end_time': None,
                'input': {
                    'lines': 0
                },
            },
            'results': {}
        }

        fieldnames = ['date']

        if args.filter_users == 'per-category':
            filter_fields = ['male_', 'female_', 'org_']
        elif args.filter_users == 'per-age':
            filter_fields = ['<40_', '>=40_']
        else:
            filter_fields = ['']

        stats_finalize = stats_template_finalize
        
        for field in filter_fields:
            for category in category_names:
                stats_name = '{}{}'.format(field, category)
                if field == '<40_':
                    xml_tag = 'less_40_{}'.format(category)
                elif field == '>=40_':
                    xml_tag = 'grt_or_eq_40_{}'.format(category)
                else:
                    xml_tag = stats_name

                stats_finalize = ''.join([
                    stats_finalize,
                    '       <%s_mean>${stats["results"]["%s_mean"] | x}</%s_mean>\n' % (xml_tag, stats_name, xml_tag)
                ])
                stats_finalize = ''.join([
                    stats_finalize,
                    '       <%s_stdv>${stats["results"]["%s_stdv"] | x}</%s_stdv>\n' % (xml_tag, stats_name, xml_tag)
                ])

                stats['results']['{}_mean'.format(stats_name)] = 0
                stats['results']['{}_stdv'.format(stats_name)] = 0

                stats_dict['{}_mean'.format(category)] = 0
                stats_dict['{}_stdv'.format(category)] = 0

                fieldnames.append(stats_name)

        stats_finalize = ''.join([
            stats_finalize,
            '   </results>\n</stats>'
        ])

        stats['performance']['start_time'] = datetime.datetime.utcnow()

        utils.log(f"Calculating mean and standard deviation for {input_file_path}...")
        basename = Path(input_file_path).stem
        if not args.output_compression is None:
            # Remove the .csv.gz
            basename = Path(basename).stem

        # Calculate mean for every emotions
        calculate_means(stats_dict, input_file_path, category_names, stats, args)

        # Calculate standard deviation for every emotions
        calculate_stdvs(stats_dict, input_file_path, category_names, stats, args)

        utils.log(f"Writing standardized values for {input_file_path}...")

        output = open(os.devnull, 'wt')
        stats_output = open(os.devnull, 'wt')
        if not args.dry_run:
            stats_path = f"{args.output_dir_path}/liwc-standardize/stats"
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

            file_path = f"{args.output_dir_path}/liwc-standardize"
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

            for category in category_names:
                mean = stats_dict[f"{category}_mean"]
                stdv = stats_dict[f"{category}_stdv"]
                for field in filter_fields:
                    stats_name = '{}{}'.format(field, category)
                    stats_value = float(line[stats_name])
                    try:
                        csv_row[stats_name] = (stats_value - mean) / stdv
                    except:
                        csv_row[stats_name] = 0
            writer.writerow(csv_row)

        output.close()
        csv_file.close()

        stats['performance']['end_time'] = datetime.datetime.utcnow()
        with stats_output:
            dumper.render_template(
                stats_finalize,
                stats_output,
                stats=stats,
            )
    
        stats_output.close()


def calculate_means(
        stats_dict:dict,
        file_path:str,
        category_names:Iterable[list],
        stats:dict,
        args:argparse.Namespace) -> None:
    
    if args.filter_users == 'per-category':
        filter_fields = ['male_', 'female_', 'org_']
    elif args.filter_users == 'per-age':
        filter_fields = ['<40_', '>=40_']
    else:
        filter_fields = ['']

    csv_file = fu.open_csv_file(file_path)
    csv_reader = csv.DictReader(csv_file)
    for line in csv_reader:
        stats_dict["days"] += 1
        for field in filter_fields:
            for category in category_names:
                stats_name = '{}{}'.format(field, category)
                stats_dict[f"{category}_mean"] += float(line[stats_name])
    
    csv_file.close()
    
    for category in category_names:
        stats_dict[f"{category}_mean"] /= stats_dict["days"]
        stats_dict[f"{category}_mean"] /= len(filter_fields)
        for field in filter_fields:
            stats_name = '{}{}'.format(field, category)
            stats['results'][f"{stats_name}_mean"] = stats_dict[f"{category}_mean"]


def calculate_stdvs(
        stats_dict:dict,
        file_path:str,
        category_names:Iterable[list],
        stats:dict,
        args:argparse.Namespace) -> None:

    if args.filter_users == 'per-category':
        filter_fields = ['male_', 'female_', 'org_']
    elif args.filter_users == 'per-age':
        filter_fields = ['<40_', '>=40_']
    else:
        filter_fields = ['']

    csv_file = fu.open_csv_file(file_path)
    csv_reader = csv.DictReader(csv_file)
    for line in csv_reader:
        for category in category_names:
            mean = stats_dict[f"{category}_mean"]
            category_value_over_all_fields = 0
            for field in filter_fields:
                stats_name = '{}{}'.format(field, category)
                category_value_over_all_fields += float(line[stats_name])
            stats_dict[f"{category}_stdv"] += (category_value_over_all_fields - mean)**2

    csv_file.close()
    
    for category in category_names:
        std = math.sqrt(stats_dict[f"{category}_stdv"] / stats_dict["days"])
        stats_dict[f"{category}_stdv"] = std
        for field in filter_fields:
            stats_name = '{}{}'.format(field, category)
            stats['results'][f"{stats_name}_stdv"] = std
