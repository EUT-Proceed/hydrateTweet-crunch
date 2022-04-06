"""
Given a json file of users, maps them to a specific file based on the location type (state, country, county, ...).

The output format is json.
"""

import os
import json
import csv
import argparse
import datetime
import arrow
from dateutil import parser
from pathlib import Path
import re

from typing import Iterable, Iterator, Mapping

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
            <locations>${stats['performance']['input']['locations'] | x}</locations>
            <users>${stats['performance']['input']['users'] | x}</users>
            <valid_locations>${stats['performance']['input']['valid_locations'] | x}</valid_locations>
            <valid_users>${stats['performance']['input']['valid_users'] | x}</valid_users>
        </input>
    </performance>
</stats>
'''


def save_geocode_locations(
        stats: Mapping,
        shared: dict,
        args: argparse.Namespace,
        lang: str) -> bool:
    # 1. search for the file to open
    for compression in ['', '.gz', '.7z', '.bz2']:
        json_file = f"{args.output_dir_path}/geocode/{lang}-locations-geocode.json{compression}"
        if os.path.exists(json_file):
            json_reader = fu.open_jsonobjects_file(json_file)
            # 2. create dict for that language
            shared[lang] = {}
            locations = shared[lang]
            for geocode_res in json_reader:
                stats['performance']['input']['locations'] += 1
                if 'address' in geocode_res and args.location_type in geocode_res['address']:
                    if args.specify_country and 'country' in geocode_res['address'] and args.specify_country != geocode_res['address']['country']:
                        continue
                    elif args.specify_country and not 'country' in geocode_res['address']:
                        continue

                    if args.specify_state and 'state' in geocode_res['address'] and args.specify_state != geocode_res['address']['state']:
                        continue
                    elif args.specify_state and not 'state' in geocode_res['address']:
                        continue

                    stats['performance']['input']['valid_locations'] += 1
                    # 3. remove spaces, make it lowercase and then save it
                    clean_loc = re.sub(r"\s+", "", geocode_res['location']).lower()
                    locations[clean_loc] = geocode_res['address'][args.location_type]
            return True
    return False


def process_lines(
        dump: Iterable[list],
        stats: Mapping,
        shared: dict,
        args: argparse.Namespace,
        lang: str) -> Iterator[list]:
    """Assign each revision to the snapshot or snapshots to which they
       belong.
    """

    if args.input_type == 'csv':
        dump = csv.DictReader(dump)

    for user in dump:
        stats['performance']['input']['users'] += 1
        nobjs = stats['performance']['input']['users']
        if (nobjs-1) % NTWEET == 0:
            utils.dot()

        user_location =  re.sub(r"\s+", "", user['location']).lower()

        locations = shared[lang]

        if user_location in locations:
            stats['performance']['input']['valid_users'] += 1
            user['location'] = locations[user_location]
            yield user


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'map-users-to-location',
        help='Given a json file of users, maps them to a specific file based on the location type (state, country, county, ...).',
    )
    parser.add_argument(
        '--location-type',
        type=str,
        default='state',
        choices={'state', 'state_district', 'country', 'country_code', 'county', 'town', 'municipality', 'postcode', 'village'},
        help='The type of the locations that will be used to map the user into the right file [default: state]'
    )
    parser.add_argument(
        '--specify-country',
        type=str,
        default=None,
        help='Optional parameter used to specify the country aside from the location type [default: None]'
    )
    parser.add_argument(
        '--specify-state',
        type=str,
        default=None,
        help='Optional parameter used to specify the state aside from the location type [default: None]'
    )

    parser.set_defaults(func=main, which='map_users_to_location')


def main(
        dump: Iterable[list],
        basename: str,
        args: argparse.Namespace,
        shared: dict) -> None:
    """Main function that parses the arguments and writes the output."""
    stats = {
        'performance': {
            'start_time': None,
            'end_time': None,
            'input': {
                'locations': 0,
                'users': 0,
                'valid_locations': 0,
                'valid_users': 0
            },
        },
    }

    desr_dict = {}
    stats['performance']['start_time'] = datetime.datetime.utcnow()

    path_list = basename.split('-')
    lang = path_list[0]

    # check if the locations for a specific language are available
    if not lang in shared:
        res = save_geocode_locations(
            stats=stats,
            shared=shared,
            args=args,
            lang=lang
        )
        if not res:
            utils.log('Locations file not found for {}. Skip to next file'.format(lang))
            return

    output = open(os.devnull, 'wt')
    stats_output = open(os.devnull, 'wt')

    # process the dump
    res = process_lines(
            dump=dump,
            stats=stats,
            shared=shared,
            args=args,
            lang=lang
        )

    if not args.dry_run:
        stats_path = f"{args.output_dir_path}/map-users-to-location/{args.location_type}/stats/"
        Path(stats_path).mkdir(parents=True, exist_ok=True)
        varname = ('{basename}.{func}'
                   .format(basename=basename,
                           func='map-users-to-location'
                           )
                   )
        
        stats_filename = f"{stats_path}/{varname}.stats.xml"

        stats_output = fu.output_writer(
            path=stats_filename,
            compression=args.output_compression,
            mode='wt'
        )

        file_path = f"{args.output_dir_path}/map-users-to-location/{args.location_type}/"
        Path(file_path).mkdir(parents=True, exist_ok=True)

        output_filename = f"{file_path}/{lang}-geocoded-users-per-{args.location_type}.json"
        
        # Save the descriptor for that particular location
        output = fu.output_writer(
            path=output_filename,
            compression=args.output_compression,
            mode='wt'
        )

    for user in res:
        output.write(json.dumps(user))
        output.write("\n")

    stats['performance']['end_time'] = datetime.datetime.utcnow()
    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )
    
    stats_output.close()