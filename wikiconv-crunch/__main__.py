"""Main module that parses command line arguments."""
import argparse
import pathlib

from . import processors, utils, file_utils


def get_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        prog='wikiconv-crunch',
        description='Graph snapshot features extractor.',
    )
    parser.add_argument(
        'files',
        metavar='FILE',
        type=pathlib.Path,
        nargs='+',
        help='Wikidump file to parse, can be compressed.',
    )
    parser.add_argument(
        'output_dir_path',
        metavar='OUTPUT_DIR',
        type=pathlib.Path,
        help='XML output directory.',
    )
    parser.add_argument(
        '--output-compression',
        choices={None, '7z', 'bz2', 'gzip'},
        required=False,
        default=None,
        help='Output compression format [default: no compression].',
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help="Don't write any file",
    )

    subparsers = parser.add_subparsers(help='sub-commands help')
    processors.lang_sort.configure_subparsers(subparsers)

    parsed_args = parser.parse_args()
    if 'func' not in parsed_args:
        parser.print_usage()
        parser.exit(1)

    return parsed_args

def init_descriptor_dict(day=0, month=0):
    return {"current_day": day, "current_month": month, "descriptors": {}}

def close_all_descriptors(desr_dict:dict):
    for descriptor in desr_dict['descriptors']:
        desr_dict['descriptors'][descriptor].close()
    utils.log('descriptors all closed')

def main():
    """Main function."""
    args = get_args()

    if not args.output_dir_path.exists():
        args.output_dir_path.mkdir(parents=True)

    desr_dict = init_descriptor_dict()

    for input_file_path in args.files:
        utils.log("Analyzing {}...".format(input_file_path))

        dump = file_utils.open_jsonobjects_file(str(input_file_path))

        # get filename without the extension
        # https://stackoverflow.com/a/47496703/2377454
        basename = input_file_path.stem

        name_list = basename.split('-')
        #checking if the day or month has changed 
        if desr_dict['current_day'] != name_list[5] or desr_dict['current_month'] != name_list[4]:
            utils.log('change day/month: closing all descriptors...')
            close_all_descriptors(desr_dict)
            desr_dict = init_descriptor_dict(name_list[5],  name_list[4])

        args.func(
            dump,
            basename,
            args,
            desr_dict
        )

        # explicitly close input files
        dump.close()

        utils.log("Done Analyzing {}.".format(input_file_path))

    close_all_descriptors(desr_dict)

if __name__ == '__main__':
    main()
