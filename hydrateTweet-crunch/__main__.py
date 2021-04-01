"""Main module that parses command line arguments."""
import argparse
import pathlib

from . import processors, utils, file_utils


def get_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        prog='hydrateTweet-crunch',
        description='Graph snapshot features extractor.',
    )
    parser.add_argument(
        'files',
        metavar='FILE',
        type=pathlib.Path,
        nargs='+',
        help='Twitter file to parse, can be compressed.',
    )
    parser.add_argument(
        'output_dir_path',
        metavar='OUTPUT_DIR',
        type=pathlib.Path,
        help='XML output directory.',
    )
    parser.add_argument(
        '--output-compression',
        choices={None, '7z', 'bz2', 'gz'},
        required=False,
        default=None,
        help='Output compression format [default: no compression].',
    )
    parser.add_argument(
        '--input-type',
        choices={None, 'json', 'csv'},
        required=False,
        default=None,
        help='Input file type [default: None].',
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help="Don't write any file",
    )

    subparsers = parser.add_subparsers(help='sub-commands help')
    processors.lang_sort.configure_subparsers(subparsers)
    processors.analyse_emotions.configure_subparsers(subparsers)
    processors.analyse_users.configure_subparsers(subparsers)
    processors.standardize.configure_subparsers(subparsers)
    processors.infer_users.configure_subparsers(subparsers)

    parsed_args = parser.parse_args()
    if 'func' not in parsed_args:
        parser.print_usage()
        parser.exit(1)

    if 'which' in parsed_args and parsed_args.which == 'analyse_emotions' and parsed_args.standardize:
        parsed_args.finalize=processors.analyse_emotions.standardize

    return parsed_args

def main():
    """Main function."""
    args = get_args()

    if not args.output_dir_path.exists():
        args.output_dir_path.mkdir(parents=True)

    shared = {}

    for input_file_path in args.files:
        utils.log("Analyzing {}...".format(input_file_path))

        # get filename without the extension
        # https://stackoverflow.com/a/47496703/2377454
        basename = input_file_path.stem

        if args.input_type is None:
            args.func(
                input_file_path=input_file_path,
                basename=basename,
                args=args,
                shared=shared
            )
        else:
            if args.input_type == 'json':
                dump = file_utils.open_jsonobjects_file(str(input_file_path))
            elif args.input_type == 'csv': 
                dump = file_utils.open_csv_file(str(input_file_path))

            args.func(
                dump=dump,
                basename=basename,
                args=args,
                shared=shared
            )

            # explicitly close input files
            dump.close()        

        utils.log("Done Analyzing {}.".format(input_file_path))
    
    if 'finalize' in args:
        utils.log("Executing finalize function...")

        args.finalize(
            args=args,
            shared=shared
        )

        utils.log("Done executing finalize function.")
    
if __name__ == '__main__':
    main()
