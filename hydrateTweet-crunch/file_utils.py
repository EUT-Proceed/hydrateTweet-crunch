"""Main module that parses command line arguments."""
import io
import json
import bz2
import gzip
import subprocess

import pathlib
from typing import IO, Optional, Union

import compressed_stream as cs


def open_csv_file(path: Union[str, IO]):
    """Open a csv file, decompressing it if necessary."""
    f = cs.functions.open_file(
        cs.functions.file(path)
    )
    return f


def open_jsonobjects_file(path: Union[str, IO]):
    """Open a file of JSON object, one per line,
       decompressing it if necessary."""
    f = cs.functions.open_file(
        cs.functions.file(path)
    )

    return (json.loads(line) for line in f)


def compressor_7z(file_path: str):
    """"Return a file-object that compresses data written using 7z."""
    p = subprocess.Popen(
        ['7z', 'a', '-si', file_path],
        stdin=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
    )
    return io.TextIOWrapper(p.stdin, encoding='utf-8')


def output_writer(path: str, compression: Optional[str], mode: Optional[str]='at'):
    """Write data to a compressed file."""
    if compression == '7z':
        return compressor_7z(path + '.7z')
    elif compression == 'bz2':
        return bz2.open(path + '.bz2', mode, encoding='utf-8')
    elif compression == 'gz':
        return gzip.open(path + '.gz', mode, encoding='utf-8')
    else:
        return open(path, mode, encoding='utf-8')


def create_path(path: Union[pathlib.Path, str]):
    """Create a path, which may or may not exist."""
    path = pathlib.Path(path)
    if not path.parent.exists():
        path.parent.mkdir(parents=True)
