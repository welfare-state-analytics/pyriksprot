from __future__ import annotations

import contextlib
import functools
import glob
import gzip
import inspect
import os
import pathlib
import pickle
import shutil
import tempfile
import time
import warnings
from collections import defaultdict
from itertools import chain
from os.path import basename, dirname, expanduser, isfile
from os.path import join as jj
from os.path import normpath, splitext
from typing import Any, List, Set, TypeVar, Union
from urllib.error import URLError
from urllib.request import urlretrieve

from loguru import logger


def norm_join(a: str, *paths: str):
    """Joins paths and normalizes resulting path to current platform (i.e. sep)"""
    return normpath(jj(a, *paths))


class dotdict(dict):
    """dot.notation access to dictionary attributes"""

    def __getattr__(self, *args):
        value = self.get(*args)
        return dotdict(value) if isinstance(value, dict) else value

    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


# FIXME: sync_delta_names hangs
def sync_delta_names(
    source_folder: str, source_extension: str, target_folder: str, target_extension: str, delete: bool = False
) -> Set(str):
    """Returns basenames in targat_folder that doesn't exist in source folder (with respectively extensions)"""

    source_names = strip_paths(glob.glob(jj(source_folder, "*", f"*.{source_extension}")))
    target_names = strip_paths(glob.glob(jj(target_folder, "*", f"*.{target_extension}")))

    delta_names = set(target_names).difference(set(source_names))

    # FIXME: Move files if not delete
    if delete:
        for basename in delta_names:
            path = jj(target_folder, f"{basename}.{target_extension}")
            if isfile(path):
                logger.warning(f"sync: file {basename} removed via delta sync")
                os.unlink(jj(target_folder, f"{basename}.{target_extension}"))

    if len(delta_names) == 0:
        logger.info("sync: no file was deleted")

    return delta_names


def strip_path_and_extension(filename: str | List[str]) -> str | List[str]:
    """Remove path and extension from filename(s). Return list."""
    if isinstance(filename, str):
        return splitext(basename(filename))[0]
    return [splitext(basename(x))[0] for x in filename]


def strip_extensions(filename: str | List[str]) -> str | List[str]:
    if isinstance(filename, str):
        return splitext(filename)[0]
    return [splitext(x)[0] for x in filename]


def path_add_suffix(path: str, suffix: str, new_extension: str = None) -> str:
    basename, extension = splitext(path)
    return f'{basename}{suffix}{extension if new_extension is None else new_extension}'


def path_add_timestamp(path: str, fmt: str = "%Y%m%d%H%M") -> str:
    return path_add_suffix(path, f'_{time.strftime(fmt)}')


def path_add_date(path: str, fmt: str = "%Y%m%d") -> str:
    return path_add_suffix(path, f'_{time.strftime(fmt)}')


def ts_data_path(directory: str, filename: str):
    return jj(directory, f'{time.strftime("%Y%m%d%H%M")}_{filename}')


def data_path_ts(directory: str, path: str):
    name, extension = splitext(path)
    return jj(directory, '{}_{}{}'.format(name, time.strftime("%Y%m%d%H%M"), extension))


def path_add_sequence(path: str, i: int, j: int = 0) -> str:
    return path_add_suffix(path, f"_{str(i).zfill(j)}")


def strip_path_and_add_counter(filename: str, i: int, n_zfill: int = 3):
    return f'{basename(strip_extensions(filename))}_{str(i).zfill(n_zfill)}.txt'


def strip_paths(filenames: str | List[str]) -> str | List[str]:
    if isinstance(filenames, str):
        return basename(filenames)
    return [basename(filename) for filename in filenames]


T = TypeVar("T")


def flatten(lofl: List[List[T]]) -> List[T]:
    """Returns a flat single list out of supplied list of lists."""

    return [item for sublist in lofl for item in sublist]


def hasattr_path(data: Any, path: str) -> bool:
    """Tests if attrib string in dot-notation is present in data."""
    attribs = path.split(".")
    for attrib in attribs:
        if not hasattr(data, attrib):
            return False
        data = getattr(data, attrib)

    return True


def dict_get_by_path(data: dict, path: str, default=None) -> Any:
    for attrib in path.split("."):
        if not attrib in data:
            return default
        data = data[attrib]
    return data


def lookup(data, *keys):
    for key in keys or []:
        data = data.get(key, {})
    return data


def load_token_set(filename: str) -> Set[str]:
    """Load tokens from `filename`, one token per line"""
    if isfile(filename):
        with gzip.open(filename, 'rb') as f:
            return set(f.read().decode().split('\n'))
    return set()


def store_token_set(tokens: Set[str], filename: str):
    with gzip.open(filename, 'wb') as f:
        f.write('\n'.join(list(tokens)).encode())


def store_dict(data: dict, filename: str):
    with open(filename, 'wb') as fp:
        pickle.dump(data, fp, pickle.HIGHEST_PROTOCOL)


def load_dict(filename: str) -> defaultdict(int):
    logger.info(f"loading {filename}")
    if isfile(filename):
        with open(filename, 'rb') as fp:
            return pickle.load(fp)
    return defaultdict(int)


@contextlib.contextmanager
def temporary_file(*, filename: str = None, content: Any = None, **mktemp):

    if filename is None:
        filename: str = tempfile.mktemp(**mktemp)

    path: pathlib.Path = pathlib.Path(filename)

    try:

        if content:
            mode: str = "wb" if isinstance(content, (bytes, bytearray)) else "w"
            with open(filename, mode) as fp:
                fp.write(content)

        yield path.resolve()

    finally:
        path: pathlib.Path = pathlib.Path(filename)
        if path.is_file():
            path.unlink()


def download_url(url: str, target_folder: str, filename: str = None) -> None:
    """Download a file from a url and place it in `target_folder`.
    https://stackoverflow.com/a/61003039/12383895
    Args:
        url (str): URL to download file from
        target_folder (str): Directory to place downloaded file in
        filename (str, optional): Name to save the file under. If None, use the basename of the URL
    """

    target_folder: str = expanduser(target_folder)
    target_filename: str = jj(target_folder, filename or basename(url))

    os.makedirs(target_folder, exist_ok=True)

    try:
        urlretrieve(url, target_filename)
    except (URLError, IOError):
        if url.startswith('https:'):
            urlretrieve(url.replace('https:', 'http:'), target_filename)


def deprecated(func):
    """Decorator that marks functions or classes as deprecated (emits a warning when used)."""

    @functools.wraps(func)
    def inner(*args, **kwargs):
        warnings.simplefilter('always', DeprecationWarning)
        warnings.warn(f"Call to deprecated function {func.__name__}.", category=DeprecationWarning, stacklevel=2)
        warnings.simplefilter('default', DeprecationWarning)
        return func(*args, **kwargs)

    return inner


def unlink(f: str) -> None:

    fo = pathlib.Path(f)

    if fo.is_dir():
        shutil.rmtree(f)

    if fo.is_file():
        fo.unlink(missing_ok=True)


def touch(f: str) -> None:
    pathlib.Path(f).touch()


def ensure_path(f: str) -> None:
    os.makedirs(dirname(f), exist_ok=True)


def get_kwargs():
    frame = inspect.currentframe().f_back
    keys, _, _, values = inspect.getargvalues(frame)
    kwargs = {}
    for key in keys:
        if key != 'self':
            kwargs[key] = values[key]
    return kwargs


def parse_range_list(rl):
    def collapse_range(ranges):
        end = None
        for value in ranges:
            yield range(max(end, value.start), max(value.stop, end)) if end else value
            end = max(end, value.stop) if end else value.stop

    def split_range(value):
        value = value.split('-')
        for val, prev in zip(value, chain((None,), value)):
            if val != '':
                val = int(val)
                if prev == '':
                    val *= -1
                yield val

    def parse_range(r):
        parts = list(split_range(r.strip()))
        if len(parts) == 0:
            return range(0, 0)
        if len(parts) > 2:
            raise ValueError("Invalid range: {}".format(r))
        return range(parts[0], parts[-1] + 1)

    ranges = sorted(set(map(parse_range, rl.split(","))), key=lambda x: (x.start, x.stop))
    return chain.from_iterable(collapse_range(ranges))
