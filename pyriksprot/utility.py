from __future__ import annotations

import contextlib
import functools
import glob
import gzip
import os
import pathlib
import pickle
import shutil
import tempfile
import time
import urllib
import warnings
from collections import defaultdict
from typing import Any, List, Set, TypeVar, Union

# from snakemake.io import expand, glob_wildcards
from loguru import logger


def norm_join(a: str, *paths: str):
    """Joins paths and normalizes resulting path to current platform (i.e. sep)"""
    return os.path.normpath(os.path.join(a, *paths))


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

    source_names = strip_paths(glob.glob(os.path.join(source_folder, "*", f"*.{source_extension}")))
    target_names = strip_paths(glob.glob(os.path.join(target_folder, "*", f"*.{target_extension}")))

    delta_names = set(target_names).difference(set(source_names))

    # FIXME: Move files if not delete
    if delete:
        for basename in delta_names:
            path = os.path.join(target_folder, f"{basename}.{target_extension}")
            if os.path.isfile(path):
                logger.warning(f"sync: file {basename} removed via delta sync")
                os.unlink(os.path.join(target_folder, f"{basename}.{target_extension}"))

    if len(delta_names) == 0:
        logger.info("sync: no file was deleted")

    return delta_names


def strip_path_and_extension(filename: str) -> List[str]:
    return os.path.splitext(os.path.basename(filename))[0]


def strip_extensions(filename: Union[str, List[str]]) -> List[str]:
    if isinstance(filename, str):
        return os.path.splitext(filename)[0]
    return [os.path.splitext(x)[0] for x in filename]


def path_add_suffix(path: str, suffix: str, new_extension: str = None) -> str:
    basename, extension = os.path.splitext(path)
    return f'{basename}{suffix}{extension if new_extension is None else new_extension}'


def path_add_timestamp(path: str, fmt: str = "%Y%m%d%H%M") -> str:
    return path_add_suffix(path, f'_{time.strftime(fmt)}')


def path_add_date(path: str, fmt: str = "%Y%m%d") -> str:
    return path_add_suffix(path, f'_{time.strftime(fmt)}')


def ts_data_path(directory: str, filename: str):
    return os.path.join(directory, f'{time.strftime("%Y%m%d%H%M")}_{filename}')


def data_path_ts(directory: str, path: str):
    name, extension = os.path.splitext(path)
    return os.path.join(directory, '{}_{}{}'.format(name, time.strftime("%Y%m%d%H%M"), extension))


def path_add_sequence(path: str, i: int, j: int = 0) -> str:
    return path_add_suffix(path, f"_{str(i).zfill(j)}")


def strip_path_and_add_counter(filename: str, i: int, n_zfill: int = 3):
    return f'{os.path.basename(strip_extensions(filename))}_{str(i).zfill(n_zfill)}.txt'


def strip_paths(filenames: Union[str, List[str]]) -> Union[str, List[str]]:
    if isinstance(filenames, str):
        return os.path.basename(filenames)
    return [os.path.basename(filename) for filename in filenames]


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
    if os.path.isfile(filename):
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
    if os.path.isfile(filename):
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


def download_url(url: str, root: str, filename: str = None) -> None:
    """Download a file from a url and place it in root.
    https://stackoverflow.com/a/61003039/12383895
    Args:
        url (str): URL to download file from
        root (str): Directory to place downloaded file in
        filename (str, optional): Name to save the file under. If None, use the basename of the URL
    """

    root = os.path.expanduser(root)
    if not filename:
        filename = os.path.basename(url)
    fpath = os.path.join(root, filename)

    os.makedirs(root, exist_ok=True)

    try:
        urllib.request.urlretrieve(url, fpath)
    except (urllib.error.URLError, IOError):
        if url[:5] == 'https':
            url = url.replace('https:', 'http:')
            urllib.request.urlretrieve(url, fpath)


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
    os.makedirs(os.path.dirname(f), exist_ok=True)
