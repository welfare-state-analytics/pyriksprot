from __future__ import annotations

import base64
import bz2
import contextlib
import functools
import glob
import gzip
import inspect
import json
import lzma
import os
import pathlib
import re
import shutil
import tempfile
import time
import unicodedata
import warnings
import zlib
from itertools import chain
from os.path import basename, dirname, expanduser, isfile
from os.path import join as jj
from os.path import normpath, splitext
from types import ModuleType
from typing import Any, Callable, List, Literal, Sequence, Set, Type, TypeVar
from urllib.request import urlopen

import requests
import unidecode  # pylint: disable=import-error
import yaml
from loguru import logger


def norm_join(a: str, *paths: str):
    """Joins paths and normalizes resulting path to current platform (i.e. sep)"""
    return normpath(jj(a, *paths))


class dotdict(dict):
    """dot.notation access to  dictionary attributes"""

    def __getattr__(self, *args):
        value = self.get(*args)
        return dotdict(value) if isinstance(value, dict) else value

    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def dget(data: dict, *path: str | list[str], default: Any = None) -> Any:
    if path is None or not data:
        return default

    ps: list[str] = path if isinstance(path, (list, tuple)) else [path]

    d = None

    for p in ps:
        d = dotget(data, p)

        if d is not None:
            return d

    return d or default


def dotexists(data: dict, *paths: list[str]) -> bool:
    for path in paths:
        if dotget(data, path, default="@@") != "@@":
            return True
    return False


def dotexpand(path: str) -> list[str]:
    """Expands paths with ',' and ':'."""
    paths = []
    for p in path.replace(' ', '').split(','):
        if not p:
            continue
        if ':' in p:
            paths.extend([p.replace(":", "."), p.replace(":", "_")])
        else:
            paths.append(p)
    return paths


def dotget(data: dict, path: str, default: Any = None) -> Any:
    """Gets element from dict. Path can be x.y.y or x_y_y or x:y:y.
    if path is x:y:y then element is search using borh x.y.y or x_y_y."""

    for key in dotexpand(path):
        d: dict = data
        for attr in key.split('.'):
            d: dict = d.get(attr) if isinstance(d, dict) else None
            if d is None:
                break
        if d is not None:
            return d
    return default


def sync_delta_names(
    source_folder: str, source_extension: str, target_folder: str, target_extension: str, delete: bool = False
) -> Set(str):
    """Returns basenames in targat_folder that doesn't exist in source folder (with respectively extensions)"""

    source_names = strip_paths(glob.glob(jj(source_folder, "*", f"*.{source_extension}")))
    target_names = strip_paths(glob.glob(jj(target_folder, "*", f"*.{target_extension}")))

    delta_names = set(target_names).difference(set(source_names))

    if delete:
        for name in delta_names:
            path = jj(target_folder, f"{name}.{target_extension}")
            if isfile(path):
                logger.warning(f"sync: file {name} removed via delta sync")
                os.unlink(jj(target_folder, f"{name}.{target_extension}"))

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


def replace_extension(filename: str, extension: str) -> str:
    if filename.endswith(extension):
        return filename
    base, _ = os.path.splitext(filename)
    return f"{base}{'' if extension.startswith('.') else '.'}{extension}"


def path_add_suffix(path: str, suffix: str, new_extension: str = None) -> str:
    name, extension = splitext(path)
    return f'{name}{suffix}{extension if new_extension is None else new_extension}'


def path_add_timestamp(path: str, fmt: str = "%Y%m%d%H%M") -> str:
    return path_add_suffix(path, f'_{time.strftime(fmt)}')


def path_add_date(path: str, fmt: str = "%Y%m%d") -> str:
    return path_add_suffix(path, f'_{time.strftime(fmt)}')


def ts_data_path(directory: str, filename: str):
    return jj(directory, f'{time.strftime("%Y%m%d%H%M")}_{filename}')


def path_add_sequence(path: str, i: int, j: int = 0) -> str:
    return path_add_suffix(path, f"_{str(i).zfill(j)}")


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


def lookup(data, *keys):
    for key in keys or []:
        data = data.get(key, {})
    return data


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


def download_url_to_file(url: str, target_name: str, force: bool = False) -> None:
    target_name: str = expanduser(target_name)

    if os.path.isfile(target_name):
        if not force:
            raise ValueError("File exists, use `force=True` to overwrite")
        os.unlink(target_name)

    ensure_path(target_name)

    logger.info(f'downloading: {target_name}')
    with open(target_name, 'w', encoding="utf-8") as fp:
        data: str = requests.get(url, allow_redirects=True, timeout=10).content.decode("utf-8")
        fp.write(data)


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

    elif fo.is_file():
        fo.unlink(missing_ok=True)


def touch(f: str) -> None:
    pathlib.Path(f).touch()


def ensure_path(path: str) -> str:
    os.makedirs(dirname(path), exist_ok=True)
    return path


def ensure_folder(path: str) -> str:
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)
    return path


def reset_folder(folder: str, force: bool = False) -> str:
    if os.path.isdir(folder) and not force:
        raise FileExistsError(folder)

    shutil.rmtree(folder, ignore_errors=True)
    os.makedirs(folder, exist_ok=True)


def reset_file(path: str, force: bool):
    if path is None:
        return
    if os.path.isfile(path):
        if not force:
            raise FileExistsError(path)
        os.unlink(path)


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
            raise ValueError(f"Invalid range: {r}")
        return range(parts[0], parts[-1] + 1)

    ranges = sorted(set(map(parse_range, rl.split(","))), key=lambda x: (x.start, x.stop))
    return chain.from_iterable(collapse_range(ranges))


def compress(text: str) -> bytes:
    return base64.b64encode(zlib.compress(text.encode('utf-8')))


def decompress(data: bytes) -> str:
    return zlib.decompress(base64.b64decode(data)).decode('utf-8')


RE_SANITIZE = re.compile(r'[^a-zåäöA-ZÅÄÖ#0-9_]')


def sanitize(filename: str, remove_accents: bool = True) -> str:
    if remove_accents:
        filename = unidecode.unidecode(filename)
    return RE_SANITIZE.sub('_', filename)


def slugify(value: str, allow_unicode: bool = False) -> str:
    """
    Taken from https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Convert to lowercase. Also strip leading and
    trailing whitespace, dashes, and underscores.
    """
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize('NFKC', value)
    else:
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value.lower())
    return re.sub(r'[-\s]+', '-', value).strip('-_')


def compose(*fns: Sequence[Callable[[str], str]]) -> Callable[[str], str]:
    """Create a composed function from a list of function. Return function."""
    if len(fns) == 0:
        return None
    return functools.reduce(lambda f, g: lambda *args: f(g(*args)), fns)


def is_empty(filename: str) -> bool:
    """Check if file is empty."""
    return os.path.exists(filename) and os.stat(filename).st_size == 0


def strip_csv_header(csv_str: str, sep: str = '\n') -> str:
    """Remove header line from `csv_str`"""
    if not csv_str:
        return ''
    idx: int = csv_str.find(sep)
    if idx <= 0:
        return ''
    return csv_str[idx + 1 :]


def merge_csv_strings(csv_strings: List[str], sep: str = '\n') -> str:
    """Merge tagged CSV strings into a single tagged CSV string"""
    if len(csv_strings or []) == 0:
        return ''
    texts: List[str] = [csv_strings[0]]
    for csv_string in csv_strings[1:]:
        text = strip_csv_header(csv_string, sep=sep)
        if text:
            texts.append(text)
    return sep.join(texts)


def store_str(filename: str, text: str, compress_type: Literal['csv', 'gzip', 'bz2', 'lzma']) -> None:
    """Stores a textfile on disk - optionally compressed"""
    modules = {'gzip': (gzip, 'gz'), 'bz2': (bz2, 'bz2'), 'lzma': (lzma, 'xz')}

    if compress_type in modules:
        module, extension = modules[str(compress_type)]
        with module.open(f"{filename}.{extension}", 'wb') as fp:
            fp.write(text.encode('utf-8'))

    elif compress_type == 'csv':
        with open(filename, 'w', encoding='utf-8') as fp:
            fp.write(text)
    else:
        raise ValueError(f"unknown mode {compress_type}")


def find_subclasses(module: ModuleType, parent: Type) -> List[Type]:
    return [
        cls
        for _, cls in inspect.getmembers(module)
        if inspect.isclass(cls) and issubclass(cls, parent) and cls is not parent
    ]


def read_yaml(file: Any) -> dict:
    """Read yaml file. Return dict."""
    if isinstance(file, str) and any(file.endswith(x) for x in ('.yml', '.yaml')):
        with open(file, "r", encoding='utf-8') as fp:
            return yaml.load(fp, Loader=yaml.FullLoader)
    data: List[dict] = yaml.load(file, Loader=yaml.FullLoader)
    return {} if len(data) == 0 else data[0]


def write_yaml(data: dict, file: str) -> None:
    """Write yaml to file.."""
    with open(file, "w", encoding='utf-8') as fp:
        return yaml.dump(data=data, stream=fp)


def update_dict_from_yaml(yaml_file: str, data: dict) -> dict:
    """Update dict `data` with values found in `yaml_file`."""
    if yaml_file is None:
        return data
    options: dict = read_yaml(yaml_file)
    data.update(options)
    return data


def load_json(url: str) -> list[dict]:
    return json.loads(urlopen(url).read())


def probe_filename(filename: list[str], exts: list[str] = None) -> str | None:
    """Probes existence of filename with any of given extensions in folder"""
    for probe_name in set([filename] + ([replace_extension(filename, ext) for ext in exts] if exts else [])):
        if isfile(probe_name):
            return probe_name
    raise FileNotFoundError(filename)


def revdict(d: dict) -> dict:
    return {v: k for k, v in d.items()}


def props(cls: Type) -> list[str]:
    return [i for i in cls.__dict__.keys() if i[:1] != '_']


XML_ESCAPES = str.maketrans(
    {
        "<": "&lt;",
        ">": "&gt;",
        "&": "&amp;",
        "'": "&apos;",
        '"': "&quot;",
    }
)


def xml_escape(txt: str) -> str:
    return txt.translate(XML_ESCAPES)


def xml_unescape(txt: str) -> str:
    return (
        txt.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .replace("&apos;", "'")
        .replace("&quot;", '"')
    )


# def register(registry: dict | Type[Any], key: str = None):
#     if not isinstance(registry, dict):
#         if not hasattr(registry, "registry"):
#             registry.registry = {}
#             registry = registry.registry

#     def registrar(func):
#         registry[key or func.__name__] = func
#         return func

#     return registrar


# class Registry:
#     items: dict = {}

#     @classmethod
#     def register(cls, args):
#         def decorator(fn):
#             cls.items[fn.__name__] = args
#             return fn

#         return decorator
