from __future__ import annotations

import io
import os
import time
from dataclasses import dataclass, field
from functools import cached_property
from inspect import isclass
from os.path import abspath, isdir, join, normpath
from pathlib import Path
from typing import Any, Type, TypeVar

import pathvalidate
import yaml
from dotenv import load_dotenv

from ..utility import dget, dotexists

load_dotenv()

# pylint: disable=too-many-arguments

T = TypeVar("T", str, int, float)


def nj(*paths) -> str | None:
    return normpath(join(*paths)) if not None in paths else None


class SafeLoaderIgnoreUnknown(yaml.SafeLoader):  # pylint: disable=too-many-ancestors
    def let_unknown_through(self, node):  # pylint: disable=unused-argument
        return None


SafeLoaderIgnoreUnknown.add_constructor(None, SafeLoaderIgnoreUnknown.let_unknown_through)

REPOSITORY_URL: str = "https://github.com/welfare-state-analytics/riksdagen-corpus.git"


@dataclass
class SourceConfig:
    repository_folder: str
    repository_tag: str
    extension: str = field(default="xml")
    repository_url: str = field(default="https://github.com/welfare-state-analytics/riksdagen-corpus.git")

    def __post_init__(self):
        if not self.repository_tag:
            raise ValueError("Corpus tag cannot be empty")

    @property
    def folder(self) -> str:
        return (
            nj(self.repository_folder, "corpus/protocols")
            if isdir(nj(self.repository_folder, "corpus/protocols"))
            else self.repository_folder
        )

    @property
    def parent_folder(self) -> str:
        return abspath(nj(self.repository_folder, '..'))


@dataclass
class TargetConfig:
    folder: str
    extension: str = field(default="zip")


@dataclass
class DehyphenConfig:
    folder: str
    tf_filename: str


@dataclass
class ExtractConfig:
    folder: str
    template: str = field(default="")
    extension: str = field(default="xml")


class Config:
    def __init__(self, *, data: dict = None, context: str = "default", filename: str | None = None):
        self.data: dict = data
        self.context: str = context
        self.filename: str | None = filename

    @cached_property
    def data_folder(self) -> str:
        return nj(dget(self.data, "data_folder", "root_folder"))

    @cached_property
    def source(self) -> SourceConfig:
        return SourceConfig(
            repository_folder=nj(
                dget(
                    self.data,
                    "repository:folder",
                    "source.repository:folder",
                    default=nj(self.data_folder, "riksdagen-corpus"),
                )
            ),
            repository_tag=dget(
                self.data,
                "tag",
                "repository:tag",
                "source.repository:tag",
                default=os.environ.get("RIKSPROT_REPOSITORY_TAG"),
            ),
        )

    @cached_property
    def target(self) -> TargetConfig:
        return TargetConfig(
            folder=nj(dget(self.data, "target:folder")),
            extension=dget(self.data, "target:extension", default="zip"),
        )

    @cached_property
    def extract(self) -> ExtractConfig:
        return ExtractConfig(
            folder=nj(dget(self.data, "export:folder", "extract:folder")),
            template=dget(self.data, "export:template", "extract:template"),
            extension=dget(self.data, "export:extension", "extract:extension"),
        )

    @cached_property
    def dehyphen(self) -> DehyphenConfig:
        return DehyphenConfig(
            folder=dget(self.data, "dehyphen:folder", default=self.data_folder),
            tf_filename=dget(
                self.data, "tf_filename", "dehyphen.tf_filename", default=nj(self.data_folder, "word-frequencies.pkl")
            ),
        )

    @cached_property
    def tagger_opts(self) -> dict:
        return (
            dget(self.data, "tagger")
            if "tagger" in self.data
            else {k.lstrip("tagger_"): v for k, v in self.data.items() if k.startswith("tagger_")}
        )

    def get(self, *keys: str, default: Any | Type[Any] = None, mandatory: bool = False) -> Any:
        if mandatory and not self.exists(*keys):
            raise ValueError(f"Missing mandatory key: {keys}")

        value: Any = dget(self.data, *keys)

        if value is not None:
            return value

        return default() if isclass(default) else default

    def exists(self, *keys) -> bool:
        return dotexists(self.data, *keys)

    @staticmethod
    def load(*, source: str | dict | Config = None, context: str = None) -> "Config":
        if isinstance(source, Config):
            return source
        data = source
        data: dict = (
            (
                yaml.load(Path(source).read_text(encoding="utf-8"), Loader=SafeLoaderIgnoreUnknown)
                if Config.is_config_path(source)
                else yaml.load(io.StringIO(source), Loader=SafeLoaderIgnoreUnknown)
            )
            if isinstance(source, str)
            else source
        )
        if not isinstance(data, dict):
            raise TypeError(f"expected dict, found {type(data)}")
        return Config(data=data, context=context, filename=source if Config.is_config_path(source) else None)

    @staticmethod
    def is_config_path(source) -> bool:
        if not isinstance(source, str):
            return False
        return source.endswith(".yaml") or source.endswith(".yml") or pathvalidate.is_valid_filepath(source)

    @cached_property
    def log_folder(self):
        return nj(self.data_folder, "logs")

    @cached_property
    def log_filename(self):
        return nj(self.log_folder, f'pyriksprot_{time.strftime("%Y%m%d%H%M")}.log')


def is_config_filename(filename: str) -> bool:
    if filename.endswith(".yaml") or filename.endswith(".yml"):
        return True

    return False
