import importlib.resources as pkg_resources
from pathlib import Path


def sql_file_paths(tag: str) -> list[Path]:
    return sorted([x for x in (pkg_resources.files(__package__) / tag).iterdir() if x.suffix == '.sql'])


def sql_folder(tag: str) -> Path:
    return pkg_resources.files(__package__) / tag
