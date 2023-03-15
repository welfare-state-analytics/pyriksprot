import importlib.resources as pkg_resources
from pathlib import Path


def sql_file_paths() -> list[Path]:
    return sorted([x for x in pkg_resources.files(__package__).iterdir() if x.suffix == '.sql'])
