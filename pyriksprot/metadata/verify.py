from __future__ import annotations

import contextlib
import os
from glob import glob

from ..utility import strip_path_and_extension
from .config import MetadataTableConfigs
from .repository import gh_dl_metadata

jj = os.path.join


class ConformBaseSpecification:
    """Verifies that tags metadata conform"""

    def __init__(self):
        self.left_key: str = "left"
        self.left_tables: dict = {}
        self.right_key: str = "right"
        self.right_tables: dict = {}
        self.errors: list[str] = []
        self.ignore_tables: set[str] = {'unknowns', 'protocols', 'utterances', 'speaker_notes'}

    @property
    def all_tablenames(self) -> set[str]:
        return set(self.left_tables.keys()) | set(self.right_tables.keys()) - self.ignore_tables

    def is_satisfied(self, **_) -> bool:
        for tablename in self.all_tablenames:
            try:
                if tablename not in self.left_tables:
                    self.errors.append(f"{tablename}: found in {self.right_key} but not found in {self.left_key}")
                    continue

                if tablename not in self.right_tables:
                    self.errors.append(f"{tablename}: found in {self.left_key} but not found in {self.right_key}")
                    continue

                self.compare_columns(tablename)

            except:  # pylint: disable=bare-except
                self.errors.append(f"{tablename}: comparision failed")

        if self.errors:
            raise ValueError("\n" + ("\n".join(self.errors)))

    def compare_columns(self, tablename: str) -> None:
        left_columns: set[str] = set(self.left_tables.get(tablename, {}))
        right_columns: set[str] = set(self.left_tables.get(tablename, {}))

        if left_columns.difference(right_columns):
            self.errors.append(
                f"{tablename}: unexpected column(s) in {self.left_key}: {' '.join(left_columns.difference(right_columns))}"
            )

        if right_columns.difference(left_columns):
            self.errors.append(
                f"{tablename}: missing column(s) in {self.right_key}: {' '.join(right_columns.difference(left_columns))}"
            )


class TagsConformSpecification(ConformBaseSpecification):
    """Verifies that tags metadata conform"""

    def __init__(self, tag1: str, tag2: str) -> None:
        super().__init__()

        self.left_key: str = tag1
        self.right_key: str = tag2

        tag1_data: dict = gh_dl_metadata(tag=tag1, folder=None)
        tag2_data: dict = gh_dl_metadata(tag=tag2, folder=None)

        self.left_tables: dict = {n: v['headers'] for n, v in tag1_data.items()}
        self.right_tables: dict = {n: v['headers'] for n, v in tag2_data.items()}


class ConfigConformsToTagSpecification(ConformBaseSpecification):
    """Verifies that current table specification conforms to given tag"""

    def __init__(self, tag: str):
        super().__init__()

        self.table_configs: MetadataTableConfigs = MetadataTableConfigs()

        self.left_key: str = "config"
        self.right_key: str = tag

        tag2_data: dict = gh_dl_metadata(tag=tag, folder=None)

        self.left_tables: dict = {t: self.table_configs[t].source_columns for t in self.table_configs.tablesnames0}
        self.right_tables: dict = {n: v['headers'] for n, v in tag2_data.items()}


class ConfigConformsToFolderSpecification(ConformBaseSpecification):
    """Verifies that current table specification has same files as in specified folder"""

    def __init__(self, folder: str):
        super().__init__()

        self.table_configs: MetadataTableConfigs = MetadataTableConfigs()

        self.left_key: str = "config"
        self.right_key: str = folder

        self.left_tables: dict = {t: self.table_configs[t].source_columns for t in self.table_configs.tablesnames0}
        self.right_tables: dict = self.get_folder_info(folder)

    def get_folder_info(self, folder: str) -> dict:
        return {strip_path_and_extension(t): self.load_columns(t) for t in glob(jj(folder, "*.csv"))}

    def load_columns(self, filename: str) -> list[str]:
        with contextlib.suppress():
            with open(filename, mode="r", encoding="utf-8") as fp:
                return fp.read().splitlines()[0].split(',')
        return []
