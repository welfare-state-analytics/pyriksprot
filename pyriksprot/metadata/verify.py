from __future__ import annotations

import contextlib
import os
from glob import glob

from pyriksprot.metadata.schema import MetadataTable

from ..utility import strip_paths
from .download import gh_fetch_metadata_folder
from .schema import MetadataSchema

jj = os.path.join


class ConformBaseSpecification:
    """Verifies that tags metadata conform"""

    def __init__(self):
        self.left_key: str = "left"
        self.left_tables: dict = {}
        self.right_key: str = "right"
        self.right_tables: dict = {}
        self.errors: list[str] = []
        self.schemas: dict[str, MetadataSchema] = {}
        self.data: dict[str, dict] = {}

    def get_schema(self, tag: str) -> MetadataSchema:
        if tag not in self.schemas:
            self.schemas[tag] = MetadataSchema(tag)
        return self.schemas[tag]

    @property
    def all_tablenames(self) -> set[str]:
        return set(self.left_tables.keys()) | set(self.right_tables.keys())

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

        return True

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

    def resolve_tablename(self, filename: str, tag: str) -> str:
        schema: MetadataSchema = self.get_schema(tag)
        cfg: MetadataTable | None = schema.get_by_filename(filename)
        if cfg is None:
            return filename
        return cfg.tablename
    
    def resolve_schema_infos(self, tag: str) -> dict:
        return {
            cfg.tablename: cfg.source_columns
            for cfg in self.get_schema(tag).definitions.values()
            if not cfg.is_derived and not cfg.is_extra
        }

    def resolve_github_infos(self, tag: str, infos: dict[str, dict]) -> dict:
        data: dict[str, list[str]] = {}
        for filename, info in infos.items():
            cfg: MetadataTable | None = self.get_schema(tag).get_by_filename(filename)
            if cfg is not None and (cfg.is_derived or cfg.is_extra):
                continue
            if cfg is None:
                data[filename] = info['headers']
            else:
                data[cfg.tablename] = info['headers']
        return data

    def resolve_folder_infos(self, folder: str, tag: str) -> dict:
        data: dict[str, list[str]] = {}
        for filename in glob(jj(folder, "*.csv")):
            cfg: MetadataTable | None = self.get_schema(tag).get_by_filename(strip_paths(filename))
            if cfg is not None and (cfg.is_derived or cfg.is_extra):
                continue
            if cfg is None:
                data[filename] = self.load_columns(filename)
            else:
                data[cfg.tablename] = self.load_columns(filename)
        return data

    def load_columns(self, filename: str) -> list[str]:
        with contextlib.suppress():
            with open(filename, mode="r", encoding="utf-8") as fp:
                return fp.read().splitlines()[0].split(',')
        return []


class TagsConformSpecification(ConformBaseSpecification):
    """Verifies that tags metadata conform"""

    def __init__(self, user: str, repository: str, path: str, tag1: str, tag2: str) -> None:
        super().__init__()

        self.left_key: str = tag1
        self.right_key: str = tag2

        for tag in (tag1, tag2):
            self.data[tag] = gh_fetch_metadata_folder(
                user=user, repository=repository, path=path, tag=tag, target_folder=None
            )

        self.left_tables: dict = self.resolve_github_infos(tag1, self.data[tag1])
        self.right_tables: dict = self.resolve_github_infos(tag2, self.data[tag2])


class ConfigConformsToTagSpecification(ConformBaseSpecification):
    """Verifies that current table specification conforms to given tag"""

    def __init__(self, user: str, repository: str, path: str, tag: str):
        super().__init__()

        self.left_key: str = "config"
        self.right_key: str = tag

        data: dict = gh_fetch_metadata_folder(
            user=user, repository=repository, path=path, tag=tag, target_folder=None
        )

        self.left_tables: dict = self.resolve_schema_infos(tag)
        self.right_tables: dict = self.resolve_github_infos(tag, data)


class ConfigConformsToFolderSpecification(ConformBaseSpecification):
    """Verifies that current table specification has same files as in specified folder"""

    def __init__(self, tag: str, folder: str):
        super().__init__()

        self.left_key: str = "config"
        self.right_key: str = folder

        self.left_tables: dict = self.resolve_schema_infos(tag)
        self.right_tables: dict = self.resolve_folder_infos(folder, tag)
