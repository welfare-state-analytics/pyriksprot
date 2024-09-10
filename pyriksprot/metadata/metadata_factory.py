from __future__ import annotations

import os
from glob import glob
from os.path import isdir
from typing import Self

import pandas as pd
from loguru import logger

from pyriksprot.gitchen import gh_create_url
from pyriksprot.metadata import database

from ..sql import sql_file_paths
from ..utility import create_class, probe_filename
from .schema import MetadataSchema, MetadataTable

jj = os.path.join


# pylint: disable=unsupported-assignment-operation, unsubscriptable-object


class MetadataFactory:
    def __init__(
        self, tag: str | None, backend=database.DefaultDatabaseType, schema: MetadataSchema = None, **opts
    ) -> None:
        if tag is None:
            raise ValueError("tag must be set")

        self.opts: dict[str, str] = opts
        self.tag: str = tag
        self.schema: MetadataSchema = schema or MetadataSchema(tag)
        self.db: database.DatabaseInterface = (
            backend
            if isinstance(backend, database.DatabaseInterface)
            else (
                create_class(backend)(**opts)
                if isinstance(backend, str)
                else (
                    backend(**opts)
                    if isinstance(backend, type) and issubclass(backend, database.DatabaseInterface)
                    else database.DefaultDatabaseType(**opts)
                )
            )
        )

    def verify_tag(self) -> MetadataFactory:
        if self.db.version != (self.tag):
            raise ValueError(f"metadata version mismatch: db version {self.db.version} differs from {self.tag}")
        return self

    def create(self, folder: str = None, scripts_folder: str = None, force: bool = False) -> Self:
        logger.info(f"Creating database for tag '{self.tag}' using folder '{folder}'.")

        self.db.create_database(tag=self.tag, force=force)

        with self.db:
            self._create_tables(self.schema)

        with self.db:
            self.db.set_deferred(True)
            self.upload(self.schema, folder)

        if scripts_folder:
            with self.db:
                self.execute_sql_scripts(folder=scripts_folder)

        return self

    def _create_tables(self, schema: MetadataSchema) -> MetadataFactory:
        """Creates tables in database based on schema. Create base tables first and then tables with constraints."""
        for p in [1, 2]:
            for tablename, cfg in schema.items():
                if (p == 1 and cfg.constraints) or (p == 2 and not cfg.constraints):
                    continue
                self.db.create(tablename, cfg.all_columns_specs, cfg.constraints)
        return self

    def upload(self, schema: MetadataSchema, folder: str) -> MetadataFactory:
        for _, cfg in schema.items():
            if cfg.is_derived:
                logger.info(f"Skipping derived table: {cfg.tablename}")
                continue
            self._import_table(cfg, folder=folder)
        return self

    def _import_table(self, cfg: MetadataTable, folder: str) -> MetadataFactory:
        logger.info(f"loading table: {cfg.tablename}")

        columns: list[str] = cfg.all_columns
        table: pd.DataFrame = load(cfg.basename, url=cfg.url, folder=folder, tag=self.tag)
        table = cfg.transform(table)[columns]
        self.db.store(table, tablename=cfg.tablename, columns=columns)

        return self

    def execute_sql_scripts(self, *, folder: str = None) -> MetadataFactory:
        """Loads SQL files from specified folder otherwise loads files in sql module"""

        with self.db:
            if not (folder or self.tag):
                raise ValueError("Either folder or tag must be specified.")

            if folder and not isdir(folder):
                raise FileNotFoundError(folder)

            filenames: list[str] = sorted(glob(jj(folder, "*.sql"))) if folder else sql_file_paths(tag=self.tag)

            with self.db:
                for filename in filenames:
                    self.db.load_script(filename=filename)

        return self

    # def load_data_tables(self, data_tables: dict[str, str | None]):
    #     with self.db:
    #         data: dict[str, pd.DataFrame] = self.db.fetch_tables(data_tables)
    #         return data


def load(tablename: str, sep: str = ',', **opts) -> pd.DataFrame:
    """Loads table from specified folder or from url in configuration"""

    if opts.get("url"):
        return pd.read_csv(opts['url'], sep=sep)

    if opts.get("folder"):
        folder: str = probe_filename(jj(opts['folder'], tablename), ['csv', "zip", "csv.gz"])
        return pd.read_csv(folder, sep=sep)

    if opts.get("tag"):
        if not all(opts.get(x) for x in ["user", "repository", "path"]):
            raise ValueError("when fetching from Github user, repository and path must be set")

        url: str = gh_create_url(
            filename=tablename,
            tag=opts.get("tag"),
            user=opts.get("user"),
            repository=opts.get("repository"),
            path=opts.get("path"),
        )
        return pd.read_csv(url)

    raise ValueError("either :url:, folder or branch must be set")
