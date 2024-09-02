import abc
import sqlite3
from os.path import basename
from typing import Any, Self, Type

import numpy as np
import pandas as pd
from loguru import logger

from pyriksprot.metadata.utility import slim_table_types
from pyriksprot.utility import ensure_path, reset_file


class SqlCompiler:
    def to_create(self, tablename: str, columns_specs: dict[str, str], constraints: list[str]) -> str:
        lf: str = '\n' + (4 * ' ')
        constraints: str = f",{lf}".join(constraints)
        if constraints:
            constraints: str = f",{lf}{constraints}"
        sql_ddl: str = f"""
create table {tablename} (
    {(','+lf).join(f'"{k}" {t}' for k, t in columns_specs.items())}{constraints}
);
"""
        return sql_ddl

    def to_insert(self, tablename: str, columns: list[str]) -> str:
        insert_sql: str = f"""
        insert into {tablename} ({', '.join([f'"{c}"' for c in columns])})
            values ({', '.join(['?'] * len(columns))});
        """
        return insert_sql


class DatabaseInterface(abc.ABC):
    def __init__(self, **opts):
        self.opts: str = opts
        self.session_depth: int = 0
        self.compiler: SqlCompiler = opts.get("compiler") or SqlCompiler()

    def open(self) -> Self:
        self.session_depth += 1
        if self.session_depth == 1:
            self._open()

    def close(self) -> None:
        self.session_depth = max(0, self.session_depth - 1)
        if self.session_depth == 0:
            self._close()

    @abc.abstractmethod
    def _open(self) -> None:
        ...

    @abc.abstractmethod
    def _close(self) -> None:
        ...

    @abc.abstractmethod
    def commit(self) -> None:
        ...

    def __enter__(self) -> Self:
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abc.abstractmethod
    def execute_script(self, sql: str) -> None:
        ...

    @abc.abstractmethod
    def fetch_scalar(self, sql: str) -> Any:
        ...

    @property
    def version(self) -> str | None:
        """Retrieve a version tag from the database"""
        return self.fetch_scalar("select version from version")

    @version.setter
    def version(self, tag: str) -> None:
        """Adds a version tag to the database"""
        with self:
            self.execute_script(
                f"""
                create table if not exists version (
                    version text
                );
                delete from version;
                insert into version(version) values ('{tag}');
            """
            )

    def create(self, tablename: str, columns: dict[str, str], constraints: list[str]) -> None:
        try:
            sql: str = self.compiler.to_create(tablename, columns, constraints)
            self.execute_script(sql)
        except sqlite3.DatabaseError as e:
            logger.error(f"Error creating table: {tablename}")
            logger.error(f"SQL: {sql}")
            logger.error(e)
        return self

    @abc.abstractmethod
    def store(self, data: pd.DataFrame, tablename: str, columns: list[str] = None) -> None:
        """Loads dataframe into the database."""
        # FIXME: Consolidate with store2 method
        ...

    @abc.abstractmethod
    def store2(self, *, data: pd.DataFrame, tablename: str) -> None:
        ...

    def load_script(self, *, filename) -> None:
        """Loads SQL files from specified folder otherwise loads files in sql module"""
        logger.info(f"loading script: {basename(filename)}")
        with open(filename, "r", encoding="utf-8") as fp:
            sql_str: str = fp.read()
        try:
            with self:
                self.execute_script(sql_str)
        except Exception as e:
            logger.error(f"Error loading script: {filename}")
            logger.error(e)
            raise

    def drop(self, tablename: str) -> None:
        with self:
            self.execute_script(f"drop table if exists {tablename};")
        return self

    @abc.abstractmethod
    def fetch_tables(self, tables: dict[str, str], *, defaults: dict[str, Any] = None, types: dict[str, Any] = None):
        ...

    def create_database(self, tag: str, force: bool) -> None:
        ...


class SqliteDatabase(DatabaseInterface):
    def __init__(self, **opts):
        super().__init__(**opts)
        self.connection: sqlite3.Connection = None

        self.filename: str = opts.get("filename")  # or ":memory:"

        self.register_numpy_adapters()

    def register_numpy_adapters(self) -> None:
        for dt in [np.int8, np.int16, np.int32, np.int64]:
            sqlite3.register_adapter(dt, int)
        for dt in [np.float16, np.float32, np.float64]:
            sqlite3.register_adapter(dt, float)
        sqlite3.register_adapter(np.nan, lambda _: "'NaN'")
        sqlite3.register_adapter(np.inf, lambda _: "'Infinity'")
        sqlite3.register_adapter(-np.inf, lambda _: "'-Infinity'")

    def _open(self) -> Self:
        if not self.connection:
            self.connection: sqlite3.Connection = sqlite3.connect(self.filename)
        return self

    def _close(self):
        if self.connection is None:
            return
        self.connection.commit()
        self.connection.close()
        self.connection = None

    def commit(self) -> None:
        if self.connection is None:
            return
        self.connection.commit()

    def execute_script(self, sql: str) -> None:
        """Executes a script in the database"""
        with self:
            self.connection.executescript(sql).close()

    def fetch_one(self, sql: str) -> list[Any]:
        """Fetches a single row from the database"""
        with self:
            return self.connection.execute(sql).fetchone() or [None]

    def fetch_scalar(self, sql: str) -> Any:
        """Fetches a single value from the database"""
        with self:
            return self.fetch_one(sql)[0]

    def fetch_sql(self, sql: str) -> pd.DataFrame:
        """Reads a table from the database"""
        with self:
            return pd.read_sql(sql, self.connection)

    def store(self, data: pd.DataFrame, tablename: str, columns: list[str] = None) -> None:
        """Loads dataframe into the database"""
        try:
            data: np.recarray = data.to_records(index=False)
            columns: list[str] = columns or data.columns.to_list()
            with self:
                sql: str = self.compiler.to_insert(tablename, columns)
                self.connection.executemany(sql, data).close()
            return self
        except Exception as e:  # noqa
            logger.error(f"Error loading table: {tablename}")
            logger.error(e)
            raise

    def store2(self, *, data: pd.DataFrame, tablename: str) -> None:
        """Loads datafrane into the database."""
        logger.info(f"loading table: {tablename}")
        with self:
            data.to_sql(tablename, self.connection, if_exists="replace")

        return self

    def fetch_tables(
        self, tables: dict[str, str], *, defaults: dict[str, Any] = None, types: dict[str, Any] = None
    ) -> dict[str, pd.DataFrame]:
        """Loads tables as pandas dataframes, slims types, fills NaN, sets pandas index"""
        data: dict[str, pd.DataFrame] = {}
        with self:
            for table_name, primary_key in tables.items():
                data[table_name] = self.fetch_table(table_name, primary_key=primary_key, defaults=defaults, types=types)
        return data

    def fetch_table(
        self, table_name: str, *, primary_key: str = None, defaults: dict[str, Any] = None, types: dict[str, Any] = None
    ) -> pd.DataFrame:
        """Reads a table from the database"""
        data: pd.DataFrame = self.fetch_sql(f"select * from {table_name}")

        table_info: pd.DataFrame = self.fetch_table_info(table_name)

        if table_info is not None:
            for bool_column in table_info[table_info.type == 'bool'].name:
                data[bool_column] = data[bool_column].astype(bool)

            for date_column in table_info[table_info.type == 'date'].name:
                if pd.api.types.is_string_dtype(data[date_column]):
                    data[date_column] = pd.to_datetime(data[date_column])

        if primary_key:
            data.set_index(primary_key, drop=True, inplace=True)

        slim_table_types(data, defaults=defaults, types=types)

        return data

    def fetch_table_info(self, table_name: str) -> pd.DataFrame:
        """Returns table information"""
        with self:
            return self.fetch_sql()(f"select * from PRAGMA_TABLE_INFO('{table_name}');")

    def create_database(self, tag: str, force: bool) -> None:
        """Resets the database by dropping all tables and creating a version table"""

        ensure_path(self.filename)
        reset_file(self.filename, force=force)

        self.version = tag
        return self


class PostgresDatabase(DatabaseInterface):
    def __init__(self, **opts):
        super().__init__(**opts)


DefaultDatabaseType: Type[DatabaseInterface] = SqliteDatabase
