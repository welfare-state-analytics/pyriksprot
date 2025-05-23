import abc
import sqlite3
from contextlib import contextmanager
from os.path import basename
from typing import Any, Self, Type

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.sql
from loguru import logger
from psycopg2 import extensions as pg
from psycopg2 import extras as pgx

from pyriksprot.metadata.utility import slim_table_types
from pyriksprot.utility import create_class, ensure_path, reset_file

from .schema import MetadataSchema, MetadataTable


class SqlCompiler:
    def to_create(self, tablename: str, columns_specs: dict[str, str], constraints: list[str]) -> str:
        lf: str = '\n' + (4 * ' ')
        constraints = [c for c in constraints if not c.strip().startswith('--')]
        clause: str = f",{lf}".join(constraints)
        if clause:
            clause: str = f",{lf}{clause}"
        sql_ddl: str = f"""
create table {tablename} (
    {(','+lf).join(f'"{k}" {t}' for k, t in columns_specs.items())}{clause}
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
        self.opts: dict[str, Any] = opts
        self.session_depth: int = 0
        self.compiler: SqlCompiler = opts.get("compiler") or SqlCompiler()
        self.quote_chars: str | tuple[str, str] = ('"', '"')

    def open(self) -> Self:
        self.session_depth += 1
        if self.session_depth == 1:
            self._open()
        return self

    def close(self) -> None:
        self.session_depth = max(0, self.session_depth - 1)
        if self.session_depth == 0:
            self._close()

    @abc.abstractmethod
    def _open(self) -> Self:
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
                    version text primary key
                );
                delete from version;
                insert into version(version) values ('{tag}');
            """
            )

    def create(self, tablename: str, columns: dict[str, str], constraints: list[str]) -> Self:
        sql: str = ""
        try:
            sql: str = self.compiler.to_create(tablename, columns, constraints)
            self.execute_script(sql)
        except Exception as e:
            logger.error(f"Error creating table: {tablename}")
            logger.error(f"SQL: {sql}")
            logger.error(e)
        return self

    @abc.abstractmethod
    def store(self, data: pd.DataFrame, tablename: str, columns: list[str] = None, cfg: MetadataTable = None) -> Self:
        """Loads dataframe into the database."""
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

    @abc.abstractmethod
    def drop(self, tablename: str, cascade: bool = False) -> Self:
        ...

    @abc.abstractmethod
    def fetch_tables(
        self, tables: dict[str, str], *, defaults: dict[str, Any] = None, types: dict[str, Any] = None
    ) -> dict[str, pd.DataFrame]:
        ...

    @abc.abstractmethod
    def createdb(self, tag: str, force: bool) -> Self:
        ...

    @abc.abstractmethod
    def dropdb(self, tag: str, force: bool) -> Self:
        ...

    def quote(self, value: Any) -> str:
        if isinstance(value, str):
            return f'{self.quote_chars[0]}{value}{self.quote_chars[1]}'
        return str(value)

    @abc.abstractmethod
    def set_deferred(self, value: bool) -> None:
        ...

    @abc.abstractmethod
    def exists(self, tablename: str) -> bool:
        ...

    @abc.abstractmethod
    def set_foreign_keys(self, value: bool) -> None:
        ...

    def create_table(self, cfg: MetadataTable) -> None:
        """Creates table in database based on schema."""
        self.create(cfg.tablename, cfg.all_columns_specs, cfg.constraints)

    def create_tables(self, schema: MetadataSchema) -> None:
        for cfg in filter(lambda x: not x.has_constraints, schema.definitions.values()):
            self.create_table(cfg)

        for cfg in filter(lambda x: x.has_constraints, schema.definitions.values()):
            self.create_table(cfg)

    def _create_tables(self, schema: MetadataSchema) -> None:
        """Creates tables in database based on schema. Create base tables first and then tables with constraints."""
        for has_constraints in [False, True]:
            for cfg in schema.definitions.values():
                if cfg.has_constraints == has_constraints:
                    self.create_table(cfg)


class SqliteDatabase(DatabaseInterface):
    def __init__(self, **opts):
        super().__init__(**opts)
        self.quote_chars: str | tuple[str, str] = ('"', '"')
        self.connection: sqlite3.Connection | None = None

        self.filename: str | None = opts.get("filename")  # or ":memory:"

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
            self.connection = sqlite3.connect(self.filename)
        return self

    def _close(self):
        if self.connection is None:
            return
        self.connection.commit()
        self.connection.close()
        self.connection = None

    def set_deferred(self, value: bool) -> None:
        if value:
            self.connection.execute("pragma defer_foreign_keys = on;")
        else:
            self.connection.execute("pragma defer_foreign_keys = off;")

    def set_foreign_keys(self, value: bool) -> None:
        with self:
            self.execute_script(f"pragma foreign_keys = {'on' if value else 'off'};")

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

    def store(self, data: pd.DataFrame, tablename: str, columns: list[str] = None, cfg: MetadataTable = None) -> Self:
        """Loads dataframe into the database"""
        try:
            records: np.recarray = data.to_records(index=False)
            columns = columns or data.columns.to_list()
            with self:
                sql: str = self.compiler.to_insert(tablename, columns)
                self.connection.executemany(sql, records).close()
            return self
        except Exception as e:  # noqa
            logger.error(f"Error loading table: {tablename}")
            logger.error(e)
            raise

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
            return self.fetch_sql(f"select * from PRAGMA_TABLE_INFO('{table_name}');")

    def createdb(self, tag: str, force: bool) -> Self:
        """Resets the database by dropping all tables and creating a version table"""

        ensure_path(self.filename)
        reset_file(self.filename, force=force)

        self.version = tag
        return self

    def dropdb(self, tag: str, force: bool) -> Self:
        """Renoved the database file"""
        reset_file(self.filename, force=force)
        return self

    def exists(self, tablename: str) -> bool:
        return (
            self.fetch_scalar(f"select count(name) from sqlite_master where type='table' and name='{tablename}'") == 1
        )

    def drop(self, tablename: str, cascade: bool = False) -> Self:
        with self:
            if cascade:
                self.set_foreign_keys(False)
            self.execute_script(f"drop table if exists {tablename};")
        return self


class PostgresDatabase(DatabaseInterface):
    def __init__(self, **opts):
        self._deferred_constraints: bool = opts.pop('deferred_constraints', True)
        self._single_transaction: bool = opts.pop('single_transaction', True)
        self._single_cursor: None | pg.cursor = None

        super().__init__(**opts)

        self.connection: None | pg.connection = None
        self.quote_chars: str | tuple[str, str] = ('"', '"')

    def _get_db_opts(self) -> dict[str, Any]:
        return {
            k: v
            for k, v in self.opts.items()
            if k
            in {"dsn", "connection_factory", "cursor_factory", "database", "dbname", "user", "password", "host", "port"}
        }

    def _open(self) -> Self:
        if not self.connection:
            self.connection = psycopg2.connect(**self._get_db_opts())
            if self._single_transaction:
                # Create a single cursor if using a single transaction
                self._single_cursor = self.connection.cursor()
                if self._deferred_constraints:
                    self._single_cursor.execute("begin")
                    self._single_cursor.execute("set constraints all deferred")
        return self

    def _close(self):
        if self.connection is None:
            return

        if self._single_cursor is not None:
            self._single_cursor.close()
            self._single_cursor = None

        self.connection.commit()
        self.connection.close()
        self.connection = None

    @contextmanager
    def _get_cursor(self):
        """Context manager for getting a cursor based on single_transaction setting"""
        if self._single_transaction:
            if not self._single_cursor:
                raise RuntimeError("Single cursor is not initialized.")
            yield self._single_cursor
        else:
            cursor: pg.cursor = self.connection.cursor()
            try:
                yield cursor
            finally:
                cursor.close()

    def commit(self) -> None:
        if self.connection is None:
            return
        self.connection.commit()

    def set_deferred(self, value: bool) -> None:
        self._deferred_constraints = value
        # with self._get_cursor() as cursor:
        #     if value:
        #         cursor.execute("set constraints all deferred;")
        #     else:
        #         cursor.execute("set constraints all immediate;")

    def set_foreign_keys(self, value: bool) -> None:
        self.set_deferred(value)

    def execute_script(self, sql: str) -> None:
        """Executes a script in the database"""
        with self:
            with self._get_cursor() as cursor:
                cursor.execute(sql)

    def fetch_one(self, sql: str) -> list[Any]:
        """Fetches a single row from the database"""
        with self:
            with self._get_cursor() as cursor:
                cursor.execute(sql)
                return list(cursor.fetchone() or [None])

    def fetch_scalar(self, sql: str) -> Any:
        """Fetches a single value from the database"""
        return self.fetch_one(sql)[0]

    def fetch_sql(self, sql: str) -> pd.DataFrame:
        """Reads a table from the database"""
        with self:
            with self._get_cursor() as cursor:
                data: list[tuple[Any, ...]] = cursor.fetchall()
                columns: list[str] = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(data, columns=columns)
                return df

    def _transform_data(self, data: pd.DataFrame, cfg: MetadataTable) -> pd.DataFrame:
        if cfg:
            for c, t in cfg.data.items():
                if t == 'date':
                    data[c] = data[c].replace('', None)
        return data

    def store(self, data: pd.DataFrame, tablename: str, columns: list[str] = None, cfg: MetadataTable = None) -> Self:
        """Loads dataframe into the database into the specified existing table"""
        try:
            columns = columns or data.columns.to_list()
            data = data[columns].where(pd.notnull(data), None)
            data = self._transform_data(data, cfg)

            data_tuples: list[tuple[Any, ...]] = [tuple(row) for row in data.itertuples(index=False, name=None)]
            insert_query: str = f'insert into {tablename} ({", ".join(map(self.quote, columns))}) values %s'

            with self._get_cursor() as cursor:
                pgx.execute_values(cursor, insert_query, data_tuples)

            return self
        except Exception as e:  # noqa
            logger.error(f"Error loading table: {tablename}")
            logger.error(e)
            raise

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
            data = _set_optimal_types(data, table_info)

        if primary_key:
            data.set_index(primary_key, drop=True, inplace=True)

        slim_table_types(data, defaults=defaults, types=types)

        return data

    def fetch_table_info(self, table_name: str) -> pd.DataFrame:
        """Returns table information"""
        with self:
            query = f"""
                select column_name, data_type
                from information_schema.columns
                where table_name = '{table_name}';
            """
            return pd.read_sql(query, self.connection)

    def createdb(self, tag: str, force: bool) -> Self:
        """Resets the database by dropping all tables and creating a version table"""
        if not force:
            return self

        if not self.opts.get('database'):
            raise ValueError("Database name not set in configuration.")

        self.dropdb(tag, force=force)
        self.pgdb_execute(sql=f"create database {self.opts.get('database')};", **self.opts)

        self.version = tag
        return self

    def dropdb(self, tag: str, force: bool) -> Self:
        if not force:
            return self
        self.pgdb_execute(sql=f"drop database if exists {self.opts.get('database')};", **self.opts)
        return self

    def exists(self, tablename: str) -> bool:
        return (
            self.fetch_scalar(
                f"select count(table_name) from information_schema.tables where table_name = '{tablename}'"
            )
            == 1
        )

    def drop(self, tablename: str, cascade: bool = False) -> Self:
        with self:
            self.execute_script(f"drop table if exists {tablename} {'cascade' if cascade else ''};")
        return self

    @staticmethod
    def pgdb_execute(*, sql: str, **opts) -> None:
        connection: pg.connection = psycopg2.connect(**(opts | {'database': 'postgres'}))
        try:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(f"{sql};")
        finally:
            connection.close()


def _map_postgres_types_to_pandas(postgres_type: str):
    """Map PostgreSQL data types to Pandas dtypes."""
    if postgres_type == 'integer':
        return 'Int64'  # nullable integer in Pandas
    elif postgres_type == 'bigint':
        return 'Int64'
    elif postgres_type == 'numeric' or postgres_type == 'double precision':
        return 'float'
    elif postgres_type == 'character varying' or postgres_type == 'text':
        return 'string'
    elif postgres_type == 'boolean':
        return 'bool'
    elif postgres_type == 'date':
        return 'datetime64[ns]'
    elif postgres_type == 'timestamp without time zone' or postgres_type == 'timestamp with time zone':
        return 'datetime64[ns]'

    return 'object'  # fallback to generic object


def _set_optimal_types(df: pd.DataFrame, schema_df: pd.DataFrame) -> pd.DataFrame:
    """Set optimal types for the DataFrame columns based on the table schema."""
    for _, row in schema_df.iterrows():
        col_name = row['column_name']
        pg_type = row['data_type']

        # Get corresponding Pandas type
        pandas_type = _map_postgres_types_to_pandas(pg_type)

        if col_name in df.columns:
            try:
                df[col_name] = df[col_name].astype(pandas_type)
            except Exception as e:
                logger.error(f"Error converting column {col_name} to {pandas_type}: {e}")

    return df


DefaultDatabaseType: Type[DatabaseInterface] = SqliteDatabase


def create_backend(
    backend: DatabaseInterface | str = DefaultDatabaseType | Type[DatabaseInterface], **opts
) -> DatabaseInterface:
    db: DatabaseInterface = (
        backend
        if isinstance(backend, DatabaseInterface)
        else (
            create_class(backend)(**opts)
            if isinstance(backend, str)
            else (backend(**opts) if issubclass(backend, DatabaseInterface) else DefaultDatabaseType(**opts))
        )
    )
    return db
