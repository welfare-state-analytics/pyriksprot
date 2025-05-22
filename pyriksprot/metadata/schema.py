import json
from functools import cached_property
from importlib import import_module
from os.path import exists, isfile, join
from pathlib import Path
from typing import Any, Callable, Iterable, Literal

import numpy as np
import pandas as pd
from loguru import logger

import pyriksprot.sql

from ..utility import revdict
from .utility import fix_incomplete_datetime_series

PARTY_COLORS = [
    (0, 'S', '#E8112d'),
    (1, 'M', '#52BDEC'),
    (2, 'gov', '#000000'),
    (3, 'C', '#009933'),
    (4, 'L', '#006AB3'),
    (5, 'V', '#DA291C'),
    (6, 'MP', '#83CF39'),
    (7, 'KD', '#000077'),
    (8, 'NYD', '#007700'),
    (9, 'SD', '#DDDD00'),
]

PARTY_COLOR_BY_ID = {x[0]: x[2] for x in PARTY_COLORS}
PARTY_COLOR_BY_ABBREV = {x[1]: x[2] for x in PARTY_COLORS}

NAME2IDNAME_MAPPING: dict[str, str] = {
    'gender': 'gender_id',
    'office_type': 'office_type_id',
    'sub_office_type': 'sub_office_type_id',
    'person_id': 'pid',
}
IDNAME2NAME_MAPPING: dict[str, str] = revdict(NAME2IDNAME_MAPPING)


def fix_ts_config(column: str, action: Literal['extend', 'truncate']) -> dict[str, Any]:
    """Returns config (dict) for fixing incomplete datetime series"""
    return {
        'fx': lambda df: fix_incomplete_datetime_series(df, column, action, inplace=True),
        'columns': {
            f'{column}0': 'text',
            f'{column}_flag': 'text',
        },
    }


def resolve_fx_by_name(name: str) -> Callable[[Any], Any]:
    if name in globals():
        return globals()[name]

    if "." in name:
        module_name, fx_name = name.rsplit(".", 1)
        module = import_module(module_name)
        return getattr(module, fx_name)

    raise ValueError(f"Function {name} not found")


def resolve_column_config(fx_name: str, *args) -> Callable[[Any], Any]:
    return resolve_fx_by_name(fx_name)(*args)


class MetadataTable:
    def __init__(self, name: str, data: dict):
        self.tablename: str = name
        self.data: dict = data

    @cached_property
    def columns(self) -> list[str]:
        """Target columns in SQL table"""
        columns: list[str] = [name for name in self.data if not name.startswith(':')]
        return columns

    @cached_property
    def source_columns(self) -> list[str]:
        """Source columns from CSV file"""
        rename_map: dict[str, str] = self.rename_map
        data_columns: list[str] = [rename_map.get(c, c) for c in self.columns]
        return data_columns

    @cached_property
    def compute_column_specs(self) -> dict[str, str]:
        """Columns to be added to SQL table that are results from compute/transform"""
        x: dict = {}
        for d in self.data.get(':compute:', {}):
            x.update(d.get('columns'))
        return x

    @cached_property
    def compute_columns(self) -> list[str]:
        return list(self.compute_column_specs.keys())

    @cached_property
    def all_columns(self) -> list[str]:
        return self.columns + self.compute_columns

    @property
    def constraints(self) -> list[str]:
        data: str | None | list[str] = self.data.get(':constraints:')
        if isinstance(data, str):
            return [data]
        if not data:
            return []
        return data

    @cached_property
    def all_columns_specs(self) -> dict[str, str]:
        """All target columns i.e. columns from source plus computed columns"""
        x: dict = {name: self.data[name] for name in self.columns}
        x.update(self.compute_column_specs)
        return x

    def resolve_source_column(self, name: str) -> str:
        """Returns name of column in CSV"""
        return (revdict(self.rename_map) | self.copy_map).get(name, name)

    @property
    def basename(self) -> str:
        return self.data.get(':filename:', f"{self.tablename}.csv")

    @property
    def has_url(self) -> bool:
        return ':url:' in self.data and self.data.get(':url:', False)

    @property
    def has_constraints(self) -> bool:
        return bool(self.constraints)

    @property
    def url(self) -> str | Callable | None:
        return self.data.get(':url:')

    @property
    def is_derived(self) -> bool:
        return self.data.get(':derived:', False)

    @property
    def is_extra(self) -> bool:
        return self.data.get(':is_extra:', False)

    @property
    def sep(self) -> str:
        return self.data.get(':sep:', ',')

    @property
    def rename_map(self) -> dict:
        return self.data.get(':rename_column:', {})

    @property
    def copy_map(self) -> dict:
        return self.data.get(':copy_column:', {})

    def transform(self, table: pd.DataFrame) -> pd.DataFrame:
        """Transforms table based on configuration"""
        table = table.copy()

        if ':compute:' in self.data:
            for cfg in self.data[':compute:']:
                table = cfg.get('fx')(table)
                for col, col_type in cfg.get('columns').items():
                    self.data[col] = col_type

        if ':drop_duplicates:' in self.data:
            table = table.drop_duplicates(subset=self.data[':drop_duplicates:'], keep='first')

        if ':rename_column:' in self.data:
            assert isinstance(self.data[':rename_column:'], dict)
            for k, v in self.data[':rename_column:'].items():
                table = table.rename(columns={k: v})

        if ':copy_column:' in self.data:
            assert isinstance(self.data[':copy_column:'], dict)
            for k, v in self.data[':copy_column:'].items():
                table[k] = table[v]

        for c in table.columns:
            if table.dtypes[c] == np.dtype('bool'):  # pylint: disable=no-member
                table[c] = [int(x) for x in table[c]]

        return table


class MetadataSchema:
    """Configuration for all tables in metadata"""

    def __init__(self, version: str | None):
        if version is None:
            raise ValueError("Tag must be defined")

        sql_folder: Path = pyriksprot.sql.sql_folder(tag=version)

        if not sql_folder.is_dir():
            raise FileNotFoundError(f"sql folder for {version} not found")

        if not sql_folder.joinpath("schema.json").is_file():
            raise FileNotFoundError(f"sql schema.json for {version} not found")

        with sql_folder.joinpath("schema.json").open() as f:
            self.data: dict = json.load(f)

        self.config: dict = {}

        if ':config:' in self.data:
            self.config = self.data[':config:']
            self.data.pop(':config:')

        """ Resolve computed columns """

        for table_name in self.data:
            if ':compute:' not in self.data[table_name]:
                continue

            cfgs: list = []

            for cfg in self.data[table_name][':compute:']:
                if isinstance(cfg, dict):
                    cfgs.append(cfg)
                elif isinstance(cfg, (list, tuple)):
                    if len(cfg) == 0 or not isinstance(cfg[0], str):
                        raise ValueError(
                            "fx must be a function or a sequence where the first element is a function name and the rest are arguments"
                        )
                    cfgs.append(resolve_column_config(cfg[0], *cfg[1:]))
                else:
                    raise ValueError(
                        "fx must be a function or a sequence where the first element is a function name and the rest are arguments"
                    )

            self.data[table_name][':compute:'] = cfgs

        self.definitions: dict[str, MetadataTable] = {
            table: MetadataTable(table, self.data[table]) for table in self.data
        }

    @property
    def tablesnames0(self) -> set[str]:
        return {x.tablename for x in self.definitions.values() if not x.is_derived and not x.is_extra}

    def __getitem__(self, key: str) -> MetadataTable:
        return self.definitions[key]

    def __iter__(self):
        return iter(self.definitions)

    def items(self) -> Iterable[tuple[str, MetadataTable]]:
        return self.definitions.items()

    @property
    def extras(self) -> dict[str, MetadataTable]:
        return {k: d for k, d in self.definitions.items() if d.is_extra}

    @property
    def extras_urls(self) -> dict[str, str]:
        return {d.basename: str(d.url) for d in self.definitions.values() if d.is_extra and d.has_url}

    def files_exist(self, folder: str) -> bool:
        """Checks that all expected files exist in given location."""
        if not exists(folder):
            return False

        files_status: dict[str, bool] = {
            x.basename: isfile(join(folder, x.basename)) for _, x in self.definitions.items()
        }
        if not all(files_status.values()):
            logger.error(f"missing files in {folder}: {' '.join([k for k, v in files_status.items() if not v])}")
            return False
        return True

    def get_by_filename(self, filename: str) -> MetadataTable | None:
        for _, table in self.definitions.items():
            if table.basename == filename:
                return table
        return None

    @property
    def tablenames(self) -> set[str]:
        return {cfg.tablename for cfg in self.definitions.values() if not cfg.is_derived}

    @property
    def derived_tablenames(self) -> set[str]:
        return {cfg.tablename for cfg in self.definitions.values() if cfg.is_derived}

    @property
    def derived_tables(self) -> Iterable[MetadataTable]:
        return (cfg for cfg in self.definitions.values() if cfg.is_derived)
