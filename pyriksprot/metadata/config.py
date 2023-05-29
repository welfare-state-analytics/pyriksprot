from functools import cached_property
from os.path import isfile, join
from typing import Callable, Iterable, Literal
from urllib.parse import quote

import numpy as np
import pandas as pd

from pyriksprot.metadata.utility import fix_incomplete_datetime_series

from ..utility import probe_filename, revdict


def input_unknown_url(tag: str = "main"):
    return f"https://raw.githubusercontent.com/welfare-state-analytics/riksdagen-corpus/{quote(tag)}/input/matching/unknowns.csv"


def table_url(tablename: str, tag: str = "main") -> str:
    if tablename == "unknowns":
        return input_unknown_url(tag)
    return f"https://raw.githubusercontent.com/welfare-state-analytics/riksdagen-corpus/{quote(tag)}/corpus/metadata/{quote(tablename)}.csv"


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


def fix_ts_fx(column: str, action: Literal['extend', 'truncate']) -> Callable[[pd.DataFrame], pd.DataFrame]:
    return lambda df: fix_incomplete_datetime_series(df, column, action, inplace=True)


INCOMPLETE_DATE_COLUMNS = {
    'start': {
        'fx': fix_ts_fx("start", "truncate"),
        'columns': {
            'start0': 'date',
            'start_flag': 'text',
        },
    },
    'end': {
        'fx': fix_ts_fx("end", "extend"),
        'columns': {
            'end0': 'date',
            'end_flag': 'text',
        },
    },
    'born': {
        'fx': fix_ts_fx("born", "truncate"),
        'columns': {
            'born0': 'date',
            'born_flag': 'text',
        },
    },
    'dead': {
        'fx': fix_ts_fx("dead", "extend"),
        'columns': {
            'dead0': 'date',
            'dead_flag': 'text',
        },
    },
}

RIKSPROT_METADATA_TABLES: dict = {
    'alias': {
        # 'alias_id': 'AUTO_INCREMENT'
        'person_id': 'text references person (person_id) not null',  # compound key
        'alias': 'text not null',  # compound key
        ':rename_column:': {'wiki_id': 'person_id'},
    },
    'government': {
        'government_id': 'text not null',  # actual primary key
        'government': 'text primary key not null',
        'start': 'text',
        'end': 'text',
        # ':options:': {'auto_increment': 'government_id'},
        ':index:': {},
        ':drop_duplicates:': 'government',
        ':compute:': [
            INCOMPLETE_DATE_COLUMNS.get("start"),
            INCOMPLETE_DATE_COLUMNS.get("end"),
        ],
    },
    'location_specifier': {
        # 'location_specifier_id': 'AUTO_INCREMENT',
        'person_id': 'text references person (person_id) not null',
        'location': 'text',
        ':rename_column:': {'wiki_id': 'person_id'},
    },
    'member_of_parliament': {
        # 'member_of_parliament_id': 'AUTO_INCREMENT',
        'person_id': 'text references person (person_id) not null',
        'district': 'text',
        'role': 'text',
        'start': 'date',
        'end': 'date',
        ':rename_column:': {'wiki_id': 'person_id'},
        ':compute:': [
            INCOMPLETE_DATE_COLUMNS.get("start"),
            INCOMPLETE_DATE_COLUMNS.get("end"),
        ],
    },
    'minister': {
        'person_id': 'text references person (person_id) not null',
        'government': 'text',
        'role': 'text',
        'start': 'date',
        'end': 'date',
        ':rename_column:': {'wiki_id': 'person_id'},
        ':compute:': [
            INCOMPLETE_DATE_COLUMNS.get("start"),
            INCOMPLETE_DATE_COLUMNS.get("end"),
        ],
    },
    'name': {
        'person_id': 'text references person (person_id) not null',
        'name': 'text not null',
        'primary_name': 'integer not null',
        ':rename_column:': {'wiki_id': 'person_id'},
    },
    'party_abbreviation': {
        'party': 'text primary key not null',
        'abbreviation': 'text not null',
        'ocr_correction': 'text',
    },
    'party_affiliation': {
        'person_id': 'text references person (person_id) not null',
        'start': 'int',
        'end': 'int',
        'party': 'text',
        'party_id': 'text',
        ':rename_column:': {'wiki_id': 'person_id'},
        ':compute:': [
            INCOMPLETE_DATE_COLUMNS.get("start"),
            INCOMPLETE_DATE_COLUMNS.get("end"),
        ],
    },
    'person': {
        'person_id': 'text primary key',
        'born': 'date',
        'dead': 'date',
        'gender': 'text',
        'wiki_id': 'text',
        'riksdagen_id': 'text',
        ':drop_duplicates:': 'wiki_id',
        ':copy_column:': {'person_id': 'wiki_id'},
        ':compute:': [
            INCOMPLETE_DATE_COLUMNS.get("born"),
            INCOMPLETE_DATE_COLUMNS.get("dead"),
        ],
    },
    'speaker': {
        'person_id': 'text references person (person_id) not null',
        'role': 'text',
        'start': 'date',
        'end': 'date',
        ':rename_column:': {'wiki_id': 'person_id'},
        ':compute:': [
            INCOMPLETE_DATE_COLUMNS.get("start"),
            INCOMPLETE_DATE_COLUMNS.get("end"),
        ],
    },
    'twitter': {
        'twitter': 'text',  # primary key',
        'person_id': 'text references person (person_id) not null',
        ':rename_column:': {'wiki_id': 'person_id'},
    },
    'name_location_specifier': {
        # 'name_location_specifier_id': 'AUTO_INCREMENT'
        'person_id': 'text references person (person_id) not null',  # compound key
        'alias': 'text',  # compound key
        'name': 'text not null',
        ':rename_column:': {'wiki_id': 'person_id'},
    },
    'unknowns': {
        'protocol_id': 'text',  # primary key',
        'uuid': 'text',
        'gender': 'text',
        'party': 'text',
        'other': 'text',
        ':url:': input_unknown_url,
        ':is_extra:': True,
    },
}

# EXTRA_TABLES = {
#     'speech_index': {
#         'document_id': 'int primary key',
#         'document_name': 'text',
#         'year': 'int',
#         'who': 'text',
#         'gender_id': 'int',
#         'party_id': 'int',
#         'office_type_id': 'int',
#         'sub_office_type_id': 'int',
#         'n_tokens': 'int',
#         'filename': 'text',
#         'u_id': 'text',
#         'n_utterances': 'int',
#         'speaker_note_id': 'text',
#         'speach_index': 'int',
#     },
# }


class MetadataTableConfig:
    def __init__(self, name: str, data: dict):
        self.name: str = name
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
    def has_url(self) -> bool:
        return ':url:' in self.data

    def resolve_url(self, tag: str) -> str:
        """Resolves proper URL to table for tag based on configuration"""
        url = self.url
        if url is None:
            return table_url(tablename=self.name, tag=tag)
        if callable(url):
            return url(tag)
        return url

    @cached_property
    def url(self) -> str | Callable | None:
        return self.data.get(':url:')

    @cached_property
    def rename_map(self) -> dict:
        return self.data.get(':rename_column:', {})

    @cached_property
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

    def load_table(self, folder: str, tag: str) -> pd.DataFrame:
        """Loads table from specified folder or from url in configuration"""
        if self.has_url and folder is None:
            return pd.read_csv(self.resolve_url(tag), sep=',')

        if isinstance(folder, str):
            url: str = probe_filename(join(folder, f"{self.name}.csv"), ["zip", "csv.gz"])
            return pd.read_csv(url)

        if isinstance(tag, str):
            url: str = table_url(self.name, tag)
            return pd.read_csv(url)

        raise ValueError("either :url:, folder or branch must be set")

    def to_sql_create(self) -> str:
        lf = '\n' + (12 * ' ')
        sql_ddl: str = f"""
            create table {self.name} (
                {(','+lf).join(f"{k} {t}" for k, t in self.all_columns_specs.items())}
            );
        """
        return sql_ddl

    def to_sql_insert(self) -> str:
        insert_sql = f"""
        insert into {self.name} ({', '.join(self.all_columns)})
            values ({', '.join(['?'] * len(self.all_columns))});
        """
        return insert_sql


class MetadataTableConfigs:
    def __init__(self):
        self.data = RIKSPROT_METADATA_TABLES
        self.definitions = {table: MetadataTableConfig(table, self.data[table]) for table in self.data}

    @property
    def tablenames(self) -> list[str]:
        tables: list[str] = list(self.data.keys())
        return tables

    @property
    def tablesnames0(self) -> list[str]:
        tables: list[str] = [x for x in self.tablenames if not bool(self.data[x].get(':is_extra:'))]
        return tables

    def __getitem__(self, key: str) -> MetadataTableConfig:
        return self.definitions[key]

    def __iter__(self):
        return iter(self.definitions)

    def items(self) -> Iterable[tuple[str, MetadataTableConfig]]:
        return self.definitions.items()

    @property
    def person_tables(self) -> list[str]:
        return [table for table in self.tablenames if 'person_id' in self[table].columns]

    def files_exist(self, folder: str) -> bool:
        """Checks that all expected files exist in given location."""
        return all(isfile(join(folder, f"{x}.csv")) for x in self.tablenames)
