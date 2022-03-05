from __future__ import annotations

import sqlite3
from contextlib import nullcontext
from dataclasses import dataclass
from functools import cached_property
from typing import Callable, List, Literal, Mapping

import numpy as np
import pandas as pd

from .utility import read_sql_table, read_sql_tables, revdict


@dataclass
class Codec:
    type: Literal['encoder', 'decoder']
    from_column: str
    to_column: str
    fx: Callable[[int], str]
    default: str = None


COLUMN_TYPES = {
    'year_of_birth': np.int16,
    'year_of_death': np.int16,
    'gender_id': np.int8,
    'party_id': np.int8,
    'chamber_id': np.int8,
    'office_type_id': np.int8,
    'sub_office_type_id': np.int8,
    'start_year': np.int16,
    'end_year': np.int16,
    'district_id': np.int16,
}

COLUMN_DEFAULTS = {
    'gender_id': 0,
    'year_of_birth': 0,
    'year_of_death': 0,
    'district_id': 0,
    'party_id': 0,
    'chamber_id': 0,
    'office_type_id': 0,
    'sub_office_type_id': 0,
    'start_year': 0,
    'end_year': 0,
}

CODE_TABLES: dict[str, str] = {
    'chamber': 'chamber_id',
    'gender': 'gender_id',
    'office_type': 'office_type_id',
    'sub_office_type': 'sub_office_type_id',
    'government': 'government_id',
    'party': 'party_id',
}

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
    # {'party_abbrev_id': 0, 'party_abbrev': 'PP', 'party': 'Piratpartiet', 'color_name': 'Lila', 'color': '#572B85'},
    # {'party_abbrev_id': 0, 'party_abbrev': 'F', 'party': 'Feministiskt', 'color_name': 'initiativ	Rosa', 'color': '#CD1B68'},
]

PARTY_COLOR_BY_ID = {x[0]: x[2] for x in PARTY_COLORS}
PARTY_COLOR_BY_ABBREV = {x[1]: x[2] for x in PARTY_COLORS}

NAME2IDNAME_MAPPING: Mapping[str, str] = {
    'gender': 'gender_id',
    'office_type': 'office_type_id',
    'sub_office_type': 'sub_office_type_id',
    'person_id': 'pid',
}
IDNAME2NAME_MAPPING: Mapping[str, str] = revdict(NAME2IDNAME_MAPPING)


def slim_table_types(tables: list[pd.DataFrame], set_default: bool = True) -> None:
    """Slims types and sets default value for NaN entries"""
    if set_default:
        for table in tables:
            for column_name, value in COLUMN_DEFAULTS.items():
                if column_name in table.columns:
                    table[column_name].fillna(value, inplace=True)

    for table in tables:
        for column_name, dt in COLUMN_TYPES.items():
            if column_name in table.columns:
                if table[column_name].dtype != dt:
                    table[column_name] = table[column_name].astype(dt)


@dataclass
class Person:
    # pid: int
    person_id: str
    name: str
    gender_id: int
    party_id: int
    year_of_birth: int
    year_of_death: int


@dataclass
class SpeakerInfo:
    person_id: str
    name: str
    start_year: int = None
    end_year: int = None
    gender_id: int = 0
    party_id: int = 0
    office_type_id: int = None
    sub_office_type_id: int = None
    year_of_birth: int = None
    year_of_death: int = None
    chamber_id: int = None
    # district: str = None
    # occupation: str = None
    # title: str = None
    property_bag: dict = None


class MetaDataCodeMaps:
    def __init__(self, **kwargs):
        self.chamber: pd.DataFrame = kwargs.get('chamber')
        self.gender: pd.DataFrame = kwargs.get('gender')
        self.office_type: pd.DataFrame = kwargs.get('office_type')
        self.sub_office_type: pd.DataFrame = kwargs.get('sub_office_type')
        self.government: pd.DataFrame = kwargs.get('government')
        self.party: pd.DataFrame = kwargs.get('party')
        self.person: pd.DataFrame = kwargs.get('person')

    @staticmethod
    def load(source: str | sqlite3.Connection | dict, person: pd.DataFrame) -> MetaDataCodeMaps:

        with (sqlite3.connect(database=source) if isinstance(source, str) else nullcontext(source)) as db:

            code_tables: dict[str, pd.DataFrame] = read_sql_tables(list(CODE_TABLES.keys()), db)

            slim_table_types(code_tables.values(), set_default=False)

            for table_name, key_name in code_tables:
                code_tables[table_name].set_index(key_name, drop=True, inplace=True)

            code_map: MetaDataCodeMaps = MetaDataCodeMaps(person=person, **code_tables)

            return code_map

    @cached_property
    def gender2name(self) -> dict:
        return self.gender['gender'].to_dict()

    @cached_property
    def gender2id(self) -> dict:
        return revdict(self.gender2name)

    @cached_property
    def office_type2name(self) -> dict:
        return self.office_type['office'].to_dict()

    @cached_property
    def office_type2id(self) -> dict:
        return revdict(self.office_type2name)

    @cached_property
    def sub_office_type2name(self) -> dict:
        return self.sub_office_type['description'].to_dict()

    @cached_property
    def sub_office_type2id(self) -> dict:
        return revdict(self.sub_office_type2name)

    @cached_property
    def party_abbrev2name(self) -> dict:
        return self.party['party_abbrev'].to_dict()

    @cached_property
    def party_abbrev2id(self) -> dict:
        return revdict(self.party_abbrev2name)

    @cached_property
    def pid2person_id(self) -> dict:
        return self.person['person_id'].to_dict()

    @cached_property
    def person_id2pid(self) -> dict:
        return revdict(self.pid2person_id)

    @cached_property
    def pid2person_name(self) -> dict:
        return self.person['name'].to_dict()

    @cached_property
    def codecs(self) -> list[Codec]:
        return [
            Codec('decode', 'office_type_id', 'office_type', self.office_type2name.get),
            Codec('decode', 'sub_office_type_id', 'sub_office_type', self.sub_office_type2name.get),
            Codec('decode', 'gender_id', 'gender', self.gender2name.get),
            Codec('decode', 'party_id', 'party_abbrev', self.party_abbrev2name.get),
            Codec('decode', 'pid', 'person_id', self.pid2person_id.get),
            Codec('encode', 'office_type', 'office_type_id', self.office_type2id.get),
            Codec('encode', 'sub_office_type', 'sub_office_type_id', self.sub_office_type2id.get),
            Codec('encode', 'gender', 'gender_id', self.gender2id.get),
            Codec('encode', 'party', 'party_id', self.party_abbrev2id.get),
            Codec('encode', 'person_id', 'pid', self.person_id2pid.get),
            Codec('encode', 'who', 'pid', self.person_id2pid.get),
        ]

    @property
    def decoders(self) -> list[Codec]:
        return [c for c in self.codecs if c.type == 'decode']

    @property
    def encoders(self) -> list[dict]:
        return [c for c in self.codecs if c.type == 'encode']

    def apply_codec(self, df: pd.DataFrame, codecs: list[Codec], drop: bool = True) -> pd.DataFrame:

        for codec in codecs:
            if codec.from_column in df.columns:
                df[codec.to_column] = df[codec.from_column].apply(codec.fx)
                if codec.default is not None:
                    df[codec.to_column] = df[codec.to_column].fillna(codec.default)
            if drop:
                df.drop(columns=[codec.from_column], inplace=True, errors='ignore')
        return df

    def decode(self, df: pd.DataFrame, drop: bool = True) -> pd.DataFrame:
        return self.apply_codec(df, self.decoders, drop=drop)

    def encode(self, df: pd.DataFrame, drop: bool = True) -> pd.DataFrame:
        return self.apply_codec(df, self.encoders, drop=drop)


# pylint: disable=too-many-public-methods
class MetaDataIndex:

    NAME2IDNAME_MAPPING: Mapping[str, str] = NAME2IDNAME_MAPPING
    IDNAME2NAME_MAPPING: Mapping[int, str] = IDNAME2NAME_MAPPING

    PARTY_COLOR_BY_ID: Mapping[int, str] = PARTY_COLOR_BY_ID
    PARTY_COLOR_BY_ABBREV: Mapping[str, str] = PARTY_COLOR_BY_ABBREV

    def __init__(self, data: dict, code_maps: MetaDataCodeMaps):
        self.data: dict = data
        self.code_maps: MetaDataCodeMaps = code_maps

    @staticmethod
    def load(database_filename: str) -> MetaDataIndex:
        with sqlite3.connect(database=database_filename) as db:

            """Read person related data"""
            data: dict = {
                'person': read_sql_table("persons_of_interest", db),
                'terms_of_office': read_sql_table("terms_of_office", db),
                'person_multiple_party': read_sql_table("person_multiple_party", db),
                'person_yearly_party': read_sql_table("person_yearly_party", db),
                'unknown_utterance_gender': read_sql_table("unknown_utterance_gender", db),
                'unknown_utterance_party': read_sql_table("unknown_utterance_party", db),
            }

            slim_table_types(data.values(), set_default=True)

            """Set index"""
            data['terms_of_office'].set_index("terms_of_office_id", drop=True, inplace=True)
            data['person_multiple_party'].set_index("person_multiple_party_id", drop=True, inplace=True)
            data['person_yearly_party'].set_index("person_yearly_party_id", drop=True, inplace=True)
            data['unknown_utterance_gender'].set_index("u_id", drop=True, inplace=True)
            data['unknown_utterance_party'].set_index("u_id", drop=True, inplace=True)
            data['person'].rename_axis("pid", inplace=True)

            code_maps: MetaDataCodeMaps = MetaDataCodeMaps.load(source=db, person=data.get("person"))

            riksprot_metadata: MetaDataIndex = MetaDataIndex(data, code_maps)

            return riksprot_metadata

    @property
    def person(self) -> pd.DataFrame:
        return self.data['person']

    @property
    def terms_of_office(self) -> pd.DataFrame:
        return self.data['terms_of_office']

    @property
    def person_multiple_party(self) -> pd.DataFrame:
        return self.data['person_multiple_party']

    @property
    def person_yearly_party(self) -> pd.DataFrame:
        return self.data['person_yearly_party']

    def get_speaker_info(self, person_id: str, u_id: str) -> dict[str, SpeakerInfo]:
        person = self.get_person(person_id)
        speaker_info: SpeakerInfo = SpeakerInfo()
        return speaker_info

    def get_person(self) -> Person:
        raise NotImplementedError()

    @cached_property
    def property_values_specs(self) -> List[Mapping[str, str | Mapping[str, int]]]:
        return [
            dict(text_name='gender', id_name='gender_id', values=self.code_maps.gender2id),
            dict(text_name='office_type', id_name='office_type_id', values=self.code_maps.office_type2id),
            dict(text_name='sub_office_type', id_name='sub_office_type_id', values=self.code_maps.sub_office_type2id),
            dict(text_name='party_abbrev', id_name='party_id', values=self.code_maps.party_abbrev2id),
            dict(text_name='person_id', id_name='pid', values=self.person_id2pid),
        ]

    def overload_by_person(
        self, df: pd.DataFrame, *, encoded: bool = True, drop: bool = True, columns: List[str] = None
    ) -> pd.DataFrame:

        columns: list = ['person_id', 'gender_id', 'party_id']

        join_column: str = next((x for x in df.columns if x in ['who', 'person_id']), None)
        join_criterias: dict = dict(left_on=join_column) if join_column else dict(left_index=True)

        xi: pd.DataFrame = df.merge(self.person[columns], right_index=True, how='left', **join_criterias)

        if not encoded and drop:
            xi = self.decode(xi)

        xi = self.as_slim_types(xi)

        return xi

    #     def __getitem__(self, key) -> ParliamentaryRole:

    #         if key is None:
    #             return None

    #         if key not in self.id2person:
    #             logger.warning(f"ID `{key}` not found in parliamentary member/minister index")
    #             self.id2person[key] = self.create_unknown(key=key)

    #         return self.id2person[key]

    #     def __contains__(self, key) -> bool:
    #         return key in self.id2person

    #     def __len__(self) -> int:
    #         return len(self.id2person)

    def get_speaker(self):

        return None


class MemberNotFoundError(ValueError):
    ...


# class ProtoMetaData:


#     def __init__(self, *, members: pd.DataFrame, verbose: bool = False):

#         self.members: pd.DataFrame = members if isinstance(members, pd.DataFrame) else self.load_members(members)
#         self.members['party_abbrev'] = self.members['party_abbrev'].fillna('unknown')

#         if verbose:
#             logger.info(f"size of mop's metadata: {size_of(self.members, 'MB', total=True)}")

#     def map_id2text_names(self, id_names: List[str]) -> List[str]:
#         return [MEMBER_IDNAME2NAME_MAPPING.get(x) for x in id_names]

#     def get_member(self, who: str) -> dict:
#         try:
#             return self.members.loc[who].to_dict()
#         except:
#             MemberNotFoundError(f"ID={who}")


# def unknown_member() -> dict:
#     return dict(
#         id='unknown',
#         role_type='unknown',
#         born=0,
#         chamber=np.nan,
#         district=np.nan,
#         start=0,
#         end=0,
#         gender='unknown',
#         name='unknown',
#         occupation='unknown',
#         party='unknown',
#         party_abbrev='unknown',
#     )


def as_slim_types(df: pd.DataFrame, columns: List[str], dtype: np.dtype) -> pd.DataFrame:
    if df is None:
        return None
    if isinstance(columns, str):
        columns = [columns]
    for column in columns:
        if column in df.columns:
            df[column] = df[column].fillna(0).astype(dtype)
    return df
