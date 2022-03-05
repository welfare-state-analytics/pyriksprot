from __future__ import annotations

import sqlite3
from contextlib import nullcontext
from dataclasses import dataclass
from functools import cached_property
from typing import Callable, List, Literal, Mapping

import pandas as pd

from . import utility as mdu

CODE_TABLES: dict[str, str] = {
    'chamber': 'chamber_id',
    'gender': 'gender_id',
    'office_type': 'office_type_id',
    'sub_office_type': 'sub_office_type_id',
    'government': 'government_id',
    'party': 'party_id',
}


@dataclass
class Codec:
    type: Literal['encoder', 'decoder']
    from_column: str
    to_column: str
    fx: Callable[[int], str]
    default: str = None


class Codecs:
    def __init__(self, **kwargs):
        self.chamber: pd.DataFrame = kwargs.get('chamber')
        self.gender: pd.DataFrame = kwargs.get('gender')
        self.office_type: pd.DataFrame = kwargs.get('office_type')
        self.sub_office_type: pd.DataFrame = kwargs.get('sub_office_type')
        self.government: pd.DataFrame = kwargs.get('government')
        self.party: pd.DataFrame = kwargs.get('party')
        # FIXME: person doesn't belong here
        self.persons: pd.DataFrame = kwargs.get('persons')

    @staticmethod
    def load(source: str | sqlite3.Connection | dict, persons: pd.DataFrame) -> Codecs:
        with (sqlite3.connect(database=source) if isinstance(source, str) else nullcontext(source)) as db:
            code_tables: dict[str, pd.DataFrame] = mdu.load_tables(CODE_TABLES, db=db)
            code_map: Codecs = Codecs(persons=persons, **code_tables)
            return code_map

    @cached_property
    def gender2name(self) -> dict:
        return self.gender['gender'].to_dict()

    @cached_property
    def gender2id(self) -> dict:
        return mdu.revdict(self.gender2name)

    @cached_property
    def office_type2name(self) -> dict:
        return self.office_type['office'].to_dict()

    @cached_property
    def office_type2id(self) -> dict:
        return mdu.revdict(self.office_type2name)

    @cached_property
    def sub_office_type2name(self) -> dict:
        return self.sub_office_type['description'].to_dict()

    @cached_property
    def sub_office_type2id(self) -> dict:
        return mdu.revdict(self.sub_office_type2name)

    @cached_property
    def party_abbrev2name(self) -> dict:
        return self.party['party_abbrev'].to_dict()

    @cached_property
    def party_abbrev2id(self) -> dict:
        return mdu.revdict(self.party_abbrev2name)

    @cached_property
    def pid2person_id(self) -> dict:
        return self.persons['person_id'].to_dict()

    @cached_property
    def person_id2pid(self) -> dict:
        return mdu.revdict(self.pid2person_id)

    @cached_property
    def pid2person_name(self) -> dict:
        return self.persons['name'].to_dict()

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

    @cached_property
    def property_values_specs(self) -> List[Mapping[str, str | Mapping[str, int]]]:
        return [
            dict(text_name='gender', id_name='gender_id', values=self.gender2id),
            dict(text_name='office_type', id_name='office_type_id', values=self.office_type2id),
            dict(text_name='sub_office_type', id_name='sub_office_type_id', values=self.sub_office_type2id),
            dict(text_name='party_abbrev', id_name='party_id', values=self.party_abbrev2id),
            dict(text_name='person_id', id_name='pid', values=self.person_id2pid),
        ]
