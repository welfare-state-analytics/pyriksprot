from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from os.path import isfile
from typing import Any, Callable, Literal, Mapping

import pandas as pd

from pyriksprot.metadata import database

from .. import utility as pu

CODE_TABLES: dict[str, str] = {
    'chamber': 'chamber_id',
    'gender': 'gender_id',
    'government': 'government_id',
    'office_type': 'office_type_id',
    'party': 'party_id',
    'sub_office_type': 'sub_office_type_id',
    'protocols': 'document_name',
}


@dataclass
class Codec:
    type: Literal['encode', 'decode']
    from_column: str
    to_column: str
    fx: Callable[[int | str], int | str] | dict[str | int, str | int]
    default: str = None

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.from_column in df.columns:
            if self.to_column not in df:
                if isinstance(self.fx, dict):
                    df[self.to_column] = df[self.from_column].map(self.fx)
                else:
                    df[self.to_column] = df[self.from_column].apply(self.fx)
            if self.default is not None:
                df[self.to_column] = df[self.to_column].fillna(self.default)
        return df

    def apply_scalar(self, value: int | str, default: Any) -> str | int:
        if isinstance(self.fx, dict):
            return self.fx.get(value, default or self.default)  # type: ignore
        return self.fx(value)


null_frame: pd.DataFrame = pd.DataFrame()


class Codecs:
    def __init__(self):
        self.chamber: pd.DataFrame = null_frame
        self.gender: pd.DataFrame = null_frame
        self.government: pd.DataFrame = null_frame
        self.office_type: pd.DataFrame = null_frame
        self.party: pd.DataFrame = null_frame
        self.sub_office_type: pd.DataFrame = null_frame
        self.protocols: pd.DataFrame = null_frame
        self.extra_codecs: list[Codec] = []
        self.source_filename: str | None = None
        self.code_tables: dict[str, str] = CODE_TABLES

    def load(self, source: str | dict) -> Codecs:
        self.source_filename = source if isinstance(source, str) else None
        if not isfile(source):
            raise FileNotFoundError(f"File not found: {source}")

        db: database.DatabaseInterface = database.DefaultDatabaseType(filename=source)

        with db:
            tables: dict[str, pd.DataFrame] = db.fetch_tables(self.code_tables)
            for table_name, table in tables.items():
                setattr(self, table_name, table)
        return self

    def tablenames(self) -> dict[str, str]:
        """Returns a mapping from code table name to id column name"""
        return CODE_TABLES

    @cached_property
    def gender2name(self) -> dict:
        return self.gender['gender'].to_dict()

    @cached_property
    def gender2abbrev(self) -> dict:
        return self.gender['gender_abbrev'].to_dict()

    @cached_property
    def gender2id(self) -> dict:
        return pu.revdict(self.gender2name)

    @cached_property
    def office_type2name(self) -> dict:
        return self.office_type['office'].to_dict()

    @cached_property
    def office_type2id(self) -> dict:
        return pu.revdict(self.office_type2name)

    @cached_property
    def sub_office_type2name(self) -> dict:
        return self.sub_office_type['description'].to_dict()

    @cached_property
    def sub_office_type2id(self) -> dict:
        return pu.revdict(self.sub_office_type2name)

    @cached_property
    def protocol_name2chamber_abbrev(self) -> dict:
        return self.protocols['chamber_abbrev'].to_dict()

    @cached_property
    def party_abbrev2name(self) -> dict:
        return self.party['party_abbrev'].to_dict()

    @cached_property
    def party_abbrev2id(self) -> dict:
        return pu.revdict(self.party_abbrev2name)

    @property
    def codecs(self) -> list[Codec]:
        return self.extra_codecs + [
            Codec("decode", "gender_id", "gender", self.gender2name),
            Codec("decode", "gender_id", "gender_abbrev", self.gender2abbrev),
            Codec("decode", "office_type_id", "office_type", self.office_type2name),
            Codec("decode", "party_id", "party_abbrev", self.party_abbrev2name),
            Codec("decode", "sub_office_type_id", "sub_office_type", self.sub_office_type2name),
            Codec("decode", "document_name", "chamber_abbrev", self.protocol_name2chamber_abbrev),
            Codec("encode", "gender", "gender_id", self.gender2id),
            Codec("encode", "office_type", "office_type_id", self.office_type2id),
            Codec("encode", "party", "party_id", self.party_abbrev2id),
            Codec("encode", "sub_office_type", "sub_office_type_id", self.sub_office_type2id),
        ]

    def decode_any_id(self, from_name: str, value: int, *, default_value: str = "unknown", to_name: str = None) -> str:
        codec: Codec | None = self.decoder(from_name, to_name)
        if codec is None:
            return default_value
        return str(codec.apply_scalar(value, default_value))

    def decoder(self, from_name: str, to_name: str = None) -> Codec | None:
        for codec in self.decoders:
            if codec.from_column == from_name and (to_name is None or codec.to_column == to_name):
                return codec
        return None

    # def encoder(self, key: str) -> Codec | None:
    #     return next((x for x in self.encoders if x.from_column == key), lambda _: 0)

    @property
    def decoders(self) -> list[Codec]:
        return [c for c in self.codecs if c.type == 'decode']

    @property
    def encoders(self) -> list[Codec]:
        return [c for c in self.codecs if c.type == 'encode']

    def apply_codec(
        self, df: pd.DataFrame, codecs: list[Codec], drop: bool = True, keeps: list[str] = None
    ) -> pd.DataFrame:
        for codec in codecs:
            df = codec.apply(df)

        if drop:
            for codec in codecs:
                if keeps and codec.from_column in keeps:
                    continue
                df.drop(columns=[codec.from_column], inplace=True, errors='ignore')

        return df

    def decode(self, df: pd.DataFrame, drop: bool = True, keeps: list[str] = None) -> pd.DataFrame:
        return self.apply_codec(df, self.decoders, drop=drop, keeps=keeps)

    def encode(self, df: pd.DataFrame, drop: bool = True, keeps: list[str] = None) -> pd.DataFrame:
        return self.apply_codec(df, self.encoders, drop=drop, keeps=keeps)

    @cached_property
    def property_values_specs(self) -> list[Mapping[str, str | Mapping[str, int]]]:
        return [
            dict(text_name='gender', id_name='gender_id', values=self.gender2id),
            dict(text_name='office_type', id_name='office_type_id', values=self.office_type2id),
            dict(text_name='party_abbrev', id_name='party_id', values=self.party_abbrev2id),
            dict(text_name='sub_office_type', id_name='sub_office_type_id', values=self.sub_office_type2id),
        ]
