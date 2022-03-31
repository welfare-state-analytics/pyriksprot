from __future__ import annotations

import sqlite3
from dataclasses import asdict, dataclass, field, fields
from functools import cached_property

import numpy as np
import pandas as pd
from loguru import logger

from . import codecs
from . import utility as mdu
from . import utterance

DATA_TABLES: dict[str, str] = {
    'persons_of_interest': None,
    'terms_of_office': 'terms_of_office_id',
    'person_multiple_party': 'person_multiple_party_id',
    'person_yearly_party': 'person_yearly_party_id',
    'unknown_utterance_gender': 'u_id',
    'unknown_utterance_party': 'u_id',
}


@dataclass
class TermOfOffice:
    # pid: int
    # person_id: str
    office_type_id: str
    sub_office_type_id: int
    # district_id: int
    start_year: int
    end_year: int
    # start_date: str
    # end_date: str


@dataclass
class PersonParty:
    party_id: str
    start_year: int
    end_year: int


@dataclass
class Person:
    pid: int
    person_id: str
    name: str
    gender_id: int
    party_id: int
    year_of_birth: int
    year_of_death: int
    terms_of_office: list[TermOfOffice] = field(default_factory=list)

    """Alt. parties: only used of person has multiple parties"""
    alt_parties: list[TermOfOffice] = field(default_factory=list)

    def party_at(self, year: int) -> int:
        if self.party_id:
            return self.party_id
        if self.alt_parties and not self.is_unknown:
            """Prioritise items with closed intervals"""
            for x in [p for p in self.alt_parties if p.start_year > 0 and p.end_year > 0]:
                if x.start_year <= year <= x.end_year:
                    return x.party_id
            """Open ended intervals"""
            for x in [p for p in self.alt_parties if p.start_year > 0 or p.end_year > 0]:
                if x.start_year <= year <= (x.end_year or 9999):
                    return x.party_id
            """Questionable rule"""
            return 0  # self.alt_parties[-1]
        return 0

    def term_of_office_at(self, year: int) -> TermOfOffice:
        if self.terms_of_office and not self.is_unknown:
            for o in self.terms_of_office:
                if o.start_year <= year <= (o.end_year or 9999):
                    return o
            """Questionable rule"""
            return self.terms_of_office[-1]
        return TermOfOffice(office_type_id=0, sub_office_type_id=0, start_year=year, end_year=year)

    @property
    def is_unknown(self) -> bool:
        return self.person_id == 'unknown'


@dataclass
class SpeakerInfo:
    speech_id: str
    person_id: str
    name: str
    gender_id: int
    party_id: int
    start_year: int
    end_year: int
    office_type_id: int
    sub_office_type_id: int
    # year_of_birth: int = None
    # year_of_death: int = None
    # district_id: int = None

    def asdict(self) -> dict:
        return asdict(self)

    def to_tuple(self) -> tuple:
        return [getattr(self, name) for name in self.columns]

    @cached_property
    def columns(self) -> list[str]:
        return [f.name for f in fields(self)]

    def to_dict(self) -> dict:
        return {
            'gender_id': self.gender_id,
            'party_id': self.party_id,
            'office_type_id': self.office_type_id,
            'sub_office_type_id': self.sub_office_type_id,
            # 'person_id': self.person_id,
        }

    @staticmethod
    def dtypes() -> dict:
        return {
            # 'speech_id': str,
            # 'person_id': str,
            # 'name': str,
            'gender_id': np.int8,
            'party_id': np.int8,
            'start_year': np.int16,
            'end_year': np.int16,
            'office_type_id': np.int8,
            'sub_office_type_id': np.int8,
        }


def swap_rows(df: pd.DataFrame, i: int, j: int):
    row_i, row_j = df.iloc[i].copy(), df.iloc[j].copy()
    df.iloc[j], df.iloc[i] = row_i, row_j


def index_of_person_id(df: pd.DataFrame, person_id: str) -> int:
    return df.index[df['person_id'] == person_id].tolist()[0]


# pylint: disable=too-many-public-methods
class PersonIndex:
    def __init__(self, database_filename: str):
        self.database_filename: str = database_filename
        self.data: dict = None

    def load(self) -> PersonIndex:
        with sqlite3.connect(database=self.database_filename) as db:
            self.data = mdu.load_tables(DATA_TABLES, db=db)
            self.data['persons_of_interest'].rename_axis("pid", inplace=True)
            """ ensure `unknown` has pid = 0 """
            if self.persons.loc[0]['person_id'] != 'unknown':
                swap_rows(self.persons, 0, index_of_person_id(self.persons, 'unknown'))
        return self

    @cached_property
    def lookups(self) -> codecs.Codecs:
        data: codecs.Codecs = codecs.Codecs().load(source=self.database_filename)
        if self.data:
            data.extra_codecs = self.codecs
        return data

    @cached_property
    def pid2person_id(self) -> dict:
        return self.persons['person_id'].to_dict()

    @cached_property
    def person_id2pid(self) -> dict:
        return mdu.revdict(self.pid2person_id)

    @cached_property
    def pid2person_name(self) -> dict:
        return self.persons['name'].to_dict()

    @property
    def persons(self) -> pd.DataFrame:
        return self.data['persons_of_interest']

    @property
    def terms_of_office(self) -> pd.DataFrame:
        return self.data['terms_of_office']

    @cached_property
    def person_lookup(self) -> dict[str, Person]:
        return {person_id: self.get_person(person_id) for person_id in self.person_id2pid}

    def get_person(self, person_id: str | int) -> Person:
        try:
            pid: int = person_id if isinstance(person_id, int) else self.person_id2pid.get(person_id)
            data: dict = self.persons.loc[pid].to_dict()
            terms: list[TermOfOffice] = self.terms_of_office_lookup.get(data['person_id'])
            alt_parties: list[PersonParty] = self.person_multiple_party_lookup.get(data['person_id'])
            return Person(pid=pid, terms_of_office=terms, alt_parties=alt_parties, **data)
        except Exception as ex:
            logger.info(f"{type(ex).__name__}: {ex}")
            raise

    def __getitem__(self, key: int | str) -> Person:
        person_id: str = key if isinstance(key, str) else self.pid2person_id.get(key)
        return self.person_lookup.get(person_id)

    def __contains__(self, key: int | str) -> bool:
        if isinstance(key, int):
            return key in self.pid2person_id
        return key in self.person_id2pid

    def __len__(self) -> int:
        return len(self.person_id2pid)

    @cached_property
    def terms_of_office_lookup(self) -> dict:
        """Builds a person_id to person's terms-of-offices dict """
        lookup: dict = mdu.group_to_list_of_records(
            df=self.terms_of_office,
            key='person_id',
            properties=[f.name for f in fields(TermOfOffice)],
            ctor=TermOfOffice,
        )
        return lookup

    @cached_property
    def person_multiple_party_lookup(self) -> dict:
        """Builds a person_id to person's parties lookup, only for person """
        lookup: dict = mdu.group_to_list_of_records(
            df=self.person_multiple_party,
            key='person_id',
            properties=[f.name for f in fields(PersonParty)],
            ctor=PersonParty,
        )
        return lookup

    @property
    def person_multiple_party(self) -> pd.DataFrame:
        return self.data['person_multiple_party']

    def overload_by_person(
        self, df: pd.DataFrame, *, encoded: bool = True, drop: bool = True, columns: list[str] = None
    ) -> pd.DataFrame:

        persons: pd.DataFrame = self.persons
        fg = self.person_id2pid.get

        join_column: str = next((x for x in df.columns if x in ['who', 'person_id']), None)
        join_criterias = dict(left_on='pid')

        if 'pid' not in df.columns and join_column:
            df['pid'] = df[join_column].apply(fg)
        else:
            if not np.issubtype(df.index.dtype, np.integer):
                """assume index is person_id"""
                df['pid'] = pd.Series(df.index).apply(fg)
            else:
                """assume pid is index"""
                join_criterias = dict(left_index=True)

        columns: list = ['gender_id', 'party_id']

        xi: pd.DataFrame = df.merge(persons[columns], right_index=True, how='left', **join_criterias)

        if not encoded and drop:
            xi = self.lookups.decode(xi, drop=True)

        mdu.slim_table_types(xi, defaults=mdu.COLUMN_DEFAULTS, types=mdu.COLUMN_TYPES)

        return xi

    @cached_property
    def codecs(self) -> list[codecs.Codec]:
        return [
            codecs.Codec('decode', 'pid', 'person_id', self.pid2person_id.get),
            codecs.Codec('encode', 'person_id', 'pid', self.person_id2pid.get),
            codecs.Codec('encode', 'who', 'pid', self.person_id2pid.get),
        ]

    @cached_property
    def property_values_specs(self) -> list[dict[str, str | dict[str, int]]]:
        return self.lookups.property_values_specs + [
            dict(text_name='person_id', id_name='pid', values=self.person_id2pid),
        ]

    def unknown_person(self) -> Person:
        return Person(
            pid=0,
            person_id='unknown',
            name='unknown',
            gender_id=0,
            party_id=0,
            year_of_birth=0,
            year_of_death=0,
            terms_of_office=[],
        )


class SpeakerInfoService:
    def __init__(self, database_filename: str, **kwargs):
        self.database_filename: str = database_filename
        self.kwargs: dict = kwargs

    @cached_property
    def utterance_index(self) -> utterance.UtteranceIndex:
        return self.kwargs.get('utterance_lookup') or utterance.UtteranceIndex().load(source=self.database_filename)

    @cached_property
    def person_index(self) -> PersonIndex:
        return self.kwargs.get('person_index') or PersonIndex(self.database_filename).load()

    def get_speaker_info(self, *, u_id: str, person_id: str = None, year: int = None) -> dict[str, SpeakerInfo]:

        if person_id is None or year is None:
            try:
                uttr: pd.Series = self.utterance_index.utterances.loc[u_id]
                person_id = uttr.person_id
                year: int = self.utterance_index.protocol(uttr.document_id).year
            except Exception as ex:
                logger.info(f"{ex}: {u_id}")

        person = self.person_index[person_id]
        gender_id: int = person.gender_id
        party_id: int = person.party_id
        if person.is_unknown:
            gender_id = gender_id or self.utterance_index.unknown_gender_lookup.get(u_id, 0)
            party_id = party_id or self.utterance_index.unknown_party_lookup.get(u_id, 0)
        elif not party_id:
            party_id = person.party_at(year)

        term_of_office: TermOfOffice = person.term_of_office_at(year)
        speaker_info: SpeakerInfo = SpeakerInfo(
            speech_id=u_id,
            person_id=person.person_id,
            name=person.name,
            gender_id=gender_id,
            party_id=party_id,
            office_type_id=term_of_office.office_type_id,
            sub_office_type_id=term_of_office.sub_office_type_id,
            start_year=term_of_office.start_year,
            end_year=term_of_office.end_year,
        )
        return speaker_info

    def store(self, target_filename: str, speakers: list[SpeakerInfo]) -> None:
        speaker_infos: pd.DataFrame = pd.DataFrame(data=[s.asdict() for s in speakers])
        speaker_infos.to_csv(
            mdu.replace_extension(target_filename, "zip"),
            sep='\t',
            compression=dict(method='zip', archive_name="speaker_index.csv"),
            header=True,
        )
