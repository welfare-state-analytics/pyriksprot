from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field, fields
from functools import cached_property
from loguru import logger

import pandas as pd

from . import codecs
from . import utterance
from . import utility as mdu

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
    # pid: int
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
            for x in [p for p in self.alt_parties if p.start_year > 0 and p.end_year > 0]:
                if x.start_year <= year <= x.end_year:
                    return x
            for x in [p for p in self.alt_parties if p.start_year == 0]:
                if x.start_year <= year <= (x.end_year or 9999):
                    return x
            """Questionable rule"""
            return self.alt_parties[-1]
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
    # chamber_id: int = None
    # district: str = None
    # occupation: str = None
    # title: str = None
    property_bag: dict = None


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
            return Person(terms_of_office=terms, alt_parties=alt_parties, **data)
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

        columns: list = ['person_id', 'gender_id', 'party_id']

        join_column: str = next((x for x in df.columns if x in ['who', 'person_id']), None)
        join_criterias: dict = dict(left_on=join_column) if join_column else dict(left_index=True)

        xi: pd.DataFrame = df.merge(self.persons[columns], right_index=True, how='left', **join_criterias)

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
            person_id='unknown',
            name='unknown',
            gender_id=0,
            party_id=0,
            year_of_birth=0,
            year_of_death=0,
            terms_of_office=[],
        )


class SpeekerInfoService:
    def __init__(self, database_filename: str):
        self.database_filename: str = database_filename

    @cached_property
    def utterance_lookup(self) -> utterance.UtteranceLookup:
        return utterance.UtteranceLookup().load(source=self.database_filename)

    @cached_property
    def person_index(self) -> PersonIndex:
        return PersonIndex(self.database_filename).load()

    def get_speaker_info(self, protocol_id: int, u_id: str, person_id: str) -> dict[str, SpeakerInfo]:

        person = self.person_index[person_id]
        gender_id: int = person.gender_id
        party_id: int = person.party_id
        year: int = self.utterance_lookup.protocols_lookup(protocol_id)['year']
        if person.is_unknown:
            gender_id = gender_id or self.utterance_lookup.unknown_gender_lookup.get(u_id, 0)
            party_id = party_id or self.utterance_lookup.unknown_party_lookup.get(u_id, 0)
        elif not party_id:
            party_id = person.party_at(year)

        term_of_office: TermOfOffice = person.term_of_office_at(year)
        speaker_info: SpeakerInfo = SpeakerInfo(
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
