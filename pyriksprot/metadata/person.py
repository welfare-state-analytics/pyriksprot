from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from functools import cached_property
from typing import List, Mapping

import numpy as np
import pandas as pd

from . import codecs
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


# pylint: disable=too-many-public-methods
class PersonIndex:
    def __init__(self, data: dict, code_maps: codecs.Codecs):
        self.data: dict = data
        self.code_maps: codecs.Codecs = code_maps

    @staticmethod
    def load(database_filename: str) -> PersonIndex:
        with sqlite3.connect(database=database_filename) as db:

            data = mdu.load_tables(DATA_TABLES, db=db, types=mdu.COLUMN_TYPES, defaults=mdu.COLUMN_DEFAULTS)
            data['persons_of_interest'].rename_axis("pid", inplace=True)
            code_maps: codecs.Codecs = codecs.Codecs.load(source=db, persons=data.get("persons_of_interest"))

            riksprot_metadata: PersonIndex = PersonIndex(data, code_maps)
            return riksprot_metadata

    @property
    def persons(self) -> pd.DataFrame:
        return self.data['persons_of_interest']

    @property
    def terms_of_office(self) -> pd.DataFrame:
        return self.data['terms_of_office']

    @property
    def person_multiple_party(self) -> pd.DataFrame:
        return self.data['person_multiple_party']

    @property
    def person_yearly_party(self) -> pd.DataFrame:
        return self.data['person_yearly_party']

    def get_speaker_info(self, person_id: str, protocol_id: int, u_id: str) -> dict[str, SpeakerInfo]:
        person = self.get_person(person_id)
        gender_id: int = person.gender_id if person.gender_id else self.unknown_utterance_genders.get(u_id, 0)
        party_id: int = 0
        # protocol: self.protcols
        speaker_info: SpeakerInfo = SpeakerInfo(
            person_id=person.person_id,
            name=person.name,
            gender_id=gender_id,
            party_id=party_id,
        )
        return speaker_info

    def get_person(self, x) -> Person:
        raise NotImplementedError()

    @cached_property
    def unknown_utterance_genders(self) -> dict[str, int]:
        return self.data['unknown_utterance_gender']['gender_id'].to_dict()

    @cached_property
    def unknown_utterance_parties(self) -> dict[str, int]:
        return self.data['unknown_utterance_party']['party_id'].to_dict()

    def overload_by_person(
        self, df: pd.DataFrame, *, encoded: bool = True, drop: bool = True, columns: List[str] = None
    ) -> pd.DataFrame:

        columns: list = ['person_id', 'gender_id', 'party_id']

        join_column: str = next((x for x in df.columns if x in ['who', 'person_id']), None)
        join_criterias: dict = dict(left_on=join_column) if join_column else dict(left_index=True)

        xi: pd.DataFrame = df.merge(self.persons[columns], right_index=True, how='left', **join_criterias)

        if not encoded and drop:
            xi = self.code_maps.decode(xi, drop=True)

        mdu.slim_table_types(xi, defaults=mdu.COLUMN_DEFAULTS, types=mdu.COLUMN_TYPES)

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
