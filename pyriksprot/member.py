"""
Index of members of the parliament.
"""
import itertools
from os.path import isdir, isfile, join
from typing import Literal

import pandas as pd
from loguru import logger

# pylint: disable=no-member, too-many-instance-attributes


def github_uri(name: str, branch: str) -> str:
    return f'https://raw.githubusercontent.com/welfare-state-analytics/riksdagen-corpus/{branch}/corpus/{name}.csv'


def persons_uri(*, source: str, name: str, branch: str = 'main') -> str:
    if source and isfile(source):
        return source
    if source and isfile(join(source, name)):
        return join(source, name)
    return github_uri(name, branch=branch)


def members_of_parliament_url(source: str = None, branch: str = 'main') -> str:
    return persons_uri(source=source, name='members_of_parliament', branch=branch)


def ministers_url(source: str = None, branch: str = 'main') -> str:
    return persons_uri(source=source, name='ministers', branch=branch)


def speakers_url(source: str = None, branch: str = 'main') -> str:
    return persons_uri(source=source, name='talman', branch=branch)


class ParliamentaryRole:
    def __init__(
        self,
        *,
        id: str,  # pylint: disable=redefined-builtin
        role_type: Literal['member', 'speaker', 'minister', 'unknown'],
        name: str,
        chamber: str = None,
        start: int = None,
        end: int = None,
        gender: str = None,
        party: str = None,
        party_abbrev: str = None,
        born: str = None,
        loc: str = None,
        district: str = None,
        occupation: str = None,
        specifier: str = None,
        title: str = None,
        **kwargs,  # pylint: disable=unused-argument
    ):
        self.id: str = id
        self.role_type = role_type
        self.name: str = name
        self.chamber: str = chamber
        self.start: int = start
        self.end: int = end
        self.gender: str = gender
        self.party: str = party
        self.party_abbrev: str = party_abbrev
        self.born: str = born
        self.loc: str = loc
        self.district: str = district
        self.occupation: str = occupation
        self.specifier: str = specifier
        self.title: str = title

        self.property_bag: dict = kwargs
        # self.start_manual: int = start_manual
        # self.end_manual: int = end_manual
        # self.riksdagen_id: str = riksdagen_id
        # self.twittername: str = twittername


# class ParliamentaryMember(ParliamentaryRole):
#     def __init__(self):
#         self.gender = "unknown"
#         self.party = "government"
#         self.party_abbrev: str = "gov"
#         # self.born: str = None


# class Minister(ParliamentaryRole):
#     def __post__init(self):
#         self.chamber: str = "n/a"


class ParliamentaryMemberIndex:
    """
    Repository for  members of the parliament.
    """

    def create_unknown(self, key: str):
        return ParliamentaryRole(
            id=key,
            role_type="unknown",
            name="unknown",
            party="Unknown",
            party_abbrev="",
            gender="",
        )

    def to_dataframe(self):
        return pd.DataFrame(data=[x.__dict__ for x in self.individuals.values()]).set_index('id',drop=False).rename_axis('').drop(columns=['property_bag'])

    def __init__(self, *, source_folder: str = None, branch: str = 'main'):

        if source_folder and isinstance(source_folder, str):
            if not isdir(source_folder):
                raise ValueError("argument `source_folder` must be an existing folder")

        self.members: pd.DataFrame = self.load_members(members_of_parliament_url(source=source_folder, branch=branch))
        self.party_abbrevs: dict = (
            self.members[~self.members.party_abbrev.isna()]
            .groupby(['party'])
            .agg({'party_abbrev': lambda x: list(set(x))[0]})
            .party_abbrev.to_dict()
        )
        self.ministers: pd.DataFrame = self.load_ministers(
            ministers_url(source=source_folder, branch=branch), party_abbrevs=self.party_abbrevs
        )
        self.speakers: pd.DataFrame = self.load_speakers(
            speakers_url(source=source_folder, branch=branch), party_abbrevs=self.party_abbrevs
        )

        self.individuals: dict = {
            meta['id']: ParliamentaryRole(role_type=role_type, **meta)
            for role_type, meta in itertools.chain(
                (("member", meta) for meta in self.members.to_dict('records')),
                (("minister", meta) for meta in self.ministers.to_dict('records')),
                (("speaker", meta) for meta in self.speakers.to_dict('records')),
            )
        }

        if 'unknown' not in self.individuals:
            self.individuals['unknown'] = self.create_unknown('unknown')

        self.parties = self.members.party.unique()
        self.chambers = self.members.chamber.unique()

    @staticmethod
    def load_members(source: str) -> pd.DataFrame:

        persons: pd.DataFrame = pd.read_csv(source).set_index('id', drop=False).rename_axis('')

        if len(persons.id) != len(persons.id.unique()):
            duplicates: str = ', '.join(persons[persons.index.duplicated()].id.tolist())
            logger.warning(f"Parliamentary ID is not unique ({duplicates})")

        persons = persons.assign(id=persons.id.fillna('unknown'))

        persons['party'] = persons.party.fillna('Unknown')
        persons['gender'] = persons.gender.fillna('unknown')
        persons.loc[persons.gender == '', 'gender'] = 'unknown'
        persons['end'] = persons.end.fillna('')

        return persons

    @staticmethod
    def load_ministers(source: str, party_abbrevs: dict) -> pd.DataFrame:
        first = lambda x: list(x)[0]
        unique_str = lambda x: ' '.join(set(x))

        persons: pd.DataFrame = pd.read_csv(source)

        persons = persons.assign(id=persons.id.fillna('unknown'))

        if 'party' in persons.columns:
            if 'party_abbrev' not in persons.columns:
                persons['party_abbrev'] = persons.party.apply(party_abbrevs.get)
            persons['party'] = persons.party.fillna('Unknown')
            persons['party_abbrev'] = persons.party.fillna('?')

        else:
            persons['party'] = 'government'
            persons['party_abbrev'] = 'gov'

        if 'gender' in persons.columns:
            persons['gender'] = persons.gender.fillna('unknown')
            persons.loc[persons.gender == '', 'gender'] = 'unknown'
        else:
            persons['gender'] = 'unknown'

        persons['start'] = persons.start.fillna('')
        persons['end'] = persons.end.fillna('')

        persons = (
            persons.groupby(['id'])
            .agg(
                {
                    'name': first,
                    'party': first,
                    'party_abbrev': first,
                    'title': unique_str,
                    # 'chamber': unique_str,
                    # 'loc': unique_str,
                    'start': ','.join,
                    'end': ','.join,
                }
            )
            .reset_index()
            .set_index('id', drop=False)
            .rename_axis('')
        )

        return persons

    @staticmethod
    def load_speakers(source: str, party_abbrevs: dict) -> pd.DataFrame:

        first = lambda x: list(x)[0]
        unique_str = lambda x: ' '.join(set(x))

        persons: pd.DataFrame = pd.read_csv(source)

        persons = persons.assign(id=persons.id.fillna('unknown'))

        persons['party_abbrev'] = persons.party.apply(party_abbrevs.get).fillna('')
        persons['party'] = persons.party.fillna('Unknown')
        persons['name'] = persons.name.fillna('unknown')
        persons['chamber'] = persons.chamber.fillna('')
        persons['loc'] = persons['loc'].fillna('')

        if 'gender' in persons.columns:
            persons['gender'] = persons.gender.fillna('unknown')
            persons.loc[persons.gender == '', 'gender'] = 'unknown'
        else:
            persons['gender'] = 'unknown'

        if 'titel' in persons.columns:
            persons['title'] = persons.titel
            persons.drop(columns='titel', inplace=True)

        persons['title'] = persons.title.fillna('Talman')

        persons = (
            persons.groupby(['id'])
            .agg(
                {
                    'name': first,
                    'party': first,
                    'party_abbrev': first,
                    'title': unique_str,
                    'chamber': unique_str,
                    'loc': unique_str,
                    'start': list,
                    'end': list,
                }
            )
            .reset_index()
            .set_index('id', drop=False)
            .rename_axis('')
        )
        return persons

    def __getitem__(self, key) -> ParliamentaryRole:

        if key is None:
            return None

        if key not in self.individuals:
            logger.warning(f"ID `{key}` not found in parliamentary member/minister index")
            self.individuals[key] = self.create_unknown(key=key)

        return self.individuals[key]

    def __contains__(self, key) -> bool:
        return key in self.individuals

    def __len__(self) -> int:
        return len(self.individuals)
