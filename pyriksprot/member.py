"""
Index of members of the parliament.
"""
import json
from datetime import date
from os.path import isdir, isfile, join
from typing import Any, List, Literal

import pandas as pd
from loguru import logger

# pylint: disable=no-member, too-many-instance-attributes

# |              | member    | member sk | speakers | ministers | suppleants  | process        |
# |--------------|-----------|-----------|----------|-----------|-------------|----------------|
# | id           | X         | X         | X        | X         | X           |                |
# | born         | X         | X         |          |           |             | to year        |
# | chamber      | X         | X         | X        |           | X           |                |
# | district     | X         | X         | (X)      |           | X           |                |
# | loc          |           |           | X        |           |             | district       |
# | municipality |           | X         |          |           |             | district       |
# | start        | X         | X         | X        | X         | X           | to year        |
# | end          | X         | X         | X        | X         | X           | to year        |
# | gender       | X         | X         |          |           | X           |                |
# | name         | X         | X         | X        | X         | X           |                |
# | occupation   | X         |           |          |           |             |                |
# | party        | X         | X         | X        |           | X           | X ⇢ rule?      |
# | party_abbrev | X         | X         | (X)      |           | X           |                |
# | specifier    | X         |           |          | X         |             | skip           |
# | twittername  | X         |           |          |           |             | skip           |
# | year         |           | X         |          |           |             | skip           |
# | start_manual |           |           | X        |           |             | skip           |
# | end_manual   |           |           | X        |           |             | skip           |
# | title        | "ledamot" | "ledamot" | (X)      | X         | "suppleant" | titel => title |
# | decade       |           | X         |          |           |             | skip           |
# | riksdagen_id | X         |           |          |           |             | skip           |
# | replacer     |           |           |          |           | X           | skip           |
# | note         |           |           |          |           | X           | skip           |


def github_uri(*, name: str, tag: str) -> str:
    return f'https://raw.githubusercontent.com/welfare-state-analytics/riksdagen-corpus/{tag}/corpus/{name}.csv'


def metadata_uri(*, source: str, name: str, tag: str) -> str:
    """Returns URI to `name` entities databases."""

    if source:
        for filename in [source, join(source, name), join(source, f'{name}.csv'), join(source, f'{name}.json')]:
            if isfile(filename):
                return filename

    if tag is None:
        raise ValueError(f"{name} not found: no git tag specified")

    logger.warning(f"{name} not found: falling back to git tag {tag}")
    return github_uri(name=name, tag=tag)


def ministers_url(*, source: str, tag: str) -> str:
    return metadata_uri(source=source, name='ministers', tag=tag)


def speakers_url(*, source: str, tag: str) -> str:
    return metadata_uri(source=source, name='talman', tag=tag)


def first(x: List[Any]) -> Any:
    return list(x)[0]


def last(x: List[Any]) -> Any:
    return list(x)[-1]


def unique_str(x: List[Any]) -> str:
    return ' '.join(set(str(y) for y in x))


PERSON_COLUMNS = [
    'role_type',
    'born',
    'chamber',
    'district',
    'start',
    'end',
    'gender',
    'name',
    'occupation',
    'party',
    'party_abbrev',
]


class ParliamentaryRole:
    def __init__(
        self,
        *,
        id: str,  # pylint: disable=redefined-builtin
        role_type: Literal['member', 'speaker', 'minister', 'suppleant', 'unknown'],
        name: str,
        chamber: str = None,
        start: int = None,
        end: int = None,
        gender: str = None,
        party: str = None,
        party_abbrev: str = None,
        born: str = None,
        district: str = None,
        occupation: str = None,
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
        self.district: str = district
        self.occupation: str = occupation
        self.title: str = title

        self.property_bag: dict = kwargs


def fill_unknown2(persons: pd.DataFrame, data: dict) -> None:

    for col in data.keys():

        col_opts: dict = data[col] if isinstance(data[col], dict) else dict(filler=data[col])

        filler: Any = col_opts.get('filler', 'unknown')

        if col not in persons.columns:
            persons[col] = filler
        else:
            persons[col].fillna(filler, inplace=True)

        if col_opts.get('extra_na') is not None:
            persons.loc[persons[col] == col_opts.get('extra_na'), col] = filler

        if col_opts.get('dtype') is not None:
            persons[col] = persons[col].astype(dtype=col_opts.get('dtype'))

    return persons


def date_to_year(persons: pd.DataFrame, cols: List[str], filler: int = 0):
    def to_year(x) -> int:
        if isinstance(x, int):
            return x
        if isinstance(x, date):
            return x.year
        if isinstance(x, str):
            if not x:
                return filler
            if len(x) < 4:
                return filler
            return int(x[:4])
        return filler

    for col in cols:
        if col not in persons.columns:
            persons[col] = filler
            continue
        persons[col] = persons[col].apply(to_year)
        persons[col].fillna(filler, inplace=True)
        persons[col] = persons[col].astype(int)

    return persons


def fill_party_abbrev(persons: pd.DataFrame, party_abbrevs: dict, filler: str):
    if 'party_abbrev' not in persons.columns and party_abbrevs is not None:
        persons['party_abbrev'] = persons.party.apply(lambda x: party_abbrevs.get(x, filler))
    persons['party_abbrev'].fillna(filler, inplace=True)
    return persons


class ParliamentaryMemberIndex:
    """
    Repository for  members of the parliament.
    """

    def __init__(self, *, source: str, tag: str = None):

        if source is None and tag is None:
            raise ValueError("git fallback not allowed without tag specified tag")

        if isinstance(source, str):
            if not isdir(source):
                raise ValueError("argument `source_folder` must be an existing folder")

        self.party_abbrevs: dict = self.load_party_abbrevs(source=source, tag=tag)

        self.data = {
            'members': self.load_members(source=source, tag=tag),
            'members_sk': self.load_members(
                source=source, tag=tag, party_abbrevs=self.party_abbrevs, name='members_of_parliament_sk'
            ),
            'suppleants': self.load_suppleants(source=source, tag=tag),
            'ministers': self.load_ministers(source=source, party_abbrevs=self.party_abbrevs, tag=tag),
            'speakers': self.load_speakers(source=source, party_abbrevs=self.party_abbrevs, tag=tag),
        }

        self.persons: pd.DataFrame = pd.concat([self.members_of_parliament, self.ministers, self.speakers])

        self.persons = pd.concat(
            [self.persons, self.members_of_parliament_sk[~self.members_of_parliament_sk.index.isin(self.persons.index)]]
        ).pipe(date_to_year, ['born', 'start', 'end'], filler=0)

        self.id2person: dict = {
            meta['id']: ParliamentaryRole(**meta) for meta in self.persons.reset_index().to_dict('records')
        }

        if 'unknown' not in self.id2person:
            self.id2person['unknown'] = self.create_unknown('unknown')

        self.chambers: List[str] = self.persons.chamber.unique().tolist()
        self.parties: List[str] = self.persons.party.unique().tolist()
        self.party_abbrevs: List[str] = self.persons.chamber.unique().tolist()
        self.genders: List[str] = self.persons.gender.unique().tolist()

    @property
    def members_of_parliament(self) -> pd.DataFrame:
        return self.data['members']

    @property
    def members_of_parliament_sk(self) -> pd.DataFrame:
        return self.data['members_sk']

    @property
    def ministers(self) -> pd.DataFrame:
        return self.data['ministers']

    @property
    def speakers(self) -> pd.DataFrame:
        return self.data['speakers']

    @staticmethod
    def load_party_abbrevs(*, source: str, tag: str = None) -> pd.DataFrame:
        try:
            uri: str = metadata_uri(source=source, name='party_mapping', tag=tag)
            if isfile(uri):
                with open(uri, 'r', encoding='utf-8') as fp:
                    return json.load(fp)
        except:  # pylint: disable=bare-except
            ...

        return (
            pd.read_csv(metadata_uri(source=source, name='members_of_parliament', tag=tag), sep=',')
            .groupby('party')
            .agg({'party_abbrev': last})
            .party_abbrev.to_dict()
        )

    @staticmethod
    def load_members(
        *, source: str, tag: str = None, party_abbrevs: dict = None, name: str = 'members_of_parliament'
    ) -> pd.DataFrame:
        persons: pd.DataFrame = (
            pd.read_csv(metadata_uri(source=source, name=name, tag=tag))
            .set_index('id')
            .assign(
                role_type='member',
                title='Riksdagsledamot',
            )
            .pipe(
                fill_unknown2,
                data={
                    'party': 'unknown',
                    'gender': dict(filler='unknown', extra_na=''),
                    'occupation': dict(filler='', extra_na=''),
                },
            )
            .pipe(fill_party_abbrev, party_abbrevs, filler='gov')
        )
        return persons[PERSON_COLUMNS]

    @staticmethod
    def load_suppleants(*, source: str, tag: str = None, name: str = 'suppleants') -> pd.DataFrame:
        persons: pd.DataFrame = (
            pd.read_csv(metadata_uri(source=source, name=name, tag=tag), sep=',', index_col='id')
            .assign(occupation='', role_type='suppleant', title='Riksdagsledamot', born=0)
            .pipe(date_to_year, ['born', 'start', 'end'], filler=0)
            .pipe(
                fill_unknown2,
                data={
                    'party': dict(filler='unknown'),
                    'gender': dict(filler='unknown', extra_na=''),
                    'born': 0,
                },
            )
        )
        return persons[PERSON_COLUMNS]

    @staticmethod
    def load_ministers(*, source: str, party_abbrevs: dict, tag: str = None) -> pd.DataFrame:

        persons: pd.DataFrame = pd.read_csv(metadata_uri(source=source, name='ministers', tag=tag), sep=',').assign(
            role_type='minister',
        )

        agg_opts = {
            **{key: first for key in PERSON_COLUMNS if key in persons.columns},
            **{'end': last, 'title': unique_str},
        }

        persons = (
            persons.groupby(['id'])
            .agg(agg_opts)
            .pipe(
                fill_unknown2,
                data={
                    'party': 'government',
                    'gender': dict(filler='unknown', extra_na=''),
                    'chamber': 'gov',
                    'district': '',
                    'occupation': '',
                    'born': 0,
                },
            )
            .pipe(fill_party_abbrev, party_abbrevs, filler='gov')
        )

        return persons[PERSON_COLUMNS]

    @staticmethod
    def load_speakers(*, source: str, party_abbrevs: dict, tag: str = None) -> pd.DataFrame:

        persons: pd.DataFrame = (
            pd.read_csv(metadata_uri(source=source, name='talman', tag=tag), sep=',')
            .rename(columns={'titel': 'title', 'loc': 'district'}, errors='ignore')
            .assign(
                role_type='talman',
            )
        )

        agg_opts = {
            **{key: first for key in PERSON_COLUMNS if key in persons.columns},
            **{'end': last, 'title': unique_str},
        }

        persons = (
            persons.groupby(['id'])
            .agg(agg_opts)
            .pipe(
                fill_unknown2,
                data={
                    'party': dict(filler='unknown', extra_na=''),
                    'gender': dict(filler='unknown', extra_na=''),
                    'name': dict(filler='unknown', extra_na=''),
                    'chamber': 'unknown',
                    'district': '',
                    'occupation': '',
                    'born': 0,
                    'title': 'talman',
                },
            )
            .pipe(fill_party_abbrev, party_abbrevs, filler='')
        )

        return persons[PERSON_COLUMNS]

    def __getitem__(self, key) -> ParliamentaryRole:

        if key is None:
            return None

        if key not in self.id2person:
            logger.warning(f"ID `{key}` not found in parliamentary member/minister index")
            self.id2person[key] = self.create_unknown(key=key)

        return self.id2person[key]

    def __contains__(self, key) -> bool:
        return key in self.id2person

    def __len__(self) -> int:
        return len(self.id2person)

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
        return (
            pd.DataFrame(data=[x.__dict__ for x in self.id2person.values()])
            .set_index('id', drop=False)
            .rename_axis('')
            .drop(columns=['property_bag'])
        )
