"""
Index of members of the parliament.
"""

from dataclasses import dataclass

import pandas as pd
from loguru import logger


def members_of_parliament_url(branch: str = 'main') -> str:
    return f'https://raw.githubusercontent.com/welfare-state-analytics/riksdagen-corpus/{branch}/corpus/members_of_parliament.csv'


@dataclass
class ParliamentaryMember:
    id: str
    name: str
    party: str
    gender: str
    start: int = None
    end: int = None
    chamber: str = None
    district: str = None
    occupation: str = None
    specifier: str = None
    riksdagen_id: str = None
    born: str = None
    twittername: str = None
    party_abbrev: str = None


class ParliamentaryMemberIndex:
    """
    Repository for  members of the parliament.
    """

    unknown = ParliamentaryMember(
        id="unknown",
        name="unknown",
        party="Unknown",
        party_abbrev="",
        gender="",
    )

    def __init__(self, member_source: str = None, branch: str = 'main'):

        if member_source is None:
            member_source = members_of_parliament_url(branch=branch)

        self._members: pd.DataFrame = pd.read_csv(member_source).set_index('id', drop=False).rename_axis('')

        if len(self._members.id) != len(self._members.id.unique()):
            duplicates: str = ', '.join(self._members[self._members.index.duplicated()].id.tolist())
            logger.warning(f"Parliamentary member ID is not unique ({duplicates})")

        self._members = self._members.assign(
            party=self._members.party.fillna('Unknown'),
            id=self._members.id.fillna('unknown'),
            gender=self._members.gender.fillna('unknown'),
        )
        self._members.loc[self._members.gender == '', 'gender'] = 'unknown'

        """A lookup dictionary for members"""
        self.members = {meta['id']: ParliamentaryMember(**meta) for meta in self._members.to_dict('records')}

        """Add entry for unknown speakers"""
        self.members['unknown'] = self.unknown
        self.parties = self._members.party.unique()
        self.chambers = self._members.chamber.unique()

    def __getitem__(self, key) -> ParliamentaryMember:
        if key is None:
            return None
        if key not in self.members:
            """Add member id's not found into the repository."""
            self.members[key] = ParliamentaryMember(
                id=key,
                name=key,
                party="Unknown",
                party_abbrev="?",
                gender="unknown",
            )
            logger.warning(f"`{key}` not found in parliamentary member index")

        return self.members.get(key)

    def __contains__(self, key) -> ParliamentaryMember:
        return key in self.members

    def __len__(self) -> int:
        return len(self.members)
