from __future__ import annotations

import glob
import hashlib
from dataclasses import asdict, dataclass, field, fields
from functools import reduce
from os.path import basename, dirname
from os.path import join as jj
from typing import Callable, Iterable, List, Literal, Mapping, Optional, Sequence, Set, Tuple

import pandas as pd
from loguru import logger

from . import iterators, utility
from .interface import IterateLevel, ProtocolIterItem

#  pylint: disable=too-many-arguments


def members_of_parliament_url(branch: str = 'main') -> str:
    return f'https://raw.githubusercontent.com/welfare-state-analytics/riksdagen-corpus/{branch}/corpus/members_of_parliament.csv'


@dataclass
class SourceIndexItem:
    path: str
    filename: str = None
    name: str = None
    subfolder: str = None
    year: Optional[int] = None

    def __post_init__(self):
        self.filename = basename(self.path)
        self.name = utility.strip_path_and_extension(self.path)
        self.subfolder = basename(dirname(self.path))
        self.year = self.to_year(basename(self.path))

    def temporal_hashcode(self, temporal_key: str | Mapping[str, Tuple[int, int]]) -> str:

        if isinstance(temporal_key, str):

            if temporal_key == 'year':
                return str(self.year)

            if temporal_key == 'lustrum':
                low_year: int = self.year - (self.year % 5)
                return f"{low_year}-{low_year+4}"

            if temporal_key == 'year':
                low_year: int = self.year - (self.year % 10)
                return f"{low_year}-{low_year+9}"

        elif isinstance(temporal_key, dict):

            for k, v in temporal_key:
                if v[0] <= self.year <= v[1]:
                    return k

        raise ValueError(f"temporal period failed for {self.name}")

    def to_year(self, filename: str) -> Optional[int]:
        try:
            filename = basename(filename)
            if filename.startswith("prot-"):
                return int(filename.split("-")[1][:4])
        except ValueError:
            ...
        return None

    def to_dict(self) -> dict:

        return asdict(self)


@dataclass
class SourceIndex:
    def __init__(self, source_items: List[SourceIndexItem]):

        self.source_items = source_items
        self.lookup = {x.name: x for x in self.source_items}

    def __getitem__(self, key: str) -> SourceIndexItem:
        return self.lookup.get(key)

    def __contains__(self, key: str) -> bool:
        return key in self.lookup

    def __len__(self) -> int:
        return len(self.source_items)

    @staticmethod
    def load(
        source_folder: str, source_pattern: str = '**/prot-*.xml', years: Optional[str | Set[int]] = None
    ) -> "SourceIndex":

        paths: List[str] = glob.glob(jj(source_folder, source_pattern), recursive=True)

        target_years: Set[int] = (
            None if years is None else (set(utility.parse_range_list(years)) if isinstance(years, str) else set(years))
        )

        source_items = [SourceIndexItem(path=path) for path in paths]

        if target_years is not None:
            source_items = [x for x in source_items if x.years in target_years]

        source_index: SourceIndex = SourceIndex(source_items)

        return source_index

    @property
    def filenames(self) -> List[str]:
        return sorted([x.filename for x in self.source_items])

    @property
    def paths(self) -> List[str]:
        return sorted([x.path for x in self.source_items])

    def to_pandas(self) -> pd.DataFrame:
        df: pd.DataFrame = pd.DataFrame(data=[x.to_dict() for x in self.source_items])
        return df

    def to_csv(self, filename: str = None) -> Optional[str]:
        return self.to_pandas().to_csv(filename, sep='\t', index=None)

    @staticmethod
    def read_csv(filename: str) -> "SourceIndex":
        df: pd.DataFrame = pd.read_csv(filename, sep="\t", index_col=None)
        source_items: List[SourceIndexItem] = [SourceIndexItem(**d) for d in df.to_dict('records')]
        source_index: SourceIndex = SourceIndex(source_items)
        return source_index


TemporalKey = Literal[None, 'year', 'decade', 'lustrum', 'custom']
GroupingKey = Literal[None, 'speaker', 'speech', 'party', 'gender']


@dataclass
class ParliamentaryMember:
    name: str
    party: str
    district: str
    chamber: str
    start: int
    end: int
    occupation: str
    gender: str
    id: str
    specifier: str
    riksdagen_id: str
    born: str
    party_abbrev: str
    twittername: str


class ParliamentaryMemberIndex:
    def __init__(self, member_source: str = None, branch: str = 'main'):

        if member_source is None:
            member_source = members_of_parliament_url(branch=branch)

        self._members: pd.DataFrame = pd.read_csv(member_source).set_index('id', drop=False)
        self._members.rename_axis('')

        self.members = {meta['id']: ParliamentaryMember(**meta) for meta in self._members.to_dict('records')}

    def __getitem__(self, key) -> ParliamentaryMember:
        if key is None:
            return None
        if key not in self.members:
            logger.warning(f"`{key}` not found in parliamentary member index")
        return self.members.get(key)

    def __contains__(self, key) -> ParliamentaryMember:
        return key in self.members

    def __len__(self) -> int:
        return len(self.members)


def create_grouping_hashcoder(
    grouping_keys: Sequence[str],
) -> Callable[[ProtocolIterItem, ParliamentaryMember, SourceIndexItem], str]:

    grouping_keys: Set[str] = set(grouping_keys)

    member_keys: Set[str] = grouping_keys.intersection({f.name for f in fields(ParliamentaryMember)})
    index_keys: Set[str] = grouping_keys.intersection({f.name for f in fields(SourceIndexItem)})
    item_keys: Set[str] = grouping_keys.intersection({f.name for f in fields(ProtocolIterItem)})

    def hashcoder(item: ProtocolIterItem, member: ParliamentaryMember, index_item: SourceIndexItem) -> Tuple[str, str]:

        parts: List[str] = []

        for attr in member_keys:
            parts.append(str(getattr(member, attr) or attr))

        for attr in index_keys:
            parts.append(str(getattr(index_item, attr) or attr))

        for attr in item_keys:
            parts.append(str(getattr(item, attr) or attr))

        hashcode_str = '#'.join(parts)

        return (hashcode_str, hashlib.md5(hashcode_str.encode('utf-8')).hexdigest())

    return hashcoder


class AggregateSlotClosed(Exception):
    ...


@dataclass
class AggregateIterItem:

    hashcode: str
    name: str
    who: str
    id: str
    page_number: str

    texts: List[str] = field(default_factory=list)

    @property
    def text(self):
        return '\n'.join(self.texts)

    def add(self, item: ProtocolIterItem):
        self.name = ""
        self.who = ""
        self.id = item.id
        self.texts.append(item.text)
        self.page_number = ""


class TextAggregator:
    """Aggregate ProtocolIterItem based on time and grouping keys

    Time aggregation is always performed before group aggregation.
    This aggregator assumes that data is sorted by the temporal key.

    The temporal key can be None, 'year', 'lustrum', 'decade' or 'custom'.
    Temporal value is a tuple (from-year, to-year) where

    """

    def __init__(
        self,
        source_index: SourceIndex,
        member_index: ParliamentaryMemberIndex,
        temporal_key: TemporalKey,
        grouping_keys: Sequence[GroupingKey],
    ):

        """Immutable data"""
        self.source_index: SourceIndex = source_index
        self.member_index: ParliamentaryMemberIndex = member_index
        self.temporal_key: TemporalKey = temporal_key
        self.custom_temporal_specification = None
        self.grouping_keys: Sequence[GroupingKey] = grouping_keys
        self.grouping_hashcoder = create_grouping_hashcoder(grouping_keys)

        """Mutable data"""
        """Current temporal value """
        self.temporal_hascode: str = None

        """Open slots within current temporal value """
        self.open: Mapping[str, AggregateIterItem] = {}

        """Closed slots within current temporal value """
        self.closed: List[AggregateIterItem] = []

    def __add__(self, item: ProtocolIterItem):
        self.add(item)

    def add(self, item: ProtocolIterItem):
        """"Add item to slot."""

        """ NOTE! if multiprocessing, then all items with same temporal values
            must be distributed to same process!
        """

        index_item: SourceIndexItem = self.source_index[item.name]
        who: ParliamentaryMember = None if item.who is None else self.member_index[item.who]
        temporal_hashcode: str = index_item.temporal_hashcode(self.temporal_key)
        group_str, group_hashcode = self.grouping_hashcoder(item, who, index_item)

        if self.temporal_hascode != temporal_hashcode:
            self.closed.extend(self.open.values())
            self.open = {}

        if group_hashcode not in self.open:
            self.open[group_hashcode] = AggregateIterItem(
                who=who.id if who else None,
                id=group_hashcode,
                hashcode=group_hashcode,
                name=group_str,
                page_number=0,
                texts=[],
            )

        self.open[group_hashcode].add(item)

        if len(self.closed) > 0:
            raise AggregateSlotClosed()

    def aggregate(self, iterator: iterators.IProtocolTextIterator) -> Iterable[AggregateIterItem]:

        if 'who' in self.grouping_keys and iterator.level == 'protocol':
            raise ValueError("Grouping by `who` whilst iterating at protocol level is not possible.")

        for item in iterator:
            try:
                self.add(item)
            except AggregateSlotClosed:
                for x in self.closed:
                    yield x


# def group_text_by_keys(
#     source_index: SourceIndex,
#     items: Iterable[ProtocolIterItem],
#     temporal_key: TemporalKey = None,
#     iter_level: IterateLevel = None,
#     grouping_keys: Sequence[str] = None,
# ) -> Iterable[Any]:
#     """Group items by keys which can be:
#     - 'year', 'lustrum', 'decade' or custom year periods
#     -
#     """

#     aggregator: TextAggregator = TextAggregator(source_index, temporal_key, iter_level)

#     for item in items:
#         for x in aggregator.aggregate(item):
#             ...


def dedent_text(text: str) -> str:
    return text


def dehyphen_text(text: str) -> str:
    return text


def compose(*fns: Sequence[Callable[[str], str]]) -> Callable[[str], str]:
    if len(fns) == 0:
        return None
    return reduce(lambda f, g: lambda *args: f(g(*args)), fns)


# pylint: disable=unused-argument,unused-variable
def extract_corpus_text(
    source_folder: str = None,
    target: str = None,
    level: IterateLevel = None,
    dedent: bool = False,
    dehyphen: bool = False,
    keep_order: str = None,
    skip_size: int = 1,
    processes: int = 1,
    years: str = None,
    # create_index: bool = True,
    groupby: Sequence[str] = None,
    **kwargs,
) -> None:

    source_index: SourceIndex = SourceIndex.load(source_folder=source_folder, years=years)

    preprocessor: Callable[[str], str] = compose(
        ([dedent_text] if dedent else []) + ([dehyphen_text] if dehyphen else [])
    )

    iterator: iterators.IProtocolTextIterator = iterators.ProtocolTextIterator(
        filenames=source_index.paths[:10],
        level=level,
        ordered=keep_order,
        processes=processes,
        skip_size=skip_size,
        preprocessor=preprocessor,
    )

    # aggregator = TextAggregator(source_index, iterator)

    # for item in aggregator.aggregate(temporal_key):
    #     ...
