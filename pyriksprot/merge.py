from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, fields
from typing import Callable, Iterable, List, Mapping, Sequence, Set, Tuple, Type

from loguru import logger

from . import corpus_index, interface, member, utility

# pylint: disable=too-many-arguments


class SegmentCategoryClosed(Exception):
    ...


@dataclass
class MergedSegmentGroup:

    content_type: interface.ContentType
    temporal_key: interface.TemporalKey
    name: str
    id: str
    page_number: str
    year: int
    grouping_keys: Sequence[interface.GroupingKey]
    grouping_values: Mapping[str, str | int]
    category_items: List[interface.ProtocolSegment] = field(default_factory=list)
    n_tokens: int = 0

    """Groups keys values, as a comma separated string"""
    key_values: str = field(init=False, default='')

    def __post_init__(self):
        self.key_values: str = '\t'.join(self.grouping_values[k] for k in self.grouping_keys)

    @property
    def data(self):
        if self.content_type == interface.ContentType.TaggedFrame:
            return utility.merge_tagged_csv(self.category_items, sep='\n')
        return '\n'.join(self.category_items)

    def add(self, item: interface.ProtocolSegment):
        self.category_items.append(item.data)

    def __repr__(self) -> str:
        return (
            f"{self.year}"
            f"{self.temporal_key}"
            f"\t{self.name}"
            f"\t{self.page_number or ''}"
            f"\t{self.key_values}"
            f"\t{self.n_chars}"
        )

    @property
    def n_chars(self):
        return len(self.data)

    @property
    def extension(self) -> str:
        return 'txt' if self.content_type == interface.ContentType.Text else 'csv'

    @property
    def filename(self) -> str:
        if self.temporal_key is None or self.name.startswith(self.temporal_key):
            filename: str = f'{self.name}.{self.extension}'
        else:
            filename: str = f'{self.temporal_key}_{self.name}.{self.extension}'
        return filename

    def to_dict(self):
        return {
            'year': self.year,
            'period': self.temporal_key,
            'document_name': self.name,
            'filename': self.filename,
            'n_tokens': self.n_tokens,
            **self.grouping_values,
        }

    @staticmethod
    def header(grouping_keys: Sequence[str], sep: str = '\t') -> str:
        header: str = f"period{sep}name{sep}{sep.join(v for v in grouping_keys)}{sep}n_chars{sep}"
        return header


class SegmentMerger:
    """Merge stream of segments based on temporal and grouping keys

    Temporal aggregation is performed before group aggregation.
    This reducer assumes that data is sorted by the temporal key.

    The temporal key can be None, Year, Lustrum, Decade or Custom.
    Temporal value is a tuple (from-year, to-year)

    NOTE! if multiprocessing, then all items belonging to same temporal category
    must be distributed to the same process!

    """

    def __init__(
        self,
        source_index: corpus_index.CorpusSourceIndex,
        member_index: member.ParliamentaryMemberIndex,
        temporal_key: interface.TemporalKey,
        grouping_keys: Sequence[interface.GroupingKey],
    ):
        """Setup merger.

        Args:
            source_index (corpus_index.CorpusSourceIndex): Source item index.
            member_index (member.ParliamentaryMemberIndex): Parliamentar member index.
            temporal_key (interface.TemporalKey): Temporal key Noe, 'Year', 'Decade', 'Lustrum', 'Custom', 'Protocol', None
            grouping_keys (Sequence[interface.GroupingKey]): Grouping within temporal key
        """
        self.source_index: corpus_index.CorpusSourceIndex = source_index
        self.member_index: member.ParliamentaryMemberIndex = member_index
        self.temporal_key: interface.TemporalKey = temporal_key
        self.custom_temporal_specification = None
        self.grouping_keys: Sequence[interface.GroupingKey] = grouping_keys or []
        self.grouping_hashcoder = create_grouping_hashcoder(self.grouping_keys)

    def merge(
        self, iterator: List[interface.ProtocolSegment] | interface.ProtocolSegmentIterator
    ) -> Iterable[Mapping[str, MergedSegmentGroup]]:
        """Merges stream of protocol segments based on grouping keys. Yield merged groups continously."""

        try:
            current_temporal_category: str = None
            current_group: Mapping[str, MergedSegmentGroup] = {}
            grouping_keys: Set[str] = set(self.grouping_keys)

            # if len(grouping_keys or []) == 0:
            #     raise ValueError("no grouping key specified")

            if hasattr(iterator, 'segment_level'):

                if iterator.segment_level == interface.SegmentLevel.Protocol:

                    if len(grouping_keys) > 0:
                        raise ValueError(
                            "cannot group by key (within protocol) when segement level is entire protocol."
                        )

            for item in iterator:

                source_item: corpus_index.CorpusSourceItem = self.source_index[item.protocol_name]

                if source_item is None:
                    logger.error(f"source item not found: {item.name}")
                    # raise ValueError(f"source item not found: {item.name}")
                    continue

                temporal_category: str = source_item.temporal_category(self.temporal_key, item)
                who: member.ParliamentaryRole = None if item.who is None else self.member_index[item.who]

                if current_temporal_category != temporal_category:

                    """Yield previous group"""
                    if current_group:
                        yield current_group

                    current_group, current_temporal_category = {}, temporal_category

                grouping_values, group_str, group_hashcode = self.grouping_hashcoder(item, who, source_item)
                grouping_values['who'] = item.who

                if group_hashcode not in current_group:

                    current_group[group_hashcode] = MergedSegmentGroup(
                        content_type=item.content_type,
                        temporal_key=temporal_category,
                        grouping_keys=self.grouping_keys,
                        grouping_values=grouping_values,
                        id=group_hashcode,
                        name=group_str,
                        page_number=0,
                        year=self.to_year(source_item, self.temporal_key),
                        category_items=[],
                    )

                current_group[group_hashcode].add(item)

            """Yield last group"""
            if current_group:
                yield current_group

        except Exception as ex:
            logger.exception(ex)
            raise

    def to_year(self, source_item: corpus_index.CorpusSourceItem, temporal_key: interface.TemporalKey) -> int:
        """Compute a year that represents the group."""
        if temporal_key == interface.TemporalKey.Decade:
            return source_item.year - source_item.year % 10
        if temporal_key == interface.TemporalKey.Lustrum:
            return source_item.year - source_item.year % 5
        return source_item.year


def props(cls: Type) -> List[str]:
    return [i for i in cls.__dict__.keys() if i[:1] != '_']


def create_grouping_hashcoder(
    grouping_keys: Sequence[str],
) -> Callable[[interface.ProtocolSegment, member.ParliamentaryRole, corpus_index.CorpusSourceItem], str]:

    """Create a hashcode function for given grouping keys"""

    grouping_keys: Set[str] = set(grouping_keys)

    member_keys: Set[str] = grouping_keys.intersection(
        {name for name in props(member.ParliamentaryRole(id='a', role_type='unknown', name='a'))}
    )
    index_keys: Set[str] = grouping_keys.intersection({f.name for f in fields(corpus_index.CorpusSourceItem)})
    item_keys: Set[str] = grouping_keys.intersection({f.name for f in fields(interface.ProtocolSegment)})

    def hashcoder(
        item: interface.ProtocolSegment,
        parla_member: member.ParliamentaryRole,
        source_item: corpus_index.CorpusSourceItem,
    ) -> Tuple[dict, str, str]:

        parts: Mapping[str, str | int] = {}

        if len(grouping_keys) == 0:
            return (parts, item.name, hashlib.md5(item.name.encode('utf-8')).hexdigest())

        for attr in member_keys:
            parts[attr] = str(getattr(parla_member, attr, "unknown") or attr)

        for attr in index_keys:
            parts[attr] = str(getattr(source_item, attr, "unknown") or attr)

        for attr in item_keys:
            parts[attr] = str(getattr(item, attr, "unknown") or attr)

        hashcode_str = utility.slugify('_'.join(x.lower().replace(' ', '_') for x in parts.values()))

        return (parts, hashcode_str, hashlib.md5(hashcode_str.encode('utf-8')).hexdigest())

    return hashcoder
