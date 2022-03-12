from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, fields, is_dataclass
from typing import Callable, Iterable, Type

from loguru import logger

from . import corpus_index
from . import metadata as md
from . import segment, utility
from .interface import ContentType, GroupingKey, SegmentLevel, TemporalKey

# pylint: disable=too-many-arguments


@dataclass
class ProtocolSegmentGroup:

    content_type: ContentType
    year: int

    """Group attributes"""
    temporal_value: int | str
    grouping_keys: list[GroupingKey]
    grouping_values: dict[str, str | int]
    group_name: str  # string from which hashcode was computed
    hashcode: str

    """Protocol segments that belong to group"""
    protocol_segments: list[segment.ProtocolSegment] = field(default_factory=list)
    n_tokens: int = 0

    @property
    def data(self):
        texts: list[str] = (s.data for s in self.protocol_segments)
        if self.content_type == ContentType.TaggedFrame:
            return utility.merge_tagged_csv(texts, sep='\n')
        return '\n'.join(texts)

    def add(self, item: segment.ProtocolSegment):
        self.protocol_segments.append(item)

    def __repr__(self) -> str:
        key_values: str = '\t'.join(self.grouping_values[k] for k in self.grouping_keys)
        return f"{self.year}" f"{self.temporal_value}" f"\t{self.group_name}" f"\t{key_values}" f"\t{self.n_chars}"

    @property
    def n_chars(self):
        return len(self.data)

    @property
    def extension(self) -> str:
        return 'txt' if self.content_type == ContentType.Text else 'csv'

    @property
    def filename(self) -> str:
        return f'{self.document_name}.{self.extension}'

    @property
    def document_name(self) -> str:
        if self.temporal_value is None or self.group_name.startswith(self.temporal_value):
            return self.group_name
        return f'{self.temporal_value}_{self.group_name}'

    def to_dict(self):
        return {
            'year': self.year,
            'period': self.temporal_value,
            'document_name': self.document_name,
            'filename': self.filename,
            'n_tokens': self.n_tokens,
            **self.grouping_values,
        }


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
        *,
        source_index: corpus_index.CorpusSourceIndex,
        temporal_key: TemporalKey,
        grouping_keys: list[GroupingKey],
    ):
        """Setup merger.

        Args:
            source_index (corpus_index.CorpusSourceIndex): Source item index.
            speaker_service (person.SpeakerInfoService): Parliamentary speaker helper service.
            temporal_key (TemporalKey): Temporal key None, 'Year', 'Decade', 'Lustrum', 'Custom', 'Protocol', None
            grouping_keys (list[GroupingKey]): Grouping within temporal key
        """

        self.source_index: corpus_index.CorpusSourceIndex = source_index
        self.temporal_key: TemporalKey = temporal_key
        self.grouping_keys: list[GroupingKey] = grouping_keys or []
        self.grouping_hashcoder = create_grouping_hashcoder(self.grouping_keys)

    def merge(
        self, iterator: list[segment.ProtocolSegment] | segment.ProtocolSegmentIterator
    ) -> Iterable[dict[str, ProtocolSegmentGroup]]:
        """Merges stream of protocol segments based on grouping keys. Yield merged groups continously."""

        """ Note: value of `item.id` depends on aggregation level, it is u_id for levels speech and utterance """

        hashcoder = self.grouping_hashcoder

        try:

            current_temporal_value: str = None
            current_group: dict[str, ProtocolSegmentGroup] = {}
            grouping_keys: set[str] = set(self.grouping_keys)
            source_item: corpus_index.CorpusSourceItem

            # if len(grouping_keys or []) == 0:
            #     raise ValueError("no grouping key specified")

            if grouping_keys and getattr(iterator, 'segment_level', None) == SegmentLevel.Protocol:
                raise ValueError("cannot group by key (within protocol) when segement level is entire protocol.")

            for item in iterator:

                source_item = self.source_index[item.protocol_name]

                if not bool(source_item):
                    logger.error(f"source item not found: {item.name}")
                    continue

                temporal_value: str = self.to_temporal_value(item)

                if current_temporal_value != temporal_value:
                    """Yield previous group"""
                    if current_group:
                        yield current_group

                    current_group, current_temporal_value = {}, temporal_value

                grouping_values, hashcode_str, hashcode = hashcoder(item=item, source_item=source_item)

                # FIXME: #14 This fix cannot work. It prevents groupings that exclude `who` added https://github.com/welfare-state-analytics/pyriksprot/commit/8479a7c03458adcc0a0f0d0750cf48e55eec4bb0
                grouping_values['who'] = item.who

                if hashcode not in current_group:

                    current_group[hashcode] = ProtocolSegmentGroup(
                        content_type=item.content_type,
                        group_name=hashcode_str,
                        hashcode=hashcode,
                        temporal_value=temporal_value,
                        grouping_keys=self.grouping_keys,
                        grouping_values=grouping_values,
                        year=source_item.year,
                        protocol_segments=[],
                    )

                current_group[hashcode].add(item)

            """Yield last group"""
            if current_group:
                yield current_group

        except Exception as ex:
            logger.exception(ex)
            raise

    def to_year(self, source_item: corpus_index.CorpusSourceItem, temporal_key: TemporalKey) -> int:
        """Compute a year that represents the group."""
        if temporal_key == TemporalKey.Decade:
            return source_item.year - source_item.year % 10
        if temporal_key == TemporalKey.Lustrum:
            return source_item.year - source_item.year % 5
        return source_item.year

    def to_temporal_value(self, item: segment.ProtocolSegment) -> str:

        year: int = item.year
        if isinstance(self.temporal_key, (TemporalKey, str, type(None))):

            if self.temporal_key in [None, '', 'document', 'protocol', TemporalKey.NONE]:
                """No temporal key gives a group per document/protocol"""
                return item.protocol_name

            if self.temporal_key == TemporalKey.Year:
                return str(year)

            if self.temporal_key == TemporalKey.Lustrum:
                low_year: int = year - (year % 5)
                return f"{low_year}-{low_year+4}"

            if self.temporal_key == TemporalKey.Decade:
                low_year: int = year - (year % 10)
                return f"{low_year}-{low_year+9}"

        elif isinstance(self.temporal_key, dict):
            """custom periods as a dict {'category-name': (from_year,to_year), ...}"""
            for k, v in self.temporal_key:
                if v[0] <= year <= v[1]:
                    return k

        raise ValueError(f"temporal period failed for {item.protocol_name}")


def hashcoder_with_no_grouping_keys(item: segment.ProtocolSegment, **_) -> tuple[dict, str, str]:
    return ({}, item.name, hashlib.md5(item.name.encode('utf-8')).hexdigest())


def create_grouping_hashcoder(
    grouping_keys: list[str],
) -> Callable[[segment.ProtocolSegment, corpus_index.CorpusSourceItem], str]:
    """Create a hashcode function for given grouping keys"""

    grouping_keys: set[str] = set(grouping_keys)

    if not grouping_keys:
        """No grouping apart from temporal key """
        return hashcoder_with_no_grouping_keys

    speaker_keys, item_keys, corpus_index_keys = utility.split_properties_by_dataclass(
        grouping_keys, md.SpeakerInfo, segment.ProtocolSegment, corpus_index.CorpusSourceItem
    )

    def hashcoder(item: segment.ProtocolSegment, source_item: corpus_index.CorpusSourceItem) -> tuple[dict, str, str]:
        """Compute hash for item, speaker and source item. Return values, hash string and hash code"""
        assert isinstance(source_item, corpus_index.CorpusSourceItem)
        try:
            speaker_data: dict = (
                {attr: str(getattr(item.speaker_info, attr)) for attr in speaker_keys} if speaker_keys else {}
            )
            # (
            #     {attr: str(getattr(item.speaker_info, attr)) for attr in speaker_keys}
            #     if item.speaker_info is not None
            #     else {}
            # )
        except AttributeError as ex:
            raise ValueError(
                f"Grouping hashcoder: failed on retrieving key values from item.speaker_info. {ex}"
            ) from ex

        parts: dict[str, str | int] = {
            **speaker_data,
            **{attr: str(getattr(source_item, attr)) for attr in corpus_index_keys},
            **{attr: str(getattr(item, attr)) for attr in item_keys},
        }
        hashcode_str = utility.slugify('_'.join(x.lower().replace(' ', '_') for x in parts.values()))

        return (parts, hashcode_str, hashlib.md5(hashcode_str.encode('utf-8')).hexdigest())

    return hashcoder if grouping_keys else hashcoder_with_no_grouping_keys
