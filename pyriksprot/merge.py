from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, fields
from typing import Callable, Iterable, Mapping, Sequence, Tuple, Type, Union

from loguru import logger

from . import corpus_index, interface
from . import metadata as md
from . import segment, utility

# pylint: disable=too-many-arguments


class SegmentCategoryClosed(Exception):
    ...


@dataclass
class MergedSegmentGroup:

    content_type: interface.ContentType
    temporal_value: Union[int, str]
    name: str
    id: str
    page_number: str
    year: int
    grouping_keys: Sequence[interface.GroupingKey]
    grouping_values: Mapping[str, str | int]
    category_items: list[segment.ProtocolSegment] = field(default_factory=list)
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

    def add(self, item: segment.ProtocolSegment):
        self.category_items.append(item.data)

    def __repr__(self) -> str:
        return (
            f"{self.year}"
            f"{self.temporal_value}"
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
        return f'{self.document_name}.{self.extension}'

    @property
    def document_name(self) -> str:
        if self.temporal_value is None or self.name.startswith(self.temporal_value):
            return self.name
        return f'{self.temporal_value}_{self.name}'

    def to_dict(self):
        return {
            'year': self.year,
            'period': self.temporal_value,
            'document_name': self.document_name,
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
        *,
        source_index: corpus_index.CorpusSourceIndex,
        speaker_service: md.SpeakerInfoService,
        temporal_key: interface.TemporalKey,
        grouping_keys: Sequence[interface.GroupingKey],
    ):
        """Setup merger.

        Args:
            source_index (corpus_index.CorpusSourceIndex): Source item index.
            speaker_service (person.SpeakerInfoService): Parliamentary speaker helper service.
            temporal_key (interface.TemporalKey): Temporal key None, 'Year', 'Decade', 'Lustrum', 'Custom', 'Protocol', None
            grouping_keys (Sequence[interface.GroupingKey]): Grouping within temporal key
        """
        self.source_index: corpus_index.CorpusSourceIndex = source_index
        self.speaker_service: md.SpeakerInfoService = speaker_service
        self.temporal_key: interface.TemporalKey = temporal_key
        self.custom_temporal_specification = None
        self.grouping_keys: Sequence[interface.GroupingKey] = grouping_keys or []
        self.grouping_hashcoder = create_grouping_hashcoder(self.grouping_keys)

    def merge(
        self, iterator: list[segment.ProtocolSegment] | segment.ProtocolSegmentIterator
    ) -> Iterable[Mapping[str, MergedSegmentGroup]]:
        """Merges stream of protocol segments based on grouping keys. Yield merged groups continously."""

        try:
            current_temporal_category: str = None
            current_group: Mapping[str, MergedSegmentGroup] = {}
            grouping_keys: set[str] = set(self.grouping_keys)

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

                """ Note:
                    Value of `item.id` depends on aggregation level.
                    It is u_id for SpeechLevel and UtteranceLevel
                """
                speaker: md.SpeakerInfo = self.speaker_service.get_speaker_info(
                    u_id=item.u_id, person_id=item.who, year=source_item.year
                )

                if current_temporal_category != temporal_category:

                    """Yield previous group"""
                    if current_group:
                        yield current_group

                    current_group, current_temporal_category = {}, temporal_category

                grouping_values, group_str, group_hashcode = self.grouping_hashcoder(item, speaker, source_item)
                # FIXME: #14 This fix cannot work. It prevents groupings that exclude `who` added https://github.com/welfare-state-analytics/pyriksprot/commit/8479a7c03458adcc0a0f0d0750cf48e55eec4bb0
                grouping_values['who'] = item.who

                if group_hashcode not in current_group:

                    current_group[group_hashcode] = MergedSegmentGroup(
                        content_type=item.content_type,
                        temporal_value=temporal_category,
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


def props(cls: Type) -> list[str]:
    return [i for i in cls.__dict__.keys() if i[:1] != '_']


def create_grouping_hashcoder(
    grouping_keys: Sequence[str],
) -> Callable[[segment.ProtocolSegment, md.SpeakerInfo, corpus_index.CorpusSourceItem], str]:

    """Create a hashcode function for given grouping keys"""

    grouping_keys: set[str] = set(grouping_keys)

    speaker_keys: set[str] = grouping_keys.intersection({f.name for f in fields(md.SpeakerInfo)})
    item_keys: set[str] = grouping_keys.intersection({f.name for f in fields(segment.ProtocolSegment)})
    corpus_index_keys: set[str] = grouping_keys.intersection({f.name for f in fields(corpus_index.CorpusSourceItem)})
    item_keys -= speaker_keys
    corpus_index_keys -= speaker_keys | item_keys

    missing_keys: set[str] = grouping_keys - (speaker_keys | corpus_index_keys | item_keys)
    if missing_keys:
        raise TypeError(f"grouping_hashcoder: key(s) {', '.join(missing_keys)} not found (ignored)")

    def hashcoder(
        item: segment.ProtocolSegment,
        speaker: md.SpeakerInfo,
        source_item: corpus_index.CorpusSourceItem,
    ) -> Tuple[dict, str, str]:
        """Compute hash for item, speaker and source item. Return values, hash string and hash code"""
        if not grouping_keys:
            return ({}, item.name, hashlib.md5(item.name.encode('utf-8')).hexdigest())
        assert isinstance(source_item, corpus_index.CorpusSourceItem)
        parts: dict[str, str | int] = {
            **{attr: str(getattr(speaker, attr)) for attr in speaker_keys},
            **{attr: str(getattr(source_item, attr)) for attr in corpus_index_keys},
            **{attr: str(getattr(item, attr)) for attr in item_keys},
        }
        hashcode_str = utility.slugify('_'.join(x.lower().replace(' ', '_') for x in parts.values()))

        return (parts, hashcode_str, hashlib.md5(hashcode_str.encode('utf-8')).hexdigest())

    return hashcoder
