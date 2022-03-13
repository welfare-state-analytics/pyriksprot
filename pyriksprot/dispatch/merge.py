from __future__ import annotations

from typing import Iterable

from loguru import logger

from ..corpus import corpus_index, iterate
from ..interface import GroupingKey, SegmentLevel, TemporalKey
from .item import DispatchItem
from .utility import create_grouping_hashcoder

# pylint: disable=too-many-arguments, unbalanced-tuple-unpacking


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
        self, iterator: list[iterate.ProtocolSegment] | iterate.ProtocolSegmentIterator
    ) -> Iterable[dict[str, DispatchItem]]:
        """Merges stream of protocol segments based on grouping keys. Yield merged groups continously.
        Note: value of `item.id` depends on aggregation level, it is u_id for levels speech and utterance.
        """

        hashcoder = self.grouping_hashcoder

        try:
            current_temporal_value: str = None
            current_group: dict[str, DispatchItem] = {}
            grouping_keys: set[str] = set(self.grouping_keys)
            source_item: corpus_index.CorpusSourceItem

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

                    current_group[hashcode] = DispatchItem(
                        segment_level=item.segment_level,
                        content_type=item.content_type,
                        group_name=hashcode_str,
                        group_hash=hashcode,
                        group_temporal_value=temporal_value,
                        group_values=grouping_values,
                        year=source_item.year,
                        protocol_segments=[],
                        n_tokens=0,
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

    def to_temporal_value(self, item: iterate.ProtocolSegment) -> str:

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
