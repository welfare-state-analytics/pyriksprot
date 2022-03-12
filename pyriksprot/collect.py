from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from loguru import logger

from . import corpus_index, segment, utility
from .interface import ContentType, SegmentLevel

# pylint: disable=too-many-arguments


@dataclass
class ProtocolSegmentGroup:

    content_type: ContentType
    year: int
    protocol_name: str
    protocol_segments: list[segment.ProtocolSegment] = field(default_factory=list)
    n_tokens: int = 0

    @property
    def data(self):
        if self.content_type == ContentType.TaggedFrame:
            return utility.merge_tagged_csv(self.protocol_segments, sep='\n')
        return '\n'.join(self.protocol_segments)

    def add(self, item: segment.ProtocolSegment):
        self.protocol_segments.append(item.data)

    def __repr__(self) -> str:
        return f"{self.year}" f"{self.protocol_name}" f"\t{self.n_chars}"

    @property
    def n_chars(self) -> int:
        return sum(map(len, (s.data for s in self.protocol_segments)))

    @property
    def extension(self) -> str:
        return 'txt' if self.content_type == ContentType.Text else 'csv'

    @property
    def filename(self) -> str:
        return f'{self.document_name}.{self.extension}'

    @property
    def document_name(self) -> str:
        return f'{self.protocol_name}'

    def to_dict(self):
        return {
            'year': self.year,
            'period': self.year,
            'document_name': self.document_name,
            'filename': self.filename,
            'n_tokens': self.n_tokens,
        }


class SpeechMerger:
    """Dispatches a stream of speeches"""

    def __init__(self, source_index: corpus_index.CorpusSourceIndex):
        self.source_index: corpus_index.CorpusSourceIndex = source_index

    def merge(self, iterator: Iterable[segment.ProtocolSegment]) -> Iterable[dict[str, ProtocolSegmentGroup]]:

        assert iterator.segment_level == SegmentLevel.Speech

        try:

            protocol_group: ProtocolSegmentGroup = None
            source_item: corpus_index.CorpusSourceItem

            for item in iterator:

                source_item = self.source_index[item.protocol_name]

                if not bool(source_item):
                    logger.error(f"source item not found: {item.name}")
                    continue

                if not protocol_group or (protocol_group.protocol_name != item.protocol_name):

                    """Yield previous group"""
                    if protocol_group:
                        yield protocol_group

                    protocol_group = ProtocolSegmentGroup(
                        content_type=item.content_type,
                        protocol_name=item.protocol_name,
                        year=source_item.year,
                    )

                protocol_group.add(item)

            """Yield last group"""
            if protocol_group:
                yield protocol_group

        except Exception as ex:
            logger.exception(ex)
            raise
