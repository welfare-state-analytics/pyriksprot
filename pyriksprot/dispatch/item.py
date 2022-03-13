from __future__ import annotations

from dataclasses import dataclass, field

from ..interface import ContentType, SegmentLevel
from ..corpus import ProtocolSegment
from ..utility import merge_tagged_csv


@dataclass
class SegmentGroup:

    segment_level: SegmentLevel
    content_type: ContentType
    year: int

    group_temporal_value: int | str
    group_values: dict[str, str | int]
    group_name: str  # string from which hashcode was computed
    group_hash: str

    protocol_segments: list[ProtocolSegment] = field(default_factory=list)
    n_tokens: int = 0

    @property
    def data(self):
        texts: list[str] = [s.data for s in self.protocol_segments]
        if self.content_type == ContentType.TaggedFrame:
            return merge_tagged_csv(texts, sep='\n')
        return '\n'.join(texts)

    def add(self, item: ProtocolSegment):
        self.protocol_segments.append(item)

    @property
    def filename(self) -> str:
        return f'{self.document_name}.{self.extension}'

    @property
    def document_name(self) -> str:
        if self.group_temporal_value is None or self.group_name.startswith(self.group_temporal_value):
            return self.group_name
        return f'{self.group_temporal_value}_{self.group_name}'

    def to_dict(self):
        """Temporary fix to include speaker's information"""
        return {
            'year': self.year,
            'period': self.group_temporal_value,
            'document_name': self.document_name,
            'filename': self.filename,
            'n_tokens': self.n_tokens,
            **self.group_values,
        }

    @property
    def group_keys(self) -> list[str]:
        return list(self.group_values.keys())

    @property
    def extension(self) -> str:
        return 'txt' if self.content_type == ContentType.Text else 'csv'
