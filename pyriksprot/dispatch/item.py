from __future__ import annotations

from dataclasses import dataclass, field

from ..corpus import ProtocolSegment
from ..interface import ContentType, IDispatchItem
from ..utility import merge_csv_strings


@dataclass
class DispatchItem(IDispatchItem):
    """Groups segments for dispatch to a zink.
    One item corresponds to either...
     - A single document made up of the aggregate of contained segments
     - A set of documents, one for each contained segment
    """

    year: int

    group_temporal_value: int | str
    group_values: dict[str, str | int]
    group_name: str  # string from which hashcode was computed
    group_hash: str

    protocol_segments: list[ProtocolSegment] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.protocol_segments)

    def add(self, item: ProtocolSegment):
        self.protocol_segments.append(item)

    @property
    def document_name(self) -> str:
        if self.group_name.startswith(self.group_temporal_value or ""):
            return self.group_name
        return f'{self.group_temporal_value}_{self.group_name}'

    @property
    def filename(self) -> str:
        return f'{self.document_name}.{self.extension}'

    @property
    def text(self) -> str:
        return (
            merge_csv_strings(self._get_texts(), sep='\n')
            if self.content_type == ContentType.TaggedFrame
            else '\n'.join(self._get_texts())
        )

    def to_dict(self):
        return {
            'year': self.year,
            'period': self.group_temporal_value,
            'document_name': self.document_name,
            'filename': self.filename,
            'n_tokens': self.n_tokens,
            **self.group_values,
        }

    """Not public"""

    def _get_texts(self) -> list[str]:
        return [s.data for s in self.protocol_segments]

    @property
    def extension(self) -> str:
        return 'txt' if self.content_type == ContentType.Text else 'csv'

    """Not used"""
    # @property
    # def group_keys(self) -> list[str]:
    #     return list(self.group_values.keys())
