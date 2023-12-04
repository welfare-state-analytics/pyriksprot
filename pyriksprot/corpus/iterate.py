from __future__ import annotations

import abc
from dataclasses import dataclass
from multiprocessing import get_context
from typing import TYPE_CHECKING, Callable, Iterable, Literal

import numpy as np
import tqdm

from .. import to_speech as mu
from ..interface import ContentType, IDispatchItem, Protocol, SegmentLevel
from ..utility import compress

if TYPE_CHECKING:
    from ..metadata import SpeakerInfo

# pylint: disable=too-many-arguments, no-member


@dataclass
class ProtocolSegment(IDispatchItem):
    """Container for a subset of utterances within a single protocol."""

    protocol_name: str
    name: str
    who: str
    id: str
    data: str
    page_number: int
    year: int
    u_id: str
    n_utterances: int = 0
    speaker_info: SpeakerInfo = None
    speaker_note_id: str = None
    speech_index: int = None

    def __len__(self) -> int:
        """IDispatchItem interface"""
        return 1

    def data_z64(self) -> bytes:
        """Compress text, return base64 encoded string."""
        return compress(self.data)

    def to_dict(self):
        """These properties ends up in the resulting document index."""
        return {
            'u_id': self.u_id,
            'year': self.year,
            'period': self.year,
            'who': self.who,
            'protocol_name': self.protocol_name,
            'document_name': self.name,
            'filename': self.filename,
            'n_tokens': self.n_tokens,
            'n_utterances': self.n_utterances,
            'speaker_note_id': self.speaker_note_id,
            'speech_index': self.speech_index,
            'page_number': self.page_number,
            **(
                {}
                if not self.speaker_info
                else {
                    'gender_id': self.speaker_info.gender_id,
                    'party_id': self.speaker_info.party_id,
                    'office_type_id': self.speaker_info.term_of_office.office_type_id,
                    'sub_office_type_id': self.speaker_info.term_of_office.sub_office_type_id,
                }  # FIXME: Call self.speaker_info.to_dict() instead
            ),
        }

    @staticmethod
    def dtypes() -> dict:
        return {
            'year': np.int16,
            'n_tokens': np.int32,
            'n_utterances': np.int8,
            'speech_index': np.int16,
            **SpeakerInfo.dtypes(),  # pylint: disable=used-before-assignment
        }

    @property
    def extension(self) -> str:
        return 'txt' if self.content_type == ContentType.Text else 'csv'

    @property
    def filename(self) -> str:
        return f'{self.name}.{self.extension}'

    @property
    def text(self) -> str:
        return self.data


def to_protocol_segment(
    *, protocol: Protocol, content_type: ContentType, which_year: Literal["filename", "date"] = "filename", **_
) -> list[ProtocolSegment]:
    """Split protocol to a single segment."""
    return [
        ProtocolSegment(
            protocol_name=protocol.name,
            content_type=content_type,
            segment_level=SegmentLevel.Protocol,
            year=protocol.get_year(which=which_year),
            name=protocol.name,
            who=None,
            id=protocol.name,
            u_id=None,
            data=protocol.text,
            page_number=0,
            n_tokens=0,
            n_utterances=len(protocol.utterances),
            speaker_info=None,
            speaker_note_id=None,
        )
    ]


def to_speech_segments(
    *,
    protocol: Protocol,
    content_type: ContentType,
    segment_skip_size: int,
    merge_strategy: mu.MergeStrategyType,
    which_year: Literal["filename", "date"] = "filename",
    **_,
) -> list[ProtocolSegment]:
    return [
        ProtocolSegment(
            protocol_name=protocol.name,
            content_type=content_type,
            segment_level=SegmentLevel.Speech,
            year=protocol.get_year(which=which_year),
            name=s.document_name,
            who=s.who,
            id=s.speech_id,
            u_id=s.speech_id,
            data=s.to_content_str(content_type),
            page_number=s.page_number,
            n_tokens=0 if not s.has_tagged_text else s.tagged_text.count("\n"),
            n_utterances=len(s),
            speaker_note_id=s.speaker_note_id,
            speech_index=s.speech_index,
        )
        for s in mu.to_speeches(protocol=protocol, merge_strategy=merge_strategy, skip_size=segment_skip_size)
    ]


def to_who_segments(
    *,
    protocol: Protocol,
    content_type: ContentType,
    segment_skip_size: int,
    which_year: Literal["filename", "date"] = "filename",
    **_,
) -> list[ProtocolSegment]:
    return [
        ProtocolSegment(
            protocol_name=protocol.name,
            content_type=content_type,
            segment_level=SegmentLevel.Who,
            year=protocol.get_year(which=which_year),
            name=s.document_name,
            who=s.who,
            id=s.speech_id,
            u_id=s.speech_id,
            data=s.to_content_str(content_type),
            page_number=s.page_number,
            n_tokens=0,
            n_utterances=len(s),
        )
        for s in mu.to_speeches(protocol=protocol, merge_strategy=mu.MergeStrategyType.who, skip_size=segment_skip_size)
    ]


def to_utterance_segments(
    *, protocol: Protocol, content_type: ContentType, which_year: Literal["filename", "date"] = "filename", **_
) -> list[ProtocolSegment]:
    return [
        ProtocolSegment(
            protocol_name=protocol.name,
            content_type=content_type,
            segment_level=SegmentLevel.Utterance,
            year=protocol.get_year(which=which_year),
            name=f'{protocol.name}_{i+1:03}',
            who=u.who,
            id=u.u_id,
            u_id=u.u_id,
            data=u.to_str(content_type),
            page_number=u.page_number,
            n_tokens=0,
            n_utterances=1,
        )
        for i, u in enumerate(protocol.utterances)
    ]


def to_paragraph_segments(
    *, protocol: Protocol, content_type: ContentType, which_year: Literal["filename", "date"] = "filename", **_
) -> list[ProtocolSegment]:
    return [
        ProtocolSegment(
            protocol_name=protocol.name,
            content_type=content_type,
            segment_level=SegmentLevel.Paragraph,
            year=protocol.get_year(which=which_year),
            name=f'{protocol.name}_{j+1:03}_{i+1:03}',
            who=u.who,
            id=f"{u.u_id}@{i}",
            u_id=u.u_id,
            data=p,
            page_number=u.page_number,
            n_tokens=0,
            n_utterances=1,
        )
        for j, u in enumerate(protocol.utterances)
        for i, p in enumerate(u.paragraphs)
    ]


def to_segments(
    *,
    protocol: Protocol,
    content_type: ContentType,
    segment_level: SegmentLevel,
    merge_strategy: mu.MergeStrategyType,
    segment_skip_size: int = 1,
    preprocess: Callable[[str], str] = None,
    which_year: Literal["filename", "date"] = "filename",
) -> Iterable[ProtocolSegment]:
    """Splits protocol to sequence of text/tagged text segments

    Args:
        content_type (ContentType): Text' or 'TaggedFrame'
        segment_level (SegmentLevel): [description]
        segment_skip_size (int, optional): [description]. Defaults to 1.
        preprocess (Callable[[str], str], optional): [description]. Defaults to None.

    Returns:
        Iterable[ProtocolSegment]: [description]
    """

    segments: list[ProtocolSegment] = SEGMENT_FUNCTIONS.get(segment_level)(
        protocol=protocol,
        content_type=content_type,
        segment_skip_size=segment_skip_size,
        merge_strategy=merge_strategy,
        which_year=which_year,
    )

    if preprocess is not None:
        for x in segments:
            x.data = preprocess(x.data)

    if segment_skip_size > 0:
        segments = [x for x in segments if len(x.data) > segment_skip_size]

    return segments


SEGMENT_FUNCTIONS: dict = {
    None: to_protocol_segment,
    SegmentLevel.Protocol: to_protocol_segment,
    SegmentLevel.Speech: to_speech_segments,
    SegmentLevel.Who: to_who_segments,
    SegmentLevel.Utterance: to_utterance_segments,
    SegmentLevel.Paragraph: to_paragraph_segments,
}


class ProtocolSegmentIterator(abc.ABC):
    def __init__(
        self,
        *,
        filenames: list[str],
        content_type: ContentType = ContentType.Text,
        segment_level: SegmentLevel = SegmentLevel.Protocol,
        segment_skip_size: int = 1,
        multiproc_processes: int = None,
        multiproc_chunksize: int = 100,
        multiproc_keep_order: bool = False,
        merge_strategy: str = 'chain',
        preprocess: Callable[[str], str] = None,
        which_year: Literal["filename", "date"] = "filename",
    ):
        """Merge utterances within protocols to segments.

        Args:
            filenames (list[str]): files to read
            content_type (ContentType, optional): Content type Text or TaggedFrame . Defaults to TaggedFrame.
            segment_level (SegmentLevel, optional): Iterate segment level. Defaults to Protocol.
            segment_skip_size (int, optional): Skip segments having char count below threshold. Defaults to 1.
            multiproc_processes (int, optional): Number of read processes. Defaults to None.
            multiproc_chunksize (int, optional): Multiprocessing multiproc_chunksize. Defaults to 100.
            multiproc_keep_order (bool, optional): Keep doc order. Defaults to False.
            merge_strategy (str, optional): Speech merge strategy. Defaults to 'chain'.
            preprocess (Callable[[str], str], optional): Preprocess funcion, only used for text. Defaults to None.
            which_year (Literal["filename", "date"]): Take year from filename or XML tag `date` in content
        """
        self.filenames: list[str] = sorted(filenames)
        self.iterator = None
        self.content_type: ContentType = content_type
        self.segment_level: SegmentLevel = segment_level
        self.segment_skip_size: int = segment_skip_size
        self.merge_strategy: str = merge_strategy  # FIXME: used??
        self.multiproc_processes: int = multiproc_processes or 1
        self.multiproc_chunksize: int = multiproc_chunksize
        self.multiproc_keep_order: bool = multiproc_keep_order
        self.preprocess: Callable[[ProtocolSegment], str] = preprocess
        self.which_year: str = which_year

    def __iter__(self):
        self.iterator = self.create_iterator()
        return self

    def __next__(self):
        return next(self.iterator)

    def create_iterator(self) -> Iterable[ProtocolSegment]:
        item: ProtocolSegment
        fx = self.preprocess
        # speaker_service: SpeakerInfoService = self.speaker_service

        if self.multiproc_processes > 1:
            args: list[tuple[str, str, str, int]] = [
                (
                    name,
                    self.content_type,
                    self.segment_level,
                    self.segment_skip_size,
                    self.merge_strategy,
                    self.which_year,
                )
                for name in self.filenames
            ]
            with get_context("spawn").Pool(processes=self.multiproc_processes) as executor:
                imap = executor.imap if self.multiproc_keep_order else executor.imap_unordered
                futures = self.map_futures(imap=imap, args=args)
                for payload in futures:
                    for item in payload:
                        if fx:
                            fx(item)
                        yield item
        else:
            for filename in tqdm.tqdm(self.filenames):
                for item in self.load(filename=filename):
                    if fx:
                        fx(item)
                    yield item

    @abc.abstractmethod
    def load(self, filename: str) -> Iterable[ProtocolSegment]:
        ...

    @abc.abstractmethod
    def map_futures(self, imap, args):
        ...
