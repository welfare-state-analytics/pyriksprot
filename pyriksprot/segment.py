from __future__ import annotations

import abc
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from itertools import groupby
from multiprocessing import get_context
from typing import Callable, Iterable, Mapping

from loguru import logger

from .interface import ContentType, Protocol, SegmentLevel, Speech, Utterance
from .metadata import SpeakerInfo
from .utility import compress

# pylint: disable=too-many-arguments, no-member


class MergeSpeechStrategyType(str, Enum):
    Who = 'who'
    WhoSequence = 'who_sequence'
    Chain = 'chain'


@dataclass
class ProtocolSegment:

    protocol_name: str
    content_type: ContentType
    segment_level: SegmentLevel
    name: str
    who: str
    id: str
    data: str
    page_number: str
    year: int
    u_id: str
    n_tokens: int = 0

    speaker_info: SpeakerInfo = None

    def __repr__(self) -> str:
        return (
            f"{self.protocol_name or '*'}\t"
            f"{self.name or '*'}\t"
            f"{self.who or '*'}\t"
            f"{self.id or '?'}\t"
            f"{self.data or '?'}\t"
            f"{self.page_number or ''}\t"
        )

    def data_z64(self) -> bytes:
        """Compress text, return base64 encoded string."""
        return compress(self.data)

    def to_dict(self):
        """These properties ends up in resulting document index."""
        speaker: dict = (
            {}
            if not self.speaker_info
            else {
                self.speaker_info.gender_id,
                self.speaker_info.party_id,
                self.speaker_info.office_type_id,
                self.speaker_info.sub_office_type_id,
                self.speaker_info.who,
            }
        )

        return {
            'year': self.year,
            'period': self.year,
            'who': self.who,
            'protocol_name': self.protocol_name,
            'document_name': self.name,
            'filename': self.filename,
            'n_tokens': self.n_tokens,
            'page_number': self.page_number,
            **speaker,
        }

    @property
    def extension(self) -> str:
        return 'txt' if self.content_type == ContentType.Text else 'csv'

    @property
    def filename(self) -> str:
        return f'{self.name}.{self.extension}'

    @property
    def temporal_key(self) -> str:
        return self.name


def to_speeches(
    protocol: Protocol, merge_strategy: MergeSpeechStrategyType = 'chain', segment_skip_size: int = 1
) -> list[Speech]:
    """Convert utterances into speeches using specified strategy. Return list."""
    speeches: list[Speech] = SpeechMergerFactory.get(merge_strategy).speeches(
        protocol, segment_skip_size=segment_skip_size
    )
    return speeches


def to_segments(
    *,
    protocol: Protocol,
    content_type: ContentType,
    segment_level: SegmentLevel,
    segment_skip_size: int = 1,
    preprocess: Callable[[str], str] = None,
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

    segments: list[ProtocolSegment] = [
        ProtocolSegment(
            protocol_name=protocol.name,
            content_type=content_type,
            segment_level=segment_level,
            year=protocol.get_year(),
            **d,
        )
        for d in _to_segments(protocol, content_type, segment_level, segment_skip_size)
    ]

    if preprocess is not None:
        for x in segments:
            x.data = preprocess(x.data)

    if segment_skip_size > 0:
        segments = [x for x in segments if len(x.data) > segment_skip_size]

    return segments


def _to_segments(
    protocol: Protocol,
    content_type: ContentType,
    segment_level: SegmentLevel,
    segment_skip_size: int,
    merge_strategy: MergeSpeechStrategyType = MergeSpeechStrategyType.WhoSequence,
) -> Iterable[dict]:

    if segment_level in [SegmentLevel.Protocol, None]:
        return [
            dict(
                name=protocol.name,
                who=None,
                id=protocol.name,
                u_id=None,
                data=protocol.to_content_str(content_type),
                page_number='0',
            )
        ]

    if segment_level == SegmentLevel.Speech:
        return [
            dict(
                name=s.document_name,
                who=s.who,
                id=s.speech_id,
                u_id=s.speech_id,
                data=s.to_content_str(content_type),
                page_number=s.page_number,
            )
            for s in to_speeches(protocol, merge_strategy=merge_strategy, segment_skip_size=segment_skip_size)
        ]

    if segment_level == SegmentLevel.Who:
        return [
            dict(
                name=s.document_name,
                who=s.who,
                id=s.speech_id,
                u_id=s.speech_id,
                data=s.to_content_str(content_type),
                page_number=s.page_number,
            )
            for s in to_speeches(
                protocol, merge_strategy=MergeSpeechStrategyType.Who, segment_skip_size=segment_skip_size
            )
        ]

    if segment_level == SegmentLevel.Utterance:
        return [
            dict(
                name=f'{protocol.name}_{i+1:03}',
                who=u.who,
                id=u.u_id,
                u_id=u.u_id,
                data=u.to_str(content_type),
                page_number=u.page_number,
            )
            for i, u in enumerate(protocol.utterances)
        ]

    if segment_level == SegmentLevel.Paragraph:
        """Only text can be returned for paragraphs"""

        return [
            dict(
                name=f'{protocol.name}_{j+1:03}_{i+1:03}',
                who=u.who,
                id=f"{u.u_id}@{i}",
                u_id=u.u_id,
                data=p,
                page_number=u.page_number,
            )
            for j, u in enumerate(protocol.utterances)
            for i, p in enumerate(u.paragraphs)
        ]

    return None


# FIXME: merge to_segment/to_text
def to_text(
    *,
    protocol: Protocol,
    segment_level: SegmentLevel,
    segment_skip_size: int = 0,
    preprocess: Callable[[str], str] = None,
) -> Iterable[ProtocolSegment]:
    """Generate text blocks from `protocol`. Yield each block as a tuple (name, who, id, text, page_number)."""
    try:

        if segment_level in [SegmentLevel.Protocol, None]:

            items: Iterable[ProtocolSegment] = [
                ProtocolSegment(
                    protocol_name=protocol.name,
                    content_type=ContentType.Text,
                    segment_level=SegmentLevel.Protocol,
                    name=protocol.name,
                    who=None,
                    id=protocol.name,
                    u_id=None,
                    data=protocol.text,
                    page_number='0',
                    year=protocol.get_year(),
                )
            ]

        elif segment_level == SegmentLevel.Who:

            who_utterances: dict[str, list[Utterance]] = defaultdict(list)
            for u in protocol.utterances:
                who_utterances[u.who].append(u)

            items: Iterable[ProtocolSegment] = [
                ProtocolSegment(
                    protocol_name=protocol.name,
                    content_type=ContentType.Text,
                    segment_level=SegmentLevel.Who,
                    name=f'{protocol.name}_{i+1:03}',
                    who=who_us[0].who,
                    id=who_us[0].who,
                    u_id=who_us[0].u_id,
                    data='\n'.join(u.text for u in who_us),
                    page_number=str(who_us[0].page_number),
                    year=protocol.get_year(),
                )
                for i, who_us in enumerate(who_utterances.values())
            ]

        elif segment_level == SegmentLevel.Speech:
            speech_utterances: list[list[Utterance]] = merge_speech_by_chain(protocol.utterances)
            items: Iterable[ProtocolSegment] = [
                ProtocolSegment(
                    protocol_name=protocol.name,
                    content_type=ContentType.Text,
                    segment_level=SegmentLevel.Speech,
                    name=f'{protocol.name}_{i+1:03}',
                    who=speech[0].who,
                    id=speech[0].u_id,
                    u_id=speech[0].u_id,
                    data='\n'.join(u.text for u in speech),
                    page_number=str(speech[0].page_number),
                    year=protocol.get_year(),
                )
                for i, speech in enumerate(speech_utterances)
            ]

        elif segment_level == SegmentLevel.Utterance:

            items: Iterable[ProtocolSegment] = [
                ProtocolSegment(
                    protocol_name=protocol.name,
                    content_type=ContentType.Text,
                    segment_level=SegmentLevel.Utterance,
                    name=f'{protocol.name}_{i+1:03}',
                    who=u.who,
                    id=u.u_id,
                    u_id=u.u_id,
                    data=u.text,
                    page_number=str(u.page_number),
                    year=protocol.get_year(),
                )
                for i, u in enumerate(protocol.utterances)
            ]

        elif segment_level == SegmentLevel.Paragraph:

            items: Iterable[ProtocolSegment] = [
                ProtocolSegment(
                    protocol_name=protocol.name,
                    content_type=ContentType.Text,
                    segment_level=SegmentLevel.Paragraph,
                    name=f'{protocol.name}_{j+1:03}_{i+1:03}',
                    who=u.who,
                    id=f"{u.u_id}@{i}",
                    u_id=u.u_id,
                    data=p,
                    page_number=str(u.page_number),
                    year=protocol.get_year(),
                )
                for j, u in enumerate(protocol.utterances)
                for i, p in enumerate(u.paragraphs)
            ]

        else:
            raise ValueError(f"undefined segment level {segment_level}")

        if preprocess is not None:
            for item in items:
                item.data = preprocess(item.data)

        if segment_skip_size > 0:

            items = [item for item in items if len(item.text) > segment_skip_size]

        return items

    except Exception as ex:
        raise ex


class ProtocolSegmentIterator(abc.ABC):
    ...

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
        speech_merge_strategy: str = 'chain',
        preprocessor: Callable[[str], str] = None,
    ):
        """Split document (protocol) into segments.

        Args:
            filenames (list[str]): files to read
            content_type (ContentType, optional): Content type Text or TaggedFrame . Defaults to TaggedFrame.
            segment_level (SegmentLevel, optional): Iterate segment level. Defaults to Protocol.
            segment_skip_size (int, optional): Skip segments having char count below threshold. Defaults to 1.
            multiproc_processes (int, optional): Number of read processes. Defaults to None.
            multiproc_chunksize (int, optional): Multiprocessing multiproc_chunksize. Defaults to 100.
            multiproc_keep_order (bool, optional): Keep doc order. Defaults to False.
            speech_merge_strategy (str, optional): Speech merge strategy. Defaults to 'chain'.
            preprocessor (Callable[[str], str], optional): Preprocess funcion, only used for text. Defaults to None.
        """
        self.filenames: list[str] = sorted(filenames)
        self.iterator = None
        self.content_type: ContentType = content_type
        self.segment_level: SegmentLevel = segment_level
        self.segment_skip_size: int = segment_skip_size
        self.speech_merge_strategy: str = speech_merge_strategy  # FIXME: used??
        self.multiproc_processes: int = multiproc_processes or 1
        self.multiproc_chunksize: int = multiproc_chunksize
        self.multiproc_keep_order: bool = multiproc_keep_order
        self.preprocessor: Callable[[str], str] = preprocessor

    def __iter__(self):
        self.iterator = self.create_iterator()
        return self

    def __next__(self):
        return next(self.iterator)

    def create_iterator(self) -> Iterable[ProtocolSegment]:

        fx = self.preprocessor
        if self.multiproc_processes > 1:
            args: list[tuple[str, str, str, int]] = [
                (name, self.content_type, self.segment_level, self.segment_skip_size) for name in self.filenames
            ]
            with get_context("spawn").Pool(processes=self.multiproc_processes) as executor:
                imap = executor.imap if self.multiproc_keep_order else executor.imap_unordered
                futures = self.map_futures(imap=imap, args=args)
                for payload in futures:
                    for item in payload:
                        if fx:
                            item.data = fx(item.data)
                        yield item
        else:
            for filename in self.filenames:
                for item in self.load(filename=filename):
                    if fx:
                        item.data = fx(item.data)
                    yield item

    @abc.abstractmethod
    def load(self, filename: str) -> Iterable[ProtocolSegment]:
        ...

    @abc.abstractmethod
    def map_futures(self, imap, args):
        ...


def merge_speech_by_chain(utterances: list[Utterance]) -> list[list[Utterance]]:
    """Split utterances based on prev/next pointers. Return list of lists."""
    speeches: list[list[Utterance]] = []
    speech: list[Utterance] = None

    for _, u in enumerate(utterances or []):
        """Rules:
        - attrib `next` --> start of new speech with consecutive utterances
        - attrib `prev` --> utterance is belongs to same speech as previous utterance
        - if neither `prev` or `next` are set, then utterance is the entire speech
        - attribs `prev` and `next` are never both set
        """
        if bool(u.prev_id) and bool(u.next_id):
            raise ValueError(f"logic error: {u.u_id} has both prev/next attrbutes set")

        part_of_chain: bool = bool(u.prev_id) or bool(u.next_id)

        if bool(u.next_id) or not part_of_chain:
            speech = [u]
            speeches.append(speech)
            continue

        if not bool(speech):
            logger.warning(f"logic error: {u.u_id} has prev attribute but no previous utterance")
            speech = [u]
            speeches.append(speech)
            continue

        if speech[-1].u_id != u.prev_id:
            logger.warning(f"u[{u.u_id}]: current u.prev_id differs from previous u.u_id '{speech[-1].u_id}'")

        speeches[-1].append(u)

    return speeches


class IMergeSpeechStrategy(abc.ABC):
    """Abstract strategy for merging a protocol into speeches"""

    def create(self, protocol: Protocol, utterances: list[Utterance] = None, speech_index: int = 0) -> Speech:
        """Create a new speech entity."""

        if not utterances:
            utterances = protocol.utterances

        return Speech(
            protocol_name=protocol.name,
            document_name=f'{protocol.name}_{speech_index:03}',
            speech_id=utterances[0].u_id,
            who=utterances[0].who,
            page_number=utterances[0].page_number,
            speech_date=protocol.date,
            speech_index=speech_index,
            utterances=utterances,
        )

    def speeches(self, protocol: Protocol, segment_skip_size: int = 1) -> list[Speech]:
        speeches: list[Speech] = self.merge(protocol=protocol)
        if segment_skip_size > 0:
            speeches = [s for s in speeches if len(s.text or "") >= segment_skip_size]
        return speeches

    def split(self, utterances: Protocol) -> list[list[Utterance]]:  # pylint: disable=unused-argument
        return []

    def merge(self, protocol: Protocol) -> list[Speech]:
        """Create a speech for each consecutive sequence with the same `who`. Return list of Speech."""
        if not protocol.utterances:
            return []
        return [
            self.create(protocol, utterances=utterances, speech_index=i + 1)
            for i, utterances in enumerate(self.split(protocol.utterances))
        ]


class MergeSpeechByWho(IMergeSpeechStrategy):
    """Merge all uterrances for a unique `who` into a single speech """

    def split(self, utterances: Protocol) -> list[list[Utterance]]:
        """Create a speech for each unique `who`. Return list of Speech."""
        data = defaultdict(list)
        for u in utterances or []:
            data[u.who].append(u)
        return [data[who] for who in data]


class MergeSpeechByWhoSequence(IMergeSpeechStrategy):
    """Merge sequences with same `who` into a speech """

    def split(self, utterances: list[Utterance]) -> list[list[Utterance]]:
        who_sequences: list[list[Utterance]] = [list(g) for _, g in groupby(utterances or [], key=lambda x: x.who)]
        return who_sequences


class MergeSpeechByChain(IMergeSpeechStrategy):
    def merge(self, protocol: Protocol) -> list[Speech]:
        """Create speeches based on prev/next pointers. Return list."""
        speech_utterances: list[list[Utterance]] = merge_speech_by_chain(protocol.utterances)
        speeches: list[Speech] = [
            self.create(protocol, utterances=utterances, speech_index=i + 1)
            for i, utterances in enumerate(speech_utterances)
        ]
        return speeches


class UndefinedMergeSpeech(IMergeSpeechStrategy):
    def merge(self, protocol: Protocol) -> list[Speech]:
        raise ValueError("undefined merge strategy encountered")


class SpeechMergerFactory:

    strategies: Mapping[MergeSpeechStrategyType, IMergeSpeechStrategy] = {
        'who': MergeSpeechByWho(),
        'who_sequence': MergeSpeechByWhoSequence(),
        'chain': MergeSpeechByChain(),
        'undefined': UndefinedMergeSpeech(),
    }

    @staticmethod
    def get(strategy: str) -> IMergeSpeechStrategy:
        return (
            SpeechMergerFactory.strategies.get(strategy)
            if strategy in SpeechMergerFactory.strategies
            else SpeechMergerFactory.strategies.get('undefined')
        )
