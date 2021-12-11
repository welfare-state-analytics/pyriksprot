from __future__ import annotations

import abc
import contextlib
import csv
import hashlib
import re
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from io import StringIO
from itertools import groupby
from multiprocessing import get_context
from typing import Any, Callable, Iterable, List, Mapping, Optional, Tuple, Union

import pandas as pd
from loguru import logger
from pandas.io import json

from .utility import compress, flatten, merge_tagged_csv, strip_extensions

# pylint: disable=too-many-arguments, no-member


class ParlaClarinError(ValueError):
    ...


class TemporalKey(str, Enum):
    NONE = None
    Year = 'year'
    Decade = 'decade'
    Lustrum = 'lustrum'
    Custom = 'custom'
    Protocol = None
    Document = None


class GroupingKey(str, Enum):
    NONE = None
    Who = 'who'
    Speech = 'speech'
    Party = 'party'
    Gender = 'gender'


class SegmentLevel(str, Enum):
    NONE = None
    Protocol = 'protocol'
    Who = 'who'
    Speech = 'speech'
    Utterance = 'utterance'
    Paragraph = 'paragraph'


class ContentType(str, Enum):
    Text = 'text'
    TaggedFrame = 'tagged_frame'


class StorageFormat(str, Enum):
    CSV = 'csv'
    JSON = 'json'


class MergeSpeechStrategyType(str, Enum):
    Who = 'who'
    WhoSequence = 'who_sequence'
    Chain = 'chain'


PARAGRAPH_MARKER: str = '@#@'


class IProtocol(abc.ABC):
    ...


class Utterance:
    """Represents an utterance in the ParlaClarin XML file"""

    delimiter: str = '\n'

    def __init__(
        self,
        u_id: str,
        n: str = "",
        who: str = None,
        prev_id: str = None,
        next_id: str = None,
        paragraphs: Union[List[str], str] = None,
        annotation: Optional[str] = None,
        page_number: Optional[str] = '',
        **_,
    ):
        self.u_id: str = u_id
        self.n: str = n
        self.who: str = who
        self.prev_id: str = prev_id if isinstance(prev_id, str) else None
        self.next_id: str = next_id if isinstance(next_id, str) else None
        self.paragraphs: List[str] = (
            [] if not paragraphs else paragraphs if isinstance(paragraphs, list) else paragraphs.split(PARAGRAPH_MARKER)
        )
        self.annotation: Optional[str] = annotation if isinstance(annotation, str) else None
        self.page_number: Optional[str] = page_number

    @property
    def document_name(self) -> str:
        return f'{self.who}_{self.u_id}'

    @property
    def tagged_text(self) -> str:
        return self.annotation

    @property
    def text(self) -> str:
        """Merge utterance paragraphs. Return text."""
        return self.delimiter.join(p for p in self.paragraphs if p != '').strip()

    def checksum(self) -> str:
        """Compute checksum of utterance text."""
        return hashlib.sha1(self.text.encode('utf-8')).hexdigest()[:16]

    def to_str(self, what: ContentType) -> str:
        return self.tagged_text if what == ContentType.TaggedFrame else self.text


class UtteranceHelper:

    CSV_OPTS = dict(
        quoting=csv.QUOTE_MINIMAL,
        escapechar="\\",
        doublequote=False,
        sep='\t',
    )

    @staticmethod
    def to_dict(utterances: List[Utterance]) -> List[Mapping[str, Any]]:
        """Convert list of utterances to a list of dicts. Return the list."""
        return [
            {
                'u_id': u.u_id,
                'n': u.n,
                'who': u.who,
                'prev_id': u.prev_id,
                'next_id': u.next_id,
                'annotation': u.tagged_text,
                'paragraphs': PARAGRAPH_MARKER.join(u.paragraphs),
                'checksum': u.checksum(),
            }
            for u in utterances
        ]

    @staticmethod
    def to_dataframe(utterances: Union[StringIO, str, List[Utterance]]) -> pd.DataFrame:
        """Convert list of utterances, CSV string or a CSV file to a dataframe."""
        if isinstance(utterances, (str, StringIO)):
            df: pd.DataFrame = pd.read_csv(
                StringIO(utterances) if isinstance(utterances, str) else utterances,
                **UtteranceHelper.CSV_OPTS,
                index_col='u_id',
            )
            df.drop(columns='checksum')
        else:
            df: pd.DataFrame = pd.DataFrame(UtteranceHelper.to_dict(utterances)).set_index('u_id')
        return df

    @staticmethod
    def to_csv(utterances: List[Utterance]) -> str:
        """Convert list of utterances to a CSV string. Return CSV string."""
        return UtteranceHelper.to_dataframe(utterances=utterances).to_csv(**UtteranceHelper.CSV_OPTS, index=True)

    @staticmethod
    def from_csv(csv_str: str) -> List[Utterance]:
        """Convert CSV string to list of utterances. Return list."""
        df: pd.DataFrame = UtteranceHelper.to_dataframe(StringIO(csv_str))
        utterances: List[Utterance] = [
            Utterance(
                u_id=d.get('u_id'),
                n=d.get('n'),
                who=d.get('who'),
                prev_id=d.get('prev_id'),
                next_id=d.get('next_id'),
                paragraphs=d.get('paragraphs', '').split(PARAGRAPH_MARKER),
                annotation=d.get('annotation'),
            )
            for d in df.reset_index().to_dict(orient='records')
        ]
        return utterances

    @staticmethod
    def to_json(utterances: List[Utterance]) -> str:
        """Convert list of utterances to a JSON string. Return JSON string."""
        json_str = json.dumps([u.__dict__ for u in utterances])
        return json_str

    @staticmethod
    def from_json(json_str: str) -> List[Utterance]:
        """Convert JSON string to list of utterances. Return list."""
        data: List[Utterance] = list(map(lambda x: Utterance(**x), json.loads(json_str)))
        return data

    @staticmethod
    def merge_tagged_texts(utterances: List[Utterance], sep: str = '\n') -> str:
        """Merge annotations into a single tagged CSV string"""
        return merge_tagged_csv([u.tagged_text for u in (utterances or [])], sep=sep)

    @staticmethod
    def merge_tagged_csv(csv_strings: List[str], sep: str = '\n') -> str:
        return merge_tagged_csv(csv_strings, sep=sep)


class UtteranceMixIn:
    def to_text(self, *, sep: str = '\n', require_letter: bool = False) -> str:
        t: str = sep.join(t for t in (u.text for u in self.utterances) if t != '')
        if require_letter and not re.search('[a-zåäöA-ZÅÄÖ]', t):
            """Empty string if no letter in text"""
            return ""
        return t.strip()

    @property
    def text(self) -> str:
        """Join text of all utterances."""
        return self.to_text(sep='\n', require_letter=False)

    @property
    def has_text(self) -> bool:
        """Check if any utterance actually has any uttered words."""
        return any(u.text != "" for u in self.utterances)

    @property
    def tagged_text(self) -> str:
        """Merge tagged texts for entire speech into a single CSV string."""
        return UtteranceHelper.merge_tagged_texts(self.utterances, sep='\n')

    def to_dict(self) -> List[Mapping[str, Any]]:
        """Convert utterances to list of dict."""
        return UtteranceHelper.to_dict(self.utterances)

    def to_dataframe(self) -> pd.DataFrame:
        """Convert utterances to dataframe"""
        return UtteranceHelper.to_dataframe(self.utterances)

    def to_csv(self) -> str:
        """Convert utterances to CSV string"""
        return UtteranceHelper.to_csv(self.utterances)

    def to_json(self) -> str:
        """Convert utterances to JSON string"""
        return UtteranceHelper.to_json(self.utterances)

    def to_str(self, what: ContentType) -> str:
        return self.tagged_text if what == ContentType.TaggedFrame else self.text

    @property
    def paragraphs(self) -> Optional[str]:
        """Flatten sequence of segments into a single text"""
        return flatten(u.paragraphs for u in self.utterances)

    def __len__(self):
        return len(self.utterances)

    def __contains__(self, item: Union[str, Utterance]) -> bool:
        if isinstance(item, Utterance):
            item = item.u_id
        return any(u.u_id == item for u in self.utterances)


@dataclass
class Speech(UtteranceMixIn):
    """Entity that represents a (processed) speech within a single document."""

    protocol_name: str
    document_name: str
    speech_id: str
    who: str
    speech_date: str
    speech_index: int
    page_number: str

    utterances: List[Utterance] = field(default_factory=list)

    num_tokens: int = 0
    num_words: int = 0
    delimiter: str = field(default='\n')

    def __post_init__(self):

        if len(self.utterances or []) == 0:
            raise ParlaClarinError("utterance list cannot be empty")

        if any(self.who != u.who for u in self.utterances):
            raise ParlaClarinError("multiple speakes in same speech not allowed")

    @property
    def filename(self):
        """Generate filename from speech name."""
        return f"{self.speech_name}.csv"

    @property
    def speech_name(self):
        """Generate a unique name for speech."""
        return f"{strip_extensions(self.document_name)}@{self.speech_index}"

    def add(self, item: Utterance) -> "Speech":
        self.utterances.append(item)
        return self


class Protocol(UtteranceMixIn):
    """Entity that represents a ParlaCLARIN document."""

    def __init__(self, date: str, name: str, utterances: List[Utterance], **_):
        self.date: str = date
        self.name: str = name
        self.utterances: List[Utterance] = utterances

    def preprocess(self, preprocess: Callable[[str], str] = None) -> "Protocol":
        """Apply text transforms. Return self."""

        if preprocess is None:
            return self

        for utterance in self.utterances:
            utterance.paragraphs = [preprocess(p.strip()) for p in utterance.paragraphs]

        return self

    def get_year(self) -> int:
        return int(self.date[:4])

    def checksum(self) -> Optional[str]:
        """Compute checksum for entire text."""
        with contextlib.suppress(Exception):
            return hashlib.sha1(''.join(u.text for u in self.utterances).encode('utf-8')).hexdigest()
        return None

    def to_speeches(
        self, merge_strategy: MergeSpeechStrategyType = 'who_sequence', segment_skip_size: int = 1
    ) -> List[Speech]:
        """Convert utterances into speeches using specified strategy. Return list."""
        speeches: List[Speech] = SpeechMergerFactory.get(merge_strategy).speeches(
            self, segment_skip_size=segment_skip_size
        )
        return speeches

    def get_content(self, content_type: ContentType) -> str:
        return self.text if content_type == ContentType.Text else self.tagged_text

    def _to_segments(
        self,
        content_type: ContentType,
        segment_level: SegmentLevel,
        segment_skip_size: int,
        merge_strategy: MergeSpeechStrategyType = MergeSpeechStrategyType.WhoSequence,
    ) -> Iterable[dict]:

        if segment_level in [SegmentLevel.Protocol, None]:
            return [dict(name=self.name, who=None, id=self.name, data=self.to_str(content_type), page_number='0')]

        if segment_level == SegmentLevel.Speech:
            return [
                dict(
                    name=s.document_name,
                    who=s.who,
                    id=s.speech_id,
                    data=s.to_str(content_type),
                    page_number=s.page_number,
                )
                for s in self.to_speeches(merge_strategy=merge_strategy, segment_skip_size=segment_skip_size)
            ]

        if segment_level == SegmentLevel.Who:
            return [
                dict(
                    name=s.document_name,
                    who=s.who,
                    id=s.speech_id,
                    data=s.to_str(content_type),
                    page_number=s.page_number,
                )
                for s in self.to_speeches(
                    merge_strategy=MergeSpeechStrategyType.Who, segment_skip_size=segment_skip_size
                )
            ]

        if segment_level == SegmentLevel.Utterance:
            return [
                dict(
                    name=f'{self.name}_{i+1:03}',
                    who=u.who,
                    id=u.u_id,
                    data=u.to_str(content_type),
                    page_number=u.page_number,
                )
                for i, u in enumerate(self.utterances)
            ]

        if segment_level == SegmentLevel.Paragraph:
            """Only text can be returned for paragraphs"""

            return [
                dict(
                    name=f'{self.name}_{j+1:03}_{i+1:03}',
                    who=u.who,
                    id=f"{u.u_id}@{i}",
                    data=p,
                    page_number=u.page_number,
                )
                for j, u in enumerate(self.utterances)
                for i, p in enumerate(u.paragraphs)
            ]

        return None

    def to_segments(
        self,
        *,
        content_type: ContentType,
        segment_level: SegmentLevel,
        segment_skip_size: int = 1,
        preprocess: Callable[[str], str] = None,
    ) -> Iterable[ProtocolSegment]:
        """Splits protocol to sequence of text/tagged text segments

        Args:
            content_type (ContentType): Ttext' or 'TaggedFrame'
            segment_level (SegmentLevel): [description]
            segment_skip_size (int, optional): [description]. Defaults to 1.
            preprocess (Callable[[str], str], optional): [description]. Defaults to None.

        Returns:
            Iterable[ProtocolSegment]: [description]
        """
        segments: List[ProtocolSegment] = [
            ProtocolSegment(protocol_name=self.name, content_type=content_type, year=self.get_year(), **d)
            for d in self._to_segments(content_type, segment_level, segment_skip_size)
        ]

        if preprocess is not None:
            for x in segments:
                x.data = preprocess(x.data)

        if segment_skip_size > 0:
            segments = [x for x in segments if len(x.data) > segment_skip_size]

        return segments


@dataclass
class ProtocolSegment:

    protocol_name: str
    content_type: ContentType
    name: str
    who: str
    id: str
    data: str
    page_number: str
    year: int
    n_tokens: int = 0

    def __repr__(self) -> str:
        return (
            f"{self.protocol_name or '*'}\t"
            f"{self.name or '*'}\t"
            f"{self.who or '*'}\t"
            f"{self.id or '?'}\t"
            f"{self.data or '?'}\t"
            f"{self.page_number or ''}\t"
        )

    #            f"{len(self.data)/1024.0:.2f}kB\t"

    def data_z64(self) -> bytes:
        """Compress text, return base64 encoded string."""
        return compress(self.data)

    def to_dict(self):
        return {
            'year': self.year,
            'period': self.year,
            'who': self.who,
            'protocol_name': self.protocol_name,
            'document_name': self.name,
            'filename': self.filename,
            'n_tokens': self.n_tokens,
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


class ProtocolSegmentIterator(abc.ABC):
    ...

    def __init__(
        self,
        *,
        filenames: List[str],
        content_type: ContentType = ContentType.Text,
        segment_level: SegmentLevel = SegmentLevel.Protocol,
        segment_skip_size: int = 1,
        multiproc_processes: int = None,
        multiproc_chunksize: int = 100,
        multiproc_keep_order: bool = False,
        speech_merge_strategy: str = 'who_sequence',
        preprocessor: Callable[[str], str] = None,
    ):
        """Split document (protocol) into segments.

        Args:
            filenames (List[str]): files to read
            content_type (ContentType, optional): Content type Text or TaggedFrame . Defaults to TaggedFrame.
            segment_level (SegmentLevel, optional): Iterate segment level. Defaults to Protocol.
            segment_skip_size (int, optional): Skip segments having char count below threshold. Defaults to 1.
            multiproc_processes (int, optional): Number of read processes. Defaults to None.
            multiproc_chunksize (int, optional): Multiprocessing multiproc_chunksize. Defaults to 100.
            multiproc_keep_order (bool, optional): Keep doc order. Defaults to False.
            speech_merge_strategy (str, optional): Speech merge strategy. Defaults to 'who_sequence'.
            preprocessor (Callable[[str], str], optional): Preprocess funcion, only used for text. Defaults to None.
        """
        self.filenames: List[str] = sorted(filenames)
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
            args: List[Tuple[str, str, str, int]] = [
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


class IMergeSpeechStrategy(abc.ABC):
    """Abstract strategy for merging a protocol into speeches"""

    def create(self, protocol: Protocol, utterances: List[Utterance] = None, speech_index: int = 0) -> Speech:
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

    def speeches(self, protocol: Protocol, segment_skip_size: int = 1) -> List[Speech]:
        speeches: List[Speech] = self.merge(protocol=protocol)
        if segment_skip_size > 0:
            speeches = [s for s in speeches if len(s.text or "") >= segment_skip_size]
        return speeches

    def split(self, utterances: Protocol) -> List[List[Utterance]]:  # pylint: disable=unused-argument
        return []

    def merge(self, protocol: Protocol) -> List[Speech]:
        """Create a speech for each consecutive sequence with the same `who`. Return list of Speech."""
        if not protocol.utterances:
            return []
        return [
            self.create(protocol, utterances=utterances, speech_index=i + 1)
            for i, utterances in enumerate(self.split(protocol.utterances))
        ]


class MergeSpeechByWho(IMergeSpeechStrategy):
    """Merge all uterrances for a unique `who` into a single speech """

    def split(self, utterances: Protocol) -> List[List[Utterance]]:
        """Create a speech for each unique `who`. Return list of Speech."""
        data = defaultdict(list)
        for u in utterances or []:
            data[u.who].append(u)
        return [data[who] for who in data]


class MergeSpeechByWhoSequence(IMergeSpeechStrategy):
    """Merge sequences with same `who` into a speech """

    def split(self, utterances: List[Utterance]) -> List[List[Utterance]]:
        who_sequences: List[List[Utterance]] = [list(g) for _, g in groupby(utterances or [], key=lambda x: x.who)]
        return who_sequences


class MergeSpeechByChain(IMergeSpeechStrategy):
    def merge(self, protocol: Protocol) -> List[Speech]:
        """Create speeches based on prev/next pointers. Return list."""
        speeches: List[Speech] = []
        speech: Speech = None

        next_id: str = None

        for _, u in enumerate(protocol.utterances or []):

            prev_id: str = u.prev_id

            if next_id is not None:
                if next_id != u.u_id:
                    logger.warning(
                        f"{protocol.name}.u[{u.u_id}]: current u.id differs from previous u.next_id '{next_id}'"
                    )

            next_id: str = u.next_id

            if prev_id is not None:

                if speech is None:
                    logger.error(f"{protocol.name}.u[{u.u_id}]: ignoring prev='{prev_id}' (no previous utterance)")
                    prev_id = None

                else:
                    if prev_id not in speech:
                        logger.error(
                            f"{protocol.name}.u[{u.u_id}]: ignoring prev='{prev_id}' (not found in current speech)"
                        )
                        prev_id = None

            if prev_id is None:
                speech = self.create(protocol, utterances=[u], speech_index=len(speeches) + 1)
                speeches.append(speech)
            else:
                speeches[-1].add(u)

        return speeches


class UndefinedMergeSpeech(IMergeSpeechStrategy):
    def merge(self, protocol: Protocol) -> List[Speech]:
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
