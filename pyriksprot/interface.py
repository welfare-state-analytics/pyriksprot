from __future__ import annotations

import abc
import contextlib
import csv
import hashlib
import re
from dataclasses import dataclass, field
from enum import Enum
from io import StringIO
from typing import Any, Callable, Literal, Mapping, Optional, Union

import pandas as pd
from pandas.io import json

from .utility import flatten, merge_csv_strings, strip_extensions

# pylint: disable=too-many-arguments, no-member

MISSING_SPEAKER_NOTE_ID: str = "missing"


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
    who = 'who'
    person_id = 'person_id'
    pid = 'pid'
    speech_id = 'speech_id'
    party_id = 'party_id'
    gender_id = 'gender_id'
    office_type_id = 'office_type_id'
    sub_office_type_id = 'sub_office_type_id'

    # speech = 'speech'
    # party = 'party'
    # gender = 'gender'


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


PARAGRAPH_MARKER: str = '@#@'


@dataclass
class IDispatchItem(abc.ABC):
    segment_level: SegmentLevel
    content_type: ContentType
    n_tokens: int

    @property
    def filename(self) -> str:
        ...

    @property
    def text(self) -> str:
        ...

    def to_dict(self):
        ...


# class IUtterance(abc.ABC):

#     delimiter: str
#     u_id: str
#     speaker_note_id: str
#     who: str
#     prev_id: str
#     next_id: str
#     paragraphs: Union[list[str], str]
#     annotation: Optional[str]
#     page_number: Optional[str]

#     @abc.abstractmethod
#     @property
#     def is_unknown(self) -> bool:
#         ...

#     @abc.abstractmethod
#     @property
#     def document_name(self) -> str:
#         ...

#     @abc.abstractmethod
#     @property
#     def tagged_text(self) -> str:
#         ...

#     @abc.abstractmethod
#     @property
#     def text(self) -> str:
#         ...

#     @abc.abstractmethod
#     def checksum(self) -> str:
#         ...

#     @abc.abstractmethod
#     def to_str(self, what: ContentType) -> str:
#         ...


class IProtocol(abc.ABC):

    date: str
    name: str
    utterances: list[Utterance]
    speaker_notes: dict[str, str]

    @abc.abstractmethod
    def get_year(self, which: Literal["filename", "date"] = "filename") -> int:
        ...

    @abc.abstractmethod
    def preprocess(self, preprocess: Callable[[str], str] = None) -> "Protocol":
        ...

    @abc.abstractmethod
    def checksum(self) -> Optional[str]:
        ...

    @abc.abstractmethod
    def get_content(self, content_type: ContentType) -> str:
        ...

    @abc.abstractmethod
    def get_speaker_notes(self) -> dict[str, str]:
        ...


class Utterance:
    """Represents an utterance in the ParlaClarin XML file"""

    delimiter: str = '\n'

    def __init__(
        self,
        *,
        u_id: str,
        speaker_note_id: str,
        who: str,
        prev_id: str = None,
        next_id: str = None,
        paragraphs: Union[list[str], str] = None,
        annotation: Optional[str] = None,
        page_number: Optional[str] = '',
        **_,
    ):

        if not speaker_note_id:
            speaker_note_id = MISSING_SPEAKER_NOTE_ID
            # FIXME #15 Exists utterances in data not preceeded by a speakers's intro note
            # logger.warning(f"utterance {u_id}: empty speaker_note_id")
            # raise ValueError(f"utterance {u_id}: empty speaker_note_id not allowed")

        self.u_id: str = u_id
        self.who: str = who
        self.prev_id: str = prev_id if isinstance(prev_id, str) else None
        self.next_id: str = next_id if isinstance(next_id, str) else None
        self.paragraphs: list[str] = (
            [] if not paragraphs else paragraphs if isinstance(paragraphs, list) else paragraphs.split(PARAGRAPH_MARKER)
        )
        self.annotation: Optional[str] = annotation if isinstance(annotation, str) else None
        self.page_number: Optional[str] = page_number if isinstance(page_number, str) else ''
        self.speaker_note_id: Optional[str] = (
            speaker_note_id if isinstance(speaker_note_id, str) else MISSING_SPEAKER_NOTE_ID
        )

    @property
    def is_unknown(self) -> bool:
        return self.who == "unknown"

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
    def to_dict(utterances: list[Utterance]) -> list[Mapping[str, Any]]:
        """Convert list of utterances to a list of dicts. Return the list."""
        return [
            {
                'u_id': u.u_id,
                'who': u.who,
                'speaker_note_id': u.speaker_note_id,
                'prev_id': u.prev_id,
                'next_id': u.next_id,
                'annotation': u.tagged_text,
                'paragraphs': PARAGRAPH_MARKER.join(u.paragraphs),
                'page_number': u.page_number or '',
                'checksum': u.checksum(),
            }
            for u in utterances
        ]

    @staticmethod
    def to_dataframe(utterances: Union[StringIO, str, list[Utterance]]) -> pd.DataFrame:
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
    def to_csv(utterances: list[Utterance]) -> str:
        """Convert list of utterances to a CSV string. Return CSV string."""
        return UtteranceHelper.to_dataframe(utterances=utterances).to_csv(**UtteranceHelper.CSV_OPTS, index=True)

    @staticmethod
    def from_csv(csv_str: str) -> list[Utterance]:
        """Convert CSV string to list of utterances. Return list."""
        df: pd.DataFrame = UtteranceHelper.to_dataframe(StringIO(csv_str))
        utterances: list[Utterance] = [Utterance(**d) for d in df.reset_index().to_dict(orient='records')]
        return utterances

    @staticmethod
    def to_json(utterances: list[Utterance]) -> str:
        """Convert list of utterances to a JSON string. Return JSON string."""
        json_str = json.dumps([u.__dict__ for u in utterances])
        return json_str

    @staticmethod
    def from_json(json_str: str) -> list[Utterance]:
        """Convert JSON string to list of utterances. Return list."""
        data: list[Utterance] = list(map(lambda x: Utterance(**x), json.loads(json_str)))
        return data

    @staticmethod
    def merge_tagged_texts(utterances: list[Utterance], sep: str = '\n') -> str:
        """Merge annotations into a single tagged CSV string"""
        return merge_csv_strings([u.tagged_text for u in (utterances or [])], sep=sep)

    @staticmethod
    def merge_tagged_csv(csv_strings: list[str], sep: str = '\n') -> str:
        return merge_csv_strings(csv_strings, sep=sep)


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
        return any(bool(u.text) for u in self.utterances)

    @property
    def tagged_text(self) -> str:
        """Merge tagged texts for entire speech into a single CSV string."""
        return UtteranceHelper.merge_tagged_texts(self.utterances, sep='\n')

    @property
    def has_tagged_text(self) -> bool:
        """Check if any utterance actually has any uttered words."""
        return any(bool(u.tagged_text) for u in self.utterances)

    def to_dict(self) -> list[Mapping[str, Any]]:
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

    def to_content_str(self, what: ContentType) -> str:
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
    """Entity that represents a (processed) speech within a Parla-CLARIN protocol."""

    protocol_name: str
    document_name: str
    speech_id: str
    who: str
    speech_date: str
    speech_index: int
    page_number: str

    utterances: list[Utterance] = field(default_factory=list)

    num_tokens: int = 0
    num_words: int = 0
    delimiter: str = field(default='\n')

    def __post_init__(self):

        if len(self.utterances or []) == 0:
            raise ParlaClarinError("utterance list cannot be empty")

        if any(self.who != u.who for u in self.utterances):
            raise ParlaClarinError("multiple speakers in same speech not allowed")

    @property
    def filename(self):
        """Generate filename from speech name."""
        return f"{self.speech_name}.csv"

    @property
    def speech_name(self):
        """Generate a unique name for speech."""
        return f"{strip_extensions(self.document_name)}@{self.speech_index}"

    @property
    def speaker_note_id(self):
        """xml:id for preceeding speaker-note."""
        return MISSING_SPEAKER_NOTE_ID if not self.utterances else self.utterances[0].speaker_note_id

    def add(self, item: Utterance) -> "Speech":
        self.utterances.append(item)
        return self


class Protocol(UtteranceMixIn, IProtocol):
    """Entity that represents a ParlaCLARIN document."""

    def __init__(self, date: str, name: str, utterances: list[Utterance], speaker_notes: dict[str, str], **_):
        self.date: str = date
        self.name: str = name
        self.utterances: list[Utterance] = utterances
        self.speaker_notes: dict[str, str] = speaker_notes or {}

    def get_year(self, which: Literal["filename", "date"] = "filename") -> int:
        """Returns protocol's year either extracted from filename or from `date` tag in XML header"""
        if which != "filename":
            return int(self.date[:4])
        return int(self.name.split("-")[1][:4])

    def preprocess(self, preprocess: Callable[[str], str] = None) -> "Protocol":
        """Apply text transforms. Return self."""

        if preprocess is None:
            return self

        for uttr in self.utterances:
            uttr.paragraphs = [preprocess(p.strip()) for p in uttr.paragraphs]

        return self

    def checksum(self) -> Optional[str]:
        """Compute checksum for entire text."""
        with contextlib.suppress(Exception):
            return hashlib.sha1(''.join(u.text for u in self.utterances).encode('utf-8')).hexdigest()
        return None

    def get_content(self, content_type: ContentType) -> str:
        return self.text if content_type == ContentType.Text else self.tagged_text

    def get_speaker_notes(self) -> dict[str, str]:
        return self.speaker_notes


class IProtocolParser(abc.ABC):
    @staticmethod
    def to_protocol(
        filename: str, segment_skip_size: int, ignore_tags: set[str]  # pylint: disable=unused-argument
    ) -> IProtocol:
        ...
