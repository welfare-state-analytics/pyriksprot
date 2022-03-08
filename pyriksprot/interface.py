from __future__ import annotations

import abc
import contextlib
import csv
import hashlib
import re
from dataclasses import dataclass, field
from enum import Enum
from io import StringIO
from typing import Any, Callable, Mapping, Optional, Union

import pandas as pd
from pandas.io import json

from .utility import flatten, merge_tagged_csv, strip_extensions

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
        paragraphs: Union[list[str], str] = None,
        annotation: Optional[str] = None,
        page_number: Optional[str] = '',
        speaker_hash: Optional[str] = '',
        **_,
    ):
        self.u_id: str = u_id
        self.n: str = n
        self.who: str = who
        self.prev_id: str = prev_id if isinstance(prev_id, str) else None
        self.next_id: str = next_id if isinstance(next_id, str) else None
        self.paragraphs: list[str] = (
            [] if not paragraphs else paragraphs if isinstance(paragraphs, list) else paragraphs.split(PARAGRAPH_MARKER)
        )
        self.annotation: Optional[str] = annotation if isinstance(annotation, str) else None
        self.page_number: Optional[str] = page_number if isinstance(page_number, str) else ''
        self.speaker_hash: Optional[str] = speaker_hash if isinstance(speaker_hash, str) else ''

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
                'n': u.n,
                'who': u.who,
                'prev_id': u.prev_id,
                'next_id': u.next_id,
                'annotation': u.tagged_text,
                'paragraphs': PARAGRAPH_MARKER.join(u.paragraphs),
                'page_number': u.page_number or '',
                'speaker_hash': u.speaker_hash or '',
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
        return merge_tagged_csv([u.tagged_text for u in (utterances or [])], sep=sep)

    @staticmethod
    def merge_tagged_csv(csv_strings: list[str], sep: str = '\n') -> str:
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
    """Entity that represents a (processed) speech within a single document."""

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
            raise ParlaClarinError("multiple speakes in same speech not allowed")

    @property
    def filename(self):
        """Generate filename from speech name."""
        return f"{self.speech_name}.csv"

    @property
    def speech_name(self):
        """Generate a unique name for speech."""
        return f"{strip_extensions(self.document_name)}@{self.speech_index}"

    @property
    def speaker_hash(self):
        """Return hash from speaker-note preceeding first utterance"""
        return None if not self.utterances else self.utterances[0].speaker_hash

    def add(self, item: Utterance) -> "Speech":
        self.utterances.append(item)
        return self


class Protocol(UtteranceMixIn):
    """Entity that represents a ParlaCLARIN document."""

    def __init__(self, date: str, name: str, utterances: list[Utterance], **_):
        self.date: str = date
        self.name: str = name
        self.utterances: list[Utterance] = utterances

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

    def get_content(self, content_type: ContentType) -> str:
        return self.text if content_type == ContentType.Text else self.tagged_text
