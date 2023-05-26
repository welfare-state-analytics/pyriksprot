from __future__ import annotations

import abc
import contextlib
import csv
import functools
import gzip
import hashlib
import re
import sys
import zipfile
from dataclasses import dataclass, field
from enum import Enum
from io import StringIO, TextIOWrapper
from typing import Any, Callable, Literal, Mapping, Optional, Union

import pandas as pd
from pandas.io import json

from .utility import flatten, merge_csv_strings, strip_csv_header, strip_extensions, xml_escape

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
    Sentence = 'sentence'


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


@dataclass
class SpeakerNote:
    speaker_note_id: str
    speaker_note: str


MISSING_SPEAKER_NOTE: SpeakerNote = SpeakerNote(MISSING_SPEAKER_NOTE_ID, "")


def vrt_exporter(func):
    @functools.wraps(func)
    def exporter(self, *tags, **kwargs):
        keys: set[str] = {e.value for e in SegmentLevel}
        if any(tag not in keys for tag in tags):
            raise ValueError(f"Unknown tag: {tags}")

        return func(self, *tags, **kwargs)

    return exporter


class Utterance:
    """Represents an utterance in the ParlaClarin XML file"""

    delimiter: str = '\n'

    def __init__(
        self,
        *,
        u_id: str,
        who: str,
        prev_id: str = None,
        next_id: str = None,
        paragraphs: Union[list[str], str] = None,
        annotation: Optional[str] = None,
        page_number: Optional[int] = 0,
        speaker_note_id: str = MISSING_SPEAKER_NOTE.speaker_note_id,
        **_,
    ):
        self.u_id: str = u_id
        self.who: str = who
        self.prev_id: str = prev_id if isinstance(prev_id, str) else None
        self.next_id: str = next_id if isinstance(next_id, str) else None
        self.paragraphs: list[str] = (
            [] if not paragraphs else paragraphs if isinstance(paragraphs, list) else paragraphs.split(PARAGRAPH_MARKER)
        )
        self.annotation: Optional[str] = annotation if isinstance(annotation, str) else None
        self.page_number: Optional[int] = page_number if isinstance(page_number, int) else None
        self.speaker_note_id: str = speaker_note_id

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
        return UtteranceHelper.compute_checksum(self.text)

    def to_str(self, what: ContentType) -> str:
        return self.tagged_text if what == ContentType.TaggedFrame else self.text

    def to_dict(self) -> dict:
        return UtteranceHelper.to_dict(self)

    @vrt_exporter
    def to_vrt(self, *tags: tuple[str]) -> str:
        if not tags:
            return xml_escape(strip_csv_header(self.annotation))

        if 'sentence' in tags or 'paragraph' in tags:
            # TODO: Tagged data frames must contain sentence/paragraph markers
            # 1) Frame has a sentence/paragraph id column
            # 2) Or sentence or paragraph markers are included in the tagged text on separate lines
            raise NotImplementedError("Export to VRT with sentence/paragraph structural tags is not implemented yet.")

        vrt_str: str = xml_escape(strip_csv_header(self.annotation))

        if 'utterance' in tags:
            vrt_str = (
                f'<utterance id="{self.u_id}" page="{self.page_number}" who="{self.who}">\n{vrt_str}</utterance>\n'
            )

        return vrt_str


class UtteranceHelper:
    CSV_OPTS = dict(
        quoting=csv.QUOTE_MINIMAL,
        escapechar="\\",
        doublequote=False,
        sep='\t',
    )

    @staticmethod
    def compute_paragraph_checksum(text: str | list[str]) -> str:
        """Compute checksum of given text."""
        if isinstance(text, str) and PARAGRAPH_MARKER in text:
            text = text.split(PARAGRAPH_MARKER)

        if isinstance(text, list):
            text = Utterance.delimiter.join(p for p in text if p != '').strip()

        return UtteranceHelper.compute_checksum(text or "")

    @staticmethod
    def compute_checksum(text: str) -> str:
        """Compute checksum of given text."""
        return hashlib.sha1(text.encode('utf-8')).hexdigest()[:16]

    @staticmethod
    def to_dict(u: Utterance) -> dict[str, Any]:
        return {
            'u_id': u.u_id,
            'who': u.who,
            'speaker_note_id': u.speaker_note_id,
            'prev_id': u.prev_id,
            'next_id': u.next_id,
            'annotation': u.tagged_text,
            'paragraphs': PARAGRAPH_MARKER.join(u.paragraphs),
            'page_number': u.page_number,
            'checksum': u.checksum(),
        }

    @staticmethod
    def to_dicts(utterances: list[Utterance]) -> list[Mapping[str, Any]]:
        """Convert list of utterances to a list of dicts. Return the list."""
        return [UtteranceHelper.to_dict(u) for u in utterances]

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
            df: pd.DataFrame = pd.DataFrame(UtteranceHelper.to_dicts(utterances)).set_index('u_id')
        return df

    @staticmethod
    def to_vrt(utterances: list[Utterance], structural_tags: str = "") -> str:
        """Convert list of utterances to a VRT string. Return VRT string."""
        return '\n'.join(u.to_vrt(structural_tags) for u in utterances)

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
        return UtteranceHelper.to_dicts(self.utterances)

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
    page_number: int

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

    @vrt_exporter
    def to_vrt(self, *tags: tuple[str]) -> str:
        if tags is None:
            return xml_escape(strip_csv_header(self.tagged_text))
        vrt_str: str = '\n'.join(u.to_vrt(*tags) for u in self.utterances)
        if 'speech' in tags:
            vrt_str: str = (
                '<speech '
                f'id="{self.speech_id}" '
                f'title="{self.document_name}" '
                f'who="{self.who}" '
                f'date="{self.speech_date}" '
                f'page="{self.page_number}"'
                '>\n'
                f'{vrt_str}\n'
                '</speech>'
            )

        return vrt_str


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

    @vrt_exporter
    def to_vrt(self, *tags: tuple[str], merge_strategy: str = "chain_consecutive_unknowns") -> str:
        """Export protocol to VRT format with optional structural tags."""

        """Local import do avoid circular dependency"""
        from . import to_speech as mu  # pylint: disable=import-outside-toplevel

        if tags is None:
            return xml_escape(strip_csv_header(self.tagged_text))

        vrt_str: str = ''
        if 'speech' in tags:
            speeches: list[Speech] = mu.to_speeches(protocol=self, merge_strategy=merge_strategy)
            vrt_str: str = '\n'.join(s.to_vrt(*tags) for s in speeches)
        else:
            vrt_str: str = '\n'.join(u.to_vrt(*tags) for u in self.utterances)

        if 'protocol' in tags:
            vrt_str: str = f'<protocol title="{self.name}" date="{self.date}">\n{vrt_str}\n</protocol>'

        return vrt_str

    @classmethod
    def to_vrts(
        cls,
        protocols: list["Protocol"],
        *tags: tuple[str],
        output: None | str = None,
        outer_tag: str = None,
        **outer_tag_attribs,
    ) -> str:
        """Export multiple protocols to VRT format with optional structural tags."""

        if isinstance(output, str) and output.endswith("zip"):
            if outer_tag:
                raise ValueError("Cannot write to zip file with outer tag")

            with zipfile.ZipFile(output, 'w') as zink:
                for p in protocols:
                    zink.writestr(f"{p.name}.vrt", p.to_vrt(*tags))
                return None

        with _open_output(output) as zink:
            if outer_tag:
                zink.write(_xml_start_tag(outer_tag, **outer_tag_attribs) + '\n')
            for p in protocols:
                zink.write(p.to_vrt(*tags))
                zink.write('\n')
            if outer_tag:
                zink.write(f'</{outer_tag}>')
            if isinstance(zink, StringIO):
                return zink.getvalue()
            return None


def _open_output(output: str) -> TextIOWrapper:
    if output == '-':
        return contextlib.nullcontext(sys.stdout)
    if output is None:
        return StringIO('', newline='')
    if output.endswith('.gz'):
        return gzip.open(output, 'wt', encoding="utf8")
    return open(output, 'w', encoding="utf8")


def _xml_start_tag(tag: str, **attribs) -> str:
    """Generate XML open tag with optional attributes."""
    attrib_str: str = " ".join((f"{k}=\"{v}\"" for k, v in attribs.items()))
    return f"<{tag} {attrib_str}>"


class IProtocolParser(abc.ABC):
    @staticmethod
    def parse(filename: str, ignore_tags: set[str]) -> IProtocol:  # pylint: disable=unused-argument
        ...
