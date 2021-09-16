from __future__ import annotations

import abc
import contextlib
import csv
import hashlib
import re
from collections import defaultdict
from dataclasses import dataclass, field
from io import StringIO
from typing import TYPE_CHECKING, Any, Callable, List, Literal, Mapping, Optional, Tuple, Union

import pandas as pd
from loguru import logger
from pandas.io import json

from .utility import deprecated, flatten, strip_extensions

if TYPE_CHECKING:
    from .interface import IterateLevel

# pylint: disable=too-many-arguments


class ParlaClarinError(ValueError):
    ...


PARAGRAPH_MARKER: str = '@#@'


class Utterance:
    """An utterance in the ParlaClarin XML file"""

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
    def text(self) -> str:
        return self.delimiter.join(p for p in self.paragraphs if p != '').strip()

    def checksum(self) -> str:
        return hashlib.sha1(self.text.encode('utf-8')).hexdigest()[:16]


CSV_OPTS = dict(
    quoting=csv.QUOTE_MINIMAL,
    escapechar="\\",
    doublequote=False,
    sep='\t',
)


class Utterances:

    # FIXME: Consider storing as JSON instead of CSV

    @staticmethod
    def to_dict(utterances: List[Utterance]) -> List[Mapping[str, Any]]:
        return [
            {
                'u_id': u.u_id,
                'n': u.n,
                'who': u.who,
                'prev_id': u.prev_id,
                'next_id': u.next_id,
                'annotation': u.annotation,
                'paragraphs': PARAGRAPH_MARKER.join(u.paragraphs),
                'checksum': u.checksum(),
            }
            for u in utterances
        ]

    @staticmethod
    def to_dataframe(utterances: Union[StringIO, str, List[Utterance]]) -> pd.DataFrame:
        """Create a data frame from a list of utterances or a CSV string or file"""
        if isinstance(utterances, (str, StringIO)):
            df: pd.DataFrame = pd.read_csv(
                StringIO(utterances) if isinstance(utterances, str) else utterances,
                **CSV_OPTS,
                index_col='u_id',
            )
            df.drop(columns='checksum')
        else:
            df: pd.DataFrame = pd.DataFrame(Utterances.to_dict(utterances)).set_index('u_id')
        return df

    @staticmethod
    def to_csv(utterances: List[Utterance]) -> str:
        return Utterances.to_dataframe(utterances=utterances).to_csv(**CSV_OPTS, index=True)

    @staticmethod
    def from_csv(csv_str: str) -> List[Utterance]:
        df: pd.DataFrame = Utterances.to_dataframe(StringIO(csv_str))
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
        json_str = json.dumps([u.__dict__ for u in utterances])
        return json_str

    @staticmethod
    def from_json(json_str: str) -> List[Utterance]:
        data: List[Utterance] = list(map(lambda x: Utterance(**x), json.loads(json_str)))
        return data


class UtteranceMixIn:
    def to_dict(self) -> List[Mapping[str, Any]]:
        return Utterances.to_dict(self.utterances)

    def to_dataframe(self) -> pd.DataFrame:
        return Utterances.to_dataframe(self.utterances)

    def to_csv(self) -> str:
        return Utterances.to_csv(self.utterances)

    def to_json(self) -> str:
        return Utterances.to_json(self.utterances)


@dataclass
class Speech(UtteranceMixIn):
    """A processed speech entity"""

    document_name: str
    speech_id: str
    speaker: str
    speech_date: str
    speech_index: int
    page_number: str

    utterances: List[Utterance] = field(default_factory=list)

    num_tokens: int = 0
    num_words: int = 0
    delimiter: str = field(default='\n')

    # self.dedent: bool = True

    def __post_init__(self):

        if len(self.utterances or []) == 0:
            raise ParlaClarinError("utterance list cannot be empty")

        if any(self.speaker != u.who for u in self.utterances):
            raise ParlaClarinError("multiple speakes in same speech not allowed")

    # @property
    # def speech_id(self) -> Optional[str]:
    #     """Defined as id of first utterance in speech"""
    #     if len(self.utterances) == 0:
    #         return None
    #     return self.utterances[0].n
    # n = speech_id

    @property
    def filename(self):
        return f"{self.speech_name}.csv"

    @property
    def speech_name(self):
        return f"{strip_extensions(self.document_name)}@{self.speech_index}"

    @property
    def text(self):
        """The entire speech text"""
        t: str = self.delimiter.join(t for t in (u.text for u in self.utterances) if t != '')
        if not re.search('[a-zåäöA-ZÅÄÖ]', t):
            """Empty string if no letter in text"""
            return ""
        return t.strip()

    def __len__(self):
        return len(self.utterances)

    def __contains__(self, item: Union[str, Utterance]) -> bool:
        if isinstance(item, Utterance):
            item = item.u_id
        return any(u.u_id == item for u in self.utterances)

    @property
    def paragraphs(self) -> Optional[str]:
        """The flattened sequence of segments"""
        return flatten(u.paragraphs for u in self.utterances)

    def add(self, item: Utterance) -> "Speech":
        self.utterances.append(item)
        return self

    @property
    def annotation(self) -> str:

        if len(self.utterances) == 0:
            return ''

        texts: List[str] = [self.utterances[0].annotation]
        for u in self.utterances[1:]:
            text: str = u.annotation
            idx: int = text.find('\n')
            if idx <= 0 or text == '':
                continue
            text = text[idx + 1 :]
            if text != '':
                texts.append(text)
        return '\n'.join(texts)


class Protocol(UtteranceMixIn):
    def __init__(self, date: str, name: str, utterances: List[Utterance], **_):
        self.date: str = date
        self.name: str = name
        self.utterances: List[Utterance] = utterances

    def has_text(self) -> bool:
        """Checks if any utterance actually has any uttered words"""
        return any(utterance.text != "" for utterance in self.utterances)

    def preprocess(self, preprocess: Callable[[str], str] = None) -> "Protocol":
        """Extracts text and metadata of non-empty speeches. Returns list of dicts."""

        if preprocess is None:
            return self

        for utterance in self.utterances:
            utterance.paragraphs = [preprocess(p.strip()) for p in utterance.paragraphs]

        return self

    def checksum(self) -> Optional[str]:
        with contextlib.suppress(Exception):
            return hashlib.sha1(''.join(u.text for u in self.utterances).encode('utf-8')).hexdigest()
        return None

    @property
    def text(self) -> str:
        return '\n'.join(u.text for u in self.utterances).strip()

    def __len__(self):
        return len(self.utterances)

    def to_speeches(self, merge_strategy: Literal['n', 'who', 'chain'] = 'n', skip_size: int = 1) -> List[Speech]:

        speeches: List[Speech] = SpeechMergerFactory.get(merge_strategy).speeches(self, skip_size=skip_size)
        return speeches

    def to_text(self, level: IterateLevel, skip_size: int = 1) -> List[Tuple[str, str, str, str, str]]:

        items: List[Tuple[str, str, str, str]] = []

        if level.startswith('protocol'):

            items = [(self.name, None, self.name, self.text, '0')]

        elif level.startswith('speech'):

            items = [
                (self.name, s.speaker, s.speech_id, s.text, s.page_number)
                for s in self.to_speeches('n', skip_size=skip_size)
            ]

        elif level.startswith('speaker'):

            items = [
                (self.name, s.speaker, s.speech_id, s.text, s.page_number)
                for s in self.to_speeches('who', skip_size=skip_size)
            ]

        elif level.startswith('utterance'):

            items = [(self.name, u.who, u.u_id, u.text, u.page_number) for u in self.utterances]

        elif level.startswith('paragraph'):

            items = [
                (self.name, u.who, f"{u.u_id}@{i}", p, u.page_number)
                for u in self.utterances
                for i, p in enumerate(u.paragraphs)
            ]

        else:
            raise ValueError(f"unexpected argument: {level}")

        if skip_size > 0:

            items = [x for x in items if len(x[3]) > skip_size]

        return items


class IMergeSpeechStrategy(abc.ABC):
    def create(self, protocol: Protocol, utterances: List[Utterance] = None, speech_index: int = 0) -> Speech:
        """Create a new speech entity."""

        if utterances is None:
            utterances = protocol.utterances

        return Speech(
            document_name=protocol.name,
            speech_id=self.speech_id(utterances),
            speaker=utterances[0].who,
            page_number=utterances[0].page_number,
            speech_date=protocol.date,
            speech_index=speech_index,
            utterances=utterances,
        )

    @abc.abstractmethod
    def speech_id(self, utterances: List[Utterance]) -> str:
        return ''

    def speeches(self, protocol: Protocol, skip_size: int = 1) -> List[Speech]:

        speeches: List[Speech] = self.merge(protocol=protocol)

        if skip_size > 0:
            speeches = [s for s in speeches if len(s.text or "") >= skip_size]

        return speeches

    @abc.abstractmethod
    def merge(self, protocol: Protocol) -> List[Speech]:
        return []


class MergeSpeechById(IMergeSpeechStrategy):
    def merge(self, protocol: Protocol) -> List[Speech]:
        """Create a speech per unique u:n value. Return list of Speech."""
        data = defaultdict(list)
        for u in protocol.utterances:
            data[u.n].append(u)
        return [self.create(protocol, utterances=data[n], speech_index=i + 1) for i, n in enumerate(data)]

    def speech_id(self, utterances: List[Utterance]) -> str:
        return utterances[0].n


class MergeSpeechByWho(IMergeSpeechStrategy):
    def merge(self, protocol: Protocol) -> List[Speech]:
        """Create a speech per unique speaker. Return list of Speech."""
        data = defaultdict(list)
        for u in protocol.utterances:
            data[u.who].append(u)
        return [self.create(protocol, utterances=data[who], speech_index=i + 1) for i, who in enumerate(data)]

    def speech_id(self, utterances: List[Utterance]) -> str:
        return utterances[0].who


class MergeSpeechByChain(IMergeSpeechStrategy):
    @deprecated
    def merge(self, protocol: Protocol) -> List[Speech]:
        """Create speeches based on prev/next chain. Return list."""
        speeches: List[Speech] = []
        speech: Speech = None

        next_id: str = None

        for _, u in enumerate(protocol.utterances or []):

            prev_id: str = u.prev_id

            if next_id is not None:
                if next_id != u.u_id:
                    logger.error(
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

    def speech_id(self, utterances: List[Utterance]) -> str:
        return utterances[0].u_id


class SpeechMergerFactory:
    class UndefinedMergeSpeech(IMergeSpeechStrategy):
        def merge(self, protocol: Protocol) -> List[Speech]:
            return []

        def speech_id(self, utterances: List[Utterance]) -> str:
            return ""

    strategies: Mapping[Literal['n', 'who', 'chain'], "IMergeSpeechStrategy"] = {
        'n': MergeSpeechById(),
        'who': MergeSpeechByWho(),
        'chain': MergeSpeechByChain(),
    }

    undefined = UndefinedMergeSpeech()

    @staticmethod
    def get(strategy: str) -> IMergeSpeechStrategy:
        return SpeechMergerFactory.strategies.get(strategy, SpeechMergerFactory.undefined)
