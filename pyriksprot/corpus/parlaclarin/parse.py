from __future__ import annotations

import abc
from typing import Iterable, List, Literal, Union

from loguru import logger

from pyriksprot import interface
from pyriksprot.foss import untangle
from pyriksprot.utility import dedent as dedent_text

XML_ID: str = '{http://www.w3.org/XML/1998/namespace}id'

# pylint: disable=too-many-statements


class XmlProtocol(abc.ABC):
    def __init__(self, data: str, segment_skip_size: int = 0, delimiter: str = '\n'):

        self.data: str = data
        self.segment_skip_size: bool = segment_skip_size
        self.delimiter: str = delimiter

        self.iterator: Iterable[interface.Utterance] = self.create_iterator()
        self.utterances: List[interface.Utterance] = self.create_utterances()

        self.date = self.get_date()
        self.name = self.get_name()

    @abc.abstractmethod
    def create_iterator(self) -> Iterable[interface.Utterance]:
        return []

    def get_year(self, which: Literal["filename", "date"] = "filename") -> int:
        """Returns protocol's year either extracted from filename or from `date` tag in XML header"""
        if which != "filename":
            return int(self.date[:4])
        return int(self.name.split("-")[1][:4])

    @abc.abstractmethod
    def create_utterances(self) -> List[interface.Utterance]:
        ...

    @abc.abstractmethod
    def get_name(self) -> str:
        ...

    @abc.abstractmethod
    def get_date(self) -> str:
        ...

    @abc.abstractmethod
    def get_speaker_notes(self) -> dict[str, str]:
        ...

    def __len__(self):
        return len(self.utterances)

    @property
    def text(self) -> str:
        """Return sequence of XML_Utterances."""
        return self.delimiter.join(x.text for x in self.utterances)

    @property
    def has_text(self) -> bool:
        """Return sequence of XML_Utterances."""
        return any(x.text != '' for x in self.utterances)


class XmlUntangleProtocol(XmlProtocol):
    """Wraps the XML representation of a single ParlaClarin document (protocol)"""

    def __init__(
        self,
        data: Union[str, untangle.Element],
        segment_skip_size: int = 0,
        delimiter: str = '\n',
        ignore_tags: set[str] | str = "teiHeader",
    ):

        ignore_tags: set[str] = set(ignore_tags.split(",")) if isinstance(ignore_tags, str) else ignore_tags

        data: untangle.Element = (
            data if isinstance(data, untangle.Element) else untangle.parse(data, ignore_tags=ignore_tags)
        )
        self.speaker_notes: dict[str, str] = {}

        super().__init__(data, segment_skip_size, delimiter)

    def create_iterator(self) -> Iterable[interface.Utterance]:
        return []

    def create_utterances(self) -> List[interface.Utterance]:

        utterances: List[interface.Utterance] = []
        page_number: int = -1

        parent: untangle.Element = self.get_content_root()
        if parent is None:
            return utterances

        speaker_note_id: str = interface.MISSING_SPEAKER_NOTE_ID
        previous_who: str = None

        for child in parent.children:

            if child.name == 'pb':
                page_number += 1

            elif child.name == "note":

                if child['type'] == "speaker":
                    speaker_note_id = child["xml:id"]

                    if not speaker_note_id:
                        raise ValueError("no xml:id in speaker's note tag")

                    self.speaker_notes[speaker_note_id] = " ".join(child.cdata.split())

                    previous_who: str = None

            elif child.name == 'u':

                who: str = child.get_attribute('who')

                if previous_who and previous_who != who:
                    speaker_note_id = interface.MISSING_SPEAKER_NOTE_ID

                previous_who: str = who

                utterances.append(
                    UtteranceMapper.create(
                        element=child,
                        page_number=str(page_number),
                        speaker_note_id=speaker_note_id,
                    )
                )

        return utterances

    def get_date(self) -> str:
        try:
            docDate = self.data.teiCorpus.TEI.text.front.div.docDate
            return docDate[0]['when'] if isinstance(docDate, list) else docDate['when']
        except (AttributeError, KeyError):
            return None

    def get_name(self) -> str:
        """Protocol name"""
        try:
            return self.data.teiCorpus.TEI.text.front.div.head.cdata
        except (AttributeError, KeyError):
            return None

    def get_content_root(self) -> untangle.Element:
        try:
            return self.data.teiCorpus.TEI.text.body.div
        except AttributeError:
            logger.warning(f'no content (text.body) found in {self.get_name()}')
        return None

    def get_speaker_notes(self) -> dict[str, str]:
        return self.speaker_notes


class UtteranceMapper:
    """Wraps a single ParlaClarin XML utterance tag."""

    @staticmethod
    def create(
        *,
        element: untangle.Element,
        page_number: str,
        speaker_note_id: str,
        dedent: bool = True,
    ) -> interface.Utterance:
        utterance: interface.Utterance = interface.Utterance(
            u_id=element.get_attribute('xml:id'),
            speaker_note_id=speaker_note_id,
            who=element.get_attribute('who') or "undefined",
            page_number=page_number,
            prev_id=element.get_attribute('prev'),
            next_id=element.get_attribute('next'),
            paragraphs=UtteranceMapper.to_paragraphs(element, dedent),
        )
        return utterance

    @staticmethod
    def to_paragraphs(element: untangle.Element, dedent: bool = True) -> List[str]:
        texts: Iterable[str] = (p.cdata for p in element.get_elements('seg'))
        if dedent:
            texts = [dedent_text(t) for t in texts]
        return texts


class ProtocolMapper:
    @staticmethod
    def to_protocol(
        data: Union[str, untangle.Element],
        segment_skip_size: int = 0,
        ignore_tags: set[str] | str = "teiHeader",
    ) -> interface.Protocol:
        """Map XML to domain entity. Return Protocol."""

        xml_protocol: XmlUntangleProtocol = XmlUntangleProtocol(
            data=data, segment_skip_size=segment_skip_size, ignore_tags=ignore_tags
        )

        protocol: interface.Protocol = interface.Protocol(
            utterances=xml_protocol.utterances,
            date=xml_protocol.date,
            name=xml_protocol.name,
            speaker_notes=xml_protocol.get_speaker_notes(),
        )

        return protocol
