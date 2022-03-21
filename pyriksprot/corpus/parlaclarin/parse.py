from __future__ import annotations

import abc
import xml.etree.cElementTree as ET
from typing import Iterable, List, Union

from loguru import logger

from pyriksprot import interface
from pyriksprot.foss import untangle
from pyriksprot.utility import dedent as dedent_text
from pyriksprot.utility import deprecated


class XmlProtocol(abc.ABC):
    def __init__(self, data: str, segment_skip_size: int = 0, delimiter: str = '\n'):

        self.data: str = data
        self.segment_skip_size: bool = segment_skip_size
        self.delimiter: str = delimiter

        self.iterator: Iterable[interface.Utterance] = self.create_iterator()
        self.utterances: List[interface.Utterance] = self.create_utterances()

        self.date = self.get_date()
        self.name = self.get_name()
        self.year = int(self.date[:4])

    @abc.abstractmethod
    def create_iterator(self) -> Iterable[interface.Utterance]:
        return []

    @abc.abstractmethod
    def create_utterances(self) -> List[interface.Utterance]:
        ...

    @abc.abstractmethod
    def get_name(self) -> str:
        ...

    @abc.abstractmethod
    def get_date(self) -> str:
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

        super().__init__(data, segment_skip_size, delimiter)

    def create_iterator(self) -> Iterable[interface.Utterance]:
        return []

    def create_utterances(self) -> List[interface.Utterance]:
        utterances: List[interface.Utterance] = []
        page_number: str = None

        parent: untangle.Element = self.get_content_root()
        if parent is None:
            return utterances

        speaker_hash: str = ''

        for child in parent.children:
            if child.name == 'pb':
                page_number = child['n']
            elif child.name == "note":
                if child['type'] == "speaker":
                    speaker_hash = child["n"]
            elif child.name == 'u':
                utterances.append(
                    UtteranceMapper.create(element=child, page_number=page_number, speaker_hash=speaker_hash)
                )
            # else:
            #     speaker_hash = None
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


class UtteranceMapper:
    """Wraps a single ParlaClarin XML utterance tag."""

    @staticmethod
    def create(
        *,
        element: untangle.Element,
        page_number: str,
        speaker_hash: str,
        dedent: bool = True,
    ) -> interface.Utterance:
        utterance: interface.Utterance = interface.Utterance(
            u_id=element.get_attribute('xml:id'),
            speaker_hash=speaker_hash,
            who=element.get_attribute('who') or "undefined",
            page_number=page_number,
            prev_id=element.get_attribute('prev'),
            next_id=element.get_attribute('next'),
            paragraphs=UtteranceMapper.to_paragraphs(element, dedent),
            n=element.get_attribute('n'),
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
        )

        return protocol


@deprecated
class XmlIterParseProtocol(XmlProtocol):
    """Load ParlaClarin XML file using SAX parsing."""

    def create_iterator(self) -> Iterable[interface.Utterance]:
        return XmlIterParseProtocol.XmlIterParser(self.data)

    def create_utterances(self) -> List[interface.Utterance]:
        return list(self.iterator)

    def get_date(self) -> str:
        return self.iterator.doc_date

    def get_name(self) -> str:
        return self.iterator.doc_name

    class XmlIterParser:
        def __init__(self, filename: str):

            self.doc_date = None
            self.doc_name = None
            self.filename = filename
            self.iterator = None
            self.dedent: bool = True

        def __iter__(self):
            self.iterator = self.create_iterator()
            return self

        def __next__(self):
            return next(self.iterator)

        def create_iterator(self) -> Iterable[interface.Utterance]:

            context = ET.iterparse(self.filename, events=("start", "end"))

            context = iter(context)
            current_page: int = 0
            current_utterance: dict = None
            speaker_hash: str = ''
            is_preface: bool = False

            for event, elem in context:

                tag = elem.tag.rpartition('}')[2]
                value = elem.text

                if event == 'start':

                    if tag == 'head' and is_preface:
                        self.doc_name = value

                    elif tag == 'docDate' and is_preface:
                        self.doc_date = elem.attrib.get('when')

                    elif tag == "note" and elem.attrib.get('type') == "speaker":
                        speaker_hash = elem.attrib['n']

                    elif tag == "pb":
                        current_page = elem.attrib['n']

                    elif tag == "u":
                        is_preface = False
                        current_utterance: interface.Utterance = interface.Utterance(
                            page_number=current_page,
                            speaker_hash=speaker_hash,
                            u_id=elem.attrib.get('{http://www.w3.org/XML/1998/namespace}id'),
                            who=elem.attrib.get('who'),
                            prev_id=elem.attrib.get('prev'),
                            next_id=elem.attrib.get('next'),
                            n=elem.attrib.get('n'),
                            paragraphs=[],
                        )
                        # speaker_hash = None
                    elif tag == "seg" and value is not None:
                        value = (dedent_text(value) if self.dedent else value).strip()
                        if value:
                            current_utterance.paragraphs.append(value)

                    elif tag == 'div' and elem.attrib.get('type') == 'preface':
                        is_preface = True

                    # else:
                    #     speaker_hash = None

                elif event == 'end':

                    if tag == "seg" and value is not None:
                        value = (dedent_text(value) if self.dedent else value).strip()
                        if value:
                            current_utterance.paragraphs.append(value)

                    elif tag == 'u':
                        yield current_utterance
                        current_utterance = None

                    elif tag == 'div' and elem.attrib.get('type') == 'preface':
                        is_preface = False

                elem.clear()
