from __future__ import annotations

import abc
import xml.etree.cElementTree as ET
from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import Callable, Iterable, List, Union

from loguru import logger

from .. import interface
from ..foss import untangle
from ..utility import dedent as dedent_text
from ..utility import deprecated


@dataclass
class IterUtterance:
    page_number: str
    u_id: str
    who: str
    prev_id: str
    next_id: str
    n: str
    paragraphs: str
    delimiter: str = '\n'

    @property
    def text(self) -> str:
        return self.delimiter.join(self.paragraphs).strip()


class XmlProtocol(abc.ABC):
    def __init__(self, data: str, segment_skip_size: int = 0, delimiter: str = '\n'):

        self.data: str = data
        self.segment_skip_size: bool = segment_skip_size
        self.delimiter: str = delimiter

        self.iterator: Iterable[IterUtterance] = self.create_iterator()
        self.utterances: List[IterUtterance] = self.create_utterances()

        self.date = self.get_date()
        self.name = self.get_name()

    @abc.abstractmethod
    def create_iterator(self) -> Iterable[IterUtterance]:
        return []

    @abc.abstractmethod
    def create_utterances(self) -> List[IterUtterance]:
        ...

    @abc.abstractmethod
    def get_name(self) -> str:
        ...

    @abc.abstractmethod
    def get_date(self) -> str:
        ...

    def __len__(self):
        return len(self.utterances)

    def get_year(self):
        return int(self.get_date()[:4])

    @property
    def text(self) -> str:
        """Return sequence of XML_Utterances."""
        return self.delimiter.join(x.text for x in self.utterances)

    @property
    def has_text(self) -> bool:
        """Return sequence of XML_Utterances."""
        return any(x.text != '' for x in self.utterances)

    def to_text(
        self,
        *,
        segment_level: interface.SegmentLevel,
        segment_skip_size: int = 0,
        preprocess: Callable[[str], str] = None,
    ) -> Iterable[interface.ProtocolSegment]:
        """Generate text blocks from `protocol`. Yield each block as a tuple (name, who, id, text, page_number)."""
        try:

            if segment_level in [interface.SegmentLevel.Protocol, None]:

                items: Iterable[interface.ProtocolSegment] = [
                    interface.ProtocolSegment(
                        protocol_name=self.name,
                        content_type=interface.ContentType.Text,
                        name=self.name,
                        who=None,
                        id=self.name,
                        data=self.text,
                        page_number='0',
                        year=self.get_year(),
                    )
                ]

            elif segment_level == interface.SegmentLevel.Who:

                data, page_numbers = defaultdict(list), {}
                for u in self.utterances:
                    data[u.who].append(u.text)
                    if u.who not in page_numbers:
                        page_numbers[u.who] = u.page_number

                items: Iterable[interface.ProtocolSegment] = [
                    interface.ProtocolSegment(
                        protocol_name=self.name,
                        content_type=interface.ContentType.Text,
                        name=f'{self.name}_{i+1:03}',
                        who=who,
                        id=who,
                        data='\n'.join(data[who]),
                        page_number=str(page_numbers[who]),
                        year=self.get_year(),
                    )
                    for i, who in enumerate(data)
                ]

            elif segment_level == interface.SegmentLevel.Speech:

                data, who, page_numbers = defaultdict(list), {}, {}
                for u in self.utterances:
                    data[u.n].append(u.text)
                    who[u.n] = u.who
                    if u.n not in page_numbers:
                        page_numbers[u.n] = u.page_number

                items: Iterable[interface.ProtocolSegment] = [
                    interface.ProtocolSegment(
                        protocol_name=self.name,
                        content_type=interface.ContentType.Text,
                        name=f'{self.name}_{i+1:03}',
                        who=who[n],
                        id=n,
                        data='\n'.join(data[n]),
                        page_number=str(page_numbers[n]),
                        year=self.get_year(),
                    )
                    for i, n in enumerate(data)
                ]

            elif segment_level == interface.SegmentLevel.Utterance:

                items: Iterable[interface.ProtocolSegment] = [
                    interface.ProtocolSegment(
                        protocol_name=self.name,
                        content_type=interface.ContentType.Text,
                        name=f'{self.name}_{i+1:03}',
                        who=u.who,
                        id=u.u_id,
                        data=u.text,
                        page_number=str(u.page_number),
                        year=self.get_year(),
                    )
                    for i, u in enumerate(self.utterances)
                ]

            elif segment_level == interface.SegmentLevel.Paragraph:

                items: Iterable[interface.ProtocolSegment] = [
                    interface.ProtocolSegment(
                        protocol_name=self.name,
                        content_type=interface.ContentType.Text,
                        name=f'{self.name}_{j+1:03}_{i+1:03}',
                        who=u.who,
                        id=f"{u.u_id}@{i}",
                        data=p,
                        page_number=str(u.page_number),
                        year=self.get_year(),
                    )
                    for j, u in enumerate(self.utterances)
                    for i, p in enumerate(u.paragraphs)
                ]

            else:
                raise ValueError(f"undefined segment level {segment_level}")

            if preprocess is not None:
                for item in items:
                    item.text = preprocess(item.text)

            if segment_skip_size > 0:

                items = [item for item in items if len(item.text) > segment_skip_size]

            return items

        except Exception as ex:
            raise ex


class XmlUntangleProtocol(XmlProtocol):
    """Wraps the XML representation of a single ParlaClarin document (protocol)"""

    def __init__(self, data: Union[str, untangle.Element], segment_skip_size: int = 0, delimiter: str = '\n'):

        data: untangle.Element = (
            data if isinstance(data, untangle.Element) else untangle.parse(data, ignore_tags={'note', 'teiHeader'})
        )

        super().__init__(data, segment_skip_size, delimiter)

    def create_iterator(self) -> Iterable[IterUtterance]:
        return []

    def create_utterances(self) -> List[IterUtterance]:
        utterances: List[IterUtterance] = []
        page_number: str = None

        parent: untangle.Element = self.get_content_root()
        if parent is None:
            return utterances

        for child in parent.children:
            if child.name == 'pb':
                page_number = child['n']
            elif child.name == 'u':
                utterances.append(
                    UtteranceMapper.create(element=child, delimiter=self.delimiter, page_number=page_number)
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


class UtteranceMapper:
    """Wraps a single ParlaClarin XML utterance tag."""

    @staticmethod
    def create(
        element: untangle.Element, page_number: str, dedent: bool = True, delimiter: str = '\n'
    ) -> IterUtterance:
        utterance: IterUtterance = IterUtterance(
            page_number=page_number,
            u_id=element.get_attribute('xml:id'),
            who=element.get_attribute('who') or "undefined",
            prev_id=element.get_attribute('prev'),
            next_id=element.get_attribute('next'),
            n=element.get_attribute('n'),
            paragraphs=UtteranceMapper.to_paragraphs(element, dedent),
            delimiter=delimiter,
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
    def to_protocol(data: Union[str, untangle.Element], segment_skip_size: int = 0) -> interface.Protocol:
        """Map XML to domain entity. Return Protocol."""

        xml_protocol: XmlUntangleProtocol = XmlUntangleProtocol(data=data, segment_skip_size=segment_skip_size)

        protocol: interface.Protocol = interface.Protocol(
            utterances=[interface.Utterance(**asdict(u)) for u in xml_protocol.utterances],
            date=xml_protocol.date,
            name=xml_protocol.name,
        )

        return protocol


@deprecated
class XmlIterParseProtocol(XmlProtocol):  # (ProtocolSegmentIterator):
    """Load ParlaClarin XML file using SAX parsing."""

    def create_iterator(self) -> Iterable[IterUtterance]:
        return XmlIterParseProtocol.XmlIterParser(self.data)

    def create_utterances(self) -> List[IterUtterance]:
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

        def create_iterator(self) -> Iterable[IterUtterance]:

            context = ET.iterparse(self.filename, events=("start", "end"))

            context = iter(context)
            current_page: int = 0
            current_utterance: dict = None
            is_preface: bool = False

            for event, elem in context:

                tag = elem.tag.rpartition('}')[2]
                value = elem.text

                if event == 'start':

                    if tag == 'head' and is_preface:
                        self.doc_name = value

                    elif tag == 'docDate' and is_preface:
                        self.doc_date = elem.attrib.get('when')

                    elif tag == "pb":
                        current_page = elem.attrib['n']

                    elif tag == "u":
                        is_preface = False
                        current_utterance: IterUtterance = IterUtterance(
                            page_number=current_page,
                            u_id=elem.attrib.get('{http://www.w3.org/XML/1998/namespace}id'),
                            who=elem.attrib.get('who'),
                            prev_id=elem.attrib.get('prev'),
                            next_id=elem.attrib.get('next'),
                            n=elem.attrib.get('n'),
                            paragraphs=[],
                        )
                    elif tag == "seg" and value is not None:
                        value = (dedent_text(value) if self.dedent else value).strip()
                        if value:
                            current_utterance.paragraphs.append(value)

                    elif tag == 'div' and elem.attrib.get('type') == 'preface':
                        is_preface = True

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
