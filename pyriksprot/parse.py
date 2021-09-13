import os
import textwrap
from io import StringIO
from typing import Iterable, List, Literal, Tuple, Union

import untangle  # pylint: disable=import-error

from . import model
from .utility import hasattr_path, path_add_suffix


class XML_Utterance:
    """Wraps a single ParlaClarin XML utterance tag."""

    def __init__(self, utterance: untangle.Element, dedent: bool = True, delimiter: str = '\n'):

        if not isinstance(utterance, untangle.Element):
            raise TypeError("expected untangle.Element")

        self.utterance: untangle.Element = utterance
        self.dedent: bool = dedent
        self.delimiter: str = delimiter

    @property
    def who(self) -> str:
        try:
            return self.utterance.get_attribute('who') or "undefined"
        except (AttributeError, KeyError):
            return 'undefined'

    @property
    def u_id(self) -> str:
        return self.utterance.get_attribute('xml:id')

    @property
    def prev_id(self) -> str:
        return self.utterance.get_attribute('prev')

    @property
    def next_id(self) -> str:
        return self.utterance.get_attribute('next')

    @property
    def n(self) -> str:
        return self.utterance.get_attribute('n')

    @property
    def paragraphs(self) -> List[str]:
        texts: Iterable[str] = (p.cdata.strip() for p in self.utterance.get_elements('seg'))
        if self.dedent:
            texts = [textwrap.dedent(t).strip() for t in texts]
        return texts

    @property
    def text(self) -> str:
        return self.delimiter.join(self.paragraphs)


class XML_Protocol:
    """Wraps the XML representation of a single ParlaClarin document (protocol)"""

    def __init__(
        self,
        data: Union[str, untangle.Element],
        skip_size: int = 0,
    ):
        """
        Args:
            data (untangle.Element): XML document
        """
        self.data: untangle.Element = None
        if isinstance(data, untangle.Element):
            self.data = data
        elif isinstance(data, str):
            if os.path.isfile(data):
                self.data = untangle.parse(data)
            else:
                self.data = untangle.parse(StringIO(data))
        else:
            raise ValueError("invalid data for untangle")

        self._utterances: List[XML_Utterance] = (
            [XML_Utterance(u) for u in self.data.teiCorpus.TEI.text.body.div.u]
            if hasattr_path(self.data, 'teiCorpus.TEI.text.body.div.u')
            else []
        )

        self.skip_size: bool = skip_size

    @property
    def date(self) -> str:
        """Date of protocol"""
        try:
            docDate = self.data.teiCorpus.TEI.text.front.div.docDate
            return docDate[0]['when'] if isinstance(docDate, list) else docDate['when']
        except (AttributeError, KeyError):
            return None

    @property
    def name(self) -> str:
        """Protocol name"""
        try:
            return self.data.teiCorpus.TEI.text.front.div.head.cdata
        except (AttributeError, KeyError):
            return None

    @property
    def utterances(self) -> List[XML_Utterance]:
        """Return sequence of XML_Utterances."""
        return self._utterances


class ProtocolMapper:
    @staticmethod
    def to_protocol(
        data: Union[str, untangle.Element],
        skip_size: int = 0,
    ) -> model.Protocol:
        """Map XML to domain entity. Return Protocol."""

        xml_protocol: XML_Protocol = XML_Protocol(data=data, skip_size=skip_size)

        protocol: model.Protocol = model.Protocol(
            utterances=[ProtocolMapper.to_utterance(u) for u in xml_protocol.utterances],
            date=xml_protocol.date,
            name=xml_protocol.name,
        )

        return protocol

    @staticmethod
    def to_utterance(u: XML_Utterance) -> model.Utterance:
        """Map XML wrapper to domain entity. Return Utterance."""
        return model.Utterance(
            u_id=u.u_id,
            n=u.n,
            who=u.who,
            prev_id=u.prev_id,
            next_id=u.next_id,
            paragraphs=u.paragraphs,
        )


IterateLevel = Literal['protocol', 'speech', 'utterance', 'paragraph']


class ProtocolTextIterator:
    """Reads xml files and returns a stream of (name, text)"""

    def __init__(
        self, *, filenames: List[str], level: IterateLevel, merge_strategy: IterateLevel = 'n', skip_size: int = 1
    ):
        self.filenames: List[str] = filenames
        self.iterator = None
        self.skip_size: int = skip_size
        self.level: IterateLevel = level
        self.merge_strategy: IterateLevel = merge_strategy

    def __iter__(self):
        self.iterator = self.create_iterator()
        return self

    def __next__(self):
        return next(self.iterator)

    @property
    def protocols(self) -> Iterable[Tuple[str, model.Protocol]]:
        return (
            (filename, ProtocolMapper.to_protocol(data=filename, skip_size=self.skip_size))
            for filename in self.filenames
        )

    def create_iterator(self):

        if self.level.startswith('protocol'):

            for filename, protocol in self.protocols:
                yield protocol.name, protocol.text

        elif self.level.startswith('speech'):

            for filename, protocol in self.protocols:
                for speech in protocol.to_speeches(self.merge_strategy, skip_size=self.skip_size):
                    yield path_add_suffix(filename, speech.speech_id), speech.text

        elif self.level.startswith('utterance'):

            for filename, protocol in self.protocols:
                for utterance in protocol.utterances:
                    yield f"{protocol.name}_{utterance.who}_{utterance.u_id}", utterance.text

        elif self.level.startswith('paragraph'):

            for filename, protocol in self.protocols:
                for utterance in protocol.utterances:
                    for i, p in enumerate(utterance.paragraphs):
                        yield f"{protocol.name}_{utterance.who}_{utterance.u_id}@{i}", p
        else:
            raise ValueError(f"unexpected argument: {self.level}")
