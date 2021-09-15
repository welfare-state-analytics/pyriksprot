from __future__ import annotations

import os
import textwrap
from collections import defaultdict
from io import StringIO
from typing import TYPE_CHECKING, Iterable, List, Tuple, Union

import untangle  # pylint: disable=import-error

from . import model
from .utility import hasattr_path

if TYPE_CHECKING:
    from .interface import IterateLevel


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
        delimiter: str = '\n',
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
        self.delimiter: str = delimiter

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

    @property
    def text(self) -> str:
        """Return sequence of XML_Utterances."""
        return self.delimiter.join(x.text for x in self._utterances)

    def to_text(self, level: IterateLevel) -> List[Tuple[str, str, str, str]]:
        """Load protocol from XML. Aggregate text to `level`. Return (name, speaker, id, text)."""
        try:

            if level.startswith('protocol'):

                items = [(self.name, None, self.name, self.text)]

            elif level.startswith('speaker'):

                data = defaultdict(list)
                for u in self.utterances:
                    data[u.who].append(u.text)

                items = [(self.name, who, who, '\n'.join(data[who])) for who in data]

            elif level.startswith('speech'):

                data, who = defaultdict(list), {}
                for u in self.utterances:
                    data[u.n].append(u.text)
                    who[u.n] = u.who

                items = [(self.name, who[n], n, '\n'.join(data[n])) for n in data]

            elif level.startswith('utterance'):

                items = [(self.name, x.who, x.u_id, x.text) for x in self.utterances]

            elif level.startswith('paragraph'):

                items = [
                    (self.name, u.who, f"{u.u_id}@{i}", p) for u in self.utterances for i, p in enumerate(u.paragraphs)
                ]

            if self.skip_size > 0:

                return [item for item in items if len(item[3]) > self.skip_size]

            return items

        except Exception as ex:
            raise ex
            # return ex


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
