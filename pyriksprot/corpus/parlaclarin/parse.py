from __future__ import annotations

from typing import Any, Iterable

from loguru import logger

from pyriksprot import interface
from pyriksprot.foss import untangle
from pyriksprot.utility import dedent as dedent_text

XML_ID: str = '{http://www.w3.org/XML/1998/namespace}id'

# pylint: disable=too-many-statements


class ProtocolMapper(interface.IProtocolParser):
    @staticmethod
    def get_date(data: untangle.Element) -> str:
        try:
            docDate = data.teiCorpus.TEI.text.front.div.docDate
            return docDate[0]['when'] if isinstance(docDate, list) else docDate['when']
        except (AttributeError, KeyError):
            return None

    @staticmethod
    def get_name(data: untangle.Element) -> str:
        """Protocol name"""
        try:
            return data.teiCorpus.TEI.text.front.div.head.cdata
        except (AttributeError, KeyError):
            return None

    @staticmethod
    def get_content_sections(data: untangle.Element) -> list[untangle.Element]:
        try:
            sections: Any = data.teiCorpus.TEI.text.body.children
            if not isinstance(sections, (list, tuple)):
                return [sections]
            return sections
        except AttributeError:
            logger.warning(f'no content (text.body) found in {data.get_name()}')
        return []

    @staticmethod
    def get_content_elements(data: untangle.Element) -> Iterable[untangle.Element]:
        for section in ProtocolMapper.get_content_sections(data):
            for item in section.children:
                yield item

    @staticmethod
    def get_data(data: untangle.Element) -> tuple[list[interface.Utterance], dict[str, interface.SpeakerNote]]:
        """All utterances in sequence"""
        utterances: list[interface.Utterance] = []
        """All speaker notes"""
        speaker_notes: dict[str, interface.SpeakerNote] = {}

        """Current Speaker Note"""
        speaker_note: interface.SpeakerNote = None
        page_number: int = -1
        first: interface.Utterance = None
        previous: interface.Utterance = None

        for element in ProtocolMapper.get_content_elements(data):
            if element.name == 'pb':
                if 'n' in element.attributes:
                    page_number = int(element.get_attribute('n'))
                else:
                    page_number += 1

            elif element.name == "note" and element['type'] == "speaker":
                speaker_note = interface.SpeakerNote(element["xml:id"], " ".join(element.cdata.split()))
                speaker_notes[element["xml:id"]] = speaker_note
                first = None
                previous = None

            elif element.name == 'u':
                utterance: interface.Utterance = interface.Utterance(
                    u_id=element.get_attribute('xml:id'),
                    who=element.get_attribute('who') or "unknown",
                    page_number=page_number,
                    prev_id=element.get_attribute('prev'),
                    next_id=element.get_attribute('next'),
                    paragraphs=ProtocolMapper.to_paragraphs(element, dedent=True),
                )

                if first is None:
                    first = utterance

                if previous:
                    """We have seen at least one utterance since last speaker intro"""
                    if previous.who != utterance.who:
                        """If other speaker then invalidate speaker intro"""
                        speaker_note = None

                    """A sequence of unknown utterances without PREV link shouldn't be assigned previous speaker note"""
                    if previous is not first:
                        """If it's not the first, then only assign if prev links to first"""
                        if utterance.prev_id != first.u_id:
                            speaker_note = None

                utterance.speaker_note_id = (
                    speaker_note.speaker_note_id if speaker_note is not None else interface.MISSING_SPEAKER_NOTE_ID
                )

                utterances.append(utterance)

                previous = utterance

        return {'utterances': utterances, 'speaker_notes': speaker_notes}

    @staticmethod
    def to_paragraphs(element: untangle.Element, dedent: bool = True) -> list[str]:
        texts: Iterable[str] = (p.cdata for p in element.get_elements('seg'))
        if dedent:
            texts = [dedent_text(t) for t in texts]
        return texts

    @staticmethod
    def parse(
        filename: str | untangle.Element,
        ignore_tags: set[str] | str = "teiHeader",
    ) -> interface.Protocol:
        """Map XML to domain entity. Return Protocol."""
        ignore_tags: set[str] = set(ignore_tags.split(",")) if isinstance(ignore_tags, str) else ignore_tags
        data: untangle.Element = (
            filename if isinstance(filename, untangle.Element) else untangle.parse(filename, ignore_tags=ignore_tags)
        )

        parsed_data: dict = ProtocolMapper.get_data(data)

        protocol: interface.Protocol = interface.Protocol(
            utterances=parsed_data.get("utterances"),
            speaker_notes=parsed_data.get("speaker_notes"),
            date=ProtocolMapper.get_date(data),
            name=ProtocolMapper.get_name(data),
        )

        return protocol
