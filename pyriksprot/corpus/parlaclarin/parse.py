from __future__ import annotations

import re
from typing import Any, Iterable

from loguru import logger

from pyriksprot import interface
from pyriksprot.foss import untangle
from pyriksprot.preprocess import dedent as dedent_text

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
        except AttributeError:
            logger.warning(f'no content (text.body) found in {ProtocolMapper.get_name(data) or "unknown"}')
            return []

        if not isinstance(sections, (list, tuple)):
            return [sections]
        return sections

    @staticmethod
    def get_content_elements(data: untangle.Element) -> Iterable[untangle.Element]:
        for section in ProtocolMapper.get_content_sections(data):
            for item in section.children:
                yield item

    @staticmethod
    def has_body(data: untangle.Element) -> bool:
        return len(ProtocolMapper.get_content_sections(data)) > 0

    @staticmethod
    def get_data(data: untangle.Element) -> tuple[list[interface.Utterance], dict[str, interface.SpeakerNote]]:
        """All utterances in sequence"""
        utterances: list[interface.Utterance] = []
        """All speaker notes"""
        speaker_notes: dict[str, interface.SpeakerNote] = {}

        page_refs: list[str, interface.PageReference] = []
        page_ref: interface.PageReference = interface.PageReference(source_id=0, page_number=-1, reference="")

        """Current Speaker Note"""
        speaker_note: interface.SpeakerNote = None
        first: interface.Utterance = None
        previous: interface.Utterance = None

        for element in ProtocolMapper.get_content_elements(data):
            if element.name == 'pb':
                page_ref = ProtocolMapper.decode_page_reference(page_ref, element)
                page_refs.append(page_ref)

            elif element.name == "note" and element['type'] == "speaker":
                speaker_note = interface.SpeakerNote(element["xml:id"], " ".join(element.cdata.split()))
                speaker_notes[element["xml:id"]] = speaker_note
                first = None
                previous = None

            elif element.name == 'u':
                if len(page_refs) == 0:
                    page_refs = [page_ref]

                utterance: interface.Utterance = interface.Utterance(
                    u_id=element.get_attribute('xml:id'),
                    who=element.get_attribute('who') or "unknown",
                    page_number=page_ref.page_number,
                    prev_id=element.get_attribute('prev'),
                    next_id=element.get_attribute('next'),
                    paragraphs=ProtocolMapper.to_paragraphs(element, dedent=True),
                )

                if first is None:
                    first = utterance

                if previous:
                    """We have seen at least one utterance since last speaker intro"""

                    """If other speaker then invalidate speaker intro"""
                    if previous.who != utterance.who:
                        speaker_note = None

                    """If prev doesn't link to previous then invalidate speaker intro"""
                    if utterance.prev_id != previous.u_id:
                        speaker_note = None

                utterance.speaker_note_id = (
                    speaker_note.speaker_note_id if speaker_note is not None else interface.MISSING_SPEAKER_NOTE_ID
                )

                utterances.append(utterance)

                previous = utterance

        return {
            'utterances': utterances,
            'speaker_notes': speaker_notes,
            'page_references': page_refs,
        }

    @staticmethod
    def decode_page_reference(page_ref, element) -> interface.PageReference:
        page_number: int = page_ref.page_number + 1
        source_id: int = page_ref.source_id
        n_attribute: str | None = element.get_attribute('n')
        if n_attribute and n_attribute.isdigit():
            page_number = int(n_attribute)

        reference: str = element.get_attribute('facs') or ""
        if not reference:
            if (n_attribute or "").startswith("http"):
                reference = n_attribute

        if 'kb.se' in reference:
            page_number: int = int(re.search(r'-(\d+)\.jp2', reference).group(1))
            source_id: int = 1
            reference: str = ""
        elif 'riksdagen.se' in reference:
            page_number: int = int(re.search(r'#page=(\d+)', reference).group(1))
            source_id: int = 2
            reference: str = reference.split("/")[-1].split("#")[0]

        page_ref = interface.PageReference(source_id=source_id, page_number=page_number, reference=reference)
        return page_ref

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
        protocol: interface.Protocol = None
        source_name: str = filename if isinstance(filename, str) else "unknown"
        try:
            ignore_tags: set[str] = set(ignore_tags.split(",")) if isinstance(ignore_tags, str) else ignore_tags
            data: untangle.Element = (
                filename
                if isinstance(filename, untangle.Element)
                else untangle.parse(filename, ignore_tags=ignore_tags)
            )

            if not isinstance(data, untangle.Element):
                raise ValueError(f"expected untangle.Element, got {type(data)} {source_name}")

            parsed_data: dict = ProtocolMapper.get_data(data)

            if len(parsed_data.get("utterances") or []) == 0:
                logger.warning(f'no utterances found in {source_name}')

            protocol: interface.Protocol = interface.Protocol(
                utterances=parsed_data.get("utterances"),
                speaker_notes=parsed_data.get("speaker_notes"),
                page_references=parsed_data.get("page_references"),
                date=ProtocolMapper.get_date(data),
                name=ProtocolMapper.get_name(data),
            )
        except Exception as ex:
            logger.error(f"error parsing {source_name}: {ex}")
            raise ex

        return protocol
