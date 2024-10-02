from __future__ import annotations

import os
from glob import glob
from typing import Any

from pyriksprot import interface
from pyriksprot.configuration.inject import ConfigStore
from pyriksprot.corpus import parlaclarin
from pyriksprot.corpus.parlaclarin.parse import ProtocolMapper
from pyriksprot.foss import untangle
from pyriksprot.utility import strip_path_and_extension


def _load_protocol(data: str | Any) -> interface.Protocol:
    return parlaclarin.ProtocolMapper.parse(data)


def get_utterance_sequence(filename: str) -> list[tuple]:
    SPEAKER_NOTE_ID_MISSING: str = "missing"

    xml_protocol = untangle.parse(filename)

    if xml_protocol.get_content_root() is None:
        return []

    data: list[tuple] = []

    speaker_note_id: str = SPEAKER_NOTE_ID_MISSING
    previous_who: str = None
    page_id: int = -1

    for element in xml_protocol.get_content_elements():
        if element.name == "pb":
            page_id += 1

        if element.name == "note":
            if element['type'] == "speaker":
                speaker_note_id = element["xml:id"]
                previous_who: str = None
                data.append(
                    (
                        str(page_id),
                        'speaker-note',
                        speaker_note_id,
                        "",
                        "",
                        "",
                        "",
                    )
                )

        if element.name == 'u':
            who: str = element["who"]

            if previous_who and previous_who != who:
                speaker_note_id = SPEAKER_NOTE_ID_MISSING

            previous_who: str = who

            data.append(
                (
                    str(page_id),
                    'utterance',
                    element["xml:id"],
                    element["prev"] or "",
                    element["next"] or "",
                    speaker_note_id,
                    element["who"],
                )
            )

    return data


def log_utterance_sequences(source: str, target: str) -> list[tuple]:
    filenames = [source] if os.path.isfile(source) else glob(source, recursive=True)
    with open(target, mode="w", encoding="utf8") as fp:
        fp.write("document;page_id;tag;id;prev,next,speaker_note_id;who\n")
        for filename in filenames:
            data = get_utterance_sequence(filename)
            fp.write(strip_path_and_extension(filename) + ('\n'.join(";".join(x) for x in data)))


def count_utterances(path: str) -> int:
    data: untangle.Element = untangle.parse(path)
    count: int = 0
    for _ in (tag for tag in ProtocolMapper().get_content_elements(data) if tag.name == 'u'):
        count += 1
    return count


def get_test_filenames() -> list[str]:
    pattern = os.path.join(ConfigStore.config().get("corpus:folder"), ConfigStore.config().get("corpus:pattern"))
    filenames: list[str] = glob(pattern, recursive=True)
    return filenames
