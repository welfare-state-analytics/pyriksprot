from __future__ import annotations

from collections import defaultdict

from pyriksprot.corpus import parlaclarin


def _load_protocol(xml_protocol: parlaclarin.XmlUntangleProtocol) -> parlaclarin.XmlUntangleProtocol:
    return parlaclarin.XmlUntangleProtocol(data=xml_protocol) if isinstance(xml_protocol, str) else xml_protocol


def log_utterance_sequence(xml_protocol: str | parlaclarin.XmlUntangleProtocol, filename: str) -> None:

    SPEAKER_NOTE_ID_MISSING: str = "missing"

    xml_protocol = _load_protocol(xml_protocol)

    if xml_protocol.get_content_root() is None:
        return

    log_data: list[tuple] = []

    speaker_note_id: str = SPEAKER_NOTE_ID_MISSING
    previous_who: str = None
    page_id: int = 0

    for child in xml_protocol.get_content_root().children:

        if child.name == "pb":
            page_id = int(child['n'])

        if child.name == "note":
            if child['type'] == "speaker":
                speaker_note_id = child["xml:id"]
                previous_who: str = None
                log_data.append((str(page_id), 'speaker-note', speaker_note_id, "", ""))

        if child.name == 'u':

            who: str = child["who"]

            if previous_who and previous_who != who:
                speaker_note_id = SPEAKER_NOTE_ID_MISSING

            previous_who: str = who

            log_data.append((str(page_id), 'utterance', child["xml:id"], speaker_note_id, child["who"]))

    with open(filename, "w", encoding="utf-8") as fp:
        fp.write("page_id;tag;id;speaker_id;who\n")
        fp.write('\n'.join(";".join(x) for x in log_data))


def count_speaker_notes(xml_protocol: str | parlaclarin.XmlUntangleProtocol) -> dict[str, int]:

    SPEAKER_NOTE_ID_MISSING: str = "missing"

    counter: defaultdict = defaultdict()
    counter.default_factory = int

    xml_protocol = _load_protocol(xml_protocol)

    if xml_protocol.get_content_root() is None:
        return {}

    speaker_note_id: str = SPEAKER_NOTE_ID_MISSING
    previous_who: str = None

    for child in xml_protocol.get_content_root().children:

        if child.name == "note":
            if child['type'] == "speaker":
                speaker_note_id = child["xml:id"]
                counter[speaker_note_id] = 0
                previous_who: str = None

        elif child.name == 'u':

            who: str = child["who"]

            if previous_who and previous_who != who:
                speaker_note_id = SPEAKER_NOTE_ID_MISSING

            counter[speaker_note_id] += 1

            previous_who: str = who

    return dict(counter)


def count_utterances(xml_protocol: str | parlaclarin.XmlUntangleProtocol) -> int:

    xml_protocol = _load_protocol(xml_protocol)

    if not xml_protocol.get_content_root():
        return 0

    return len([tag for tag in xml_protocol.get_content_root().children if tag.name == 'u'])
