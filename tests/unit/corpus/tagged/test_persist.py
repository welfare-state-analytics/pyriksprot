import json
import os
import zipfile
from pathlib import Path

import pytest

from pyriksprot import interface
from pyriksprot.corpus.parlaclarin.parse import zero_fill_filename_sequence
from pyriksprot.corpus.tagged.persist import (
    METADATA_FILENAME,
    glob_protocols,
    load_metadata,
    load_protocol,
    load_protocols,
    store_protocol,
    update_speaker_note_id,
    validate_checksum,
)
from tests.fixtures.synthetic import minimal_protocol

# pylint: disable=unused-import, redefined-outer-name


pytestmark = pytest.mark.unit


def _protocol(name: str = "prot-1900-1", speaker_note_id: str = "old-note", text: str = "Hello") -> interface.Protocol:
    return interface.Protocol(
        date="1900",
        name=name,
        utterances=[
            interface.Utterance(
                u_id="u1",
                who="speaker_a",
                speaker_note_id=speaker_note_id,
                paragraphs=[text],
            )
        ],
        speaker_notes={},
        page_references=[],
    )


def test_store_protocol_requires_zip_extension(tmp_path: Path, minimal_protocol: interface.Protocol):
    target = tmp_path / "protocol.json"

    with pytest.raises(ValueError):
        store_protocol(str(target), minimal_protocol, checksum="abc123")


def test_store_and_load_protocol_roundtrip_json(tmp_path: Path, minimal_protocol: interface.Protocol):
    target = tmp_path / f"{minimal_protocol.name}.zip"

    store_protocol(str(target), minimal_protocol, checksum="abc123")

    loaded = load_protocol(str(target))

    assert loaded is not None
    assert loaded.name == target.stem
    assert loaded.preface_name == zero_fill_filename_sequence(minimal_protocol.name)
    assert loaded.date == minimal_protocol.date
    assert [u.to_dict() for u in loaded.utterances] == [u.to_dict() for u in minimal_protocol.utterances]


def test_store_and_load_protocol_roundtrip_csv(tmp_path: Path):
    protocol = _protocol(name="prot-1900-2", text="CSV content")
    target = tmp_path / "prot-1900-2.zip"

    store_protocol(str(target), protocol, checksum="checksum", storage_format=interface.StorageFormat.CSV)

    loaded = load_protocol(str(target))

    assert loaded is not None
    assert loaded.utterances[0].text == "CSV content"


def test_load_metadata_applies_zero_fill(tmp_path: Path):
    protocol = _protocol(name="prot-1900-1")
    target = tmp_path / "prot-1900-1.zip"
    store_protocol(str(target), protocol, checksum="checksum")

    metadata = load_metadata(str(target))

    assert metadata is not None
    assert metadata["name"] == "prot-1900-001"


def test_load_metadata_returns_none_when_missing_metadata(tmp_path: Path):
    target = tmp_path / "missing-meta.zip"
    with zipfile.ZipFile(target, "w") as fp:
        fp.writestr("dummy.txt", "content")

    assert load_metadata(str(target)) is None


def test_validate_checksum(tmp_path: Path, minimal_protocol: interface.Protocol):
    target = tmp_path / f"{minimal_protocol.name}.zip"
    checksum = "deadbeef"
    store_protocol(str(target), minimal_protocol, checksum=checksum)

    assert validate_checksum(str(target), checksum)
    assert not validate_checksum(str(target), "cafebabe")
    assert not validate_checksum(str(tmp_path / "missing.zip"), checksum)


def test_load_protocol_unsupported_format(tmp_path: Path):
    target = tmp_path / "prot-1900-unsupported.zip"
    metadata = {"name": "prot-1900-unsupported", "date": "1900", "checksum": "x"}

    with zipfile.ZipFile(target, "w") as fp:
        fp.writestr(METADATA_FILENAME, json.dumps(metadata))
        fp.writestr("prot-1900-unsupported.xml", "<data></data>")

    with pytest.raises(ValueError):
        load_protocol(str(target))


def test_load_protocols_skips_invalid_entries(tmp_path: Path):
    valid = tmp_path / "prot-1901-valid.zip"
    invalid = tmp_path / "prot-1901-invalid.zip"

    store_protocol(str(valid), _protocol(name="prot-1901-valid"), checksum="checksum")
    invalid.write_bytes(b"")

    protocols = list(load_protocols(str(tmp_path), pattern="*.zip"))

    assert [p.name for p in protocols] == [valid.stem]


def test_glob_protocols_strip_path(tmp_path: Path):
    nested = tmp_path / "nested"
    nested.mkdir()

    files = [tmp_path / "prot-a.zip", nested / "prot-b.zip"]
    for file in files:
        file.touch()

    found = glob_protocols(str(tmp_path), pattern="**/*.zip", strip_path=True)

    assert set(found) == {"prot-a.zip", "prot-b.zip"}


def test_update_speaker_note_id_writes_updated_zip(tmp_path: Path):
    source = tmp_path / "source"
    target = tmp_path / "target"
    os.makedirs(source, exist_ok=True)

    protocol = _protocol(name="prot-1902-1")
    source_zip = source / "prot-1902-1.zip"
    store_protocol(str(source_zip), protocol, checksum="checksum")

    lookup = {"u1": "new-note"}

    update_speaker_note_id(lookup, str(source), str(target))

    target_zip = target / "prot-1902-1.zip"
    with zipfile.ZipFile(target_zip, "r") as fp:
        updated_protocol = json.loads(fp.read("prot-1902-1.json").decode("utf-8"))

    assert updated_protocol[0]["speaker_note_id"] == "new-note"
