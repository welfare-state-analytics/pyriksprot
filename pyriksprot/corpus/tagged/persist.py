from __future__ import annotations

import glob
import json
import os
import zipfile
from pathlib import Path
from typing import Callable, Iterable, List, Optional

from loguru import logger
from tqdm import tqdm

from pyriksprot.utility import ensure_path, is_empty, strip_path_and_extension, strip_paths, touch

from ... import interface
from ..parlaclarin.parse import zero_fill_filename_sequence

METADATA_FILENAME: str = 'metadata.json'

jj = os.path.join
relpath = os.path.relpath


PROTOCOL_LOADERS: dict[str, Callable[[str], list[interface.Utterance]]] = {
    "json": interface.UtteranceHelper.from_json,
    "csv": interface.UtteranceHelper.from_csv,
}

PROTOCOL_SERIALIZERS: dict[str, Callable[[list[interface.Utterance]], str]] = {
    "json": interface.UtteranceHelper.to_json,
    "csv": interface.UtteranceHelper.to_csv,
}


def store_protocol(
    output_filename: str,
    protocol: interface.Protocol,
    checksum: str,
    storage_format: interface.StorageFormat | str = interface.StorageFormat.JSON,
) -> None:
    """Store tagged protocol in `output_filename`, with metadata."""

    if not output_filename.endswith("zip"):
        raise ValueError("Only Zip store currently implemented")

    with zipfile.ZipFile(output_filename, 'w', zipfile.ZIP_DEFLATED) as fp:

        metadata: dict[str, str] = {"name": protocol.name, "date": protocol.date, "checksum": checksum}

        fp.writestr(METADATA_FILENAME, json.dumps(metadata, indent=4))

        serializer: Callable[[list[interface.Utterance]], str] = PROTOCOL_SERIALIZERS.get(storage_format)
        if serializer is None:
            raise ValueError(f"Unsupported storage format: {storage_format}")

        utterances_str: str = serializer(protocol.utterances)

        fp.writestr(f'{protocol.name}.{str(storage_format)}', utterances_str or "")


def load_metadata(filename: str) -> Optional[dict[str, str]]:
    """Read metadata attributes stored in `metadata.json`"""

    try:
        with zipfile.ZipFile(filename, 'r') as fp:
            filenames: List[str] = [f.filename for f in fp.filelist]
            if METADATA_FILENAME not in filenames:
                return None
            json_str = fp.read(METADATA_FILENAME).decode('utf-8')
            data: dict[str, str] = json.loads(json_str)
            # FIXME: #74 Zero-filled sequence number in protocol name
            data["name"] = zero_fill_filename_sequence(data["name"])
            return data
    except (zipfile.BadZipFile, FileNotFoundError):
        return None
    except Exception as ex:
        logger.info(f"caught {type(ex).__name__}")
        return None


class FileIsEmptyError(Exception): ...


def load_protocol(filename: str) -> Optional[interface.Protocol]:
    """Loads a tagged protocol stored in ZIP as JSON or CSV
    Note: Speaker notes and page references are not stored in the protocol file
    These files are instead stored in separate files in the metadata folder
    """

    if is_empty(filename) or not zipfile.is_zipfile(filename):
        return None

    metadata: dict[str, str] | None = load_metadata(filename)

    if metadata is None:
        return None

    with zipfile.ZipFile(filename, 'r') as fp:

        data_files: list[str] = [f.filename for f in fp.filelist if not f.is_dir() and f.filename != METADATA_FILENAME]

        if len(data_files) != 1:
            raise ValueError(f"Expected 1 file in {filename}, found {len(data_files)} files.")

        stored_filename: str = data_files[0]
        ext: str = stored_filename.rsplit(".", 1)[-1].lower()
        data_str: str = fp.read(stored_filename).decode('utf-8')

        loader = PROTOCOL_LOADERS.get(ext)
        if loader is None:
            raise ValueError(f"Unsupported protocol format '.{ext}' in {filename} (file: {stored_filename})")

        utterances: List[interface.Utterance] = loader(data_str)

        protocol: interface.Protocol = interface.Protocol(
            utterances=utterances,
            name=Path(filename).stem,
            date=metadata.get('date', None),
            preface_name=metadata['name'],
            speaker_notes={},
            page_references=[],
        )

        return protocol

        # for ext in PROTOCOL_LOADERS:
        #     stored_filename: str = f"{basename}.{ext}"

        #     if not stored_filename in filenames:
        #         continue

        #     data_str: str = fp.read(stored_filename).decode('utf-8')
        #     utterances: List[interface.Utterance] = PROTOCOL_LOADERS.get(ext)(data_str)

        #     # Note: Speaker notes and page references are not stored in the protocol file
        #     # These files are instead stored in separate files in the metadata folder
        #     protocol: interface.Protocol = interface.Protocol(
        #         utterances=utterances,
        #         name=basename,
        #         date=metadata.get('date', None),
        #         preface_name=preface_name,
        #         speaker_notes={},
        #         page_references=[],
        #     )

        #     return protocol

    # raise FileNotFoundError(f"Probed {basename}.json and {basename}.csv in {filename} but not found among {filenames}.")


def load_protocols(source: str | list[str], pattern: str = '**/prot-*.zip') -> Iterable[interface.Protocol]:
    """Loads all protocols in `source` folder, matching `file_pattern`"""
    filenames: list[str] = glob_protocols(source, pattern)
    for p in (load_protocol(filename) for filename in filenames):
        if p is None:
            continue
        yield p


def glob_protocols(source: str, pattern: str = None, strip_path: bool = False):
    """Glob for protocols in `source` folder, matching `file_pattern`"""
    filenames: list[str] = []
    if isinstance(source, str):
        path: str = jj(source, pattern) if pattern else source
        filenames = glob.glob(path, recursive=True)
    elif isinstance(source, list):
        filenames = source

    filenames: list[str] = (
        glob.glob(jj(source, pattern), recursive=True)
        if isinstance(source, str)
        else source if isinstance(source, list) else []
    )
    if strip_path:
        filenames = strip_paths(filenames)
    return filenames


def validate_checksum(filename: str, checksum: str) -> bool:
    """Validate that computed checksum of a protocol matches `checksum`."""
    if not os.path.isfile(filename):
        return False
    metadata: dict = load_metadata(filename)
    if metadata is None:
        return False
    return checksum == metadata.get('checksum', 'oo')


def update_speaker_note_id(speaker_note_id_lookup: dict[str, str], source_folder: str, target_folder: str):
    """Updates speaker's note's xml:id in an existing PoS-tagged corpus
    speaker_note_id_lookup = SpeakerInfoService(database_filename).utterance_index.utterances['speaker_note_id'].to_dict()
    """

    os.makedirs(target_folder, exist_ok=True)

    for filename in tqdm(glob.glob(jj(source_folder, "**/*.zip"), recursive=True)):
        target_filename: str = jj(target_folder, relpath(filename, source_folder))
        ensure_path(target_filename)
        json_name = f"{strip_path_and_extension(filename)}.json"
        try:
            with zipfile.ZipFile(filename, "r") as fp:
                protocol_str = fp.read(json_name).decode('utf-8')
                metadata_str = fp.read("metadata.json").decode('utf-8')
        except zipfile.BadZipFile:
            touch(target_filename)
            continue
        protocol: list[dict] = json.loads(protocol_str)
        for u in protocol:
            speaker_note_id: str = speaker_note_id_lookup.get(u['u_id'])
            if speaker_note_id:
                u['speaker_note_id'] = speaker_note_id
        utterances_csv_str: str = json.dumps(protocol)
        with zipfile.ZipFile(target_filename, "w", compression=zipfile.ZIP_DEFLATED) as fp:
            fp.writestr(json_name, utterances_csv_str or "")
            fp.writestr("metadata.json", metadata_str or "")
