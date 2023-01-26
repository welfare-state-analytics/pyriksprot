from __future__ import annotations

import glob
import json
import os
import zipfile
from typing import Iterable, List, Optional

from loguru import logger
from tqdm import tqdm

from pyriksprot.utility import ensure_path, is_empty, strip_path_and_extension, strip_paths, touch

from ... import interface

METADATA_FILENAME: str = 'metadata.json'

jj = os.path.join
relpath = os.path.relpath


def store_protocol(
    output_filename: str,
    protocol: interface.Protocol,
    checksum: str,
    storage_format: interface.StorageFormat = interface.StorageFormat.JSON,
) -> None:
    """Store tagged protocol in `output_filename`, with metadata."""

    if output_filename.endswith("zip"):

        with zipfile.ZipFile(output_filename, 'w', zipfile.ZIP_DEFLATED) as fp:

            metadata: dict = dict(name=protocol.name, date=protocol.date, checksum=checksum)
            fp.writestr(METADATA_FILENAME, json.dumps(metadata, indent=4))

            if storage_format == interface.StorageFormat.CSV:
                utterances_csv_str: str = protocol.to_csv()
                fp.writestr(f'{protocol.name}.csv', utterances_csv_str or "")

            elif storage_format == interface.StorageFormat.JSON:
                utterances_json_str: str = protocol.to_json()
                fp.writestr(f'{protocol.name}.json', utterances_json_str or "")

            # document_index: pd.DataFrame = (
            #     pd.DataFrame(speeches)
            #     .set_index('document_name', drop=False)
            #     .rename_axis('')
            #     .assign(document_id=range(0, len(speeches)))
            # )
            # fp.writestr('document_index.csv', document_index.to_csv(sep='\t', header=True))

    else:

        raise ValueError("Only Zip store currently implemented")


def load_metadata(filename: str) -> Optional[dict]:
    """Read metadata attributes stored in `metadata.json`"""

    try:
        with zipfile.ZipFile(filename, 'r') as fp:
            filenames: List[str] = [f.filename for f in fp.filelist]
            if METADATA_FILENAME not in filenames:
                return None
            json_str = fp.read(METADATA_FILENAME).decode('utf-8')
            return json.loads(json_str)
    except (zipfile.BadZipFile, FileNotFoundError):
        return None
    except Exception as ex:
        logger.info(f"caught {type(ex).__name__}")
        return None


PROTOCOL_LOADERS: dict = dict(
    json=interface.UtteranceHelper.from_json,
    csv=interface.UtteranceHelper.from_csv,
)


class FileIsEmptyError(Exception):
    ...


def load_protocol(filename: str) -> Optional[interface.Protocol]:
    """Loads a tagged protocol stored in ZIP as JSON or CSV"""

    if is_empty(filename) or not zipfile.is_zipfile(filename):
        return None

    metadata: dict = load_metadata(filename)

    if metadata is None:
        return None

    with zipfile.ZipFile(filename, 'r') as fp:

        basename: str = metadata['name']

        filenames: List[str] = [f.filename for f in fp.filelist]

        for ext in PROTOCOL_LOADERS:

            stored_filename: str = f"{basename}.{ext}"

            if not stored_filename in filenames:
                continue

            data_str: str = fp.read(stored_filename).decode('utf-8')
            utterances: List[interface.Utterance] = PROTOCOL_LOADERS.get(ext)(data_str)

            protocol: interface.Protocol = interface.Protocol(utterances=utterances, **metadata, speaker_notes={})

            return protocol


def load_protocols(source: str | List, file_pattern: str = 'prot-*.zip') -> Iterable[interface.Protocol]:

    return (p for p in (load_protocol(filename) for filename in glob_protocols(source, file_pattern)) if p is not None)


def glob_protocols(source: str, file_pattern: str, strip_path: bool = False):
    filenames: List[str] = (
        glob.glob(os.path.join(source, file_pattern), recursive=True)
        if isinstance(source, str)
        else source
        if isinstance(source, list)
        else []
    )
    if strip_path:
        filenames = strip_paths(filenames)
    return filenames


def validate_checksum(filename: str, checksum: str) -> bool:
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
