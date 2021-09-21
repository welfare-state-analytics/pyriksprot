from __future__ import annotations

import contextlib
import glob
import json
import os
import zipfile
from typing import Iterable, List, Literal, Optional

from pyriksprot.utility import is_empty

from . import model

CHECKSUM_FILENAME: str = 'sha1_checksum.txt'
METADATA_FILENAME: str = 'metadata.json'

StorageFormat = Literal['csv', 'json']


def store_protocol(
    output_filename: str, protocol: model.Protocol, checksum: str, storage_format: StorageFormat = 'json'
) -> None:
    """Store tagged protocol in `output_filename`, with metadata."""

    if output_filename.endswith("zip"):

        with zipfile.ZipFile(output_filename, 'w', zipfile.ZIP_DEFLATED) as fp:

            metadata: dict = dict(name=protocol.name, date=protocol.date, checksum=checksum)
            fp.writestr(METADATA_FILENAME, json.dumps(metadata, indent=4))

            if storage_format == 'csv':
                utterances_csv_str: str = protocol.to_csv()
                fp.writestr(f'{protocol.name}.csv', utterances_csv_str or "")

            elif storage_format == 'json':
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
    """Read metadata attributes stored in `metadata.json` """
    if not os.path.isfile(filename):
        raise FileNotFoundError(filename)

    with contextlib.suppress(Exception):

        with zipfile.ZipFile(filename, 'r') as fp:

            filenames: List[str] = [f.filename for f in fp.filelist]

            if METADATA_FILENAME not in filenames:
                return None

            json_str = fp.read(METADATA_FILENAME).decode('utf-8')

            return json.loads(json_str)

    return None


PROTOCOL_LOADERS: dict = dict(
    json=model.Utterances.from_json,
    csv=model.Utterances.from_csv,
)


class FileIsEmptyError(Exception):
    ...


def load_protocol(filename: str) -> Optional[model.Protocol]:
    """Loads a tagged protocol stored in ZIP as JSON or CSV"""

    if is_empty(filename):
        raise FileIsEmptyError(filename)

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
            utterances: List[model.Utterance] = PROTOCOL_LOADERS.get(ext)(data_str)

            protocol: model.Protocol = model.Protocol(utterances=utterances, **metadata)

            return protocol


def load_protocols(source: str | List, file_pattern: str = 'prot-*.zip') -> Iterable[model.Protocol]:

    filenames: List[str] = (
        glob.glob(os.path.join(source, file_pattern), recursive=True)
        if isinstance(source, str)
        else source
        if isinstance(source, list)
        else []
    )

    return (load_protocol(filename) for filename in filenames if not is_empty(filename))


def validate_checksum(filename: str, checksum: str) -> bool:
    metadata: dict = load_metadata(filename)
    if metadata is None:
        return False
    return checksum == metadata.get('checksum', 'oo')
