import contextlib
import json
import os
import zipfile
from typing import List, Literal, Optional

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

    if not os.path.isfile(filename):
        return None

    with contextlib.suppress(Exception):

        with zipfile.ZipFile(filename, 'r') as fp:

            filenames: List[str] = [f.filename for f in fp.filelist]

            if METADATA_FILENAME not in filenames:
                return None

            json_str = fp.read(METADATA_FILENAME).decode('utf-8')

            return json.loads(json_str)


PROTOCOL_LOADERS: dict = dict(
    json=model.Utterances.from_json,
    csv=model.Utterances.from_csv,
)


def load_protocol(filename: str) -> Optional[model.Protocol]:

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


def validate_checksum(filename: str, checksum: str) -> bool:
    metadata: dict = load_metadata(filename)
    if metadata is None:
        return False
    return checksum == metadata.get('checksum', 'oo')
