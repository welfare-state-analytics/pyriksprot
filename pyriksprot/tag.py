from typing import List, Literal

from loguru import logger

from . import interface, model, parse, persist
from .utility import ensure_path, strip_path_and_extension, touch, unlink

CHECKSUM_FILENAME: str = 'sha1_checksum.txt'
METADATA_FILENAME: str = 'metadata.json'

StorageFormat = Literal['csv', 'json']

# @deprecated
# def bulk_tag_protocols(tagger: ITagger, protocols: List[Protocol], skip_size: int = 5) -> List[List[dict]]:

#     speech_items: List[Dict[str, Any]] = []
#     protocol_refs = {}

#     for protocol in protocols:
#         idx = len(speech_items)
#         speech_items.extend(protocol.to_dict(skip_size=skip_size))
#         protocol_refs[protocol.name] = (idx, len(speech_items) - idx)

#     speech_items: List[Dict[str, Any]] = tag_speech_items(tagger, speech_items)

#     protocol_speech_items = []
#     for protocol in protocols:
#         idx, n_count = protocol_refs[protocol.name]
#         protocol_speech_items.append(speech_items[idx : idx + n_count])

#     return protocol_speech_items


def tag_protocol(tagger: interface.ITagger, protocol: model.Protocol, preprocess=False) -> model.Protocol:

    texts = [u.text for u in protocol.utterances]

    documents: List[interface.TaggedDocument] = tagger.tag(texts, preprocess=preprocess)

    for i, document in enumerate(documents):
        protocol.utterances[i].annotation = tagger.to_csv(document)
        protocol.utterances[i].num_tokens = document.get("num_tokens")
        protocol.utterances[i].num_words = document.get("num_words")

    return protocol


def tag_protocol_xml(
    input_filename: str,
    output_filename: str,
    tagger: interface.ITagger,
    skip_size: int = 5,
    force: bool = False,
    storage_format: StorageFormat = 'json',
) -> None:
    """Annotate XML protocol `input_filename` to `output_filename`.

    Args:
        input_filename (str, optional): Defaults to None.
        output_filename (str, optional): Defaults to None.
        tagger (StanzaTagger, optional): Defaults to None.
    """

    try:

        ensure_path(output_filename)

        protocol: model.Protocol = parse.ProtocolMapper.to_protocol(input_filename, skip_size=skip_size)

        if not protocol.has_text():

            unlink(output_filename)
            touch(output_filename)

            return

        protocol.preprocess(tagger.preprocess)

        checksum: str = protocol.checksum()

        if not force and persist.validate_checksum(output_filename, checksum):

            logger.info(f"SKIPPING {strip_path_and_extension(input_filename)} (checksum validates OK)")

            touch(output_filename)

        else:

            unlink(output_filename)

            protocol = tag_protocol(tagger, protocol=protocol)

            persist.store_protocol(output_filename, protocol=protocol, checksum=checksum, storage_format=storage_format)

    except Exception:
        logger.error(f"FAILED: {input_filename}")
        unlink(output_filename)
        raise
