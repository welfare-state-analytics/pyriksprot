from __future__ import annotations

import abc
import itertools
from functools import reduce
from glob import glob
from os.path import dirname, getmtime, isfile, join, split
from typing import Any, Callable, List, Mapping, Type, Union

from loguru import logger
from tqdm import tqdm

from pyriksprot.corpus.parlaclarin import parse
from pyriksprot.corpus.tagged import persist

from .. import interface
from ..utility import ensure_path, strip_path_and_extension, touch, unlink

METADATA_FILENAME: str = 'metadata.json'

TaggedDocument = Mapping[str, List[str]]


class ITaggerFactory(abc.ABC):

    identifier: str = "undefined"

    @abc.abstractmethod
    def create(self) -> "ITagger":
        ...


class ITagger(abc.ABC):
    def __init__(self, preprocessors: Callable[[str], str] = None):
        self.preprocessors: Callable[[str], str] = preprocessors or []

    def tag(self, text: Union[str, List[str]], preprocess: bool = True) -> List[TaggedDocument]:
        """Tag text. Return dict if lists."""
        if isinstance(text, str):
            text = [text]

        if not isinstance(text, list):
            return ValueError("invalid type")

        if len(text) == 0:
            return []

        if preprocess:
            text: List[str] = [self.preprocess(d) for d in text]

        tagged_documents = self._tag(text)

        return tagged_documents

    @abc.abstractmethod
    def _tag(self, text: Union[str, List[str]]) -> List[TaggedDocument]:
        ...

    @abc.abstractmethod
    def _to_dict(self, tagged_document: Any) -> TaggedDocument:
        return {}

    @staticmethod
    def to_csv(tagged_document: TaggedDocument, sep='\t') -> str:
        """Converts a TaggedDocument to a TSV string"""

        tokens, lemmas, pos, xpos = (
            tagged_document['token'],
            tagged_document['lemma'],
            tagged_document['pos'],
            tagged_document['xpos'],
        )
        csv_str = '\n'.join(
            itertools.chain(
                [f"token{sep}lemma{sep}pos{sep}xpos"],
                (f"{tokens[i]}{sep}{lemmas[i]}{sep}{pos[i]}{sep}{xpos[i]}" for i in range(0, len(tokens))),
            )
        )
        return csv_str

    def preprocess(self, text: str) -> str:
        """Transform `text` with preprocessors."""
        text: str = reduce(lambda res, f: f(res), self.preprocessors, text)
        return text


class TaggerRegistry:
    """Simple tagger cache since somee taggers are expensive to setup"""

    instances: Mapping[Type[ITagger], ITagger] = {}

    @staticmethod
    def get(factory: "ITaggerFactory") -> ITagger:
        if isinstance(factory, ITagger):
            return factory

        if factory.identifier not in TaggerRegistry.instances:
            TaggerRegistry.instances[factory.identifier] = factory.create()

        return TaggerRegistry.instances[factory.identifier]


def tag_protocol(tagger: ITagger, protocol: interface.Protocol, preprocess=False) -> interface.Protocol:
    texts = [u.text for u in protocol.utterances]

    documents: List[TaggedDocument] = tagger.tag(texts, preprocess=preprocess)

    for i, document in enumerate(documents):
        protocol.utterances[i].annotation = tagger.to_csv(document)
        protocol.utterances[i].num_tokens = document.get("num_tokens")
        protocol.utterances[i].num_words = document.get("num_words")

    return protocol


def tag_protocol_xml(
    input_filename: str,
    output_filename: str,
    tagger: ITagger,
    force: bool = False,
    storage_format: interface.StorageFormat = interface.StorageFormat.JSON,
) -> None:
    """Annotate XML protocol `input_filename` to `output_filename`.

    Args:
        input_filename (str, optional): Defaults to None.
        output_filename (str, optional): Defaults to None.
        tagger (StanzaTagger, optional): Defaults to None.
    """

    try:
        ensure_path(output_filename)

        protocol: interface.Protocol = parse.ProtocolMapper.parse(input_filename)

        if not protocol.has_text:
            unlink(output_filename)
            touch(output_filename)
            return

        protocol.preprocess(tagger.preprocess)
        checksum: str = protocol.checksum()

        # print(f"checksum: {checksum}")
        # print(f"   force: {force}")
        # print(f"filename: {os.path.abspath(output_filename)}  {os.path.isfile(os.path.abspath(output_filename))}")

        if not force and persist.validate_checksum(output_filename, checksum):
            logger.info(f"skipped: {strip_path_and_extension(input_filename)} (checksum validates OK)")
            touch(output_filename)
        else:
            unlink(output_filename)
            logger.info(f"tagging: {strip_path_and_extension(input_filename)}")
            protocol = tag_protocol(tagger, protocol=protocol)
            persist.store_protocol(output_filename, protocol=protocol, checksum=checksum, storage_format=storage_format)

    except Exception:
        logger.error(f"FAILED: {input_filename}")
        unlink(output_filename)
        raise


def tag_protocols(
    tagger: ITagger,
    source_folder: str,
    target_folder: str,
    force: bool,
    recursive: bool = False,
):
    """Tags protocols in `source_folder`. Stores result in `target_folder`.
    Note: not used by Snakemake workflow (used by tag CLI script)
    """

    source_files: list[str] = glob(join(source_folder, "prot-*.xml"), recursive=recursive)
    for source_file in tqdm(source_files):
        subfolder: str = split(dirname(source_file))[1] if recursive else ''
        target_file = join(target_folder, subfolder, f"{strip_path_and_extension(source_file)}.zip")
        if force or expired(target_file, source_file):
            tag_protocol_xml(source_file, target_file, tagger, storage_format="json", force=force)
        else:
            touch(target_file)


def expired(filename: str, expiry_instance: float | str) -> bool:
    if isfile(filename):
        if isinstance(expiry_instance, str):
            expiry_instance = getmtime(expiry_instance)
        return expiry_instance > getmtime(filename)
    return True
