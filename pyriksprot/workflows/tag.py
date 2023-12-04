from __future__ import annotations

import abc
import importlib
from functools import reduce
from glob import glob
from os.path import dirname, getmtime, isfile, join, split
from typing import Any, Mapping, Protocol, Type, Union

from loguru import logger
from tqdm import tqdm

from pyriksprot.corpus.parlaclarin import parse
from pyriksprot.corpus.tagged import persist

from .. import interface
from .. import preprocess as pp
from ..configuration import ConfigValue, inject_config
from ..utility import ensure_path, strip_path_and_extension, touch, unlink

METADATA_FILENAME: str = 'metadata.json'
TAGGED_COLUMNS: list[str] = ['token', 'lemma', 'pos', 'xpos', 'sentence_id']

TaggedDocument = Mapping[str, list[str]]


class ITaggerFactory(abc.ABC):
    identifier: str = "undefined"

    @abc.abstractmethod
    def create(self) -> "ITagger":
        ...


class Processor(Protocol):
    def __call__(self, text: str) -> str:
        ...


class ITagger(abc.ABC):
    def __init__(self, preprocessors: Processor = None):
        self.preprocessors: Processor = ProcessorResolver.resolve_preprocessors(preprocessors)

    def tag(self, text: Union[str, list[str]], preprocess: bool = True) -> list[TaggedDocument]:
        """Tag text. Return dict if lists."""
        if isinstance(text, str):
            text = [text]

        if not isinstance(text, list):
            return ValueError("invalid type")

        if len(text) == 0:
            return []

        if preprocess:
            text: list[str] = [self.preprocess(d) for d in text]

        tagged_documents = self._tag(text)

        return tagged_documents

    @abc.abstractmethod
    def _tag(self, text: Union[str, list[str]]) -> list[TaggedDocument]:
        ...

    @abc.abstractmethod
    def _to_dict(self, tagged_document: Any) -> TaggedDocument:
        return {}

    @staticmethod
    def to_csv(tagged_document: TaggedDocument, sep='\t') -> str:
        """Converts a TaggedDocument to a TSV string"""

        word_count: int = len(tagged_document['token'])

        columns: list[str] = [c for c in TAGGED_COLUMNS if c in tagged_document]

        if word_count > 0:
            ### ignore empty columns with no data
            columns = [x for x in columns if len(tagged_document[x]) == word_count]

        csv_str: str = (
            sep.join(columns)
            + '\n'
            + '\n'.join(sep.join(str(tagged_document[c][i]) for c in columns) for i in range(0, word_count))
        )
        return csv_str

    def preprocess(self, text: str) -> str:
        """Transform `text` with preprocessors."""
        text: str = reduce(lambda res, f: f(res), self.preprocessors, text)
        return text


class ProcessorResolver:
    @staticmethod
    def resolve_preprocessors(preprocessors: str | list[Processor | str]) -> list[Processor]:
        if isinstance(preprocessors, str):
            preprocessors = preprocessors.split(",")

        if not any(isinstance(p, str) for p in preprocessors):
            return preprocessors

        ProcessorResolver.is_satisfied(preprocessors)

        return [pp.Registry.items.get(p) if isinstance(p, str) else p for p in preprocessors]

    @staticmethod
    def is_satisfied(preprocessors: str | list[Processor | str]) -> bool:
        keys: list[str] = [p for p in preprocessors if isinstance(p, str)]
        unknown_keys: list[str] = [p for p in keys if not p in pp.Registry.items]
        if unknown_keys:
            raise ValueError(f"unknown preprocessor: {', '.join(unknown_keys)}")
        return True


class TaggerProvider:
    @inject_config
    @staticmethod
    def tagger_factory(
        module_name: str | ConfigValue = ConfigValue(key="tagger:module"),
    ) -> ITaggerFactory:
        if module_name is None:
            return None
        tagger_module = importlib.import_module(module_name)
        abstract_factory = getattr(tagger_module, "tagger_factory")
        if abstract_factory is None:
            raise ValueError(f"Module {module_name} does not implement `tagger_factory`.")
        return abstract_factory()


class TaggerRegistry:
    """Simple tagger cache since some taggers are expensive to setup"""

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

    documents: list[TaggedDocument] = tagger.tag(texts, preprocess=preprocess)

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
    pattern: str = "**/prot-*.xml",
):
    """Tags protocols in `source_folder`. Stores result in `target_folder`.
    Note: not used by Snakemake workflow (used by tag CLI script)
    """
    source_files: list[str] = glob(join(source_folder, pattern), recursive=recursive)
    for source_file in tqdm(source_files):
        target_file: str = resolve_target_filename(source_file, target_folder, recursive)
        if force or expired(target_file, source_file):
            tag_protocol_xml(source_file, target_file, tagger, storage_format="json", force=force)
        else:
            touch(target_file)


def resolve_target_filename(source_file: str, target_folder: str, recursive: bool) -> str:
    """Add subfolder to target folder if recursive and not already included in target folder"""
    filename: str = f"{strip_path_and_extension(source_file)}.zip"
    if recursive and split(target_folder)[1] != split(dirname(source_file))[1]:
        return join(target_folder, split(dirname(source_file))[1], filename)
    return join(target_folder, filename)


def expired(filename: str, expiry_instance: float | str) -> bool:
    if isfile(filename):
        if isinstance(expiry_instance, str):
            expiry_instance = getmtime(expiry_instance)
        return expiry_instance > getmtime(filename)
    return True
