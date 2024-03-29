import functools
import os
import shutil
from glob import glob
from os.path import basename, getsize, isdir, isfile
from os.path import join as jj
from os.path import splitext

import pandas as pd
from dotenv import load_dotenv
from loguru import logger

from pyriksprot import dispatch, interface
from pyriksprot import metadata as md
from pyriksprot import to_speech as ts
from pyriksprot import utility as pu
from pyriksprot import workflows
from pyriksprot.corpus import tagged as tagged_corpus
from pyriksprot.workflows import subset_corpus_and_metadata

load_dotenv()


RIKSPROT_REPOSITORY_TAG = os.environ["RIKSPROT_REPOSITORY_TAG"]
ROOT_FOLDER = "tests/test_data/source/"

RIKSPROT_PARLACLARIN_FOLDER = jj(ROOT_FOLDER, RIKSPROT_REPOSITORY_TAG, "parlaclarin")
RIKSPROT_PARLACLARIN_METADATA_FOLDER = jj(RIKSPROT_PARLACLARIN_FOLDER, "metadata")
RIKSPROT_PARLACLARIN_PATTERN = jj(RIKSPROT_PARLACLARIN_FOLDER, "**/prot-*.xml")

RIKSPROT_PARLACLARIN_FAKE_FOLDER = f'tests/test_data/fakes/{RIKSPROT_REPOSITORY_TAG}/parlaclarin/protocols/'
RIKSPROT_PARLACLARIN_FAKE_EXPECTED_FOLDER = f'tests/test_data/fakes/{RIKSPROT_REPOSITORY_TAG}/expected'

TAGGED_SOURCE_FOLDER = jj(ROOT_FOLDER, RIKSPROT_REPOSITORY_TAG, "tagged_frames")
TAGGED_SOURCE_PATTERN = jj(TAGGED_SOURCE_FOLDER, "**/prot-*.zip")
TAGGED_SPEECH_FOLDER = jj(ROOT_FOLDER, RIKSPROT_REPOSITORY_TAG, "tagged_frames_speeches.feather")

SAMPLE_METADATA_DATABASE_NAME = jj(ROOT_FOLDER, RIKSPROT_REPOSITORY_TAG, "riksprot_metadata.db")


@functools.lru_cache(maxsize=1)
def load_test_documents() -> list[str]:
    return open('tests/test_data/test_documents.txt', encoding="utf-8").read().splitlines()


def generate_merged_speech_test_data(protocol_name: str):
    filename: str = jj(TAGGED_SOURCE_FOLDER, f'{protocol_name}.zip')

    protocol: interface.Protocol = tagged_corpus.load_protocol(filename=filename)
    utterances: pd.DataFrame = pd.DataFrame(
        data=[(x.u_id, x.who, x.next_id, x.prev_id, x.speaker_note_id) for x in protocol.utterances],
        columns=['u_id', 'who', 'next_id', 'prev_id', 'speaker_note_id'],
    )
    for merge_strategy in [
        'who_sequence',
        'who_speaker_note_id_sequence',
        'speaker_note_id_sequence',
        'chain',
        'chain_consecutive_unknowns',
    ]:
        merger: ts.IMergeStrategy = ts.MergerFactory.get(merge_strategy)

        items: list[list[interface.Utterance]] = merger.group(protocol.utterances)

        speech_ids = []
        for i, item in enumerate(items):
            speech_ids.extend(len(item) * [i])

        utterances[merge_strategy] = speech_ids

    utterances.to_excel(f"utterances_{protocol_name}.xlsx")


def sample_parlaclarin_corpus_exists():
    return all(
        isfile(jj(RIKSPROT_PARLACLARIN_FOLDER, "protocols", x.split('-')[1], f"{x}.xml")) for x in load_test_documents()
    )


def sample_metadata_exists():
    configs: md.MetadataTableConfigs = md.MetadataTableConfigs()
    return configs.files_exist(jj(RIKSPROT_PARLACLARIN_FOLDER, "metadata"))


def sample_tagged_frames_corpus_exists():
    return all(
        isfile(jj(TAGGED_SOURCE_FOLDER, f"{x}.zip")) or jj(TAGGED_SOURCE_FOLDER, f"{x.split('-')[1]}/{x}.zip")
        for x in load_test_documents()
    )


def sample_tagged_speech_corpus_exists():
    """Checks if the test data contains a complete tagged speech corpus. Empty files are ignored."""

    def isfile_and_non_empty(filename: str) -> bool:
        """Check if file exists in soruce folder, or in sub-folder, and is non-empty."""
        for path in [TAGGED_SOURCE_FOLDER, jj(TAGGED_SOURCE_FOLDER, f"{filename.split('-')[1]}")]:
            if isfile(jj(path, filename)) and getsize(jj(path, filename)) > 0:
                return True
        return False

    def non_empty_tagged_frames_document_names() -> list[str]:
        return [x for x in load_test_documents() if isfile_and_non_empty(f"{x}.zip")]

    expected_files: set[str] = set(non_empty_tagged_frames_document_names())
    document_names: set[str] = {
        splitext(basename(p))[0] for p in glob(jj(TAGGED_SPEECH_FOLDER, '**', 'prot-*.*'), recursive=True)
    }
    return document_names == expected_files


def ensure_test_corpora_exist(force: bool = False):
    if force or not sample_metadata_exists():
        subset_corpus_and_metadata(
            documents=load_test_documents(),
            tag=RIKSPROT_REPOSITORY_TAG,
            target_folder=ROOT_FOLDER,
            force=force,
        )

    if force or not sample_tagged_frames_corpus_exists():
        data_folder: str = os.environ["RIKSPROT_DATA_FOLDER"]
        riksprot_tagged_folder: str = jj(data_folder, RIKSPROT_REPOSITORY_TAG, 'tagged_frames')
        create_test_tagged_frames_corpus(
            protocols=load_test_documents(),
            source_folder=riksprot_tagged_folder,
            target_folder=TAGGED_SOURCE_FOLDER,
        )

    if force or not sample_tagged_speech_corpus_exists():
        create_test_speech_corpus(
            source_folder=TAGGED_SOURCE_FOLDER,
            tag=RIKSPROT_REPOSITORY_TAG,
            database_name=SAMPLE_METADATA_DATABASE_NAME,
        )


def create_test_tagged_frames_corpus(
    protocols: list[str],
    source_folder: str,
    target_folder: str,
) -> None:
    """Copies tagged protocols (data frames) from `source_folder`."""
    logger.info("Creating sample tagged frames corpus")
    logger.info(f"  source: {source_folder}")
    logger.info(f"  target: {target_folder}")

    if not isdir(source_folder):
        logger.warning(f"test data: {source_folder} not found (unable to copy tagged test protocols)")
        return

    shutil.rmtree(target_folder, ignore_errors=True)
    os.makedirs(target_folder, exist_ok=True)

    for name in protocols:
        filename: str = pu.replace_extension(name, 'zip')
        subfolder: str = filename.split('-')[1]
        source_filename: str = jj(source_folder, subfolder, filename)
        target_filename: str = jj(target_folder, filename)

        if not isfile(source_filename):
            logger.warning(f"test data: {source_filename} not found (unable to copy tagged test protocols)")
            continue

        shutil.copy(src=source_filename, dst=target_filename)
        logger.info(f"  copied: {source_filename} to {jj(target_folder, filename)}")


def create_test_speech_corpus(*, source_folder: str, tag: str, database_name: str) -> None:
    """Creates a tagged frames speech corpus from tagged frames corpus"""
    # target_type: str, merge_strategy: to_speech.MergeStrategyType, compress_type: str):
    target_type: str = 'single-id-tagged-frame-per-group'
    merge_strategy: str = 'chain'
    compress_types: list[str] = ['csv', 'feather']

    logger.info("Creating sample speech tagged ID frame corpus")
    logger.info(f"    source: {source_folder}")
    logger.info(f"  metadata: {database_name}")

    for compress_type in compress_types:
        target_name: str = jj("tests/test_data/source/", tag, f"tagged_frames_speeches.{compress_type}")

        logger.info(f"    target: {target_name}")

        fixed_opts: dict = dict(
            source_folder=source_folder,
            metadata_filename=database_name,
            segment_level=interface.SegmentLevel.Speech,
            temporal_key=interface.TemporalKey.NONE,
            content_type=interface.ContentType.TaggedFrame,
            multiproc_keep_order=None,
            multiproc_processes=None,
            multiproc_chunksize=100,
            segment_skip_size=1,
            years=None,
            group_keys=('who',),
            force=True,
            skip_lemma=False,
            skip_text=True,
            skip_puncts=True,
            skip_stopwords=True,
            lowercase=True,
            progress=False,
        )
        workflows.extract_corpus_tags(
            **fixed_opts,
            target_name=target_name,
            target_type=target_type,
            compress_type=dispatch.CompressType(compress_type),
            merge_strategy=merge_strategy,
        )
