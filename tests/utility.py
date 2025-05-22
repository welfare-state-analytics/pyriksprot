import os
import shutil
from glob import glob
from os.path import isdir, isfile
from os.path import join as jj
from os.path import splitext

import pandas as pd
from loguru import logger

from pyriksprot import dispatch, interface
from pyriksprot import metadata as md
from pyriksprot import to_speech as ts
from pyriksprot import utility as pu
from pyriksprot import workflows
from pyriksprot.configuration import ConfigValue
from pyriksprot.corpus import tagged as tagged_corpus
from pyriksprot.workflows import subset_corpus_and_metadata
from pyriksprot.workflows.subset_corpus import load_document_patterns


def generate_merged_speech_test_data(protocol_name: str) -> None:
    tagged_source_folder: str = ConfigValue("tagged_frames:folder").resolve()

    filename: str = jj(tagged_source_folder, f'{protocol_name}.zip')

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


def sample_parlaclarin_corpus_exists() -> bool:
    source_folder: str = ConfigValue("corpus:folder").resolve()
    test_protocols: list[str] = load_document_patterns(filename='tests/test_data/test_documents.txt', extension='xml')
    is_complete: bool = all(isfile(jj(source_folder, x.split('-')[1], x)) for x in test_protocols)
    if not is_complete:
        logger.info(f"ParlaClarin corpus in {source_folder} is not complete!")
    return is_complete


def sample_metadata_exists() -> bool:
    source_folder: str = ConfigValue("metadata:folder").resolve()
    corpus_version: str = ConfigValue("metadata:version").resolve()

    filename: str = ConfigValue("metadata.database.options.filename").resolve()
    if not isfile(filename):
        logger.info(f"metadata database file not found: {filename}")
        return False

    configs: md.MetadataSchema = md.MetadataSchema(version=corpus_version)
    if not configs.files_exist(source_folder):
        logger.info(f"metadata schema files not found in: {source_folder}")
        return False

    tf_filename: str = ConfigValue("dehyphen:tf_filename").resolve()
    if not isfile(tf_filename):
        logger.info(f"term frequency file not found: {tf_filename}")
        return False

    return True


def sample_tagged_frames_corpus_exists(folder: str = None) -> bool:
    folder = folder or ConfigValue("tagged_frames:folder").resolve()
    test_protocols: list[str] = load_document_patterns(filename='tests/test_data/test_documents.txt')
    is_complete: bool = all(isfile(jj(folder, f"{x}.zip")) or jj(folder, x.split('-')[1], x) for x in test_protocols)
    if not is_complete:
        logger.info(f"tagged frames corpus in {folder} is not complete")
    return is_complete


def sample_tagged_speech_corpus_exists():
    tagged_speech_folder: str = ConfigValue("tagged_speeches:folder").resolve()
    extension: str = splitext(tagged_speech_folder)[1]
    is_at_least_not_empty: bool = len(glob(jj(tagged_speech_folder, "**", f"prot-*{extension}"), recursive=True)) > 0
    if not is_at_least_not_empty:
        logger.info(f"tagged speech corpus in {tagged_speech_folder} is empty")
    return is_at_least_not_empty


def get_test_documents(extension=None) -> list[str]:
    return load_document_patterns(filename='tests/test_data/test_documents.txt', extension=extension)


def ensure_test_corpora_exist(
    *,
    force: bool = False,
    corpus_version: str = None,
    metadata_version: str = None,
    tagged_source_folder: str = None,
    root_folder: str = None,
    database: str = None,
    only_check: bool = False,
):
    corpus_version = corpus_version or ConfigValue("corpus:version").resolve()
    metadata_version = metadata_version or ConfigValue("metadata:version").resolve()

    if not corpus_version:
        logger.warning("ensure_test_corpora_exist: corpus version not set, unable to verify test corpora")
        return

    if only_check:
        if not all(
            [
                sample_parlaclarin_corpus_exists(),
                sample_metadata_exists(),
                sample_tagged_frames_corpus_exists(),
                sample_tagged_speech_corpus_exists(),
            ]
        ):
            raise Exception(f"test data for {corpus_version} (corpus) and {metadata_version} (metadata) is not complete")

    tagged_source_folder = tagged_source_folder or ConfigValue("tagged_frames:folder").resolve()
    root_folder = root_folder or ConfigValue("root_folder").resolve()
    database = database or ConfigValue("metadata:database").resolve()

    filenames: list[str] = get_test_documents(extension="xml")

    if force or not sample_metadata_exists():
        subset_corpus_and_metadata(
            corpus_version=corpus_version,
            metadata_version=metadata_version,
            corpus_folder=ConfigValue("corpus:folder").resolve(),
            metadata_folder=ConfigValue("metadata:folder").resolve(),
            documents=filenames,
            global_corpus_folder=ConfigValue("corpus:folder").resolve(),
            global_metadata_folder=ConfigValue("metadata:folder").resolve(),
            target_root_folder=ConfigValue("root_folder").resolve(),
            scripts_folder=None,
            gh_metadata_opts=ConfigValue("metadata:github").resolve(),
            gh_records_opts=ConfigValue("corpus:github").resolve(),
            db_opts=ConfigValue("metadata:database").resolve(),
            tf_filename=ConfigValue("dehyphen:tf_filename").resolve(),
            skip_download=True,
            force=force,
        )

    if force or not sample_tagged_frames_corpus_exists():
        data_folder: str = root_folder
        riksprot_tagged_folder: str = jj(data_folder, corpus_version, 'tagged_frames')
        create_test_tagged_frames_corpus(
            protocols=filenames,
            source_folder=riksprot_tagged_folder,
            target_folder=tagged_source_folder,
        )

    if force or not sample_tagged_speech_corpus_exists():
        create_test_speech_corpus(
            source_folder=tagged_source_folder,
            corpus_version=corpus_version,
            database_name=database,
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


def create_test_speech_corpus(*, source_folder: str, corpus_version: str, database_name: str) -> None:
    """Creates a tagged frames speech corpus from tagged frames corpus"""
    # target_type: str, merge_strategy: to_speech.MergeStrategyType, compress_type: str):
    target_type: str = 'single-id-tagged-frame-per-group'
    merge_strategy: str = 'chain'
    compress_types: list[str] = ['csv', 'feather']

    logger.info("Creating sample speech tagged ID frame corpus")
    logger.info(f"    source: {source_folder}")
    logger.info(f"  metadata: {database_name}")

    for compress_type in compress_types:
        target_name: str = jj("tests/test_data/source/", corpus_version, f"tagged_frames_speeches.{compress_type}")

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
