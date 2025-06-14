import os
import sys

import click
from loguru import logger

from pyriksprot.configuration import ConfigStore
from pyriksprot.configuration.inject import ConfigValue
from pyriksprot.workflows.tf import compute_term_frequencies
from tests.utility import (
    create_test_speech_corpus,
    create_test_tagged_frames_corpus,
    ensure_test_corpora_exist,
    get_test_documents,
    sample_metadata_exists,
    sample_parlaclarin_corpus_exists,
    sample_tagged_frames_corpus_exists,
    sample_tagged_speech_corpus_exists,
    subset_corpus_and_metadata,
)


@click.group(help="CLI tool to manage riksprot metadata")
@click.argument('config-filename', type=str, required=False)
def main(config_filename: str):
    ConfigStore.configure_context(source=config_filename, env_prefix=None)


@click.command()
@click.option('--force', is_flag=True, help='Force overwrite', default=False)
def generate_complete_sample_data(force: bool):
    try:
        ensure_test_corpora_exist(force=force)
    except ValueError as ex:
        logger.error(ex)
        sys.exit(-1)


@click.command()
@click.option('--force', is_flag=True, help='Force overwrite', default=False)
def generate_corpus_and_metadata(force: bool = False):
    try:
        if sample_parlaclarin_corpus_exists() and sample_metadata_exists():
            if not force:
                raise ValueError("Metadata already exists. Use --force to overwrite.")

        filenames: list[str] = get_test_documents(extension="xml")
        logger.info(f"filenames: {filenames}")
        subset_corpus_and_metadata(
            corpus_version=ConfigValue("corpus:version").resolve(),
            metadata_version=ConfigValue("metadata:version").resolve(),
            corpus_folder=ConfigValue("corpus:folder").resolve(),
            metadata_folder=ConfigValue("metadata:folder").resolve(),
            documents=filenames,
            global_corpus_folder=ConfigValue("global:corpus:folder").resolve(),
            global_metadata_folder=ConfigValue("global:metadata:folder").resolve(),
            target_root_folder=ConfigValue("root_folder").resolve(),
            scripts_folder=None,
            gh_metadata_opts=ConfigValue("metadata:github").resolve(),
            gh_records_opts=ConfigValue("corpus:github").resolve(),
            db_opts=ConfigValue("metadata:database").resolve(),
            tf_filename=ConfigValue("dehyphen:tf_filename").resolve(),
            skip_download=True,
            force=force,
        )

    except ValueError as ex:
        logger.error(ex)
        sys.exit(-1)


@click.command()
@click.option('--force', is_flag=True, help='Force overwrite', default=False)
@click.argument('source-folder', type=str, required=True)
@click.argument('target-folder', type=str, required=False)
def generate_tagged_frames(force: bool = False, source_folder: str = None, target_folder: str = None):
    target_folder = target_folder or ConfigValue("tagged_frames:folder").resolve()
    try:
        if sample_tagged_frames_corpus_exists(target_folder):
            if not force:
                raise ValueError("Tagged frames corpus already exists. Use --force to overwrite.")

        filenames: list[str] = get_test_documents(extension="xml")
        create_test_tagged_frames_corpus(
            protocols=filenames,
            source_folder=source_folder,
            target_folder=target_folder,
        )
    except ValueError as ex:
        logger.error(ex)
        sys.exit(-1)


@click.command()
@click.option('--source-folder', type=str, help='XML corpus folder', default=None, required=False)
@click.option('--target-filename', type=str, help='Target TF filename', default=None, required=False)
@click.option('--force', is_flag=True, help='Force overwrite', default=False)
def generate_word_frequencies(source_folder: str = None, target_filename: str = None, force: bool = False):
    try:
        source_folder = source_folder or ConfigValue("corpus:folder").resolve()
        logger.info(f"source_folder: {source_folder}")

        target_filename = target_filename or ConfigValue("dehyphen:tf_filename").resolve()
        logger.info(f"target_filename: {target_filename}")

        if os.path.exists(target_filename):
            if not force:
                raise ValueError("Word frequency already exists. Use --force to overwrite.")
            os.remove(target_filename)

        compute_term_frequencies(source=source_folder, filename=target_filename)
    except ValueError as ex:
        logger.error(ex)
        sys.exit(-1)


@click.command()
@click.option('--force', is_flag=True, help='Force overwrite', default=False)
@click.option('--tag', type=click.STRING, help='Corpus version', default=None, required=False)
@click.option('--database-filename', type=click.STRING, help='Metadata database', default=None, required=False)
@click.argument('source-folder', type=str, required=False)
def generate_tagged_speech_corpora(
    force: bool = False, tag: str = None, database_filename: str = None, source_folder: str = None
):
    """Generate tagged speech corpora for sample dataset by copying files from existing global tagged frames corpus"""
    tag = tag or ConfigValue("corpus:version").resolve()
    database_filename = database_filename or ConfigValue("metadata:database.options.filename").resolve()
    try:
        if sample_tagged_speech_corpus_exists():
            if not force:
                raise ValueError("Tagged frames corpus already exists. Use --force to overwrite.")

        create_test_speech_corpus(
            source_folder=source_folder or ConfigValue("tagged_frames:folder").resolve(),
            corpus_version=tag,
            database_name=database_filename,
        )

    except ValueError as ex:
        logger.error(ex)
        sys.exit(-1)


if __name__ == "__main__":
    main.add_command(generate_complete_sample_data, "complete")
    main.add_command(generate_corpus_and_metadata, "corpus-and-metadata")
    main.add_command(generate_word_frequencies, "word-frequencies")
    main.add_command(generate_tagged_frames, "tagged-frames")
    main.add_command(generate_tagged_speech_corpora, "tagged-speech-corpora")

    main()  # pylint: disable=no-value-for-parameter

    # # from click.testing import CliRunner
    # # runner = CliRunner()
    # # result = runner.invoke(
    # #     main,
    # #     [
    # #         'generate_corpus_and_metadata',
    # #         'tests/test_data/config.yml',
    # #     ],
    # # )
    # # print(result.output)
    # config_filename = "tests/config.yml"
    # force = True
    # ConfigStore.configure_context(source=config_filename, env_prefix=None)
    # try:
    #     if sample_parlaclarin_corpus_exists() and sample_metadata_exists():
    #         if not force:
    #             raise ValueError("Metadata already exists. Use --force to overwrite.")

    #     filenames: list[str] = get_test_documents(extension="xml")
    #     logger.info(f"filenames: {filenames}")
    #     subset_corpus_and_metadata(
    #         corpus_version=ConfigValue("corpus:version").resolve(),
    #         metadata_version=ConfigValue("metadata:version").resolve(),
    #         documents=filenames,
    #         global_corpus_folder=ConfigValue("global:corpus:folder").resolve(),
    #         global_metadata_folder=ConfigValue("global:metadata:folder").resolve(),
    #         target_folder=ConfigValue("root_folder").resolve(),
    #         scripts_folder=None,
    #         gh_metadata_opts=ConfigValue("metadata:github").resolve(),
    #         gh_records_opts=ConfigValue("corpus:github").resolve(),
    #         db_opts=ConfigValue("metadata:database").resolve(),
    #         tf_filename=ConfigValue("dehyphen:tf_filename").resolve(),
    #         skip_download=True,
    #         force=force,
    #     )

    # except ValueError as ex:
    #     logger.error(ex)
    #     sys.exit(-1)

    # config_filename = "tests/config.yml"
    # force = True
    # source_folder = None
    # ConfigStore.configure_context(source=config_filename, env_prefix=None)
    # version: str = ConfigValue("corpus:version").resolve()
    # database_filename = ConfigValue("metadata:database.options.filename").resolve()
    # try:
    #     if sample_tagged_speech_corpus_exists():
    #         if not force:
    #             raise ValueError("Tagged frames corpus already exists. Use --force to overwrite.")

    #     create_test_speech_corpus(
    #         source_folder=source_folder or ConfigValue("tagged_frames:folder").resolve(),
    #         tag=version,
    #         database_name=database_filename,
    #     )

    # except ValueError as ex:
    #     logger.error(ex)
    #     sys.exit(-1)
