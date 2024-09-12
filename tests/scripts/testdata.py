import sys

import click
from loguru import logger

from pyriksprot.configuration import ConfigStore
from pyriksprot.configuration.inject import ConfigValue
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
def main(config_filename):
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

        subset_corpus_and_metadata(
            documents=filenames,
            source_folder=None,
            target_folder=ConfigValue("root_folder").resolve(),
            tag=ConfigValue("corpus:version").resolve(),
            scripts_folder=None,
            gh_metadata_opts= ConfigValue("metadata:github").resolve(),
            gh_records_opts=ConfigValue("corpus:github").resolve(),
            db_opts=ConfigValue("metadata:database").resolve(),
            force=force
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
@click.option('--force', is_flag=True, help='Force overwrite', default=False)
@click.option('--tag', type=click.STRING, help='Corpus version', default=None, required=False)
@click.option('--database', type=click.STRING, help='Metadata database', default=None, required=False)
@click.argument('source-folder', type=str, required=False)
def generate_tagged_speech_corpora(
    force: bool = False, tag: str = None, database: str = None, source_folder: str = None
):
    version: str = tag or ConfigValue("corpus:version").resolve()
    database = database or ConfigValue("metadata:database").resolve()
    try:
        if sample_tagged_speech_corpus_exists():
            if not force:
                raise ValueError("Tagged frames corpus already exists. Use --force to overwrite.")

        create_test_speech_corpus(
            source_folder=source_folder or ConfigValue("tagged_frames:folder").resolve(),
            tag=version,
            database_name=database,
        )

    except ValueError as ex:
        logger.error(ex)
        sys.exit(-1)

if __name__ == "__main__":
    main.add_command(generate_complete_sample_data, "complete")
    main.add_command(generate_corpus_and_metadata, "corpus-and-metadata")
    main.add_command(generate_tagged_frames, "tagged-frames")
    main.add_command(generate_tagged_speech_corpora, "tagged-speech-corpora")

    # setup_logs()

    main()  # pylint: disable=no-value-for-parameter
