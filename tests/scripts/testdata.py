import os
import sys
from os.path import join

import click
from dotenv import load_dotenv
from loguru import logger

from tests.utility import (
    RIKSPROT_REPOSITORY_TAG,
    ROOT_FOLDER,
    SAMPLE_METADATA_DATABASE_NAME,
    TAGGED_SOURCE_FOLDER,
    create_test_speech_corpus,
    create_test_tagged_frames_corpus,
    ensure_test_corpora_exist,
    load_test_documents,
    sample_metadata_exists,
    sample_parlaclarin_corpus_exists,
    sample_tagged_frames_corpus_exists,
    sample_tagged_speech_corpus_exists,
    subset_corpus_and_metadata,
)


@click.group(help="CLI tool to manage riksprot metadata")
def main():
    pass


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

        subset_corpus_and_metadata(
            tag=RIKSPROT_REPOSITORY_TAG, target_folder=ROOT_FOLDER, documents=load_test_documents()
        )

    except ValueError as ex:
        logger.error(ex)
        sys.exit(-1)


@click.command()
@click.option('--force', is_flag=True, help='Force overwrite', default=False)
def generate_tagged_frames(force: bool = False):
    try:
        if sample_tagged_frames_corpus_exists():
            if not force:
                raise ValueError("Tagged frames corpus already exists. Use --force to overwrite.")

        data_folder: str = os.environ["RIKSPROT_DATA_FOLDER"]
        riksprot_tagged_folder: str = join(data_folder, RIKSPROT_REPOSITORY_TAG, 'tagged_frames')
        create_test_tagged_frames_corpus(
            protocols=load_test_documents(),
            source_folder=riksprot_tagged_folder,
            target_folder=TAGGED_SOURCE_FOLDER,
        )
    except ValueError as ex:
        logger.error(ex)
        sys.exit(-1)


@click.command()
@click.option('--force', is_flag=True, help='Force overwrite', default=False)
def generate_tagged_speech_corpora(force: bool = False):
    try:
        if sample_tagged_speech_corpus_exists():
            if not force:
                raise ValueError("Tagged frames corpus already exists. Use --force to overwrite.")

        create_test_speech_corpus(
            source_folder=TAGGED_SOURCE_FOLDER,
            tag=RIKSPROT_REPOSITORY_TAG,
            database_name=SAMPLE_METADATA_DATABASE_NAME,
        )

    except ValueError as ex:
        logger.error(ex)
        sys.exit(-1)


# @click.command()
# @click.argument('target', type=click.STRING)
# @click.option('--tag', type=click.STRING, help='Metadata version', default=None)
# @click.option('--source-folder', type=click.STRING, default=None)
# @click.option('--force', type=click.BOOL, is_flag=True, help='Force overwrite', default=False)
# @click.option('--load-index', type=click.BOOL, is_flag=True, help='Load utterance index', default=False)
# @click.option(
#     '--scripts-folder',
#     type=click.STRING,
#     help='Apply scripts in specified folder to DB. If not specified the scripts are loaded from SQL-module.',
#     default=None,
# )
# @click.option('--skip-scripts', type=click.BOOL, is_flag=True, help='Skip loading SQL scripts', default=False)
# def create_database(
#     target: str,
#     tag: str = None,
#     source_folder: str = None,
#     force: bool = False,
#     scripts_folder: str = None,
#     load_index: bool = True,
#     skip_scripts: bool = False,
# ) -> None:
#     try:
#         service: md.DatabaseHelper = md.DatabaseHelper(target)
#         service.create(
#             tag=tag,
#             folder=source_folder,
#             force=force,
#         )

#         if load_index:
#             logger.info("loading index...")
#             service.load_corpus_indexes(folder=source_folder or dirname(target))

#         if not skip_scripts:
#             logger.info(f"loading scripts from {scripts_folder if scripts_folder else 'sql module'}...")
#             service.load_scripts(folder=scripts_folder)

#     except Exception as ex:
#         logger.error(ex)
#         click.echo(ex)
#         sys.exit(1)


# # type: ignore


# def setup_logs(log_folder: str = "./tests/output"):
#     os.makedirs(log_folder, exist_ok=True)

#     logger.remove(0)
#     logger.add(join(log_folder, "{time:YYYYMMDDHHmmss}_testdata.log"), backtrace=True, diagnose=True)


if __name__ == "__main__":
    load_dotenv()

    main.add_command(generate_complete_sample_data, "complete")
    main.add_command(generate_corpus_and_metadata, "corpus-and-metadata")
    main.add_command(generate_tagged_frames, "tagged-frames")
    main.add_command(generate_tagged_speech_corpora, "tagged-speech-corpora")

    # setup_logs()

    main()  # pylint: disable=no-value-for-parameter
