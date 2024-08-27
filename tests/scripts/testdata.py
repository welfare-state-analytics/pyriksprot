import os
import sys
from os.path import join

import click
from loguru import logger

from pyriksprot.configuration import ConfigStore
from pyriksprot.configuration.inject import ConfigValue
from tests.utility import (
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
@click.argument('config-filename', type=str, required=False)
def main(config_filename):
    ConfigStore.configure_context(source=config_filename, env_prefix=None)
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
    version: str = ConfigStore.config().get("corpus:version")
    root_folder: str = ConfigStore.config().get("root_folder")
    try:
        if sample_parlaclarin_corpus_exists() and sample_metadata_exists():
            if not force:
                raise ValueError("Metadata already exists. Use --force to overwrite.")

        subset_corpus_and_metadata(tag=version, target_folder=root_folder, documents=load_test_documents())

    except ValueError as ex:
        logger.error(ex)
        sys.exit(-1)


@click.command()
@click.option('--force', is_flag=True, help='Force overwrite', default=False)
@click.argument('source-folder', type=str, required=True)
@click.argument('target-folder', type=str, required=False)
def generate_tagged_frames(force: bool = False, source_folder: str = None, target_folder: str = None):
    target_folder: str = target_folder or ConfigValue("tagged_frames:folder").resolve()
    try:
        if sample_tagged_frames_corpus_exists(target_folder):
            if not force:
                raise ValueError("Tagged frames corpus already exists. Use --force to overwrite.")

        create_test_tagged_frames_corpus(
            protocols=load_test_documents(),
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
def generate_tagged_speech_corpora(force: bool = False, tag: str = None, database: str=None, source_folder: str = None):
    version: str = tag or ConfigValue("corpus:version").resolve()
    database: str = database or ConfigValue("metadata:database").resolve()
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

    main.add_command(generate_complete_sample_data, "complete")
    main.add_command(generate_corpus_and_metadata, "corpus-and-metadata")
    main.add_command(generate_tagged_frames, "tagged-frames")
    main.add_command(generate_tagged_speech_corpora, "tagged-speech-corpora")

    # setup_logs()

    main()  # pylint: disable=no-value-for-parameter
