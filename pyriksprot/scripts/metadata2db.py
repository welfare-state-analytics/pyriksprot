import os
import sys
from os.path import join
from typing import Any

import click
from loguru import logger

from pyriksprot import metadata as md
from pyriksprot.configuration.inject import ConfigStore, ConfigValue
from pyriksprot.corpus.parlaclarin import ProtocolMapper
from pyriksprot.workflows import create_database_workflow

# pylint: disable=too-many-arguments


@click.group(help="CLI tool to manage riksprot metadata")
def main(): ...


@click.command()
@click.argument('config_filename', type=str)
@click.argument('tag', type=str)
def verify_metadata_filenames(config_filename: str, tag: str):
    try:
        ConfigStore().configure_context(source=config_filename)

        user: str = ConfigValue("metadata.github.user").resolve()
        repository: str = ConfigValue("metadata.github.repository").resolve()
        path: str = ConfigValue("metadata.github.path").resolve()
        tag = tag or ConfigValue("version").resolve()

        md.ConfigConformsToTagSpecification(user=user, repository=repository, path=path, tag=tag).is_satisfied()

    except ValueError as ex:
        logger.error(ex)
        sys.exit(-1)


@click.command()
@click.argument('config_filename', type=str)
@click.argument('tags', nargs=-1, type=str)
def verify_metadata_columns(config_filename: str, tags: str):
    try:
        ConfigStore().configure_context(source=config_filename)

        user: str = ConfigValue("metadata.github.user").resolve()
        repository: str = ConfigValue("metadata.github.repository").resolve()
        path: str = ConfigValue("metadata.github.path").resolve()

        if len(tags) == 1:
            md.ConfigConformsToTagSpecification(user=user, repository=repository, path=path, tag=tags[0]).is_satisfied()
        elif len(tags) == 2:
            md.TagsConformSpecification(
                user=user, repository=repository, path=path, tag1=tags[0], tag2=tags[1]
            ).is_satisfied()
        else:
            raise ValueError("please specify 1 or 2 tags")
    except ValueError as ex:
        logger.error(ex)
        sys.exit(-1)


@click.command()
@click.argument('target_folder', type=str)
@click.argument('tag', type=str)
def download_metadata(target_folder: str, tag: str):
    md.gh_fetch_metadata_by_config(schema=md.MetadataSchema(tag=tag), tag=tag, folder=target_folder, force=True)


@click.command()
@click.argument('corpus_folder', type=str)
@click.argument('target_folder', type=str)
@click.argument('tag', type=str)
def create_corpus_indexes(corpus_folder: str, target_folder: str, tag: str) -> None:
    factory: md.CorpusIndexFactory = md.CorpusIndexFactory(ProtocolMapper, schema=md.MetadataSchema(tag=tag))
    factory.generate(corpus_folder=corpus_folder, target_folder=target_folder)


@click.command()
@click.argument('target', type=str)
@click.argument('config_filename', type=str)
@click.option('--target-filename', type=str, help='Sqlite target filename', default=None)
@click.option('--tag', type=str, help='Metadata version', default=None)
@click.option('--source-folder', type=str, default=None)
@click.option('--force', type=bool, is_flag=True, help='Force overwrite', default=False)
@click.option('--skip-create-index', type=bool, is_flag=True, help='Skip generationg derived data index', default=False)
@click.option('--corpus-folder', type=str, help='ParlaCLARIN source folder', default=None)
@click.option(
    '--scripts-folder',
    type=str,
    help='Apply scripts in specified folder to DB. If not specified the scripts are loaded from SQL-module.',
    default=None,
)
@click.option('--skip-load-scripts', type=bool, is_flag=True, help='Skip loading SQL scripts', default=False)
@click.option(
    '--skip-download-metadata', type=bool, is_flag=False, help='Skip download of Github metadata', default=False
)
def create_database(
    config_filename: str,
    target_filename: str = None,
    tag: str = None,
    source_folder: str = None,
    force: bool = False,
    scripts_folder: str = None,
    corpus_folder: str = None,
    skip_create_index: bool = True,
    skip_load_scripts: bool = False,
    skip_download_metadata: bool = False,
) -> None:
    """Create a database from metadata configuration"""
    try:
        ConfigStore().configure_context(source=config_filename)

        tag = tag or ConfigValue("version").resolve()
        source_folder = source_folder or ConfigValue("metadata.folder").resolve()
        target_filename = target_filename or ConfigValue("metadata.database.options.filename").resolve()
        corpus_folder = corpus_folder or ConfigValue("corpus.folder").resolve()

        gh_opts: dict[str, Any] | None = corpus_folder or ConfigValue("metadata.github").resolve()

        create_database_workflow(
            tag=tag,
            metadata_folder=source_folder,
            db_opts=target_filename,
            gh_opts=gh_opts,
            corpus_folder=corpus_folder,
            skip_create_index=skip_create_index,
            scripts_folder=scripts_folder,
            skip_download_metadata=skip_download_metadata,
            skip_load_scripts=skip_load_scripts,
            force=force,
        )

    except Exception as ex:
        logger.error(ex)
        click.echo(ex)
        sys.exit(1)


# type: ignore


def setup_logs(log_folder: str = "./metadata/logs"):
    os.makedirs(log_folder, exist_ok=True)

    logger.remove(0)
    logger.add(join(log_folder, "{time:YYYYMMDDHHmmss}_metadata.log"), backtrace=True, diagnose=True)


if __name__ == "__main__":
    main.add_command(verify_metadata_filenames, "filenames")
    main.add_command(verify_metadata_columns, "columns")
    main.add_command(create_corpus_indexes, "index")
    main.add_command(create_database, "database")
    main.add_command(download_metadata, "download")

    setup_logs()

    main()  # pylint: disable=no-value-for-parameter
