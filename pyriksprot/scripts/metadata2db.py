import os
import sys
from os.path import join

import click
from loguru import logger

from pyriksprot import metadata as md
from pyriksprot.configuration.inject import ConfigStore, ConfigValue
from pyriksprot.corpus.parlaclarin import ProtocolMapper
from pyriksprot.metadata import database

# pylint: disable=too-many-arguments


@click.group(help="CLI tool to manage riksprot metadata")
def main():
    ...


@click.command()
@click.argument('config_filename', type=str)
@click.option('--tag', type=str, help='Metadata version', default=None)
def verify_metadata_filenames(config_filename: str, tag: str = None):
    try:
        ConfigStore().configure_context(source=config_filename)

        user: str = ConfigValue("metadata.github.user").resolve()
        repository: str = ConfigValue("metadata.github.repository").resolve()
        path: str = ConfigValue("metadata.github.path").resolve()
        tag: str = tag or ConfigValue("version").resolve()

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
            md.TagsConformSpecification(user=user, repository=repository, path=path, **tags).is_satisfied()
        else:
            raise ValueError("please specify 1 or 2 tags")
    except ValueError as ex:
        logger.error(ex)
        sys.exit(-1)


@click.command()
@click.option('--tag', type=str, help='Metadata version', default=None)
@click.argument('target_folder', type=str)
def download_metadata(tag: str, target_folder: str):
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
@click.option('--load-index', type=bool, is_flag=True, help='Load utterance index', default=False)
@click.option('--corpus-folder', type=str, help='ParlaCLARIN source folder', default=None)
@click.option(
    '--scripts-folder',
    type=str,
    help='Apply scripts in specified folder to DB. If not specified the scripts are loaded from SQL-module.',
    default=None,
)
@click.option('--skip-scripts', type=bool, is_flag=True, help='Skip loading SQL scripts', default=False)
def create_database(
    config_filename: str,
    target_filename: str = None,
    tag: str = None,
    source_folder: str = None,
    force: bool = False,
    scripts_folder: str = None,
    create_index: bool = True,
    corpus_folder: str = None,
    skip_scripts: bool = False,
) -> None:
    """Create a database from metadata configuration"""
    try:
        ConfigStore().configure_context(source=config_filename)

        tag = tag or ConfigValue("version").resolve()

        db: md.DatabaseInterface = resolve_backend(target_filename)

        service: md.MetadataFactory = md.MetadataFactory(tag=tag, backend=db, **db.opts)
        service.create(folder=source_folder, force=force)

        if create_index:
            logger.info("generating indexes...")

            corpus_folder = corpus_folder or ConfigValue("corpus.folder").resolve()

            factory: md.CorpusIndexFactory = md.CorpusIndexFactory(ProtocolMapper, schema=service.schema)
            factory.generate(corpus_folder=corpus_folder, target_folder=source_folder)
            factory.upload(db=db, folder=source_folder)

        if not skip_scripts:
            logger.info(f"loading scripts from {scripts_folder if scripts_folder else 'sql module'}...")
            service.execute_sql_scripts(folder=scripts_folder)

    except Exception as ex:
        logger.error(ex)
        click.echo(ex)
        sys.exit(1)


def resolve_backend(target_filename) -> md.DatabaseInterface:
    if target_filename:
        backend = database.SqliteDatabase
        opts: dict = {'filename': target_filename}
    else:
        backend: str = ConfigValue("metadata.database.type").resolve()
        opts: dict = ConfigValue("metadata.database.options").resolve()

    db: database.DatabaseInterface = database.create_backend(backend=backend, **opts)
    return db


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
