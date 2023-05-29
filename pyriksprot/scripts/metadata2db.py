import os
import sys
from os.path import dirname, join

import click
from loguru import logger

from pyriksprot import metadata as md
from pyriksprot.corpus.parlaclarin import ProtocolMapper


@click.group(help="CLI tool to manage riksprot metadata")
def main():
    pass


@click.command()
@click.argument('source_folder', type=click.STRING)
def verify_metadata_filenames(source_folder: str):
    try:
        md.ConfigConformsToTagSpecification(source_folder).is_satisfied()
    except ValueError as ex:
        logger.error(ex)
        sys.exit(-1)


@click.command()
@click.argument('tags', nargs=-1, type=click.STRING)
def verify_metadata_columns(tags: str):
    try:
        if len(tags) == 1:
            md.ConfigConformsToTagSpecification(tags[0]).is_satisfied()
        elif len(tags) == 2:
            md.TagsConformSpecification(**tags).is_satisfied()
        else:
            raise ValueError("please specify 1 or 2 tags")
    except ValueError as ex:
        logger.error(ex)
        sys.exit(-1)


@click.command()
@click.argument('tag', type=click.STRING)
@click.argument('target_folder', type=click.STRING)
def download_metadata(tag: str, target_folder: str):
    md.gh_dl_metadata_by_config(configs=md.MetadataTableConfigs(), tag=tag, folder=target_folder, force=True)


@click.command()
@click.argument('corpus_folder', type=click.STRING)
@click.argument('target_folder', type=click.STRING)
def create_corpus_indexes(corpus_folder: str, target_folder: str):
    factory: md.CorpusIndexFactory = md.CorpusIndexFactory(ProtocolMapper)
    factory.generate(corpus_folder=corpus_folder, target_folder=target_folder)


@click.command()
@click.argument('target', type=click.STRING)
@click.option('--tag', type=click.STRING, help='Metadata version', default=None)
@click.option('--source-folder', type=click.STRING, default=None)
@click.option('--force', type=click.BOOL, is_flag=True, help='Force overwrite', default=False)
@click.option('--load-index', type=click.BOOL, is_flag=True, help='Load utterance index', default=False)
@click.option(
    '--scripts-folder',
    type=click.STRING,
    help='Apply scripts in specified folder to DB. If not specified the scripts are loaded from SQL-module.',
    default=None,
)
@click.option('--skip-scripts', type=click.BOOL, is_flag=True, help='Skip loading SQL scripts', default=False)
def create_database(
    target: str,
    tag: str = None,
    source_folder: str = None,
    force: bool = False,
    scripts_folder: str = None,
    load_index: bool = True,
    skip_scripts: bool = False,
) -> None:
    try:
        service: md.DatabaseHelper = md.DatabaseHelper(target)
        service.create(
            tag=tag,
            folder=source_folder,
            force=force,
        )

        if load_index:
            logger.info("loading index...")
            service.load_corpus_indexes(folder=source_folder or dirname(target))

        if not skip_scripts:
            logger.info(f"loading scripts from {scripts_folder if scripts_folder else 'sql module'}...")
            service.load_scripts(folder=scripts_folder)

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
