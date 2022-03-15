import sys
from os.path import dirname

import click

from pyriksprot import metadata as md
from pyriksprot.corpus.parlaclarin import ProtocolMapper


@click.group(help="CLI tool to manage riksprot metadata")
def main():
    pass


@click.command()
@click.argument('tag', type=click.STRING)
@click.argument('target_folder', type=click.STRING)
def download_metadata(tag: str, target_folder: str):
    md.download_to_folder(tag=tag, folder=target_folder, force=True)


@click.command()
@click.argument('corpus_folder', type=click.STRING)
@click.argument('target_folder', type=click.STRING)
def create_utterance_index(corpus_folder: str, target_folder: str):
    md.generate_utterance_index(ProtocolMapper, corpus_folder=corpus_folder, target_folder=target_folder)


@click.command()
@click.argument('target', type=click.STRING)
@click.option('--branch', type=click.STRING, help='Text or tags', default=None)
@click.option('--source-folder', type=click.STRING, default=None)
@click.option('--force', type=click.BOOL, is_flag=True, help='Force overwrite', default=False)
@click.option('--load-index', type=click.BOOL, is_flag=True, help='Load utterance index', default=False)
@click.option('--scripts-folder', type=click.STRING, help='If set, apply scripts in folder to DB', default=None)
def create_database(
    target: str,
    branch: str = None,
    source_folder: str = None,
    force: bool = False,
    scripts_folder: str = None,
    load_index: bool = True,
) -> None:
    try:

        md.create_database(
            database_filename=target,
            branch=branch,
            folder=source_folder,
            force=force,
        )

        if load_index:
            md.load_utterance_index(database_filename=target, source_folder=source_folder or dirname(target))

        if scripts_folder:
            md.load_scripts(database_filename=target, script_folder=scripts_folder)

    except Exception as ex:
        click.echo(ex)
        sys.exit(1)


# type: ignore

if __name__ == "__main__":
    main.add_command(create_utterance_index, "index")
    main.add_command(create_database, "database")
    main.add_command(download_metadata, "download")
    main()  # pylint: disable=no-value-for-parameter
