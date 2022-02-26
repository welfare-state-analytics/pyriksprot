import sys

import click

from pyriksprot import metadata as md
from os.path import dirname


@click.command()
@click.argument('target', type=click.STRING)
@click.option('--branch', type=click.STRING, help='Text or tags', default=None)
@click.option('--source-folder', type=click.STRING, default=None)
@click.option('--force', type=click.BOOL, is_flag=True, help='Force overwrite', default=False)
@click.option('--load-index', type=click.BOOL, is_flag=True, help='Load utterance index', default=False)
@click.option('--corpus-folder', type=click.STRING, help='If set, then generate index from this folder', default=None)
@click.option('--scripts-folder', type=click.STRING, help='If set, apply scripts in folder to DB', default=None)
def main(
    target: str,
    branch: str = None,
    source_folder: str = None,
    force: bool = False,
    corpus_folder: bool = True,
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

        if corpus_folder:
            md.generate_utterance_index(corpus_folder=corpus_folder, target_folder=dirname(target))

        if load_index:
            md.load_utterance_index(database_filename=target, source_folder=dirname(target))

        if scripts_folder:
            md.load_scripts(database_filename=target, script_folder=scripts_folder)

    except Exception as ex:
        click.echo(ex)
        sys.exit(1)

# type: ignore

if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
