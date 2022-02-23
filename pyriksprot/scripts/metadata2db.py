import sys

import click

from pyriksprot import metadata as md


@click.command()
@click.argument('target', type=click.STRING)
@click.option('--branch', type=click.STRING, help='Text or tags', default=None)
@click.option('--source-folder', type=click.STRING, default=None)
@click.option('--force', type=click.BOOL, is_flag=True, help='Force overwrite', default=False)
def main(target: str, branch: str = None, source_folder: str = None, force: bool = False) -> None:
    try:
        md.create_metadata_db(
            database_name=target,
            branch=branch,
            folder=source_folder,
            force=force,
        )
    except Exception as ex:
        click.echo(ex)
        sys.exit(1)


if __name__ == "__main__":
    main()
