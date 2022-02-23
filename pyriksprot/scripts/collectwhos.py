import os
import sys

import click
import pandas as pd

from pyriksprot import metadata as md


@click.command()
@click.argument('corpus_folder', type=click.STRING)
@click.argument('target', type=click.STRING)
@click.option('--force', type=click.BOOL, is_flag=True, help='Force overwrite', default=False)
def main(corpus_folder: str, target: str, force: bool = False) -> None:
    try:
        whos: pd.DataFrame = md.collect_utterance_whos(
            corpus_folder=corpus_folder,
        )

        if os.path.isfile(target) and not force:
            raise ValueError("file exists, use --force to overwrite")

        whos.set_index('hash').to_csv(target)
    except Exception as ex:
        click.echo(ex)
        sys.exit(1)


if __name__ == "__main__":
    main()
