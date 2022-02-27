import sys

import click

from pyriksprot import metadata as md


@click.command()
@click.argument('corpus_folder', type=click.STRING)
@click.argument('target_folder', type=click.STRING)
def main(corpus_folder: str, target_folder: str) -> None:
    try:

        md.generate_utterance_index(corpus_folder=corpus_folder, target_folder=target_folder)

    except Exception as ex:
        click.echo(ex)
        sys.exit(1)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
