import os
import sys

import click

from pyriksprot import metadata as md


@click.command()
@click.argument('corpus_folder', type=click.STRING)
@click.argument('documents_filename', type=click.STRING)
@click.argument('utterances_filename', type=click.STRING)
@click.option('--force', type=click.BOOL, is_flag=True, help='Force overwrite', default=False)
def main(corpus_folder: str, documents_filename: str, utterances_filename: str, force: bool = False) -> None:
    try:
        documents, utterances = md.generate_utterance_index(
            corpus_folder=corpus_folder,
        )

        for filename in [documents_filename, utterances_filename]:
            if os.path.isfile(filename):
                if not force:
                    raise ValueError("file exists, use --force to overwrite")
                os.unlink(filename)

        documents.to_csv(documents_filename, sep="\t")
        utterances.to_csv(utterances_filename, sep="\t")

    except Exception as ex:
        click.echo(ex)
        sys.exit(1)


if __name__ == "__main__":
    main()
