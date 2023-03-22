from __future__ import annotations

import os
import sys

import click

from pyriksprot.workflows import subset_corpus_and_metadata

jj = os.path.join
relpath = os.path.relpath


@click.command()
@click.argument('documents-source', type=click.STRING, help="File with protocol names to subset")
@click.argument('target-folder', type=click.STRING, help="Root folder for corpus subset")
@click.argument('tag', type=click.STRING, help="Corpus version")
@click.option('--scripts-folder', type=click.STRING, help="SQL scripts folder")
def main(
    documents_source: list[str] | str = None,
    target_folder: str = None,
    tag: str = None,
    scripts_folder: str = None,
):
    try:
        subset_corpus_and_metadata(
            documents=documents_source, target_folder=target_folder, tag=tag, scripts_folder=scripts_folder
        )
    except Exception as ex:
        click.echo(ex)
        sys.exit(1)


if __name__ == "__main__":
    main()
