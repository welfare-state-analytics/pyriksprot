from __future__ import annotations

import os
import sys

import click

from pyriksprot.configuration.inject import ConfigStore, ConfigValue
from pyriksprot.workflows import subset_corpus_and_metadata

jj = os.path.join
relpath = os.path.relpath


@click.command()
@click.argument('config_filename', type=click.STRING)  # , help="File with protocol names to subset")
@click.argument('documents', type=click.STRING)  # , help="File with protocol names to subset")
@click.argument('target-folder', type=click.STRING)  # , help="Root folder for corpus subset")
@click.option('--scripts-folder', type=click.STRING, help="SQL scripts folder")
@click.option('--source-folder', type=click.STRING, help="Copy from source folder instead of downloading")
def main(
    config_filename: str = None,
    documents: list[str] | str = None,
    target_folder: str = None,
    scripts_folder: str = None,
    source_folder: str = None,
):
    print(locals())
    try:
        ConfigStore().configure_context(source=config_filename)
        subset_corpus_and_metadata(
            source_folder=source_folder,
            documents=documents,
            target_folder=target_folder,
            tag=ConfigValue("version").resolve(),
            scripts_folder=scripts_folder,
            **ConfigValue("metadata.github").resolve(),
        )
    except Exception as ex:
        click.echo(ex)
        sys.exit(1)


if __name__ == "__main__":
    # options = {
    #     'documents': 'swedeb-sample-protocols.txt',
    #     'target_folder': 'data/swedeb-samples/',
    #     'tag': 'v0.X.0',
    # }
    # subset_corpus_and_metadata(**options)
    main()
