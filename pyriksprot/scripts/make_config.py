from __future__ import annotations

import os
import warnings

import click

from pyriksprot.utility import generate_default_config

jj = os.path.join
relpath = os.path.relpath

warnings.filterwarnings("ignore", category=FutureWarning)

# pylint: disable=no-value-for-parameter


@click.command()
@click.argument('target_filename', type=str)
@click.option('--corpus-version', type=str, required=True)
@click.option('--metadata-version', type=str, required=True)
@click.option('--root-folder', type=str, required=True)
@click.option('--corpus-folder', type=str, required=True)
@click.option('--stanza-datadir', type=str, default='/data/sparv/models/stanza')
def main(
    target_filename: str,
    corpus_version: str | None = None,
    metadata_version: str | None = None,
    root_folder: str = None,
    corpus_folder: str | None = None,
    stanza_datadir: str = '/data/sparv/models/stanza',
):
    """Generate a default config file for the given corpus and metadata versions."""
    try:
        generate_default_config(
            target_filename,
            root_folder=root_folder,
            corpus_version=corpus_version,
            corpus_folder=corpus_folder,
            metadata_version=metadata_version,
            stanza_datadir=stanza_datadir,
        )

    except Exception as ex:
        click.echo(ex)
        raise
        # sys.exit(1)


if __name__ == "__main__":

    main()
