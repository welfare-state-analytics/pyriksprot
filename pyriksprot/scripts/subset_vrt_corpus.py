from __future__ import annotations

import os
import sys
import warnings

import click

from pyriksprot.workflows.subset_corpus import subset_vrt_corpus

jj = os.path.join
relpath = os.path.relpath

warnings.filterwarnings("ignore", category=FutureWarning)


@click.command()
@click.argument('global_vrt_folder', type=click.STRING)
@click.argument('local_xml_folder', type=click.STRING)
@click.argument('local_vrt_folder', type=click.STRING)
def main(
    global_vrt_folder: list[str] | str,
    local_xml_folder: str,
    local_vrt_folder: str,
):
    try:
        subset_vrt_corpus(global_vrt_folder, local_xml_folder, local_vrt_folder)
    except Exception as ex:
        click.echo(ex)
        sys.exit(1)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
