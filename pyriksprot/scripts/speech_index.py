from __future__ import annotations

import os
import sys

import click

from pyriksprot import interface, to_speech
from pyriksprot.scripts.utils import option2, update_arguments_from_options_file
from pyriksprot.workflows.speech_index import extract_speech_index

jj = os.path.join
relpath = os.path.relpath

# pylint: disable=too-many-arguments, W0613


@click.command()
@click.argument('source-folder', type=click.STRING)
@click.argument('target-name', type=click.STRING)
@click.argument('database-filename', type=click.STRING)
@option2('--options-filename')
@option2('--merge-strategy')
@option2('--multiproc-processes')
def main(
    options_filename: str = None,
    source_folder: str = None,
    target_name: str = None,
    database_filename: str = None,
    merge_strategy: str = 'chain',
    multiproc_processes: int = 1,
):
    """Extracts a speech index from given speech corpus"""
    arguments: dict = update_arguments_from_options_file(arguments=locals(), filename_key='options_filename')
    arguments['merge_strategy'] = to_speech.MergeStrategyType(arguments['merge_strategy'])

    try:
        extract_speech_index(
            **arguments,
            **dict(
                segment_skip_size=1,
                years=None,
                content_type="tagged_frame",
                segment_level=interface.SegmentLevel.Speech,
                multiproc_keep_order=True,
                multiproc_chunksize=10,
            ),
        )
    except Exception as ex:
        click.echo(ex)
        sys.exit(1)


if __name__ == "__main__":
    main()

    # tag: str = "v0.4.6"
    # data_folder: str = "/data/riksdagen_corpus_data"

    # arguments = {
    #     'source_folder': f'{data_folder}/tagged_frames_{tag}',
    #     'target_name': f'speech_index.{tag}.csv.gz',
    #     'database_filename': f'{data_folder}/metadata/riksprot_metadata.{tag}.db',
    #     'merge_strategy': 'chain',
    #     'multiproc_processes': None,
    #     'segment_skip_size': 0,
    #     'years': None,
    #     'content_type': "tagged_frame",
    #     'segment_level': interface.SegmentLevel.Speech,
    #     'multiproc_keep_order': True,
    #     'multiproc_chunksize': 10,
    # }
    # extract_speech_index(**arguments)

    #  # poetry run python pyriksprot/scripts/speech_index.py /data/riksdagen_corpus_data/tagged_frames_v0.4.6 speech_index.v0.4.6.csv.gz /data/riksdagen_corpus_data/metadata/data/v0.4.6/riksprot_metadata.v0.4.6.db