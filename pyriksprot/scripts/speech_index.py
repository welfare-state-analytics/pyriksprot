from __future__ import annotations

import os
import sys
import typing as t

import click
import pandas as pd
from loguru import logger
from tqdm import tqdm

from pyriksprot import interface
from pyriksprot import metadata as md
from pyriksprot import to_speech
from pyriksprot.corpus import corpus_index, iterate, tagged
from pyriksprot.scripts.utils import option2, update_arguments_from_options_file

jj = os.path.join
relpath = os.path.relpath

# pylint: disable=too-many-arguments, W0613


def extract_speech_index(
    *,
    source_folder: str,
    database_filename: str,
    target_name: str,
    content_type: interface.ContentType = interface.ContentType.TaggedFrame,
    segment_level: interface.SegmentLevel = None,
    segment_skip_size: int = 1,
    years: str = None,
    multiproc_keep_order: str = None,
    multiproc_processes: int = 1,
    multiproc_chunksize: int = 100,
    merge_strategy: to_speech.MergeStrategyType = 'chain',
) -> None:

    logger.info("extracting corpus speech index...")

    source_index: corpus_index.CorpusSourceIndex = corpus_index.CorpusSourceIndex.load(
        source_folder=source_folder, source_pattern='**/prot-*.zip', years=years
    )
    logger.info("loading parliamentary metadata...")

    # FIXME: How to ensure metadata tag is the same as corpus??? Add tag to DB?
    speaker_service: md.SpeakerInfoService = md.SpeakerInfoService(database_filename=database_filename)

    def get_speaker(item: iterate.ProtocolSegment) -> None:
        item.speaker_info = speaker_service.get_speaker_info(u_id=item.u_id, person_id=item.who, year=item.year)

    preprocess: t.Callable[[iterate.ProtocolSegment], None] = (
        get_speaker if segment_level not in ('protocol', None) else None
    )

    speeches: iterate.ProtocolSegmentIterator = tagged.ProtocolIterator(
        filenames=source_index.paths,
        content_type=content_type,
        segment_level=segment_level,
        segment_skip_size=segment_skip_size,
        multiproc_keep_order=multiproc_keep_order,
        multiproc_processes=multiproc_processes,
        multiproc_chunksize=multiproc_chunksize,
        merge_strategy=merge_strategy,
        preprocess=preprocess,
    )

    data: list[dict] = []
    for speech in tqdm(speeches):
        data.append(speech.to_dict())

    pd.DataFrame(data=data).to_csv('speech_index.csv.gz', sep='\t')


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

    # arguments = {
    #     'source_folder': '/data/riksdagen_corpus_data/tagged_frames_vx.x.x',
    #     'target_name': 'speech_index.csv.gz',
    #     'database_filename': '/data/riksdagen_corpus_data/metadata/riksprot_metadata.main.db',
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
