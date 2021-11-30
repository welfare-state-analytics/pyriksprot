import sys
from inspect import currentframe, getargvalues
from typing import Sequence

import click

from pyriksprot.dispatch import TargetType
from pyriksprot.interface import ContentType, GroupingKey, SegmentLevel, TemporalKey
from pyriksprot.tagged_corpus import extract

sys.path.append(".")

# pylint: disable=too-many-arguments


def get_kwargs():
    keys, _, _, values = getargvalues(currentframe().f_back)
    return {k: v for k, v in zip(keys, values) if k != 'self'}


"""
Extract an aggregated subset of a tagged ParlaCLARIN corpus.
"""
SEGMENT_LEVELS = ['protocol', 'speech', 'utterance', 'paragraph', 'who']
TARGET_TYPES = [e.value for e in TargetType]
CONTENT_TYPES = [e.value for e in ContentType]


@click.command()
@click.argument('source-folder', type=click.STRING)
@click.argument('target-name', type=click.STRING)
@click.option('--target-type', default='zip', type=click.Choice(TARGET_TYPES), help='Target type')
@click.option(
    '--content-type', default='tagged_frame', type=click.Choice(CONTENT_TYPES), help='Content type to extract'
)
@click.option('--segment-level', default='who', type=click.Choice(SEGMENT_LEVELS), help='Protocol iterate level')
@click.option('--segment-skip-size', default=1, type=click.IntRange(1, 1024), help='Skip smaller than threshold')
@click.option('--temporal-key', default=None, help='Temporal partition key(s)', type=click.STRING)
@click.option('--group-key', help='Partition key(s)', multiple=True, type=click.STRING)
@click.option('--years', default=None, help='Years to include in output', type=click.STRING)
@click.option('--multiproc-processes', default=None, type=click.IntRange(1, 40), help='Number of processes to use')
@click.option('--multiproc-keep-order', default=False, is_flag=True, help='Process is sort order (slower, multiproc)')
def main(
    source_folder: str = None,
    target_name: str = None,
    content_type: ContentType = None,
    target_type: TargetType = None,
    segment_level: SegmentLevel = None,
    segment_skip_size: int = 1,
    temporal_key: TemporalKey = None,
    group_key: Sequence[GroupingKey] = None,
    years: str = None,
    multiproc_processes: int = 1,
    multiproc_keep_order: str = None,
):
    try:

        extract.extract_corpus_tags(
            source_folder=source_folder,
            content_type=ContentType(content_type),
            target_name=target_name,
            target_type=target_type,
            segment_level=segment_level,
            segment_skip_size=segment_skip_size,
            temporal_key=temporal_key,
            group_keys=group_key,
            years=years,
            multiproc_keep_order=multiproc_keep_order,
            multiproc_processes=multiproc_processes,
            multiproc_chunksize=100,
        )

    except Exception as ex:
        click.echo(ex)
        sys.exit(1)


if __name__ == "__main__":
    main()