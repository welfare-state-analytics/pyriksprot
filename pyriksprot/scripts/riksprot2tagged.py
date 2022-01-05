import sys
from inspect import currentframe, getargvalues
from typing import Sequence

import click

from pyriksprot import dispatch, interface
from pyriksprot.scripts.utils import option2, update_arguments_from_options_file
from pyriksprot.tagged_corpus import extract

sys.path.insert(0, '.')


# pylint: disable=too-many-arguments, unused-argument


def get_kwargs():
    keys, _, _, values = getargvalues(currentframe().f_back)
    return {k: v for k, v in zip(keys, values) if k != 'self'}


"""
Extract an aggregated subset of a tagged ParlaCLARIN corpus.
"""


@click.command()
@click.argument('source-folder', type=click.STRING, required=False)
@click.argument('target-name', type=click.STRING, required=False)
@option2('--options-filename')
@option2('--target-type')
@option2('--compress-type')
@option2('--content-type')
@option2('--segment-level')
@option2('--segment-skip-size')
@option2('--temporal-key')
@option2('--group-key')
@option2('--years')
@option2('--multiproc-processes')
@option2('--multiproc-keep-order')
@option2('--force')
def main(
    options_filename: str = None,
    source_folder: str = None,
    target_name: str = None,
    target_type: str = None,
    compress_type: str = "feather",
    content_type: str = 'tagged_frame',
    segment_level: interface.SegmentLevel = None,
    segment_skip_size: int = 1,
    temporal_key: interface.TemporalKey = None,
    group_key: Sequence[interface.GroupingKey] = None,
    years: str = None,
    multiproc_processes: int = 1,
    multiproc_keep_order: str = None,
    force: bool = False,
):
    try:
        arguments: dict = update_arguments_from_options_file(arguments=locals(), filename_key='options_filename')
        arguments['content_type'] = interface.ContentType(arguments['content_type'])
        arguments['compress_type'] = dispatch.CompressType(arguments['compress_type'].lower())

        arguments['group_keys'] = arguments['group_key']
        del arguments['group_key']

        extract.extract_corpus_tags(**arguments)

    except Exception as ex:
        click.echo(ex)
        sys.exit(1)


if __name__ == "__main__":

    main()

    # from click.testing import CliRunner

    # runner = CliRunner()
    # result = runner.invoke(
    #     main,
    #     [
    #         './data/tagged_protocols_1965',
    #         'test_65',
    #         '--compress-type',
    #         'feather',
    #         '--content-type',
    #         'tagged_frame',
    #         '--group-key',
    #         'who',
    #         '--segment-level',
    #         'who',
    #         '--target-type',
    #         'single-id-tagged-frame-per-group',
    #         '--temporal-key',
    #         'year',
    #         '--force',
    #     ],
    # )
    # print(result.output)
