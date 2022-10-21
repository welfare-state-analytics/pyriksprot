import sys
from inspect import currentframe, getargvalues
from typing import Sequence

import click

from pyriksprot.dispatch import dispatch
from pyriksprot.interface import GroupingKey, SegmentLevel, TemporalKey
from pyriksprot.workflows import extract_text

from pyriksprot.scripts.utils import option2, update_arguments_from_options_file

# pylint: disable=too-many-arguments, unused-argument


def get_kwargs():
    keys, _, _, values = getargvalues(currentframe().f_back)
    return {k: v for k, v in zip(keys, values) if k != 'self'}


"""
Extract an aggregated subset of ParlaCLARIN XML corpus.
"""


@click.command()
@click.argument('source-folder', type=click.STRING)
@click.argument('metadata-filename', type=click.STRING)
@click.argument('target-name', type=click.STRING)
@option2('--options-filename')
@option2('--target-type')
@option2('--compress-type')
@option2('--segment-level')
@option2('--segment-skip-size')
@option2('--temporal-key')
@option2('--group-key')
@option2('--years')
@option2('--multiproc-processes')
@option2('--multiproc-keep-order')
@option2('--dedent')
@option2('--dehyphen')
@option2('--force')
def main(
    options_filename: str = None,
    source_folder: str = None,
    metadata_filename: str = None,
    target_name: str = None,
    target_type: str = None,
    compress_type: str = "zip",
    segment_level: SegmentLevel = None,
    segment_skip_size: int = 1,
    temporal_key: TemporalKey = None,
    group_key: Sequence[GroupingKey] = None,
    years: str = None,
    multiproc_processes: int = 1,
    multiproc_keep_order: str = None,
    dedent: bool = False,
    dehyphen: bool = False,
    force: bool = False,
):
    try:
        arguments: dict = update_arguments_from_options_file(arguments=locals(), filename_key='options_filename')
        arguments['compress_type'] = dispatch.CompressType(arguments['compress_type'].lower())
        arguments['group_keys'] = arguments['group_key']
        del arguments['group_key']
        extract_text.extract_corpus_text(**arguments)

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
    #         "--multiproc-processes",
    #         "1",
    #         "--compress-type",
    #         "zip",
    #         "--segment-level",
    #         "speech",
    #         "--target-type",
    #         "files-in-zip",
    #         "--temporal-key",
    #         "decade",
    #         "--group-key",
    #         "gender_id",
    #         "--force",
    #         "/data/riksdagen_corpus_data/riksdagen-corpus/corpus/protocols/",
    #         "/data/riksdagen_corpus_data/metadata/riksprot_metadata.v0.4.6.db",
    #         "apa.zip",
    #     ],
    # )

    # print(result.output)

    # PYTHONPATH=. python pyriksprot/scripts/riksprot2text.py --multiproc-processes 1 --compress-type zip --segment-level speech --target-type files-in-zip --temporal-key decade --group-key gender_id --force /data/riksdagen_corpus_data/riksdagen-corpus/corpus/protocols/ /data/riksdagen_corpus_data/metadata/riksprot_metadata.v0.4.6.db apa.zip
