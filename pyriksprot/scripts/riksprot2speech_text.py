import sys
from inspect import currentframe, getargvalues
from typing import Sequence

import click

from pyriksprot.dispatch import dispatch
from pyriksprot.interface import GroupingKey, TemporalKey
from pyriksprot.scripts.utils import option2, update_arguments_from_options_file
from pyriksprot.utility import strip_path_and_extension
from pyriksprot.workflows import extract_speech_text

# pylint: disable=too-many-arguments, unused-argument


def get_kwargs():
    keys, _, _, values = getargvalues(currentframe().f_back)
    return {k: v for k, v in zip(keys, values) if k != 'self'}


"""
Extract speech texts from a ParlaCLARIN corpus.
"""


@click.command()
@click.argument('source-folder', type=click.STRING)
@click.argument('metadata-filename', type=click.STRING)
@click.argument('target-name', type=click.STRING)
@option2('--options-filename')
@option2('--target-type')
@option2('--compress-type')
@option2('--subfolder-key')
@option2('--naming-key')
@option2('--merge-strategy')
@option2('--years')
@option2('--skip-size')
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
    subfolder_key: TemporalKey = None,
    naming_key: Sequence[GroupingKey] = None,
    merge_strategy: str = "chain",
    years: str = None,
    skip_size: int = 1,
    multiproc_processes: int = 1,
    multiproc_keep_order: str = None,
    dedent: bool = False,
    dehyphen: bool = False,
    force: bool = False,
):
    """Extracts `speeches` from a Parla-CLARIN XML corpus.  Speeches are (optionally) stored in subfolders."""
    try:
        arguments: dict = update_arguments_from_options_file(
            arguments=locals(), filename_key='options_filename', suffix=strip_path_and_extension(target_name)
        )
        arguments['compress_type'] = dispatch.CompressType(arguments['compress_type'].lower())
        arguments['naming_keys'] = arguments['naming_key']
        del arguments['naming_key']
        extract_speech_text.extract_speech_texts(**arguments)

    except Exception as ex:
        click.echo(ex)
        sys.exit(1)


if __name__ == "__main__":
    main()

    # from click.testing import CliRunner

    # # source_folder: str = "/data/riksdagen_corpus_data/riksdagen-corpus/corpus/protocols/"
    # # metadata_filename: str = "/data/riksdagen_corpus_data/metadata/riksprot_metadata.v0.4.6.db"

    # source_folder: str = "tests/test_data/source/v0.4.6/parlaclarin/protocols/"
    # metadata_filename: str = "tests/test_data/source/v0.4.6/riksprot_metadata.db"

    # source_folder: str = "tests/test_data/tmp/"
    # metadata_filename: str = "/data/riksdagen_corpus_data/metadata/riksprot_metadata.v0.4.6.db"

    # runner = CliRunner()
    # result = runner.invoke(
    #     main,
    #     [
    #         "--multiproc-processes",
    #         "1",
    #         "--compress-type",
    #         "zip",
    #         "--multiproc-keep-order",
    #         "--temporal-key",
    #         "decade",
    #         "--group-key",
    #         "gender_id",
    #         "--group-key",
    #         "party_id",
    #         "--force",
    #         source_folder,
    #         metadata_filename,
    #         "apa.zip",
    #     ],
    # )

    # print(result.output)
