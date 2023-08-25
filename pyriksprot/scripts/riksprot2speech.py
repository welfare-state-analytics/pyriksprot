import click

from pyriksprot import interface, to_speech
from pyriksprot.dispatch import dispatch
from pyriksprot.scripts.utils import option2, update_arguments_from_options_file
from pyriksprot.utility import strip_path_and_extension
from pyriksprot.workflows import extract_tags

# pylint: disable=too-many-arguments, unused-argument


@click.command()
@click.argument('source-folder', type=click.STRING)
@click.argument('metadata-filename', type=click.STRING)
@click.argument('target-name', type=click.STRING)
@option2('--options-filename')
@option2('--target-type')
@option2('--compress-type')
@option2('--content-type')
@option2('--merge-strategy')
@option2('--multiproc-processes')
@option2('--skip-lemma')
@option2('--skip-text')
@option2('--skip-puncts')
@option2('--skip-stopwords')
@option2('--lowercase')
@option2('--force')
def main(
    options_filename: str = None,
    source_folder: str = None,
    metadata_filename: str = None,
    target_name: str = None,
    target_type: str = None,
    content_type: str = 'tagged_frame',
    merge_strategy: str = 'chain',
    compress_type: str = 'feather',
    multiproc_processes: int = 1,
    skip_lemma: bool = False,
    skip_text: bool = False,
    skip_puncts: bool = False,
    skip_stopwords: bool = False,
    lowercase: bool = True,
    force: bool = False,
):
    arguments: dict = update_arguments_from_options_file(
        arguments=locals(), filename_key='options_filename', suffix=strip_path_and_extension(target_name)
    )
    arguments['content_type'] = interface.ContentType(arguments['content_type'])
    arguments['merge_strategy'] = to_speech.MergeStrategyType(arguments['merge_strategy'])
    arguments['compress_type'] = dispatch.CompressType(arguments['compress_type'].lower())

    extract_tags.extract_corpus_tags(
        **{
            **arguments,
            **dict(
                segment_level=interface.SegmentLevel.Speech,
                segment_skip_size=1,
                years=None,
                temporal_key=None,
                group_keys=None,
                multiproc_keep_order=False,
                multiproc_chunksize=10,
            ),
        }
    )


if __name__ == "__main__":
    main()

    # from click.testing import CliRunner
    # runner = CliRunner()
    # result = runner.invoke(
    #     main,
    #     [
    #         '--options-filename',
    #         'data/swedeb-data/dataset-01/opts/tagged-speeches/tagged_frames_speeches_lemma.feather.yml',
    #         '--force',
    #         'data/swedeb-data/dataset-01/v0.X.0/tagged_frames',
    #         'data/swedeb-data/dataset-01/v0.X.0/riksprot_metadata.db',
    #         'data/swedeb-data/dataset-01/v0.X.0/speeches/tagged_frames_speeches_lemma.feather'

    #     ],
    # )
    # print(result.output)
