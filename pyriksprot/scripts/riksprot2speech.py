import click

from pyriksprot import dispatch, interface
from pyriksprot.tagged_corpus import extract

from .utils import option2, update_arguments_from_options_file

# pylint: disable=too-many-arguments, unused-argument


@click.command()
@click.argument('source-folder', type=click.STRING)
@click.argument('target-name', type=click.STRING)
@option2('--options-filename')
@option2('--target-type')
@option2('--compress-type')
@option2('--content-type')
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
    target_name: str = None,
    target_type: str = None,
    content_type: str = 'tagged_frame',
    compress_type: str = 'feather',
    multiproc_processes: int = 1,
    skip_lemma: bool = False,
    skip_text: bool = False,
    skip_puncts: bool = False,
    skip_stopwords: bool = False,
    lowercase: bool = True,
    force: bool = False,
):
    arguments: dict = update_arguments_from_options_file(arguments=locals(), filename_key='options_filename')
    arguments['content_type'] = interface.ContentType(arguments['content_type'])
    arguments['compress_type'] = dispatch.CompressType(arguments['compress_type'].lower())

    extract.extract_corpus_tags(
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
                speech_merge_strategy=interface.MergeSpeechStrategyType.WhoSequence,
            ),
        }
    )


if __name__ == "__main__":

    if True:  # pylint: disable=using-constant-test

        main()

    else:
        from click.testing import CliRunner

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                '/data/riksdagen_corpus_data/tagged_frames',
                '/data/riksdagen_corpus_data/tagged-speech-corpus.feather',
                '--target-type',
                'feather',
                # '--compression-type',
                # 'LZMA',
                '--processes',
                1,
            ],
        )
        print(result.output)
