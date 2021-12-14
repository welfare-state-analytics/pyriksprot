import click

from pyriksprot import dispatch, interface
from pyriksprot.tagged_corpus import extract

CONTENT_TYPES = [e.value for e in interface.ContentType]
TARGET_TYPES = dispatch.IDispatcher.dispatcher_keys()
COMPRESS_TYPES = dispatch.CompressType.values()

# pylint: disable=too-many-arguments


@click.command()
@click.argument('source-folder', type=click.STRING)
@click.argument('target-name', type=click.STRING)
@click.option('--target-type', default='checkpoint', type=click.Choice(TARGET_TYPES), help='Target type')
@click.option('--compress-type', default='lzma', type=click.Choice(COMPRESS_TYPES), help='Compress type')
@click.option('--content-type', default='tagged_frame', type=click.Choice(CONTENT_TYPES), help='Text or tags')
@click.option('--processes', default=1, type=click.INT, help='Number of processes')
@click.option('--skip-lemma', default=False, type=click.BOOL, is_flag=True, help='Skip lemma')
@click.option('--skip-text', default=False, type=click.BOOL, is_flag=True, help='Skip text')
@click.option('--skip-puncts', default=False, type=click.BOOL, is_flag=True, help='Skip puncts')
@click.option('--skip-stopwords', default=False, type=click.BOOL, is_flag=True, help='Skip stopwords')
@click.option('--lowercase', default=True, type=click.BOOL, is_flag=True, help='Lowercase tokem/text')
def main(
    source_folder: str = None,
    target_name: str = None,
    target_type: str = None,
    content_type: str = 'tagged_frame',
    compress_type: str = 'zip',
    processes: int = 1,
    skip_lemma: bool = False,
    skip_text: bool = False,
    skip_puncts: bool = False,
    skip_stopwords: bool = False,
    lowercase: bool = True,
):
    extract.extract_corpus_tags(
        source_folder=source_folder,
        target_name=target_name,
        content_type=interface.ContentType(content_type.lower()),
        target_type=target_type,
        segment_level=interface.SegmentLevel.Speech,
        segment_skip_size=1,
        years=None,
        temporal_key=None,
        group_keys=None,
        multiproc_keep_order=False,
        multiproc_processes=processes,
        multiproc_chunksize=10,
        speech_merge_strategy=interface.MergeSpeechStrategyType.WhoSequence,
        compress_type=dispatch.CompressType(compress_type.lower()),
        skip_lemma=skip_lemma,
        skip_text=skip_text,
        skip_puncts=skip_puncts,
        skip_stopwords=skip_stopwords,
        lowercase=lowercase,
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
