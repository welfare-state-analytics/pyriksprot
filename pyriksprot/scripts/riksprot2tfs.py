import warnings

import click

from pyriksprot.utility import ensure_path
from pyriksprot.workflows.tf import compute_term_frequencies

warnings.filterwarnings("ignore", category=FutureWarning)


@click.command()
@click.argument('input-folder', type=click.STRING)
@click.argument('output-filename', type=click.STRING)
@click.option('--segment-skip-size', type=int, default=1, required=False, help="Segment skip size.")
@click.option('--multiproc-processes', type=int, default=2, required=False, help="Number of cores.")
@click.option('--multiproc-keep-order', is_flag=True, default=False, help="Keep order (slower).")
@click.option('--progress', is_flag=True, default=True, help="Show progress bar.")
def main(
    input_folder: str = None,
    output_filename: str = None,
    segment_skip_size: int = 1,
    multiproc_processes: int = 2,
    multiproc_keep_order: bool = False,
    progress: bool = True,
):
    # try:
    ensure_path(output_filename)
    compute_term_frequencies(
        source=input_folder,
        filename=output_filename,
        segment_skip_size=segment_skip_size,
        multiproc_processes=multiproc_processes,
        multiproc_keep_order=multiproc_keep_order,
        progress=progress,
    )
    # except Exception as ex:
    #     raise ex
    #     click.echo(ex)
    #     sys.exit(1)


if __name__ == "__main__":
    main()
