import sys

import click

from pyriksprot.parlaclarin.tf import compute_term_frequencies


@click.command()
@click.argument('input-folder', type=click.STRING)
@click.argument('output-filename', type=click.STRING)
def main(
    input_folder: str = None,
    output_filename: str = None,
):
    try:
        compute_term_frequencies(source=input_folder, filename=output_filename)
    except Exception as ex:
        click.echo(ex)
        sys.exit(1)


if __name__ == "__main__":
    main()
