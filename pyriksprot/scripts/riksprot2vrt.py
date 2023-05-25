import os
import sys
from glob import glob
from os.path import basename
from os.path import join as jj

import click

from pyriksprot.workflows.export_vrt import VrtExportBatch, export_vrt


@click.command()
@click.option(
    '--folder-batches',
    '-i',
    type=(str, str),
    multiple=False,
    required=False,
    help="Create gzipped batches for all subfolders to batches.",
)
@click.option(
    '--batch', '-b', type=(str, str, str), multiple=True, required=False, help="Batch `source` `target `to export."
)
@click.option(
    '--structural-tags',
    '-t',
    type=click.Choice(['protocol', 'speech', 'utterance']),
    multiple=True,
    help="Structural elements to include in the VRT.",
)
@click.option('--batch-tag', type=str, default=None, help="Root tag to surround all batches in exported VRT file.")
@click.option('--batch-date', type=str, default=None, help="Batch tag date attribute.")
@click.option('--processes', type=int, default=1, help="Number of processes to use.")
@click.option('--force', is_flag=True, default=False, help="Force overwrite of existing files")
def main(
    folder_batches: str = None,
    batch: list[tuple[str, str, str]] = None,
    structural_tags: str = None,
    batch_tag: str = None,
    batch_date: str = None,
    processes: int = 1,
    force: bool = False,
):
    """Export protocols to VRT format."""
    try:
        if folder_batches and batch:
            click.echo("Cannot specify both --folder-batches and --batch. Aborting.")
            sys.exit(1)

        if not folder_batches and not batch:
            click.echo("Must specify either --folder-batches or --batch. Aborting.")
            sys.exit(1)

        batches: list[VrtExportBatch] = (
            get_folder_batches(*folder_batches) if folder_batches else [VrtExportBatch(*b) for b in batch]
        )

        if any(not os.path.exists(b.source) for b in batches):
            click.echo("Source folder does not exist. Aborting.")
            sys.exit(1)

        if len(batches) == 0:
            click.echo("No batches found. Aborting.")
            sys.exit(1)

        os.makedirs(os.path.dirname(batches[0].target), exist_ok=True)
        existing_targets: list[str] = [b.target for b in batches if os.path.exists(b.target)]
        if existing_targets:
            if not force:
                click.echo(f"Target(s) {existing_targets} already exists. Aborting.")
                sys.exit(1)

            for target in existing_targets:
                os.remove(target)

        export_vrt(
            batches,
            *structural_tags,
            tag=batch_tag,
            date=batch_date,
            processes=processes,
        )
    except Exception as ex:
        click.echo(ex)
        sys.exit(1)


def get_folder_batches(source_folder: str, target_folder: str) -> list[VrtExportBatch]:
    """Get all subfolders in source folder and create a batch for each."""
    return [
        VrtExportBatch(basename(folder), folder, jj(target_folder, f"{basename(folder)}.vrt.gz"))
        for folder in glob(jj(source_folder, "*"))
        if os.path.isdir(folder)
    ]


if __name__ == "__main__":
    main()
