import sys
from glob import glob
from os import makedirs, remove
from os.path import basename, dirname, exists, isdir, join

import click

from pyriksprot.metadata import SpeakerInfoService
from pyriksprot.workflows.export_vrt import VrtBatchExporter, VrtExportBatch


@click.command()
@click.option('--source-folder', '-i', type=str, multiple=False, required=True, help="Source folder.")
@click.option('--target-folder', '-o', type=str, multiple=False, required=True, help="Target folder.")
@click.option(
    '--structural-tag',
    '-t',
    type=click.Choice(['protocol', 'speech', 'utterance']),
    multiple=True,
    help="Structural elements to include in the VRT.",
)
@click.option('--metadata-filename', '-m', type=str, required=True, help="Metadata database filename.")
@click.option('--merge-strategy', type=str, default="chain_consecutive_unknowns", help="Speech merge strategy.")
@click.option('--processes', type=int, default=1, help="Number of processes to use.")
@click.option('--force', is_flag=True, default=False, help="Force overwrite of existing files")
@click.option('--batch-tag', '-t', type=str, multiple=False, required=False, default='year', help="Batch tag.")
def export_yearly_folders(
    source_folder: str,
    target_folder: str,
    batch_tag: str = 'year',
    structural_tag: str = None,
    metadata_filename: str = None,
    merge_strategy: str = "chain_consecutive_unknowns",
    processes: int = 1,
    force: bool = False,
):
    """Export protocols to VRT format."""
    # try:
    attribs = {'year': lambda x: int(basename(x)[:4]), 'title': basename}
    batches: list[VrtExportBatch] = create_yearly_folder_batches(
        source_folder, target_folder, batch_tag=batch_tag, **attribs
    )

    if any(not exists(b.source) for b in batches):
        click.echo("Source folder does not exist. Aborting.")
        sys.exit(1)

    if len(batches) == 0:
        click.echo("No batches found. Aborting.")
        sys.exit(1)

    makedirs(dirname(batches[0].target), exist_ok=True)
    existing_targets: list[str] = [b.target for b in batches if exists(b.target)]
    if existing_targets:
        if not force:
            click.echo(f"Target(s) {existing_targets} already exists. Aborting.")
            sys.exit(1)

        for target in existing_targets:
            remove(target)
    speaker_service: SpeakerInfoService = SpeakerInfoService(metadata_filename)
    exporter: VrtBatchExporter = VrtBatchExporter(speaker_service, merge_strategy=merge_strategy, processes=processes)
    exporter.export(batches, *structural_tag)
    # except Exception as ex:
    #    click.echo(ex)
    #    sys.exit(1)


def create_yearly_folder_batches(
    source_folder: str, target_folder: str, batch_tag: str = None, **attribs
) -> list[VrtExportBatch]:
    """Get all subfolders in source folder and create a batch for each."""
    return [
        VrtExportBatch(
            folder,
            join(target_folder, f"{basename(folder)}.vrt.gz"),
            batch_tag,
            {attr: fx(folder) for attr, fx in attribs.items()},
        )
        for folder in glob(join(source_folder, "*"))
        if isdir(folder) and basename(folder).isnumeric()
    ]


if __name__ == "__main__":
    export_yearly_folders()  # pylint: disable=no-value-for-parameter
