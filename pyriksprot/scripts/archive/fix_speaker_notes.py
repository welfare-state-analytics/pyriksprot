import os
import sys

import click

from pyriksprot import metadata as md
from pyriksprot.corpus.tagged.persist import update_speaker_note_id

jj = os.path.join
relpath = os.path.relpath


# FIXME #20 Tagging does not assign correct speaker hashes to utterances
def temporary_update(tag: str, target_tag: str):
    source_folder: str = f"/data/riksdagen_corpus_data/tagged_frames_{tag}"
    target_folder: str = f"/data/riksdagen_corpus_data/tagged_frames_{target_tag}"
    database_filename: str = f"/data/riksdagen_corpus_data/metadata/riksprot_metadata.{tag}.db"

    service = md.SpeakerInfoService(database_filename)
    speaker_note_id_lookup = service.utterance_index.utterances['speaker_note_id'].to_dict()
    update_speaker_note_id(speaker_note_id_lookup, source_folder, target_folder)


@click.command()
@click.argument('source-folder', type=click.STRING)
@click.argument('target-folder', type=click.STRING)
@click.argument('database-filename', type=click.STRING)
def main(source_folder: str = None, target_folder: str = None, database_filename: str = None):
    """Updates missing or wrong speaker note identities in an exist speech corpus (stored as zipped JSON files)"""
    try:
        service: md.SpeakerInfoService = md.SpeakerInfoService(database_filename)
        speaker_note_id_lookup: dict[str, str] = service.utterance_index.utterances['speaker_note_id'].to_dict()
        update_speaker_note_id(speaker_note_id_lookup, source_folder, target_folder)
    except Exception as ex:
        click.echo(ex)
        sys.exit(1)


if __name__ == "__main__":
    # main()
    temporary_update(tag="v0.4.1", target_tag="v0.4.1.beta")
