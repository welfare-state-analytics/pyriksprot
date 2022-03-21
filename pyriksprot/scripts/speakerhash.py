import os
import sys

import click

from pyriksprot import metadata as md
from pyriksprot.corpus.tagged.persist import update_speaker_hash

jj = os.path.join
relpath = os.path.relpath

# FIXME #20 Tagging does not assign correct speaker hashes to utterances
def temporary_update():

    source_folder: str = "/data/riksdagen_corpus_data/tagged_frames_v0.4.1"
    target_folder: str = "/data/riksdagen_corpus_data/tagged_frames_v0.4.1.beta"
    database_filename: str = "/data/riksdagen_corpus_data/metadata/riksprot_metadata.v0.4.1.db"

    service = md.SpeakerInfoService(database_filename)
    speaker_hash_lookup = service.utterance_index.utterances['speaker_hash'].to_dict()
    update_speaker_hash(speaker_hash_lookup, source_folder, target_folder)


@click.command()
@click.argument('source-folder', type=click.STRING)
@click.argument('target-folder', type=click.STRING)
@click.argument('database-filename', type=click.STRING)
def main(source_folder: str = None, target_folder: str = None, database_filename: str = None):
    try:
        service: md.SpeakerInfoService = md.SpeakerInfoService(database_filename)
        speaker_hash_lookup: dict[str, str] = service.utterance_index.utterances['speaker_hash'].to_dict()
        update_speaker_hash(speaker_hash_lookup, source_folder, target_folder)
    except Exception as ex:
        click.echo(ex)
        sys.exit(1)


if __name__ == "__main__":
    # main()
    temporary_update()
