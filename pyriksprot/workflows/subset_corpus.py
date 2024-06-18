import os
import shutil
from glob import glob
from os.path import basename, dirname, exists, join, splitext

from loguru import logger

from pyriksprot import corpus as pc
from pyriksprot import metadata as md
from pyriksprot.corpus.parlaclarin import ProtocolMapper
from pyriksprot.utility import reset_folder


def subset_vrt_corpus(global_vrt_folder: str, local_xml_folder: str, local_vrt_folder: str) -> None:
    """Given a local parlaclarin folder, copy the global VRT (tagged frame) for all protocols that exists in the local parlaclarin folder"""

    for filename in glob(join(local_xml_folder, '**/*.xml'), recursive=True):
        subfolder: str = basename(dirname(filename))

        global_vrt_filename: str = join(global_vrt_folder, subfolder, f'{splitext(basename(filename))[0]}.zip')

        if not exists(global_vrt_filename):
            raise Exception(f'subset_vrt_corpus: file {global_vrt_filename} does not exist (cannot proceed)')

        target_vrt_filename: str = join(local_vrt_folder, subfolder, f'{splitext(basename(filename))[0]}.zip')

        os.makedirs(dirname(target_vrt_filename), exist_ok=True)

        shutil.copyfile(global_vrt_filename, target_vrt_filename)

        logger.info(f"copied {filename}")


def subset_corpus_and_metadata(
    documents: list[str] | str = None,
    source_folder: str = None,
    target_folder: str = None,
    tag: str = None,
    scripts_folder: str = None,
    force: bool = True,
):
    """Subset metadata to folder `target_folder`/tag"""

    root_folder: str = join(target_folder, tag)

    metadata_folder: str = join(root_folder, "tmp")
    parlaclarin_folder: str = join(root_folder, "parlaclarin")
    metadata_target_folder: str = join(parlaclarin_folder, "metadata")
    protocols_target_folder: str = join(parlaclarin_folder, "protocols")
    database_name: str = join(root_folder, "riksprot_metadata.db")

    reset_folder(root_folder, force=force)

    if isinstance(documents, str):
        documents: list[str] = _load_document_filenames(documents)

    md.gh_dl_metadata_extra(folder=metadata_folder, tag=tag, force=True)

    if source_folder is None:
        pc.download_protocols(
            filenames=documents, target_folder=protocols_target_folder, create_subfolder=True, tag=tag
        )
    else:
        pc.copy_protocols(
            source_folder=source_folder,
            filenames=documents,
            target_folder=protocols_target_folder,
        )

    md.subset_to_folder(
        ProtocolMapper,
        protocols_source_folder=parlaclarin_folder,
        source_folder=join(metadata_folder, tag),
        target_folder=metadata_target_folder,
    )
    """Add generated corpus indexes (speeches, utterances)"""
    factory: md.CorpusIndexFactory = md.CorpusIndexFactory(ProtocolMapper)
    factory.generate(corpus_folder=parlaclarin_folder, target_folder=metadata_target_folder)

    """Create metadata database with base tables"""
    md.DatabaseHelper(database_name).create(tag=tag, folder=metadata_target_folder, force=True).load_corpus_indexes(
        folder=metadata_target_folder
    ).load_scripts(folder=scripts_folder)

    shutil.rmtree(path=metadata_folder, ignore_errors=True)


def _load_document_filenames(filename):
    """Loads a list of ParlaCLARIN document names from a file"""
    if not os.path.isfile(filename):
        raise FileNotFoundError(filename)

    with open(filename, "r", encoding="utf8") as fp:
        return [x for x in fp.read().splitlines() if x.endswith(".xml")]
