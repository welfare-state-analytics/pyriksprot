import os
import shutil
from os.path import basename, dirname, exists, join, splitext

from loguru import logger

from pyriksprot import corpus as pc
from pyriksprot import metadata as md
from pyriksprot.corpus.parlaclarin import ProtocolMapper
from pyriksprot.corpus.utility import ls_corpus_folder
from pyriksprot.metadata import database
from pyriksprot.utility import replace_extension, reset_folder

# pylint: disable=too-many-arguments


def subset_vrt_corpus(global_vrt_folder: str, local_xml_folder: str, local_vrt_folder: str) -> None:
    """Given a local parlaclarin folder, copy the global VRT (tagged frame) for all protocols that exists in the local parlaclarin folder"""

    for filename in ls_corpus_folder(local_xml_folder):
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
    source_folder: str | None = None,
    target_folder: str | None = None,
    tag: str | None = None,
    scripts_folder: str | None = None,
    force: bool = True,
    gh_metadata_opts: dict[str, str] = None,
    gh_records_opts: dict[str, str] = None,
    db_opts: dict[str, str | dict[str, str]] = None,
):
    """Subset metadata to folder `target_folder`/tag"""

    root_folder: str = join(target_folder or "", tag or "")

    metadata_folder: str = join(root_folder, "tmp")
    parlaclarin_folder: str = join(root_folder, "parlaclarin")
    metadata_target_folder: str = join(parlaclarin_folder, "metadata")
    protocols_target_folder: str = join(parlaclarin_folder, "protocols")

    if isinstance(documents, str):
        documents = load_document_patterns(documents, extension='xml')

    # FIXME Remove this if statement
    update_metadata: bool = False

    if update_metadata or not exists(metadata_folder):
        reset_folder(root_folder, force=force)

        md.gh_fetch_metadata_folder(
            target_folder=metadata_folder,
            **gh_metadata_opts,
            tag=tag,
            force=True,
        )

    if source_folder is None:
        pc.download_protocols(
            filenames=documents,
            target_folder=protocols_target_folder,
            create_subfolder=True,
            tag=tag,
            **gh_records_opts,
        )
    else:
        pc.copy_protocols(
            source_folder=source_folder,
            filenames=documents,
            target_folder=protocols_target_folder,
        )

    md.subset_to_folder(
        ProtocolMapper,
        tag=tag,
        protocols_source_folder=parlaclarin_folder,
        source_folder=metadata_folder,
        target_folder=metadata_target_folder,
    )

    db: database.DatabaseInterface = md.create_backend(backend=db_opts.get('type'), **db_opts.get('options', {}))

    """Create metadata database with base tables"""
    service: md.MetadataFactory = md.MetadataFactory(tag=tag, backend=db)
    service.create(folder=metadata_target_folder, force=True)

    """Add generated corpus indexes (speeches, utterances)"""
    factory: md.CorpusIndexFactory = md.CorpusIndexFactory(ProtocolMapper)
    factory.generate(corpus_folder=parlaclarin_folder, target_folder=metadata_target_folder)
    factory.upload(db=db, folder=metadata_target_folder, tablenames=service.schema.derived_tablenames)

    service.execute_sql_scripts(folder=scripts_folder)

    shutil.rmtree(path=metadata_folder, ignore_errors=True)


def load_document_patterns(filename: str, extension: str = None) -> list[str]:
    """Loads a list of ParlaCLARIN document names/patterns from a file"""
    if not os.path.isfile(filename):
        raise FileNotFoundError(filename)

    patterns: list[str] = []
    with open(filename, "r", encoding="utf8") as fp:
        for pattern in fp.read().splitlines():
            if not pattern:
                continue
            if extension and '*' not in pattern:
                pattern: str = replace_extension(pattern, 'xml')
            patterns.append(pattern)

    return patterns
