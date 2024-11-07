from __future__ import annotations

import os
import shutil
from glob import glob
from os.path import basename
from typing import Any

import pandas as pd
from loguru import logger

from pyriksprot.utility import reset_folder

from ..interface import IProtocolParser
from .corpus_index_factory import CorpusIndexFactory
from .schema import MetadataSchema, MetadataTable

jj = os.path.join

# pylint: disable=unsubscriptable-object


def subset_to_folder(
    parser: IProtocolParser, tag: str, protocols_source_folder: str, source_folder: str, target_folder: str
):
    """Creates a subset of metadata in source metadata that includes only protocols found in source_folder"""

    logger.info("Subsetting metadata database.")
    logger.info(f"      ParlaClarin folder: {protocols_source_folder}")
    logger.info(f"  Source metadata folder: {source_folder}")
    logger.info(f"  Target metadata folder: {target_folder}")

    reset_folder(target_folder, force=True)

    data: dict[str, pd.DataFrame] = (
        CorpusIndexFactory(parser, schema=tag)
        .generate(corpus_folder=protocols_source_folder, target_folder=target_folder)
        .data
    )

    protocols: pd.DataFrame = data.get("protocols")
    utterances: pd.DataFrame = data.get("utterances")

    person_ids: list[str] = set(utterances.person_id.unique().tolist())

    logger.info(f"found {len(person_ids)} unique persons in subsetted utterances.")

    schema: MetadataSchema = MetadataSchema(tag)

    filenames: set[str] = {basename(x) for x in glob(jj(source_folder, "*.csv"))}

    schema_filenames: set[str] = {x.basename for x in schema.definitions.values() if not x.is_derived}

    if not set(schema_filenames).issubset(filenames):
        missing_files: set[str] = schema_filenames - filenames
        raise Exception(f"subset_to_folder: missing schema files: {', '.join(missing_files)}")

    for filename in filenames:
        source_name: str = jj(source_folder, filename)
        target_name: str = jj(target_folder, filename)

        if filename not in schema_filenames:
            logger.warning(f"Skipping file {filename} as it is not defined in metadata schema.")
            continue

        cfg: MetadataTable = schema.get_by_filename(filename)

        if cfg is None or not 'person_id' in cfg.columns:
            shutil.copy(source_name, target_name)
            continue

        id_column: str = cfg.resolve_source_column('person_id')
        copy_csv_subset(source_name, target_name, {id_column: person_ids})

    protocol_ids: set[str] = {f"{x}.xml" for x in protocols['document_name']}

    if os.path.isfile(jj(source_folder, "unknowns.csv")):
        copy_csv_subset(
            jj(source_folder, "unknowns.csv"), jj(target_folder, "unknowns.csv"), {'protocol_id': protocol_ids}
        )


def copy_csv_subset(source_name: str, target_name: str, key_values: dict[str, list[Any]]) -> None:
    table: pd.DataFrame = pd.read_csv(source_name, sep=',', index_col=None)
    for key, values in key_values.items():
        if isinstance(values, (tuple, list, set)):
            table = table[table[key].isin(set(values))]
        else:
            table = table[table[key] == values]
    table.to_csv(target_name, sep=',', index=False)
