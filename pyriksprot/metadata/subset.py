from __future__ import annotations

import os
import shutil
from typing import Any

import pandas as pd
from loguru import logger

from ..interface import IProtocolParser
from .config import MetadataTableConfigs
from .generate import CorpusIndexFactory

jj = os.path.join

# pylint: disable=unsubscriptable-object


def subset_to_folder(parser: IProtocolParser, protocols_source_folder: str, source_folder: str, target_folder: str):
    """Creates a subset of metadata in source metadata that includes only protocols found in source_folder"""

    logger.info("Subsetting metadata database.")
    logger.info(f"      ParlaClarin folder: {protocols_source_folder}")
    logger.info(f"  Source metadata folder: {source_folder}")
    logger.info(f"  Target metadata folder: {target_folder}")

    data: dict[str, pd.DataFrame] = (
        CorpusIndexFactory(parser).generate(corpus_folder=protocols_source_folder, target_folder=target_folder).data
    )

    protocols: pd.DataFrame = data.get("protocols")
    utterances: pd.DataFrame = data.get("utterances")

    person_ids: list[str] = set(utterances.person_id.unique().tolist())

    logger.info(f"found {len(person_ids)} unqiue persons in subsetted utterances.")

    config: MetadataTableConfigs = MetadataTableConfigs()

    for tablename in config.tablesnames0:
        source_name: str = jj(source_folder, f"{tablename}.csv")
        target_name: str = jj(target_folder, f"{tablename}.csv")

        if not 'person_id' in config[tablename].columns:
            shutil.copy(source_name, target_name)
            continue

        id_column: str = config[tablename].resolve_source_column('person_id')
        copy_csv_subset(source_name, target_name, {id_column: person_ids})

    protocol_ids: set[str] = {f"{x}.xml" for x in protocols['document_name']}

    copy_csv_subset(jj(source_folder, "unknowns.csv"), jj(target_folder, "unknowns.csv"), {'protocol_id': protocol_ids})


def copy_csv_subset(source_name: str, target_name: str, key_values: dict[str, list[Any]]) -> None:
    table: pd.DataFrame = pd.read_csv(source_name, sep=',', index_col=None)
    for key, values in key_values.items():
        if isinstance(values, (tuple, list, set)):
            table = table[table[key].isin(set(values))]
        else:
            table = table[table[key] == values]
    table.to_csv(target_name, sep=',', index=False)
