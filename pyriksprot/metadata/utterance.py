from __future__ import annotations

import sqlite3
from contextlib import nullcontext
from functools import cached_property
from typing import List, Mapping

import pandas as pd

from . import utility as mdu

DATA_TABLES: dict[str, str] = {
    'protocols': 'document_id',
    'utterances': 'u_id',
    'unknown_utterance_gender': 'u_id',
    'unknown_utterance_party': 'u_id',
}

null_frame: pd.DataFrame = pd.DataFrame()

class UtteranceLookup:
    def __init__(self):

        self.protocols: pd.DataFrame = null_frame
        self.utterances: pd.DataFrame = null_frame
        self.unknown_utterance_gender: pd.DataFrame = null_frame
        self.unknown_utterance_party: pd.DataFrame = null_frame

    def load(self, source: str | sqlite3.Connection | dict) -> UtteranceLookup:
        with (sqlite3.connect(database=source) if isinstance(source, str) else nullcontext(source)) as db:
            tables: dict[str, pd.DataFrame] = mdu.load_tables(DATA_TABLES, db=db)
            for table_name, table in tables.items():
                setattr(self, table_name, table)
        return self

    @cached_property
    def unknown_party_lookup(self) -> dict[str, int]:
        """Utterance `u_id` to `party_id` mapping"""
        return self.unknown_utterance_party['party_id'].to_dict()

    @cached_property
    def unknown_gender_lookup(self) -> dict[str, int]:
        """Utterance `u_id` to `gender_id` mapping"""
        return self.unknown_utterance_gender['gender_id'].to_dict()
