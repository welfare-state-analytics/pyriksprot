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


class UtteranceLookup:
    def __init__(self, **kwargs):

        self.protocols: pd.DataFrame = kwargs.get('protocols')
        self.utterances: pd.DataFrame = kwargs.get('utterances')
        self.unknown_utterance_gender: pd.DataFrame = kwargs.get('unknown_utterance_gender')
        self.unknown_utterance_party: pd.DataFrame = kwargs.get('unknown_utterance_party')

    @staticmethod
    def load(source: str | sqlite3.Connection | dict) -> UtteranceLookup:
        with (sqlite3.connect(database=source) if isinstance(source, str) else nullcontext(source)) as db:
            data: UtteranceLookup = UtteranceLookup(**mdu.load_tables(DATA_TABLES, db=db))
            return data

    @cached_property
    def unknown_parties(self) -> dict[str, int]:
        """Utterance `u_id` to `party_id` mapping"""
        return self.unknown_utterance_party['party_id'].to_dict()

    @cached_property
    def unknown_genders(self) -> dict[str, int]:
        """Utterance `u_id` to `gender_id` mapping"""
        return self.unknown_utterance_gender['gender_id'].to_dict()
