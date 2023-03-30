from __future__ import annotations

from functools import cached_property

import pandas as pd

from . import generate

null_frame: pd.DataFrame = pd.DataFrame()


class UtteranceIndex:
    def __init__(self):
        self.protocols: pd.DataFrame = null_frame
        self.utterances: pd.DataFrame = null_frame
        self.unknown_utterance_gender: pd.DataFrame = null_frame
        self.unknown_utterance_party: pd.DataFrame = null_frame

        self._table_infos: dict[str, str] = {
            'protocols': 'document_id',
            'utterances': 'u_id',
            'unknown_utterance_gender': 'u_id',
            'unknown_utterance_party': 'u_id',
        }

    def load(self, database_filename: str) -> UtteranceIndex:
        tables: dict[str, pd.DataFrame] = generate.DatabaseHelper(database_filename).load_data_tables(self._table_infos)
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

    def protocol(self, document_id: int) -> pd.Series:
        return self.protocols.loc[document_id]
