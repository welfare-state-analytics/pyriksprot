from __future__ import annotations

import glob
from dataclasses import asdict, dataclass
from os.path import basename, dirname, isdir
from os.path import join as jj
from typing import TYPE_CHECKING, List, Mapping, Optional, Set, Tuple

import pandas as pd

from pyriksprot.interface import TemporalKey
from pyriksprot.tagged_corpus.persist import load_metadata

from . import utility

if TYPE_CHECKING:
    from . import iterate

"""
Creates a recursive index of files in a source folder that match given pattern.
"""


@dataclass
class CorpusSourceItem:

    path: str
    filename: str = None
    name: str = None
    subfolder: str = None
    year: Optional[int] = None
    metadata: dict | None = None
    is_empty: Optional[int] = None

    def __post_init__(self):

        self.filename = basename(self.path)
        self.name = utility.strip_path_and_extension(self.path)
        self.subfolder = basename(dirname(self.path))
        self.year = self.to_year(basename(self.path))
        self.metadata = load_metadata(self.path)
        self.is_empty = self.metadata is None

        if not self.name.startswith("prot-"):
            raise ValueError(f"illegal filename {self.name}")

    def temporal_category(
        self, temporal_key: TemporalKey | Mapping[str, Tuple[int, int]], item: iterate.ProtocolSegment = None
    ) -> str:

        if isinstance(temporal_key, (TemporalKey, str, type(None))):

            if temporal_key in [None, '', 'document', 'protocol', TemporalKey.NONE]:
                """No temporal key gives a group per document/protocol"""
                return item.protocol_name

            if temporal_key == TemporalKey.Year:
                return str(self.year)

            if temporal_key == TemporalKey.Lustrum:
                low_year: int = self.year - (self.year % 5)
                return f"{low_year}-{low_year+4}"

            if temporal_key == TemporalKey.Decade:
                low_year: int = self.year - (self.year % 10)
                return f"{low_year}-{low_year+9}"

        elif isinstance(temporal_key, dict):
            """custom periods as a dict {'category-name': (from_year,to_year), ...}"""
            for k, v in temporal_key:
                if v[0] <= self.year <= v[1]:
                    return k

        raise ValueError(f"temporal period failed for {self.name}")

    def to_year(self, filename: str) -> Optional[int]:
        try:
            return int(filename.split("-")[1][:4])
        except ValueError:
            ...
        return None

    def to_dict(self) -> dict:

        return asdict(self)


@dataclass
class CorpusSourceIndex:
    def __init__(self, source_items: List[CorpusSourceItem]):

        self.source_items: List[CorpusSourceItem] = source_items
        self.lookup: dict = {x.name: x for x in self.source_items}

    def __getitem__(self, key: str) -> CorpusSourceItem:
        return self.lookup.get(key)

    def __contains__(self, key: str) -> bool:
        return key in self.lookup

    def __len__(self) -> int:
        return len(self.source_items)

    @staticmethod
    def load(
        *, source_folder: str, source_pattern: str, years: Optional[str | Set[int]] = None, skip_empty: bool = True
    ) -> "CorpusSourceIndex":

        if not isdir(source_folder):
            raise ValueError(f"folder {source_folder} not found")

        paths: List[str] = glob.glob(jj(source_folder, source_pattern), recursive=True)

        target_years: Set[int] = (
            None if years is None else (set(utility.parse_range_list(years)) if isinstance(years, str) else set(years))
        )

        source_items = [CorpusSourceItem(path=path) for path in paths]

        if target_years is not None:
            source_items = [x for x in source_items if x.year in target_years]

        if skip_empty:
            source_items = [x for x in source_items if not x.is_empty]

        source_index: CorpusSourceIndex = CorpusSourceIndex(source_items)

        return source_index

    @property
    def filenames(self) -> List[str]:
        return sorted([x.filename for x in self.source_items])

    @property
    def paths(self) -> List[str]:
        return sorted([x.path for x in self.source_items])

    def to_pandas(self) -> pd.DataFrame:
        df: pd.DataFrame = pd.DataFrame(data=[x.to_dict() for x in self.source_items])
        return df

    def to_csv(self, filename: str = None) -> Optional[str]:
        return self.to_pandas().to_csv(filename, sep='\t', index=None)

    @staticmethod
    def read_csv(filename: str) -> "CorpusSourceIndex":
        df: pd.DataFrame = pd.read_csv(filename, sep="\t", index_col=None)
        source_items: List[CorpusSourceItem] = [CorpusSourceItem(**d) for d in df.to_dict('records')]
        source_index: CorpusSourceIndex = CorpusSourceIndex(source_items)
        return source_index
