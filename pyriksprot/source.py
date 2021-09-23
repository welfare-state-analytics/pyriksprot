from __future__ import annotations

import glob
from dataclasses import asdict, dataclass
from os.path import basename, dirname
from os.path import join as jj
from typing import List, Mapping, Optional, Set, Tuple

import pandas as pd

from pyriksprot.interface import ProtocolIterItem

from . import utility

"""
Creates an index of XML files in a source folder. Recursively looks for `prot-*.xml` files.
"""


@dataclass
class SourceIndexItem:
    path: str
    filename: str = None
    name: str = None
    subfolder: str = None
    year: Optional[int] = None

    def __post_init__(self):
        self.filename = basename(self.path)
        self.name = utility.strip_path_and_extension(self.path)
        self.subfolder = basename(dirname(self.path))
        self.year = self.to_year(basename(self.path))

    def temporal_hashcode(
        self, temporal_key: str | Mapping[str, Tuple[int, int]], item: ProtocolIterItem = None
    ) -> str:

        if isinstance(temporal_key, str):

            if temporal_key in ['protocol', 'none']:
                return item.name

            if temporal_key == 'year':
                return str(self.year)

            if temporal_key == 'lustrum':
                low_year: int = self.year - (self.year % 5)
                return f"{low_year}-{low_year+4}"

            if temporal_key == 'year':
                low_year: int = self.year - (self.year % 10)
                return f"{low_year}-{low_year+9}"

        elif isinstance(temporal_key, dict):

            for k, v in temporal_key:
                if v[0] <= self.year <= v[1]:
                    return k

        raise ValueError(f"temporal period failed for {self.name}")

    def to_year(self, filename: str) -> Optional[int]:
        try:
            filename = basename(filename)
            if filename.startswith("prot-"):
                return int(filename.split("-")[1][:4])
        except ValueError:
            ...
        return None

    def to_dict(self) -> dict:

        return asdict(self)


@dataclass
class SourceIndex:
    def __init__(self, source_items: List[SourceIndexItem]):

        self.source_items = source_items
        self.lookup = {x.name: x for x in self.source_items}

    def __getitem__(self, key: str) -> SourceIndexItem:
        return self.lookup.get(key)

    def __contains__(self, key: str) -> bool:
        return key in self.lookup

    def __len__(self) -> int:
        return len(self.source_items)

    @staticmethod
    def load(
        source_folder: str, source_pattern: str = '**/prot-*.xml', years: Optional[str | Set[int]] = None
    ) -> "SourceIndex":

        paths: List[str] = glob.glob(jj(source_folder, source_pattern), recursive=True)

        target_years: Set[int] = (
            None if years is None else (set(utility.parse_range_list(years)) if isinstance(years, str) else set(years))
        )

        source_items = [SourceIndexItem(path=path) for path in paths]

        if target_years is not None:
            source_items = [x for x in source_items if x.year in target_years]

        source_index: SourceIndex = SourceIndex(source_items)

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
    def read_csv(filename: str) -> "SourceIndex":
        df: pd.DataFrame = pd.read_csv(filename, sep="\t", index_col=None)
        source_items: List[SourceIndexItem] = [SourceIndexItem(**d) for d in df.to_dict('records')]
        source_index: SourceIndex = SourceIndex(source_items)
        return source_index
