from __future__ import annotations

import glob
from dataclasses import asdict, dataclass
from os.path import basename, dirname, isdir
from os.path import join as jj
from typing import List, Optional, Set

import pandas as pd

from .. import utility
from .tagged.persist import load_metadata

METADATA_FILENAME: str = 'metadata.json'

"""
Creates a recursive index of files in a source folder that match given pattern.
"""


@dataclass
class ICorpusSourceItem:
    """A CorpusSourceItem without metadata"""

    path: str
    filename: str = None
    name: str = None
    subfolder: str = None
    year: Optional[int] = None

    def __post_init__(self):
        self.filename = basename(self.path)
        self.name = utility.strip_path_and_extension(self.path)
        self.subfolder = basename(dirname(self.path))

        if not self.name.startswith("prot-"):
            raise ValueError(f"illegal filename {self.name}")

        """Year extracted from filename"""
        self.year = int(self.filename.split("-")[1][:4])

    @property
    def is_empty(self) -> bool:
        return False

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class TaggedCorpusSourceItem(ICorpusSourceItem):
    """A CorpusSourceItem with metadata"""

    metadata: dict | None = None
    # actual_year: int = None

    def __post_init__(self):
        super().__post_init__()
        self.metadata = load_metadata(self.path)
        """Year from data in metadata (the actual year)"""
        # self.actual_year = int(self.metadata['date'][:4]) if self.metadata else self.year

    @property
    def is_empty(self) -> bool:
        """Checks if file is empty. Only valid for tagged ZIP files"""
        return not bool(self.metadata)


@dataclass
class CorpusSourceIndex:
    """Index of files in a source folder"""

    def __init__(self, source_items: list[ICorpusSourceItem]):
        self.source_items: list[ICorpusSourceItem] = source_items
        self.lookup: dict = {x.name: x for x in self.source_items}

    def __getitem__(self, key: str) -> ICorpusSourceItem:
        return self.lookup.get(key)

    def __contains__(self, key: str) -> bool:
        return key in self.lookup

    def __len__(self) -> int:
        return len(self.source_items)

    @staticmethod
    def load(
        *, source_folder: str, source_pattern: str, years: Optional[str | Set[int]] = None, skip_empty: bool = True
    ) -> "CorpusSourceIndex":
        """Loads a CorpusSourceIndex from a folder with files matching a pattern"""
        if not isdir(source_folder):
            raise ValueError(f"folder {source_folder} not found")

        paths: List[str] = glob.glob(jj(source_folder, source_pattern), recursive=True)

        target_years: Set[int] = (
            None if years is None else (set(utility.parse_range_list(years)) if isinstance(years, str) else set(years))
        )

        source_items = [CorpusSourceIndex.create(path=path) for path in paths]

        if target_years is not None:
            source_items = [x for x in source_items if x.year in target_years]

        if skip_empty:
            source_items = [x for x in source_items if not x.is_empty]

        source_index: CorpusSourceIndex = CorpusSourceIndex(source_items)

        return source_index

    @staticmethod
    def create(path: str) -> ICorpusSourceItem:
        """Creates a CorpusSourceItem given a path"""
        if path.endswith("zip"):
            return TaggedCorpusSourceItem(path)
        return ICorpusSourceItem(path)

    @property
    def filenames(self) -> List[str]:
        """Returns a list of filenames in the index"""
        return sorted([x.filename for x in self.source_items])

    @property
    def paths(self) -> List[str]:
        """Returns a list of paths to files in the index"""
        return sorted([x.path for x in self.source_items])

    def to_pandas(self) -> pd.DataFrame:
        """Returns a pandas DataFrame with columns path, filename, name, subfolder, year"""
        df: pd.DataFrame = pd.DataFrame(data=[x.to_dict() for x in self.source_items])
        return df

    def to_csv(self, filename: str = None) -> Optional[str]:
        """Writes a CSV file with columns path, filename, name, subfolder, year"""
        return self.to_pandas().to_csv(filename, sep='\t', index=None)

    @staticmethod
    def read_csv(filename: str) -> "CorpusSourceIndex":
        """Reads a CSV file with columns path, filename, name, subfolder, year"""
        df: pd.DataFrame = pd.read_csv(filename, sep="\t", index_col=None)
        source_items: list[ICorpusSourceItem] = [ICorpusSourceItem(**d) for d in df.to_dict('records')]
        source_index: CorpusSourceIndex = CorpusSourceIndex(source_items)
        return source_index
