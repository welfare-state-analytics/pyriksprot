import pickle
from collections import defaultdict
from typing import Any, Callable, Dict, Iterable, List, Union

from tqdm.auto import tqdm

from pyriksprot.foss.sparv_tokenize import SegmenterRepository

from .iterate import XmlProtocolSegmentIterator, XmlUntangleSegmentIterator


class TermFrequencyCounter:
    """Term frequency container.

    Attributes:
        frequencies (Dict[str, int]): Term frequencies.
    """

    def __init__(self, tokenizer: Callable[[str], List[str]] = None, do_lower_case: bool = True, progress: bool = True):
        """
        Args:
            tokenizer (Callable[[str], List[str]], optional): Tokenizer to use when ingesting tokens. Defaults to None.
            do_lower_case (bool, optional): [description]. Defaults to True.
        """

        self._tokenize: Callable[[Any], List[str]] = tokenizer or SegmenterRepository.create_tokenize(False, False)
        self._do_lower_case: bool = do_lower_case
        self._progress: bool = progress

        self.frequencies: Dict[str, int] = defaultdict(int)

    def ingest(self, value: Union[str, Iterable[str], XmlUntangleSegmentIterator]) -> "TermFrequencyCounter":
        """Update term frequencies with term counts in `value`"""
        texts = (
            value
            if isinstance(value, str)
            else (x.data for x in value)
            if isinstance(value, (XmlUntangleSegmentIterator, XmlProtocolSegmentIterator))
            else value
        )
        for text in tqdm(texts, disable=not self._progress):
            if self._do_lower_case:
                text = text.lower()
            for word in self._tokenize(text):
                self.frequencies[word] += 1
        return self

    def store(self, filename: str, cut_off: int = None) -> None:
        """Store term frequencies to`filename`."""
        if cut_off:
            frequencies = {w: c for w, c in self.frequencies if c > cut_off}
        else:
            frequencies = self.frequencies
        with open(filename, 'wb') as fp:
            pickle.dump(frequencies, fp, pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def load(filename: str) -> defaultdict(int):
        """Load term frequency counts from pickled file `filename`."""
        with open(filename, 'rb') as fp:
            return pickle.load(fp)
