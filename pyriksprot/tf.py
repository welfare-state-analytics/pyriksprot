import os
import pickle
from collections import defaultdict
from glob import glob
from typing import Any, Callable, Dict, Iterable, List, Union

from tqdm.auto import tqdm

from .parse import ProtocolTextIterator
from .tokenize import tokenize as default_tokenize


class TermFrequencyCounter:
    """Term frequency container.

    Attributes:
        frequencies (Dict[str, int]): Term frequencies.
    """

    def __init__(self, tokenize: Callable[[str], List[str]] = None, do_lower_case: bool = True):
        """
        Args:
            tokenize (Callable[[str], List[str]], optional): Tokenizer to use when ingesting tokens. Defaults to None.
            do_lower_case (bool, optional): [description]. Defaults to True.
        """

        self._tokenize: Callable[[Any], List[str]] = tokenize or default_tokenize
        self._do_lower_case: bool = do_lower_case

        self.frequencies: Dict[str, int] = defaultdict(int)

    def ingest(self, value: Union[str, Iterable[str], ProtocolTextIterator]) -> "TermFrequencyCounter":
        """Update term frequencies with term counts in `value`"""
        texts = (
            (value,)
            if isinstance(value, str)
            else (t for _, t in value)
            if isinstance(value, ProtocolTextIterator)
            else value
        )
        for text in tqdm(texts):
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


def compute_term_frequencies(source: Union[str, List[str]], filename: str) -> TermFrequencyCounter:
    """Compute (corpus) term frequency for documents in `source `.

    Args:
        source (Union[str, List[str]]): ParlaClarin filename(s), folder, filename patterna
        filename (str): [description]

    Raises:
        ValueError: Unsupported source.

    Returns:
        TermFrequencyCounter: Combinded term frequencies for given source(s).
    """
    try:
        if isinstance(source, ProtocolTextIterator):
            texts = source
        else:
            if isinstance(source, str):
                if os.path.isfile(source):
                    filenames = [source]
                elif os.path.isdir(source):
                    filenames = glob(os.path.join(source, "*.xml"))
                else:
                    filenames = glob(source)
            elif isinstance(source, list):
                filenames = source
            else:
                raise ValueError(f"unknown source of type {type(source)}")

            texts = ProtocolTextIterator(filenames=filenames, level='protocol')

        counter = TermFrequencyCounter()

        counter.ingest(texts)

        if filename is not None:
            counter.store(filename)

        return counter

    except Exception as ex:
        print(ex)
        raise
