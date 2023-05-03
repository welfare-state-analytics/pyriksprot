"""
The MIT License (MIT)

Copyright (c) 2012-2018 Språkbanken, University of Gothenburg

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

# NOTE: A large part of this code is based on Sparv v4.0.0.
# Sparv dependency is made optional (lacks Stanza v1.6 support which has improved performance)

import abc
import pickle
import re
from os.path import dirname
from os.path import join as jj
from typing import Any, Iterable, Type

import nltk

# pylint: disable=no-else-continue, attribute-defined-outside-init consider-using-f-string

RESOURCES_FOLDER: str = jj(dirname(__file__), "resources")
MODEL_FILENAME: str = jj(RESOURCES_FOLDER, "bettertokenizer.sv")
SALDO_TOKENS_FILENAME: str = jj(RESOURCES_FOLDER, "bettertokenizer.sv.saldo-tokens")
SENTENCER_MODEL_FILENAME: str = jj(RESOURCES_FOLDER, "punkt-nltk-svenska.pickle")


class ISegmenter(abc.ABC):
    @classmethod
    @property
    def default_args(cls) -> dict[str, Any]:
        return {}

    @classmethod
    def create(cls, **args) -> "ISegmenter":
        return cls(**(cls.default_args | args))

    @abc.abstractmethod
    def tokenize(self, s: str) -> list[str | list[str]]:
        return []

    @abc.abstractmethod
    def span_tokenize(self, s) -> list[tuple[int, int]]:
        return []


# This class belongs to sparv-pipeline (temporarily copied out)
class BetterWordTokenizer(ISegmenter):
    """A word tokenizer based on the PunktWordTokenizer code.

    Heavily modified to add support for custom regular expressions, wordlists, and external configuration files.
    http://nltk.googlecode.com/svn/trunk/doc/api/nltk.tokenize.punkt.PunktSentenceTokenizer-class.html
    """

    # Format for the complete regular expression to be used for tokenization
    _word_tokenize_fmt = r'''(
        %(misc)s
        |
        %(multi)s
        |
        (?:(?:(?<=^)|(?<=\s))%(number)s(?=\s|$))  # Numbers with decimal mark
        |
        (?=[^%(start)s])
        (?:%(tokens)s%(abbrevs)s(?<=\s)(?:[^\.\s]+\.){2,}|\S+?)  # Accept word characters until end is found
        (?= # Sequences marking a word's end
            \s|                                 # White-space
            $|                                  # End-of-string
            (?:[%(within)s])|%(multi)s|         # Punctuation
            [%(end)s](?=$|\s|(?:[%(within)s])|%(multi)s)  # Misc characters if at end of word
        )
        |
        \S
    )'''

    # Used to realign punctuation that should be included in a sentence although it follows the period (or ?, !).
    re_boundary_realignment = re.compile(r'[“”"\')\]}]+?(?:\s+|(?=--)|$)', re.MULTILINE)

    re_punctuated_token = re.compile(r"\w.*\.$", re.UNICODE)

    def __init__(self, model, token_list=None, **args):  # pylint: disable=unused-argument
        """Parse configuration file (model) and token_list (if supplied)."""
        self.case_sensitive = False
        self.patterns = {"misc": [], "tokens": []}
        self.abbreviations = set()
        in_abbr = False

        if token_list:
            with open(token_list, encoding="UTF-8") as saldotokens:
                self.patterns["tokens"] = [re.escape(t.strip()) for t in saldotokens.readlines()]

        with open(model, encoding="UTF-8") as conf:
            for line in conf:
                if line.startswith("#") or not line.strip():
                    continue
                if not in_abbr:
                    if not in_abbr and line.strip() == "abbreviations:":
                        in_abbr = True
                        continue
                    else:
                        try:
                            key, val = line.strip().split(None, 1)
                        except ValueError:  # pylint: disable=bare-except
                            print(f"ERROR parsing configuration file: {line}")
                            raise
                        key = key[:-1]

                        if key == "case_sensitive":
                            self.case_sensitive = val.lower() == "true"
                        elif key.startswith("misc_"):
                            self.patterns["misc"].append(val)
                        elif key in ("start", "within", "end"):
                            self.patterns[key] = re.escape(val)
                        elif key in ("multi", "number"):
                            self.patterns[key] = val
                        # For backwards compatibility
                        elif key == "token_list":
                            pass
                        else:
                            raise ValueError("Unknown option: %s" % key)
                else:
                    self.abbreviations.add(line.strip())

    def _word_tokenizer_re(self):
        """Compile and return a regular expression for word tokenization."""
        try:
            return self._re_word_tokenizer
        except AttributeError:
            modifiers = (re.UNICODE | re.VERBOSE) if self.case_sensitive else (re.UNICODE | re.VERBOSE | re.IGNORECASE)
            self._re_word_tokenizer = re.compile(
                self._word_tokenize_fmt
                % {
                    "tokens": ("(?:" + "|".join(self.patterns["tokens"]) + ")|") if self.patterns["tokens"] else "",
                    "abbrevs": ("(?:" + "|".join(re.escape(a + ".") for a in self.abbreviations) + ")|")
                    if self.abbreviations
                    else "",
                    "misc": "|".join(self.patterns["misc"]),
                    "number": self.patterns["number"],
                    "within": self.patterns["within"],
                    "multi": self.patterns["multi"],
                    "start": self.patterns["start"],
                    "end": self.patterns["end"],
                },
                modifiers,
            )
            return self._re_word_tokenizer

    def word_tokenize(self, s):
        """Tokenize a string to split off punctuation other than periods."""
        words = self._word_tokenizer_re().findall(s)
        if not words:
            return words
        pos = len(words) - 1

        # Split sentence-final . from the final word.
        # i.e., "peter." "piper." ")" => "peter." "piper" "." ")"
        # but not "t.ex." => "t.ex" "."
        while pos >= 0 and self.re_boundary_realignment.match(words[pos]):
            pos -= 1
        endword = words[pos]
        if self.re_punctuated_token.search(endword):
            endword = endword[:-1]
            if endword not in self.abbreviations:
                words[pos] = endword
                words.insert(pos + 1, ".")

        return words

    def span_tokenize(self, s):
        """Tokenize s."""
        begin = 0
        for w in self.word_tokenize(s):
            begin = s.find(w, begin)
            yield begin, begin + len(w)
            begin += len(w)

    def tokenize(self, s: str) -> Iterable[str]:
        for x, y in self.span_tokenize(s):
            yield s[x:y]

    @classmethod
    @property
    def default_args(cls) -> dict[str, Any]:
        return dict(model=MODEL_FILENAME, token_list=SALDO_TOKENS_FILENAME)


class BetterSentenceTokenizer(ISegmenter):
    def __init__(self, model_path: str = SENTENCER_MODEL_FILENAME):
        with open(model_path, "rb") as fp:
            train_text = pickle.load(fp, encoding="UTF-8")
            self.segmenter = nltk.PunktSentenceTokenizer(train_text)

    def span_tokenize(self, s: str) -> Iterable[tuple[int, int]]:
        for a, b in self.segmenter.span_tokenize(s):
            if s[a:b].strip():
                yield (a, b)

    def tokenize(self, s: str) -> Iterable[str]:
        for a, b in self.span_tokenize(s):
            yield s[a:b]


class SegmenterRepository:
    _instances: dict[Type, ISegmenter] = {}

    @staticmethod
    def get(seg_cls: Type[ISegmenter], **args) -> ISegmenter:
        if seg_cls not in SegmenterRepository._instances:
            SegmenterRepository._instances[seg_cls] = seg_cls.create(**args)
        return SegmenterRepository._instances[seg_cls]

    @staticmethod
    def tokenizer():
        return SegmenterRepository.get(BetterWordTokenizer)

    @staticmethod
    def sentenizer():
        return SegmenterRepository.get(BetterSentenceTokenizer)


class ModelNotFoundError(Exception):
    ...


def default_tokenize(text: str, sentenize: bool = False) -> list[str]:
    tokenize = SegmenterRepository.tokenizer().tokenize
    if sentenize:
        return list(tokenize(s) for s in SegmenterRepository.sentenizer().tokenize(text))
    return tokenize(text)


def default_sentenize(text: str) -> list[str]:
    return SegmenterRepository.sentenizer().tokenize(text)
