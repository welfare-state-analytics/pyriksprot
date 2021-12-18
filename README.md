# Python package for reading and tagging Riksdagens Protokoll

Batteries (tagger) not included.

## Overview

This package is intended to cover the following use cases:

### Extract "text documents" from the Parla-CLARIN XML files

Text can be extracted from the XML files at different granularity (paragraphs, utterance, speech, who, protocol). The text can be grouped (combined) into larger temporal blocks based on time (year, lustrum, decade or custom periods). Within each of these block the text in turn can be grouped by speaker attributes (who, party, gender).

The text extraction can done using the `riksprot2text` utility, which is a CLI interface installed with the package, or in Python code using the API that this package exposes. The Python API exposed both streaming (SAX based) methods and a domain model API (i.e. Python classes representing protocols, speeches and utterances).

Both the CLI and the API supports dehyphenation using method described in [Anföranden: Annotated and Augmented Parliamentary Debates from Sweden, Stian Rødven Eide, 2020](https://gup.ub.gu.se/publication/302449). The API also supports user defined text transformations.

### Extract PoS-tagged versions of the Parla-CLARIN XML files

Part-of-speech tagged versions of the protocols can be extracted with the same granularity and aggregation as described above for the raw text. The returned documents are tab-separated files with fields for text, baseform and pos-tag (UPOS, XPOS). Note that the actual part-of-speech tagging is done using tools found in the `pyriksprot_tagging` repository ([link](https://github.com/welfare-state-analytics/westac_parlaclarin_pipeline)).

Currently there are no open-source tagged versions of the corpos avaliable. The tagging is done using [Stanza](https://stanfordnlp.github.io/stanza/) with Swedish language models produced and made publically avaliable by Språkbanken Text.

### Store extracted text

The extracted text can be stored as optionally compressed plain text files on disk, or in a ZIP-archive.

## Pre-requisites

- Python >=3.8
- A folder containing the Riksdagen Protokoll (parliamentary protocols) Github repository.

```bash
cd some-folder \
git clone --branch "tag" tags/"tag" --depth 1 https://github.com/welfare-state-analytics/riksdagen-corpus.git
cd riksdagen-corpus
git config core.quotepath off

```

## Installation (Linux)

Create an new isolated virtual environment for pyriksprot:

```bash
mkdir /path/to/new/pyriksprot-folder
cd /path/to/new/pyriksprot-folder
python -m venv .venv
```

Activate the environment:

```bash
cd /path/to/new/pyriksprot-folder
source .venv/bin/activate
```

Install `pyriksprot` in activated virtual environment.

```bash
pip install pyriksprot
```

## CLI riksprot2text:  Extract aggregated text corpus from Parla-CLARIN XML files

```bash

λ riksprot2text --help

Usage: riksprot2text [OPTIONS] SOURCE_FOLDER TARGET

Options:
  -m, --mode [plain|zip|gzip|bz2|lzma]
                                  Target type
  -t, --temporal-key TEXT         Temporal partition key(s)
  -y, --years TEXT                Years to include in output
  -g, --group-key TEXT            Partition key(s)
  -p, --processes INTEGER RANGE   Number of processes to use
  -l, --segment-level [protocol|speech|utterance|paragraph|who]
                                  Protocol extract segment level
  -e, --keep-order                Keep output in filename order (slower, multiproc)

  -s, --skip-size INTEGER RANGE   Skip blocks of char length less than
  -d, --dedent                    Remove indentation
  -k, --dehyphen                  Dehyphen text
  --help                          Show this message and exit.

```

### Examples CLI

Aggregate text per year grouped by speaker. Store result in a single zip. Skip documents less than 50 characters.

```python
riksprot2text /path/to/corpus output.zip -m zip -t year -l protocol -g who --skip-size 50
```

Aggregate text per decade grouped by speaker. Store result in a single zip. Remove indentations and hyphenations.

```bash
riksprot2text /path/to/corpus output.zip -m zip -t decade -l who -g who --dedent --dehyphen
```

Aggregate text using customized temporal periods and grouped by party.

```bash
riksprot2text /path/to/corpus output.zip -m zip -t "1920-1938,1929-1945,1946-1989,1990-2020" -l who -g party
```

Aggregate text per document and group by gender and party.

```bash
riksprot2text /path/to/corpus output.zip -m zip -t protocol -l who -g party -g gender
```

Aggregate text per year grouped by gender and party and include only 1946-1989.

```bash
riksprot2text /path/to/corpus output.zip -m zip -t year -l who -g party -g gender -y 1946-1989
```

## Python API - Iterate XML protocols

Aggregate text per year grouped by speaker. Store result in a single zip. Skip documents less than 50 characters.

<!--pytest-codeblocks:skip-->
```python
import pyriksprot

target_filename: str = f'output.zip'
opts = {
    'source_folder': '/path/to/corpus',
    'target': 'outout.zip',
    'target_type': 'files-in-zip',
    'segment_level': SegmentLevel.Who,
    'dedent': True,
    'dehyphen': False,
    'years': '1955-1965',
    'temporal_key': TemporalKey.Protocol,
    'group_keys': (GroupingKey.Party, GroupingKey.Gender),
}

pyriksprot.extract_corpus_text(**opts)

```


Iterate over protocol and speaker:

```python

from pyriksprot import interface, iterstors

items: Iterable[interface.ProtocolSegment] = iterators.XmlProtocolTextIterator(
    filenames=filenames, segment_level=SegmentLevel.Who, segment_skip_size=0, processes=4
)

for item in items:
    print(item.who, len(item.text))

```

Iterate over protocol and speech, skip empty:

```python

from pyriksprot import interface, iterstors

items: Iterable[interface.ProtocolSegment] = iterators.XmlProtocolTextIterator(
    filenames=filenames, segment_level=SegmentLevel.Who, segment_skip_size=1, processes=4
)

for item in items:
    print(item.who, len(item.text))

```

Iterate over protocol and speech, apply preprocess function(s):

```python

from pyriksprot import interface, iterstors
import ftfy  # pip install ftfy
import unidecode

fix_text: Callable[[str], str] = pyriksprot.compose(
    [str.lower, pyriksprot.dedent, ftfy.fix_character_width, unidecode.unidecode ]
)
items: Iterable[interface.ProtocolSegment] = iterators.XmlProtocolTextIterator(
    filenames=filenames, segment_level=SegmentLevel.Speech, segment_skip_size=1, processes=4, preprocessor=fix_text,
)

for item in items:
    print(item.who, len(item.text))

```

## Python API - Iterate protocols as domain entities

## CLI riksprot2tags:  Extract aggregated part-of-speech tagged corpus
