# AI Coding Agent Instructions - Pyriksprot

## Project Overview
Python API and CLI tools for extracting, processing, and transforming Swedish parliamentary debates (Riksdagens Protokoll) from ParlaCLARIN XML format. Core library for the SWERIK/Westac project ecosystem that generates tagged corpora, metadata databases, speech indexes, and document-term matrices from historical parliamentary data (1867-2020+).

## Architecture & Core Components

### Configuration System (`pyriksprot/configuration/`)
- **Critical**: Initialize `ConfigStore.configure_context(source='config.yml')` before accessing any config values
- Access via `ConfigValue("corpus.version").resolve()` - supports dot notation for nested keys
- YAML config supports custom constructors: `!jj` (path join), `!join` (string concatenation), `!path_join`
- Environment variable override via `PYRIKSPROT_` prefix (e.g., `PYRIKSPROT_CORPUS_VERSION`)
- Config generation: `make-config` CLI generates from template at `pyriksprot/templates/config-template.yml.j2`
- Test config: Bootstrap in `tests/conftest.py` creates `tests/output/config.yml` from template

### Domain Model (`pyriksprot/interface.py`)
- **Protocol**: ParlaCLARIN XML document with `utterances`, `speaker_notes`, `page_references`
- **Utterance**: Individual speech segment with `who`, `u_id`, `speaker_note_id`, `page_number`
- **Speech**: Merged utterances via `MergeStrategyType` (chain, who, who_sequence, speaker_note_id_sequence)
- **SpeakerNote**: Speaker metadata extracted from XML `<note>` tags
- Enums: `SegmentLevel` (protocol|speech|utterance|paragraph), `ContentType` (text|tagged_frame), `TemporalKey` (year|decade|lustrum)

### Corpus Processing Pipeline (`pyriksprot/corpus/`)
- **ParlaCLARIN XML → Speeches**: `parse.py::ProtocolMapper` → `to_speech.py::to_speeches()` with merge strategies
- **Tagged frames**: POS-tagged VRT stored as zipped JSON via `tagged/persist.py`
- **Corpus indexing**: `corpus_index.py::CorpusSourceIndex` - file discovery with year filtering, glob patterns
- **Streaming iteration**: `iterate.py::ProtocolIterator` yields `ProtocolSegment` items for multiprocessing

### Metadata System (`pyriksprot/metadata/`)
- **SQLite database**: Generated via `metadata2db` CLI from GitHub-hosted CSV metadata (riksdagen-persons repo)
- **Schema versions**: Tracked separately from corpus versions (e.g., corpus v1.4.1 + metadata v1.1.3)
- **Index generation**: `CorpusIndexFactory` scans ParlaCLARIN corpus to create protocol/utterance/speaker indexes
- **Speaker enrichment**: `SpeakerInfoService` joins speaker metadata with corpus utterances during extraction

### CLI Scripts (`pyriksprot/scripts/`)
- **riksprot2text**: Extract plain text aggregated by temporal/grouping keys with dehyphenation
- **riksprot2speech**: Generate tagged speech corpora (feather/zip/csv) from tagged frames
- **riksprot2speech_text**: Plain text speeches from ParlaCLARIN XML (not tagged)
- **riksprot2speech_index**: Create speech index (CSV/feather) from tagged frames
- **riksprot2vrt**: Export to VRT format for CWB corpus encoding
- **metadata2db**: Commands: `database` (create), `download` (GitHub metadata), `index` (corpus scan)
- **subset-corpus**: Extract subset of protocols + regenerate metadata for sample datasets
- **make-config**: Generate config YAML from template with version/path interpolation
- All scripts use `@option2` decorator pattern + `update_arguments_from_options_file()` for YAML option files

### Workflows (`pyriksprot/workflows/`)
- **extract_tags**: Unified extraction pipeline for tagged corpora (speeches, DTMs)
- **extract_text**: Plain text extraction with temporal/grouping aggregation
- **create_metadata**: GitHub metadata download + SQLite DB creation + corpus index generation
- **subset_corpus_and_metadata**: Extract protocol subset + regenerate metadata (used by sample-data)
- **tag**: Placeholder for external tagger integration (actual tagging in pyriksprot_tagger repo)
- **tf**: Compute term frequencies for dehyphenation word lists

## Development Workflows

### Running & Testing
```bash
# Install dependencies
poetry install

# Run tests (uses tests/conftest.py bootstrap)
make test          # pytest with coverage
make pytest        # pytest without coverage
make retest        # run only failed tests

# Code quality
make tidy          # black + isort
make lint          # pylint + flake8
make ready         # full pre-release checks

# Profiling
make profile-extract-text   # profile text extraction
make profile-extract-tags   # profile tagged frame extraction
```

### Test Data Setup
- Bootstrap in `tests/conftest.py::bootstrap_testing()` creates test config and checks corpus availability
- Test data managed in `humlab-swedeb/sample-data` repo (5files, decade-1960, random_sample_10files)
- `tests/scripts/make_test_data.py` generates test corpora (deprecated - use sample-data Makefiles)
- Environment variables: `CORPUS_VERSION`, `METADATA_VERSION` must be set in `.env` for tests

### Configuration Pattern
```python
# At application/test startup
from pyriksprot.configuration import ConfigStore, ConfigValue
ConfigStore.configure_context(source='config.yml', env_prefix='PYRIKSPROT')

# Accessing values
corpus_folder = ConfigValue("corpus.folder").resolve()
metadata_opts = ConfigValue("metadata.database").resolve()  # nested dict
```

### Merge Strategies (Critical for Speech Generation)
- `chain`: Utterances assigned to same speaker (speaker_note_id) merged sequentially
- `who`: All utterances for unique `who` value merged (ignores sequence)
- `who_sequence`: Consecutive utterances with same `who` merged (preserves order)
- `speaker_note_id_sequence`: Consecutive utterances with same speaker_note_id merged
- Default: `chain` for most extraction workflows

## Integration Points

### External Dependencies
- **pyriksprot_tagger**: Separate repo for POS tagging (Stanza-based) - produces tagged frames consumed by this package
- **penelope**: Text analysis library (humlab-penelope) for DTM/vectorization (`vectorize-id` CLI)
- **CWB (Corpus Workbench)**: VRT export targets CWB encoding (`riksprot2vrt`)
- **GitHub repos**: `swerik-project/riksdagen-records` (corpus), `swerik-project/riksdagen-persons` (metadata)

### Data Flow
1. **ParlaCLARIN XML** (from riksdagen-records GitHub) → `riksprot2text` → plain text aggregations
2. **Tagged frames** (from pyriksprot_tagger) → `riksprot2speech` → speech corpora (feather)
3. **Speech corpora** → `riksprot2speech_index` → speech index (feather/CSV.gz)
4. **Metadata CSV** (from riksdagen-persons GitHub) → `metadata2db` → SQLite database
5. **Tagged frames** → `riksprot2vrt` → VRT files → CWB corpus (for Swedeb API)

### Swedeb API Integration
- API consumes: speech index (feather), CWB corpus, metadata DB, DTM corpora
- Config template includes paths for Swedeb deployment (`tagged_speeches.folder`, `dehyphen.folder`)
- Speech index must include metadata joins for API filtering (party, gender, office)

## Code Patterns & Conventions

### Multiprocessing Pattern
```python
from pyriksprot.dispatch import dispatch
# Configure via multiproc_processes, multiproc_chunksize, multiproc_keep_order
# Used in: extract_tags, extract_text, speech_index workflows
```

### Protocol Parsing
```python
from pyriksprot.corpus.parlaclarin import ProtocolMapper
from pyriksprot.foss import untangle
# ProtocolMapper extracts: name, date, utterances, speaker_notes from XML
# Use untangle for lightweight XML parsing (not lxml)
```

### Option Files Pattern
```python
# CLI scripts accept --options-filename pointing to YAML with all CLI options
# Simplifies complex invocations and enables reproducible builds
# Example: opts/tagged-speeches/*.yml in sample-data Makefiles
```

### Dehyphenation
- Word frequency data (`riksprot2tfs`) used by dehyphenation preprocessor
- Applied via `--dehyphen` flag in text extraction CLIs
- Frequency pickle stored at `{corpus_version}/dehyphen/word-frequencies.pkl`

## Common Tasks

### Adding a New CLI Script
1. Create script in `pyriksprot/scripts/` with `@click.command()` + `@option2` decorators
2. Register in `pyproject.toml` under `[tool.poetry.scripts]`
3. Use `update_arguments_from_options_file()` for YAML option support
4. Follow naming: `riksprot2{output}` or `{noun}2{format}` pattern

### Modifying Speech Extraction
1. Understand merge strategy in `to_speech.py::MergerFactory`
2. Test with different strategies: `extract_tags.py` uses `merge_strategy` parameter
3. Update speech index generation if schema changes (`workflows/speech_index.py`)

### Updating Metadata Schema
1. Modify `metadata/schema.py::MetadataSchema` for new version
2. Update SQL scripts in `pyriksprot/sql/` for derived tables
3. Regenerate test metadata: `metadata2db database --force` with test config

## Version Management
- Corpus version (e.g., v1.4.1): Git tag of riksdagen-records repo
- Metadata version (e.g., v1.1.3): Git tag of riksdagen-persons repo
- Package version: Poetry semantic versioning (`make bump.patch`, `make tag`)
- Test via `tag-info` CLI to verify Git repo tags

## Notes
- **Performance**: Use feather format for large DataFrames, multiprocessing for corpus iteration
- **Memory**: Tagged frame streaming via `iterate.py` for large corpora
- **Debugging**: Loguru configured in conftest; use `logger.info()` in workflows
- **Git**: `gitchen.py` provides Git inspection utilities for repo tags/commits
