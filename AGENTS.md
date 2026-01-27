# AI Coding Agent Guide - Pyriksprot

## Quick Start for AI Agents

This document provides essential guidance for AI coding agents working on the Pyriksprot project.

## Project Summary

**Pyriksprot** is a Python API and CLI toolset for extracting, processing, and transforming Swedish parliamentary debates (Riksdagens Protokoll) from ParlaCLARIN XML format. It's a core library in the SWERIK/Westac project ecosystem that generates tagged corpora, metadata databases, speech indexes, and document-term matrices from historical parliamentary data (1867-2020+).

## Essential Knowledge

### Configuration First
```python
# ALWAYS initialize configuration before any other operations
from pyriksprot.configuration import ConfigStore, ConfigValue
ConfigStore.configure_context(source='config.yml', env_prefix='PYRIKSPROT')

# Access values using dot notation
corpus_folder = ConfigValue("corpus.folder").resolve()
```

### Key Architecture Components

1. **Domain Model** (`pyriksprot/interface.py`)
   - Protocol → Utterances → Speeches
   - Merge strategies: chain, who, who_sequence, speaker_note_id_sequence

2. **Configuration System** (`pyriksprot/configuration/`)
   - YAML-based with environment variable overrides
   - Custom constructors: `!jj`, `!join`, `!path_join`

3. **CLI Scripts** (`pyriksprot/scripts/`)
   - `riksprot2text` - Extract plain text
   - `riksprot2speech` - Generate tagged speech corpora
   - `riksprot2vrt` - Export to VRT format
   - `metadata2db` - Metadata database management

4. **Workflows** (`pyriksprot/workflows/`)
   - `extract_tags` - Tagged corpora extraction
   - `extract_text` - Plain text extraction
   - `create_metadata` - Metadata processing
   - `subset_corpus_and_metadata` - Sample data generation

## Development Commands

```bash
# Setup
poetry install

# Testing
make test          # Run tests with coverage
make pytest        # Run tests without coverage
make retest        # Re-run failed tests only

# Code Quality
make tidy          # Format with black + isort
make lint          # Run pylint + flake8
make ready         # Full pre-release checks

# Profiling
make profile-extract-text
make profile-extract-tags
```

## Common Patterns

### CLI Script Pattern
```python
import click
from pyriksprot.utility import update_arguments_from_options_file

@click.command()
@click.option('--options-filename', type=click.Path())
@click.option('--output-folder', required=True)
def main(options_filename: str, **kwargs):
    opts = update_arguments_from_options_file(kwargs, options_filename)
    # Process with opts
```

### Protocol Parsing
```python
from pyriksprot.corpus.parlaclarin import ProtocolMapper
from pyriksprot.foss import untangle

root = untangle.parse(xml_filename)
protocol = ProtocolMapper.to_protocol(root)
```

### Speech Generation
```python
from pyriksprot.to_speech import to_speeches

speeches = to_speeches(
    protocols=protocol_iterator,
    merge_strategy='chain',  # or 'who', 'who_sequence', 'speaker_note_id_sequence'
    skip_size=1
)
```

## Critical Rules

1. **Always** initialize ConfigStore before accessing configuration values
2. **Never** hardcode paths - use ConfigValue resolution
3. **Prefer** feather format for large DataFrames
4. **Use** multiprocessing via `dispatch.dispatch()` for corpus iteration
5. **Test** with sample data from `humlab-swedeb/sample-data` repo
6. **Follow** naming: `riksprot2{output}` or `{noun}2{format}` for CLI scripts

## Testing Setup

Test configuration is bootstrapped in `tests/conftest.py`:
- Creates `tests/output/config.yml` from template
- Requires `CORPUS_VERSION` and `METADATA_VERSION` environment variables
- Test data managed in `humlab-swedeb/sample-data` repo

## Data Flow

```
ParlaCLARIN XML (riksdagen-records)
    ↓
riksprot2text → Plain text aggregations
    ↓
Tagged frames (pyriksprot_tagger)
    ↓
riksprot2speech → Speech corpora (feather)
    ↓
riksprot2speech_index → Speech index
    ↓
Swedeb API / CWB Corpus
```

## External Dependencies

- **pyriksprot_tagger** - POS tagging (separate repo)
- **penelope** - Text analysis/vectorization (humlab-penelope)
- **CWB** - Corpus Workbench for VRT encoding
- **GitHub repos**:
  - `swerik-project/riksdagen-records` - Corpus data
  - `swerik-project/riksdagen-persons` - Metadata

## Version Management

- **Corpus version**: Git tag of riksdagen-records repo (e.g., v1.4.1)
- **Metadata version**: Git tag of riksdagen-persons repo (e.g., v1.1.3)
- **Package version**: Poetry semantic versioning

Check versions with: `tag-info` CLI

## File Locations

- **Configs**: `configs/config_v{corpus}_{metadata}.yml`
- **Test data**: `tests/test_data/`
- **SQL scripts**: `pyriksprot/sql/`
- **Templates**: `pyriksprot/templates/`
- **Sample corpora**: External `sample-data` repo

## Performance Tips

- Use `multiproc_processes`, `multiproc_chunksize`, `multiproc_keep_order` for parallel processing
- Stream tagged frames via `iterate.py` for memory efficiency
- Apply `--dehyphen` flag for text extraction with word frequency data
- Use option files (YAML) for complex CLI invocations

## Adding New Features

### New CLI Script
1. Create in `pyriksprot/scripts/` with `@option2` decorators
2. Register in `pyproject.toml` under `[tool.poetry.scripts]`
3. Support `--options-filename` for YAML configuration
4. Follow existing script patterns

### Modifying Speech Extraction
1. Understand merge strategies in `to_speech.py::MergerFactory`
2. Test with different strategies
3. Update speech index if schema changes

### Updating Metadata Schema
1. Modify `metadata/schema.py::MetadataSchema`
2. Update SQL scripts in `pyriksprot/sql/`
3. Regenerate with `metadata2db database --force`

## Debugging

- Loguru configured in conftest for test logging
- Use `logger.info()` in workflows
- Git inspection via `gitchen.py` utilities
- Profile with dedicated make targets

## Further Reading

- Full technical details: `.github/copilot-instructions.md`
- Package documentation: `docs/`
- README: Project overview and installation
- CHANGELOG: Version history and breaking changes

---

**For detailed architecture, domain models, and integration points, see `.github/copilot-instructions.md`**
