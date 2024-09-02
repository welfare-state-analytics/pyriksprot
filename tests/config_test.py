import os
from os.path import join as jj
from pathlib import Path
from unittest import mock

from pyriksprot import ITagger, ITaggerFactory
from pyriksprot.configuration import Config, ConfigValue
from pyriksprot.utility import temporary_file
from pyriksprot.workflows import tag


def inject_config(fn_or_cls):
    def decorated(*args, **kwargs):
        args = [a.resolve() if isinstance(a, ConfigValue) else a for a in args]
        kwargs = {k: v.resolve() if isinstance(v, ConfigValue) else v for k, v in kwargs.items()}
        return fn_or_cls(*args, **kwargs)

    return decorated


SIMPLE_YAML_STR: str = """
section0: value0
section1:
    attr1: value1
    attr2: value2
    attr3: value3
"""


def test_temporary_file():
    filename = jj("tests", "output", "trazan.txt")

    with temporary_file(filename=filename) as path:
        path.touch()
        assert path.is_file(), "file doesn't exists"
    assert not Path(filename).is_file(), "file exists"

    with temporary_file(filename=filename, content="X") as path:
        assert path.is_file(), "file doesn't exists"
        with open(filename, "r", encoding="utf-8") as fp:
            assert fp.read() == "X"
    assert not Path(filename).is_file(), "file exists"

    with temporary_file(filename=None, content="X") as path:
        filename = str(path)
        assert path.is_file(), "file doesn't exists"
        with open(filename, "r", encoding="utf-8") as fp:
            assert fp.read() == "X"
    assert not Path(filename).is_file(), "file exists"


def test_load_yaml_str():
    config: Config = Config.load(source=SIMPLE_YAML_STR)
    assert isinstance(config, Config)

    assert config.get("section0") == "value0"
    assert config.get("section1:attr1") == "value1"
    assert config.get("section1:attr2") == "value2"
    assert config.get("section1:attr3") == "value3"


def test_create_tagger_factory():
    yaml_str: str = """
root_folder: tests/output
target_folder: tests/output/tagged_frames
tagger:
  module: pyriksprot_tagger.taggers.stanza_tagger
  stanza_datadir: null
  preprocessors: "dedent,dehyphen,strip,pretokenize"
  lang: "sv"
  processors: "tokenize,lemma,pos"
  tokenize_pretokenized: true
  tokenize_no_ssplit: true
  use_gpu: false
  num_threads: 1
dehyphen:
  folder: tests/output
  tf_filename: tests/test_data/word-frequencies.pkl
"""
    config: Config = Config.load(source=yaml_str)

    assert config.get("tagger:module") == "pyriksprot_tagger.taggers.stanza_tagger"
    assert config.get("tagger:num_threads") == 1
    assert config.get("tagger:use_gpu") is False
    assert config.get("dehyphen:folder") == "tests/output"

    with mock.patch("importlib.import_module", return_value=mock.MagicMock()):
        # TODO Patch config store
        # ConfigStore.configure_context(source=config)

        factory: ITaggerFactory = tag.TaggerProvider.tagger_factory(
            module_name=config.get("tagger:module", default=None)
        )

    assert factory is not None

    tagger: ITagger = factory.create()

    assert tagger is not None


def test_args():
    def fox(*args, default=None):
        return print(args, default)

    fox(1, 2, 3, default=4)
    fox(1, 2, 3)


def test_configure_by_string_and_env():
    config: Config = Config.load(source=SIMPLE_YAML_STR, env_prefix=None)

    assert isinstance(config, Config)

    assert config.get("section0") == "value0"
    assert config.get("section1:attr1") == "value1"
    assert config.get("section1:attr2") == "value2"
    assert config.get("section1:attr3") == "value3"

    mock_env: dict[str, str] = {"APA_ABC_XYZ": "apa"}
    with mock.patch.dict(os.environ, mock_env):
        config: Config = Config.load(source=SIMPLE_YAML_STR, env_prefix="APA")
        assert config.get("abc:xyz") == "apa"


def test_configure_by_string_and_update():
    yml_str: str = """

root_folder: &folder /data/riksdagen_corpus_data
data_folder: *folder
version: &version v0.x.y

corpus:
  version: *version
  folder: !path_join [ *folder, riksdagen-records ]
  github:
    user: swerik-project
    repository: riksdagen-records
    path: data

metadata:
  version: *version
  folder: !path_join [ *folder, riksdagen-persons ]
  github:
    user: swerik-project
    repository: riksdagen-persons
    path: data

"""

    config: Config = Config.load(source=yml_str, env_prefix=None)

    assert isinstance(config, Config)

    assert config.get("version") == "v0.x.y"
    assert config.get("root_folder") == config.get("data_folder")
    assert config.get("corpus:version") == "v0.x.y"
    assert config.get("corpus:folder") == "/data/riksdagen_corpus_data/riksdagen-records"
    assert config.get("metadata:version") == "v0.x.y"
    assert config.get("metadata:folder") == "/data/riksdagen_corpus_data/riksdagen-persons"
    assert config.get("metadata:github:user") == "swerik-project"
