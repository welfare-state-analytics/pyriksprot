import os
from os.path import join as jj
from os.path import normpath as nj
from pathlib import Path
from unittest import mock

import pytest

from pyriksprot import ITagger, ITaggerFactory, configuration
from pyriksprot.configuration import ConfigValue
from pyriksprot.utility import temporary_file
from pyriksprot.workflows import tag


def inject_config(fn_or_cls):
    def decorated(*args, **kwargs):
        args = [a.resolve() if isinstance(a, ConfigValue) else a for a in args]
        kwargs = {k: v.resolve() if isinstance(v, ConfigValue) else v for k, v in kwargs.items()}
        return fn_or_cls(*args, **kwargs)

    return decorated


# @inject_config
# @dataclass
# class Hej:
#     a: int = ConfigValue.create_field(key="apa", default=88)
#     b: int = ConfigValue.create_field(key="gorilla", default=99)

#     def __post_init__(self):
#         print("inside Hej.__post_init__")
#         print(f"   a: {self.a} ")
#         print(f"   b: {self.b} ")


# @inject_config
# class HEJ:
#     def __init__(self, *args, **kwargs):
#         print("inside HEJ.__init__")
#         print(f"   args: {' '.join(str(a) for a in args)} ")
#         print(f" kwargs: {' '.join(str(a)+'/'+type(a).__name__ for _, a in kwargs.items())} ")


# @inject_config
# def hej(*args, **kwargs):
#     print("inside hej")
#     print(f"   args: {' '.join(str(a) for a in args)} ")
#     print(f" kwargs: {' '.join(str(a)+'/'+type(a).__name__ for _, a in kwargs.items())} ")


# def test_decorator():
#     configure_context("test", {"apa": 99, "gorilla": 77})

#     HEJ(apa=ConfigValue(key='apa', default=1), gorilla=2)

#     Hej()

#     hej(1, 2)


# def inject(cls):

#     @functools.wraps(cls)
#     def resolver(*args, **kwargs):
#         args = (a.resolve() if isinstance(a, ConfigValue) else a for a in args)
#         for k in kwargs.items():
#             if isinstance(kwargs[k], ConfigValue):
#                 kwargs[k] = kwargs[k].resolve()
#         for arg in args:
#             if isinstance(arg, ConfigValue):
#                 arg.resolve()
#         return cls(*args, **kwargs)

#     return resolver


# @inject
# class foox:

#     def __init__(
#             self,
#         target: TargetConfig = ConfigValue(key=None,default=TargetConfig)
#     ):

#         self.target = target

# def test_wireup():
#     configure_context("test", "tests/test_data/config.yml")
#     x = foox()


TEST_DATA_FOLDER = "tests/output/data"
TEST_TAG = "v0.9.9"

SIMPLE_YAML_STR1: str = f"""
root_folder: {TEST_DATA_FOLDER}
source:
    folder: /data/riksdagen-corpus/corpus/protocols
    repository_tag: {TEST_TAG}
    repository_folder: /data/riksdagen-corpus
    repository_url: https://github.com/welfare-state-analytics/riksdagen-corpus.git
target_folder: {TEST_DATA_FOLDER}/tagged_frames
export_folder: /data/exports
export_template: /data/templates/speeches.cdata.xml
export_extension: xml
"""

SIMPLE_YAML_STR2: str = f"""
root_folder: {TEST_DATA_FOLDER}
source:
    folder: /data/riksdagen-corpus/corpus/protocols
    repository_folder: /data/riksdagen-corpus
    repository_tag: {TEST_TAG}
    repository_url: https://github.com/welfare-state-analytics/riksdagen-corpus.git
target_folder: {TEST_DATA_FOLDER}/tagged_frames
export:
  folder: /data/exports
  template: /data/templates/speeches.cdata.xml
  extension: xml
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


@pytest.mark.parametrize("yaml_str", [SIMPLE_YAML_STR1, SIMPLE_YAML_STR2])
def test_load_yaml_str(yaml_str: str):
    data_folder: str = TEST_DATA_FOLDER

    config: configuration.Config = configuration.Config.load(source=yaml_str)
    assert isinstance(config, configuration.Config)

    assert config.data_folder == nj(data_folder)
    assert config.log_folder == f"{data_folder}/logs"
    assert config.log_filename.startswith(config.log_folder)

    assert config.source.repository_url == "https://github.com/welfare-state-analytics/riksdagen-corpus.git"
    assert config.source.repository_folder == "/data/riksdagen-corpus"
    assert config.source.repository_tag == TEST_TAG

    expected_source_folder: str = "/data/riksdagen-corpus/corpus/protocols"
    assert (
        config.get("source:folder") == expected_source_folder
        if os.path.isdir(expected_source_folder)
        else config.get("source:repository_folder")
    )

    assert config.get("export:folder") == "/data/exports"
    assert config.get("export:template") == "/data/templates/speeches.cdata.xml"
    assert config.get("export:extension") == "xml"

    assert config.dehyphen.folder == nj(data_folder)
    assert config.dehyphen.tf_filename == jj(data_folder, "word-frequencies.pkl")


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
    config: configuration.Config = configuration.Config.load(source=yaml_str)
    configuration.configure_context(context="default", source=config)

    assert config.get("tagger:module") == "pyriksprot_tagger.taggers.stanza_tagger"
    assert config.get("tagger:num_threads") == 1
    assert config.get("tagger:use_gpu") is False
    assert config.get("dehyphen:folder") == "tests/output"

    with mock.patch("importlib.import_module", return_value=mock.MagicMock()):
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
