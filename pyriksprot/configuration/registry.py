# import functools
# from typing import Any


# class ConfigSectionRegistry:
#     items: dict = {}

#     @classmethod
#     def get(cls, key: str):
#         if key not in cls.items:
#             raise ValueError(f"config element {key} is not registered")
#         return ConfigSectionRegistry.items.get(key)

#     @classmethod
#     def register(cls, target: Any = None, key: str = None, **_):
#         ConfigSectionRegistry.items[key or target.__name__] = target

#         @functools.wraps(target)
#         def decorator(*args, **kwargs):
#             return target(*args, **kwargs)

#         return decorator

#     @classmethod
#     def is_registered(cls, key: str):
#         return key in ConfigSectionRegistry.items


# @ConfigSectionRegistry.register
# @dataclass
# class SourceConfig:
#     repository_folder: str
#     repository_tag: str
#     extension: str = field(default="xml")
#     repository_url: str = field(default="https://github.com/welfare-state-analytics/riksdagen-corpus.git")

#     def __post_init__(self):
#         if not self.repository_tag:
#             raise ValueError("Corpus tag cannot be empty")

#         self.repository_url: str = field(default="https://github.com/welfare-state-analytics/riksdagen-corpus.git")
#         self.repository_folder: str = nj(self.repository_folder)

#     @property
#     def folder(self) -> str:
#         if isdir(nj(self.repository_folder, "corpus/protocols")):
#             return nj(self.repository_folder, "corpus/protocols")
#         return self.repository_folder

#     @property
#     def parent_folder(self) -> str:
#         return abspath(nj(self.repository_folder, '..'))


# @inject_config
# @ConfigSectionRegistry.register
# @dataclass
# class TargetConfig:
#     folder: str | ConfigValue = ConfigValue.create_field(key="target:folder")
#     extension: str | ConfigValue = ConfigValue.create_field(key="target:extension", default="zip")


# @inject_config
# @ConfigSectionRegistry.register
# @dataclass
# class DehyphenConfig(Configurable):
#     folder: str | ConfigValue = ConfigValue.create_field(key="dehyphen:folder")
#     tf_filename: str | ConfigValue = ConfigValue.create_field(key="dehyphen:tf_filename", default="zip")


# @inject_config
# @ConfigSectionRegistry.register
# @dataclass
# class ExtractConfig:
#     folder: str | ConfigValue = ConfigValue.create_field(key="extract:folder")
#     template: str | ConfigValue = ConfigValue.create_field(key="extract:template", default="")
#     extension: str | ConfigValue = ConfigValue.create_field(key="extract:extension", default="xml")
