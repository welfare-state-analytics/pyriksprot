from __future__ import annotations

import functools
import inspect
from dataclasses import dataclass, field, fields
from inspect import isclass
from typing import Any, Callable, Generic, Type, TypeVar

from ..utility import dget
from .config import Config

T = TypeVar("T", str, int, float)


@dataclass
class Configurable:
    def resolve(self):
        for attrib in fields(self):
            if isinstance(getattr(self, attrib.name), ConfigValue):
                setattr(self, attrib.name, getattr(self, attrib.name).resolve())

    # def __post_init__(self):
    #     self.resolve()


@dataclass
class ConfigValue(Generic[T]):
    key: str | Type[T]
    default: T | None = None
    description: str | None = None
    after: Callable[[T], T] | None = None
    mandatory: bool = False

    def resolve(self) -> T:
        if isinstance(self.key, Config):
            return ConfigStore.config
        if isclass(self.key):
            return self.key()
        if self.mandatory and not self.default:
            if not ConfigStore.config.exists(self.key):
                raise ValueError(f"ConfigValue {self.key} is mandatory but missing from config")

        value = ConfigStore.config.get(*self.key.split(","), default=self.default)
        if value and self.after:
            return self.after(value)
        return value

    @staticmethod
    def create_field(key: str, default: Any = None, description: str = None) -> Any:
        return field(  # pylint: disable=invalid-field-call
            default_factory=lambda: ConfigValue(key=key, default=default, description=description).resolve()
        )


class ConfigStore:
    store: dict[str, str | Config] = {"default": "./config.yml"}
    context: str = "default"

    @classmethod
    def configure_context(cls, context: str, source: str | dict | Config) -> Config:
        return cls._set_config(
            context=context,
            cfg=source,
        )

    @classmethod
    @property
    def config(cls) -> "Config":
        if isinstance(cls.store.get(cls.context), Config):
            return cls.store[cls.context]
        return cls.load(context=cls.context)

    @classmethod
    def resolve(cls, value: T | ConfigValue) -> T:
        if not isinstance(value, ConfigValue):
            return value
        return dget(cls.config, value.key)

    @classmethod
    def load(cls, *, context: str = "default", source: Config | str | dict = None) -> "Config":
        if isinstance(source, (dict, Config)):
            return cls._set_config(context=context, cfg=source)

        current: str | dict | Config | None = cls.store.get(context)

        if not current and not source:
            raise ValueError(f"Config context {context} not found")

        if isinstance(current, Config):
            return current

        if isinstance(current, dict):
            cls._set_config(context=context, cfg=current)

        return cls._set_config(
            context=context,
            cfg=Config.load(source=source or cls.store.get(context), context=context),
        )

    @classmethod
    def _set_config(cls, *, context: str = "default", cfg: Config | dict = None) -> "Config":
        cfg: Config = cfg if isinstance(cfg, Config) else Config.load(source=cfg, context=context)
        cfg.context = context
        cls.store[context] = cfg
        cls.context = context
        return cls.store[context]


configure_context = ConfigStore.configure_context


def resolve_arguments(fn_or_cls, args, kwargs):
    """Resolve any ConfigValue arguments in a function or class constructor"""
    kwargs = {
        k: v.default
        for k, v in inspect.signature(fn_or_cls).parameters.items()
        if isinstance(v.default, ConfigValue) and v.default is not inspect.Parameter.empty
    } | kwargs
    args = (a.resolve() if isinstance(a, ConfigValue) else a for a in args)
    for k, v in kwargs.items():
        if isinstance(v, ConfigValue):
            kwargs[k] = v.resolve()
    return args, kwargs


def inject_config(fn_or_cls: T) -> Callable[..., T]:
    @functools.wraps(fn_or_cls)
    def decorated(*args, **kwargs):
        args, kwargs = resolve_arguments(fn_or_cls, args, kwargs)
        return fn_or_cls(*args, **kwargs)

    return decorated


## Decorator template when argument is needed (returns a decorator)
# def outer_decorator(*outer_args,**outer_kwargs):
#     def decorator(fn):
#         def decorated(*args,**kwargs):
#             do_something(*outer_args,**outer_kwargs)
#             return fn(*args,**kwargs)
#         return decorated
#     return decorator


# def inject_config(cls: T) -> Callable[[T],T]:
#     @functools.wraps(cls)
#     def resolver(*args, **kwargs) -> T:
#         args = (a.resolve() if isinstance(a, ConfigValue) else a for a in args)
#         for k in kwargs.items():
#             if isinstance(kwargs[k], ConfigValue):
#                 kwargs[k] = kwargs[k].resolve()
#         return cls(*args, **kwargs)

#     return resolver
