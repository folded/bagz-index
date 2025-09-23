import abc
import dataclasses
import json
import pathlib
from collections.abc import Sequence
from typing import Any, ClassVar, Protocol, Self, TypeVar

import bagz
from google.protobuf import message

from bagz_index import key_utils

T = TypeVar("T")


class IndexReader(Protocol):
  pass


class IndexWriter(Protocol):
  def write(self, bagz_path: str | pathlib.Path) -> None: ...


class IndexMerger(Protocol):
  def __call__(self, input_bagz_paths: list[str], output_bagz_path: str) -> None: ...


class SupportsKeyLookup(IndexReader, Protocol):
  """Interface for readers that support key-based lookups."""

  @property
  @abc.abstractmethod
  def key_proto(self) -> type[message.Message]:
    """Returns the protobuf message class used for keys."""
    ...

  @abc.abstractmethod
  def lookup(self, key: message.Message) -> tuple[int, ...] | None:
    """Looks up a record by its fully constructed protobuf key."""
    ...


class SupportsTextSearch(IndexReader, Protocol):
  """Interface for readers that support text search."""

  @property
  @abc.abstractmethod
  def requires_post_filtering(self) -> bool: ...

  @abc.abstractmethod
  def search(self, query: str) -> tuple[int, ...]: ...


class SupportsKeyAddition(IndexWriter, Protocol):
  """Interface for writers that support key-based lookups."""

  @property
  @abc.abstractmethod
  def key_proto(self) -> type[message.Message]:
    """Returns the protobuf message class used for keys."""
    ...

  @abc.abstractmethod
  def add(self, key: message.Message, record_ids: Sequence[int]) -> None:
    """Adds a record by its fully constructed protobuf key."""
    ...


class SupportsTextAddition(IndexWriter, Protocol):
  """Interface for writers that support text search."""

  @abc.abstractmethod
  def add_text(self, text: str, record_id: int) -> None: ...


@dataclasses.dataclass(frozen=True)
class Config(abc.ABC):
  _registry: ClassVar[dict[str, type["Config"]]] = {}

  def __init_subclass__(cls, **kwargs: dict[str, Any]) -> None:
    super().__init_subclass__(**kwargs)
    # Only register concrete subclasses. An abstract class will have a
    # non-empty __abstractmethods__ set.
    if not getattr(cls, "__abstractmethods__", set()):
      type_name = cls._get_type()
      if type_name in Config._registry:
        raise ValueError(f"Duplicate config type: {type_name}")
      Config._registry[type_name] = cls

  @classmethod
  def get_config_class(cls, config_type: str) -> type["Config"]:
    config_class = cls._registry.get(config_type)
    if config_class is None:
      raise ValueError(f"Unknown config type: {config_type}")
    return config_class

  def to_json(self) -> str:
    return json.dumps(dataclasses.asdict(self) | {"type": self._get_type()}, indent=4)

  @classmethod
  @abc.abstractmethod
  def _get_type(cls) -> str: ...

  @classmethod
  @abc.abstractmethod
  def from_json(cls, config_dict: dict[str, Any]) -> Self: ...

  @abc.abstractmethod
  def make_writer(self) -> IndexWriter: ...

  @abc.abstractmethod
  def make_reader(self, reader: bagz.Reader) -> IndexReader: ...

  @abc.abstractmethod
  def make_merger(self) -> IndexMerger: ...

  @abc.abstractmethod
  def supports_protocol(self, protocol: type[IndexReader | IndexWriter]) -> bool: ...


@dataclasses.dataclass(frozen=True)
class KeyConfig(Config):
  key_proto_name: str

  @property
  def key_proto(self) -> type[message.Message]:
    return key_utils.get_key_message_class(self.key_proto_name)


def config_from_json(json_string: str) -> "Config":
  data = json.loads(json_string)
  config_type = data.pop("type", None)
  if config_type is None:
    raise ValueError("Config JSON must contain a 'type' field.")

  config_class = Config.get_config_class(config_type)
  return config_class.from_json(data)


def make_writer(config: Config, expected_protocol: type[T]) -> T:
  if not config.supports_protocol(expected_protocol):
    raise TypeError(
      f"Index writer of type '{type(config).__name__}' "
      f"does not support the '{expected_protocol.__name__}' protocol.",
    )
  return config.make_writer()  # type: ignore[return-value]


def make_reader(bagz_path: str, expected_protocol: type[T]) -> T:
  reader = bagz.Reader(bagz_path)
  config = config_from_json(reader[len(reader) - 1].decode("utf-8"))
  if not config.supports_protocol(expected_protocol):
    raise TypeError(
      f"Index reader of type '{type(config).__name__}' "
      f"does not support the '{expected_protocol.__name__}' protocol.",
    )
  return config.make_reader(reader)  # type: ignore[return-value]


def _config_from_bagz(bagz_path: str) -> Config:
  reader = bagz.Reader(bagz_path)
  config_json = reader[len(reader) - 1].decode("utf-8")
  return config_from_json(config_json)


def merge_indices(input_bagz_paths: list[str], output_bagz_path: str) -> None:
  if not input_bagz_paths:
    raise ValueError("At least one input bagz path must be provided.")

  configs = [_config_from_bagz(input_bagz_path) for input_bagz_path in input_bagz_paths]

  if not all(config == configs[0] for config in configs[1:]):
    raise ValueError("All indices must have the same config.")

  config = configs[0]
  merger = config.make_merger()
  return merger(input_bagz_paths, output_bagz_path)
