import collections
import dataclasses
import functools
import pathlib
from collections.abc import Iterator
from typing import Any

import bagz
import pytest

from bagz_index import core, hashtable


@dataclasses.dataclass
class KeyTypeExpectations:
  key_proto_name: str
  _key_values: list[Any]
  _lookup_keys: list[Any]

  @property
  def key_values(self) -> Iterator[tuple[list[int], dict[str, Any]]]:
    record_ids = [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10], [11]]
    return zip(
      record_ids,
      ({"value": x} for x in self._key_values),
      strict=True,
    )

  @functools.cached_property
  def index_content(self) -> dict[str, Any]:
    key_to_record_ids = collections.defaultdict(set)
    for record_ids, key in self.key_values:
      key_to_record_ids[key["value"]].update(record_ids)
    return {k: tuple(sorted(v)) for k, v in key_to_record_ids.items()}

  @property
  def lookup_expectations(self) -> Iterator[tuple[Any, tuple[int, ...] | None]]:
    for key in self._lookup_keys:
      yield key, self.index_content.get(key)


KEY_TYPE_EXPECTATIONS = [
  KeyTypeExpectations(
    key_proto_name="bagz_index.keys.StringKey",
    _key_values=["hello", "world", "hello", "foo", "bar"],
    _lookup_keys=["hello", "world", "foo", "bar", "nonexistent"],
  ),
  KeyTypeExpectations(
    key_proto_name="bagz_index.keys.Int64Key",
    _key_values=[1, 2, 1, 3, 4],
    _lookup_keys=[1, 2, 3, 4, 5],
  ),
  KeyTypeExpectations(
    key_proto_name="bagz_index.keys.TupleStringKey",
    _key_values=[
      ("a", "b"),
      ("c", "d"),
      ("a", "b"),
      ("e", "f"),
      ("g", "h"),
    ],
    _lookup_keys=[
      ("a", "b"),
      ("c", "d"),
      ("e", "f"),
      ("g", "h"),
      ("x", "y"),
    ],
  ),
]


@pytest.fixture(params=KEY_TYPE_EXPECTATIONS)
def key_type_expectations(request: pytest.FixtureRequest) -> KeyTypeExpectations:
  return request.param


@pytest.fixture(params=[hashtable.HashBucketConfig])
def key_config_class(request: pytest.FixtureRequest) -> type[core.KeyConfig]:
  return request.param


@pytest.fixture
def key_lookup_config(
  key_config_class: type[core.KeyConfig],
  key_type_expectations: KeyTypeExpectations,
) -> core.KeyConfig:
  if key_config_class is hashtable.HashBucketConfig:
    return hashtable.HashBucketConfig(
      avg_bucket_size=0.9,
      key_proto_name=key_type_expectations.key_proto_name,
    )
  raise NotImplementedError(f"Unsupported config class: {key_config_class}")


@pytest.fixture
def key_values(
  key_type_expectations: KeyTypeExpectations,
) -> Iterator[tuple[list[int], dict[str, Any]]]:
  return key_type_expectations.key_values


@pytest.fixture
def built_bagz_file(
  tmp_path: pathlib.Path,
  key_lookup_config: core.KeyConfig,
  key_values: Iterator[tuple[list[int], dict[str, Any]]],
) -> pathlib.Path:
  writer = core.make_writer(key_lookup_config).as_key_writer
  key_proto_class = writer.key_proto
  for record_ids, key_value in key_values:
    key = key_proto_class(**key_value)
    writer.add(key, record_ids)

  bagz_path = tmp_path / "test.bagz"
  writer.write(bagz_path)
  return bagz_path


def test_build(
  built_bagz_file: pathlib.Path,
  key_lookup_config: core.KeyConfig,
) -> None:
  bag = bagz.Reader(built_bagz_file)
  assert len(bag) == 5

  config_from_bag = core.config_from_json(bag[len(bag) - 1].decode("utf-8"))
  assert config_from_bag == key_lookup_config


def test_json_serialization(key_lookup_config: core.KeyConfig) -> None:
  json_string = key_lookup_config.to_json()
  new_config = core.config_from_json(json_string)
  assert key_lookup_config == new_config


def test_index_reader_lookup(
  built_bagz_file: pathlib.Path,
  key_type_expectations: KeyTypeExpectations,
) -> None:
  reader = core.make_reader(built_bagz_file).as_key_lookup
  key_proto_class = reader.key_proto
  for key, expected_record_ids in key_type_expectations.lookup_expectations:
    key_message = key_proto_class(value=key)
    assert reader.lookup(key_message) == expected_record_ids


def test_hashtable_merge(
  tmp_path: pathlib.Path,
  key_lookup_config: core.KeyConfig,
  key_type_expectations: KeyTypeExpectations,
) -> None:
  # Create two indices from key_values
  all_key_values = list(key_type_expectations.key_values)

  # Index 1
  writer1 = core.make_writer(key_lookup_config).as_key_writer
  key_proto_class1 = writer1.key_proto
  key_values1 = all_key_values[: len(all_key_values) // 2]
  for record_ids, key_value in key_values1:
    key = key_proto_class1(**key_value)
    writer1.add(key, record_ids)
  bagz_path1 = tmp_path / "test1.bagz"
  writer1.write(bagz_path1)

  # Index 2
  writer2 = core.make_writer(key_lookup_config).as_key_writer
  key_proto_class2 = writer2.key_proto
  key_values2 = all_key_values[len(all_key_values) // 2 :]
  for record_ids, key_value in key_values2:
    key = key_proto_class2(**key_value)
    writer2.add(key, record_ids)
  bagz_path2 = tmp_path / "test2.bagz"
  writer2.write(bagz_path2)

  # Merge them
  merged_path = tmp_path / "merged.bagz"
  core.merge_indices([bagz_path1, bagz_path2], merged_path)

  # Test the merged index
  reader = core.make_reader(merged_path).as_key_lookup
  key_proto_class = reader.key_proto
  for key, expected_record_ids in key_type_expectations.lookup_expectations:
    key_message = key_proto_class(value=key)
    assert reader.lookup(key_message) == expected_record_ids
