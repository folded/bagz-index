import collections
import dataclasses
import pathlib
from collections.abc import Sequence
from typing import Any, Self

import bagz
import blake3
from google.protobuf import message

from bagz_index import core
from bagz_index.protos import messages_pb2


@dataclasses.dataclass(frozen=True)
class HashBucketConfig(core.KeyConfig):
  avg_bucket_size: float

  @classmethod
  def _get_type(cls) -> str:
    return "hashbucket"

  @classmethod
  def from_json(cls, config_dict: dict[str, Any]) -> Self:
    return cls(
      avg_bucket_size=config_dict["avg_bucket_size"],
      key_proto_name=config_dict["key_proto_name"],
    )

  def make_writer(self) -> core.IndexWriter:
    return HashBucketWriter(self)

  def make_reader(self, reader: bagz.Reader) -> core.IndexReader:
    return HashBucketReader(self, reader)

  def make_merger(self) -> core.IndexMerger:
    return HashBucketMerger(self)

  def hash_key(self, key: bytes) -> int:
    h = blake3.blake3(key).digest()
    return int.from_bytes(h, "little")

  def supports_protocol(
    self,
    protocol: type[core.IndexReader | core.IndexWriter],
  ) -> bool:
    return protocol in (core.SupportsKeyAddition, core.SupportsKeyLookup)


class HashBucketMerger(core.IndexMerger):
  def __init__(self, config: HashBucketConfig) -> None:
    self._config = config

  def _merge_records(
    self,
    records: list[messages_pb2.HashRecord],
  ) -> messages_pb2.HashRecord:
    merged_record = messages_pb2.HashRecord()
    merged_record.key = records[0].key
    record_id_set = set()
    for record in records:
      record_id_set.update(record.record_ids)
    merged_record.record_ids.extend(sorted(record_id_set))
    return merged_record

  def _extract_key_record(
    self,
    key: bytes,
    bucket_data: bytes,
  ) -> messages_pb2.HashRecord:
    bucket = messages_pb2.HashBucket()
    bucket.ParseFromString(bucket_data)
    for record in bucket.records:
      if record.key == key:
        return record
    raise RuntimeError("Key not found in bucket")

  def _collect_keys(
    self,
    input_bagz_paths: list[str],
  ) -> tuple[set[bytes], dict[bytes, list[tuple[int, int]]]]:
    keys: set[bytes] = set()
    keys_to_records: dict[bytes, list[tuple[int, int]]] = collections.defaultdict(list)
    for path_num, path in enumerate(input_bagz_paths):
      reader = bagz.Reader(path)
      num_buckets = len(reader) - 1
      for bucket in range(num_buckets):
        bucket_data = reader[bucket]
        if bucket_data:
          hash_bucket = messages_pb2.HashBucket()
          hash_bucket.ParseFromString(bucket_data)
          for record in hash_bucket.records:
            keys.add(record.key)
            keys_to_records[record.key].append((path_num, bucket))
    return keys, keys_to_records

  def __call__(
    self, input_bagz_paths: list[str], output_bagz_path: str | pathlib.Path,
  ) -> None:
    keys, keys_to_records = self._collect_keys(input_bagz_paths)

    num_buckets = max(1, int(len(keys) / self._config.avg_bucket_size))

    bucket_to_keys = collections.defaultdict(list)
    for key in keys:
      bucket_to_keys[self._config.hash_key(key) % num_buckets].append(key)

    readers = [bagz.Reader(p) for p in input_bagz_paths]

    with bagz.Writer(output_bagz_path) as bag:
      for bucket_index in range(num_buckets):
        keys_for_this_bucket = bucket_to_keys.get(bucket_index)
        if not keys_for_this_bucket:
          bag.write(b"")
          continue

        out_hash_bucket = messages_pb2.HashBucket()

        for key in sorted(keys_for_this_bucket):
          record_data = [
            self._extract_key_record(key, readers[path_num][bucket])
            for path_num, bucket in keys_to_records[key]
          ]
          out_hash_bucket.records.add().CopyFrom(self._merge_records(record_data))

        bag.write(out_hash_bucket.SerializeToString())

      bag.write(self._config.to_json().encode("utf-8"))


class HashBucketWriter(core.IndexWriter):
  def __init__(self, config: HashBucketConfig) -> None:
    self._config = config
    self._data: dict[bytes, set[int]] = collections.defaultdict(set)

  @property
  def key_proto(self) -> type[message.Message]:
    return self._config.key_proto

  def add(self, key: message.Message, record_ids: Sequence[int]) -> None:
    self._data[key.SerializeToString()].update(record_ids)

  def write(self, bagz_path: str | pathlib.Path) -> None:
    num_buckets = max(1, int(len(self._data) / self._config.avg_bucket_size))

    bucket_to_keys = collections.defaultdict(list)
    for key in self._data:
      bucket_to_keys[self._config.hash_key(key) % num_buckets].append(key)

    with bagz.Writer(bagz_path) as bag:
      for bucket_index in range(num_buckets):
        if bucket_index not in bucket_to_keys:
          bag.write(b"")
        else:
          bucket = messages_pb2.HashBucket()
          for key in sorted(bucket_to_keys[bucket_index]):
            record_ids = self._data[key]
            record = messages_pb2.HashRecord(key=key, record_ids=sorted(record_ids))
            bucket.records.add().CopyFrom(record)
          bag.write(bucket.SerializeToString())
      bag.write(self._config.to_json().encode("utf-8"))


class HashBucketReader(core.IndexReader):
  def __init__(self, config: HashBucketConfig, bag: bagz.Reader) -> None:
    self._bag = bag
    self._config = config
    self._num_buckets = len(self._bag) - 1

  @property
  def key_proto(self) -> type[message.Message]:
    return self._config.key_proto

  def lookup(self, key: message.Message) -> tuple[int, ...] | None:
    key_bytes = key.SerializeToString()
    bucket_index = self._config.hash_key(key_bytes) % self._num_buckets

    bucket_data = self._bag[bucket_index]
    hash_bucket = messages_pb2.HashBucket()
    hash_bucket.ParseFromString(bucket_data)

    for record in hash_bucket.records:
      if record.key == key_bytes:
        return tuple(record.record_ids)
    return None
