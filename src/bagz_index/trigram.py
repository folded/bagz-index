import abc
import dataclasses
import functools
import itertools
import pathlib
import re
from collections.abc import Iterator, Sequence
from typing import Any, Self

import bagz

from bagz_index import core
from bagz_index.protos import messages_pb2


def _delta_encode(posting_list: messages_pb2.PostingList) -> None:
  if not posting_list.record_ids:
    return
  last = posting_list.record_ids[0]
  for i in range(1, len(posting_list.record_ids)):
    posting_list.record_ids[i], last = (
      posting_list.record_ids[i] - last,
      posting_list.record_ids[i],
    )


def _delta_decode(posting_list: messages_pb2.PostingList) -> None:
  if not posting_list.record_ids:
    return
  for i in range(1, len(posting_list.record_ids)):
    posting_list.record_ids[i] += posting_list.record_ids[i - 1]


@dataclasses.dataclass(frozen=True)
class TrigramConfig(core.Config):
  character_set: str
  ngram_size: int = 3
  normalize: bool = False
  store_positions: bool = False
  delta_encode_record_ids: bool = False

  @property
  def effective_character_set(self) -> str:
    if self.normalize:
      return self.character_set + " "
    return self.character_set

  @functools.cached_property
  def compiled_normalize_regex(self) -> re.Pattern:
    return re.compile(f"[^{re.escape(self.character_set)}]+")

  @functools.cached_property
  def char_to_int_map(self) -> dict[str, int]:
    return {c: i for i, c in enumerate(self.effective_character_set)}

  @classmethod
  def _get_type(cls) -> str:
    return "trigram"

  @classmethod
  def from_json(cls, config_dict: dict[str, Any]) -> Self:
    return cls(
      character_set="".join(sorted(set(config_dict["character_set"]))),
      ngram_size=config_dict.get("ngram_size", 3),
      normalize=config_dict.get("normalize", False),
      store_positions=config_dict.get("store_positions", False),
      delta_encode_record_ids=config_dict.get("delta_encode_record_ids", False),
    )

  def make_writer(self) -> core.IndexWriter:
    return TrigramWriter(self)

  def make_reader(self, reader: bagz.Reader) -> core.IndexReader:
    return TrigramReader(self, reader)

  def make_merger(self) -> core.IndexMerger:
    return TrigramMerger(self)

  def supports_protocol(
    self,
    protocol: type[core.IndexReader | core.IndexWriter],
  ) -> bool:
    return protocol in (core.SupportsTextAddition, core.SupportsTextSearch)


class TrigramMerger(core.IndexMerger):
  def __init__(self, config: TrigramConfig) -> None:
    self._config = config

  def _merge_with_positions(
    self,
    postings_to_merge: list[messages_pb2.PostingList],
  ) -> messages_pb2.PostingList:
    all_tuples = []
    for pl in postings_to_merge:
      all_tuples.extend(zip(pl.record_ids, pl.record_offsets, strict=True))

    all_tuples.sort()

    # Deduplicate using itertools.groupby
    unique_tuples = [k for k, _ in itertools.groupby(all_tuples)]

    record_ids = [t[0] for t in unique_tuples]
    merged_pl = messages_pb2.PostingList(
      record_ids=record_ids,
      record_offsets=[t[1] for t in unique_tuples],
    )
    if self._config.delta_encode_record_ids:
      _delta_encode(merged_pl)
    return merged_pl

  def _merge_without_positions(
    self,
    postings_to_merge: list[messages_pb2.PostingList],
  ) -> messages_pb2.PostingList:
    all_record_ids = set()
    for pl in postings_to_merge:
      all_record_ids.update(pl.record_ids)

    record_ids = sorted(all_record_ids)
    merged_pl = messages_pb2.PostingList(
      record_ids=record_ids,
    )
    if self._config.delta_encode_record_ids:
      _delta_encode(merged_pl)
    return merged_pl

  def _merge_trigram(
    self,
    i: int,
    readers: list[bagz.Reader],
    writer: bagz.Writer,
  ) -> None:
    postings_to_merge = []
    for r in readers:
      data = r[i]
      if data:
        pl = messages_pb2.PostingList()
        pl.ParseFromString(data)
        postings_to_merge.append(pl)

    if not postings_to_merge:
      writer.write(b"")
      return

    if len(postings_to_merge) == 1:
      writer.write(postings_to_merge[0].SerializeToString())
      return

    if self._config.delta_encode_record_ids:
      for pl in postings_to_merge:
        _delta_decode(pl)

    if self._config.store_positions:
      merged_pl = self._merge_with_positions(postings_to_merge)
    else:
      merged_pl = self._merge_without_positions(postings_to_merge)

    writer.write(merged_pl.SerializeToString())

  def __call__(
    self,
    input_bagz_paths: list[str],
    output_bagz_path: str | pathlib.Path,
  ) -> None:
    readers = [bagz.Reader(p) for p in input_bagz_paths]
    with bagz.Writer(output_bagz_path) as writer:
      if not readers:
        writer.write(self._config.to_json().encode("utf-8"))
        return

      num_postings = len(readers[0]) - 1
      for i in range(num_postings):
        self._merge_trigram(i, readers, writer)

      writer.write(self._config.to_json().encode("utf-8"))


def _normalize_text(text: str, compiled_regex: re.Pattern) -> str:
  return compiled_regex.sub(" ", text.lower()).strip()


def _ngram_to_index(ngram: str, char_map: dict[str, int]) -> int:
  base = len(char_map)
  index = 0
  for char in ngram:
    if char not in char_map:
      return -1
    index = index * base + char_map[char]
  return index


def get_ngram_index(ngram: str, config: TrigramConfig) -> int:
  return _ngram_to_index(ngram, config.char_to_int_map)


class TrigramWriter(core.IndexWriter):
  def __init__(self, config: TrigramConfig) -> None:
    self._config = config
    if config.store_positions:
      self._impl: _WriterImpl = _PositionalImpl(config)
    else:
      self._impl = _SimpleImpl(config)

  def add_text(self, text: str, record_id: int) -> None:
    if self._config.normalize:
      text = _normalize_text(text, self._config.compiled_normalize_regex)
    self._impl.add_text(text, record_id)

  def write(self, bagz_path: str) -> None:
    self._impl.write(bagz_path)


class _WriterImpl(abc.ABC):
  def __init__(self, config: TrigramConfig) -> None:
    self._config = config

  @abc.abstractmethod
  def add_text(self, text: str, record_id: int) -> None: ...

  @abc.abstractmethod
  def write(self, bagz_path: str) -> None: ...


class _SimpleImpl(_WriterImpl):
  def __init__(self, config: TrigramConfig) -> None:
    super().__init__(config)
    num_postings = len(config.effective_character_set) ** config.ngram_size
    self._postings: list[set[int]] = [set() for _ in range(num_postings)]

  def add_text(self, text: str, record_id: int) -> None:
    for i in range(len(text) - self._config.ngram_size + 1):
      ngram = text[i : i + self._config.ngram_size]
      index = _ngram_to_index(ngram, self._config.char_to_int_map)
      if index >= 0:
        self._postings[index].add(record_id)

  def write(self, bagz_path: str) -> None:
    with bagz.Writer(bagz_path) as bag:
      for posting_set in self._postings:
        record_ids = sorted(posting_set)
        message = messages_pb2.PostingList(record_ids=record_ids)
        if self._config.delta_encode_record_ids:
          _delta_encode(message)
        bag.write(message.SerializeToString())
      bag.write(self._config.to_json().encode("utf-8"))


class _PositionalImpl(_WriterImpl):
  def __init__(self, config: TrigramConfig) -> None:
    super().__init__(config)
    num_postings = len(config.effective_character_set) ** config.ngram_size
    self._postings: list[tuple[list[int], list[int]]] = [
      ([], []) for _ in range(num_postings)
    ]

  def add_text(self, text: str, record_id: int) -> None:
    for i in range(len(text) - self._config.ngram_size + 1):
      ngram = text[i : i + self._config.ngram_size]
      index = _ngram_to_index(ngram, self._config.char_to_int_map)
      if index != -1:
        self._postings[index][0].append(record_id)
        self._postings[index][1].append(i)

  def write(self, bagz_path: str) -> None:
    with bagz.Writer(bagz_path) as bag:
      for rids, offsets in self._postings:
        posting_tuples = sorted(zip(rids, offsets, strict=True))
        record_ids = [item[0] for item in posting_tuples]
        record_offsets = [item[1] for item in posting_tuples]
        message = messages_pb2.PostingList(
          record_ids=record_ids,
          record_offsets=record_offsets,
        )
        if self._config.delta_encode_record_ids:
          _delta_encode(message)
        bag.write(message.SerializeToString())
      bag.write(self._config.to_json().encode("utf-8"))


class _Matcher(abc.ABC):
  @abc.abstractmethod
  def add(self, i: int, posting_list: messages_pb2.PostingList) -> None: ...

  @property
  @abc.abstractmethod
  def no_remaining_matches(self) -> bool: ...

  @property
  @abc.abstractmethod
  def record_ids(self) -> tuple[int, ...]: ...


class _SimpleNGramMatcher(_Matcher):
  def __init__(self) -> None:
    self._matches: set[int] | None = None

  def add(self, i: int, posting_list: messages_pb2.PostingList) -> None:
    del i
    if self._matches is None:
      self._matches = set(posting_list.record_ids)
    else:
      self._matches.intersection_update(posting_list.record_ids)

  @property
  def no_remaining_matches(self) -> bool:
    return self._matches is not None and not self._matches

  @property
  def record_ids(self) -> tuple[int, ...]:
    return tuple(sorted(self._matches)) if self._matches is not None else ()


class _PositionNGramMatcher(_Matcher):
  def __init__(self) -> None:
    self._first_position = True
    self._matches: set[tuple[int, int]] = set()  # (record_id, position) pairs

  @property
  def no_remaining_matches(self) -> bool:
    return not (self._first_position or self._matches)

  def add(self, i: int, posting_list: messages_pb2.PostingList) -> None:
    def start_positions() -> Iterator[tuple[int, int]]:
      for rid, pos in zip(
        posting_list.record_ids,
        posting_list.record_offsets,
        strict=True,
      ):
        yield (rid, pos - i)

    if self._first_position:
      self._matches = set(start_positions())
      self._first_position = False
    else:
      self._matches.intersection_update(start_positions())

  @property
  def matches(self) -> tuple[tuple[int, int], ...]:
    return tuple(sorted(self._matches))

  @property
  def record_ids(self) -> tuple[int, ...]:
    return tuple(sorted({m[0] for m in self._matches}))


class TrigramReader(core.IndexReader):
  def __init__(self, config: TrigramConfig, bag: bagz.Reader) -> None:
    self._config = config
    self._bag = bag

  def _get_record_ids(self, posting_list: messages_pb2.PostingList) -> Sequence[int]:
    if self._config.delta_encode_record_ids:
      _delta_decode(posting_list)
    return posting_list.record_ids

  @property
  def requires_post_filtering(self) -> bool:
    return not self._config.store_positions

  def search(self, query: str) -> tuple[int, ...]:
    if self._config.normalize:
      query = _normalize_text(query, self._config.compiled_normalize_regex)

    if len(query) < self._config.ngram_size:
      return ()

    if self._config.store_positions:
      matches: _Matcher = _PositionNGramMatcher()
    else:
      matches = _SimpleNGramMatcher()

    for i in range(len(query) - self._config.ngram_size + 1):
      ngram = query[i : i + self._config.ngram_size]
      index = _ngram_to_index(ngram, self._config.char_to_int_map)
      if index != -1:
        message_bytes = self._bag[index]
        posting_list = messages_pb2.PostingList()
        posting_list.ParseFromString(message_bytes)
        posting_list.record_ids[:] = self._get_record_ids(posting_list)
        matches.add(i, posting_list)
        if matches.no_remaining_matches:
          return ()

    return matches.record_ids
