import pathlib
import re
import string

import pytest

from bagz_index import core, trigram

_TEST_DOCS = (
  "hello world",  # 0
  "world of wonders",  # 1
  "hello there",  # 2
  "a whole new world",  # 3
  "ear sea archers",  # 4. has 'sea', 'ear', and 'rch' but not 'search'
  "search and rescue",  # 5
)


def trigram_config() -> trigram.TrigramConfig:
  return trigram.TrigramConfig(character_set=string.ascii_lowercase)


def normalized_trigram_config() -> trigram.TrigramConfig:
  return trigram.TrigramConfig(character_set=string.ascii_lowercase, normalize=True)


def positional_trigram_config() -> trigram.TrigramConfig:
  return trigram.TrigramConfig(
    character_set=string.ascii_lowercase,
    normalize=True,
    store_positions=True,
  )


def delta_trigram_config() -> trigram.TrigramConfig:
  return trigram.TrigramConfig(
    character_set=string.ascii_lowercase,
    normalize=True,
    delta_encode_record_ids=True,
  )


def _build_index(
  tmp_path: pathlib.Path, name: str, config: trigram.TrigramConfig,
) -> pathlib.Path:
  writer = core.make_writer(config, core.SupportsTextAddition)
  for i, doc in enumerate(_TEST_DOCS):
    writer.add_text(doc, i)

  bagz_path = tmp_path / f"trigram_index_{name}.bagz"
  writer.write(str(bagz_path))
  return bagz_path


def _test_searches(
  config: trigram.TrigramConfig, reader: core.SupportsTextSearch,
) -> None:
  results = reader.search("search")
  if config.store_positions:
    # This will not have false positives
    assert sorted(results) == [5]
  else:
    # This will have false positives
    assert sorted(results) == [4, 5]

  results = reader.search("world")
  assert sorted(results) == [0, 1, 3]

  results = reader.search("ld of w")
  if config.normalize:
    assert sorted(results) == [1]
  else:
    assert sorted(results) == []  # not found because ' ' is not in the charset.

  # Search for a non-existent term
  results = reader.search("xyzxyz")
  assert len(results) == 0


def test_normalize_text() -> None:
  charset = string.ascii_lowercase
  compiled_regex = re.compile(f"[^{re.escape(charset)}]+")
  assert trigram._normalize_text("Hello, World!", compiled_regex) == "hello world"
  assert (
    trigram._normalize_text("  leading and trailing...", compiled_regex)
    == "leading and trailing"
  )
  assert (
    trigram._normalize_text("multiple   spaces", compiled_regex) == "multiple spaces"
  )
  assert trigram._normalize_text("ALLCAPS", compiled_regex) == "allcaps"
  assert trigram._normalize_text("no-punctuation", compiled_regex) == "no punctuation"


def test_ngram_to_index(
) -> None:
  # Test standard config
  config = trigram_config()
  assert trigram.get_ngram_index("aaa", config) == 0
  assert trigram.get_ngram_index("aab", config) == 1
  assert trigram.get_ngram_index("aba", config) == 26
  assert trigram.get_ngram_index("zzz", config) == 26**3 - 1

  # Test normalized config (with implicit space)
  config = normalized_trigram_config()
  base = 27
  assert trigram.get_ngram_index("aaa", config) == 0
  assert trigram.get_ngram_index("aa ", config) == 26
  assert trigram.get_ngram_index("a a", config) == 26 * base


@pytest.mark.parametrize(
  ("name", "config"),
  [
    ("simple", trigram_config()),
    ("normalized", normalized_trigram_config()),
    ("positional", positional_trigram_config()),
    ("delta", delta_trigram_config()),
  ],
)
def test_trigram_search_simple(
  tmp_path: pathlib.Path, name: str, config: trigram.TrigramConfig,
) -> None:
  path = _build_index(tmp_path, name, config)
  reader = core.make_reader(str(path), core.SupportsTextSearch)
  _test_searches(config, reader)


@pytest.mark.parametrize(
  ("name", "config"),
  [
    ("simple", trigram_config()),
    ("normalized", normalized_trigram_config()),
    ("positional", positional_trigram_config()),
    ("delta", delta_trigram_config()),
  ],
)
def test_merge_with_config(
  tmp_path: pathlib.Path,
  name: str,
  config: trigram.TrigramConfig,
) -> None:
  # Create two indices
  writer1 = core.make_writer(config, core.SupportsTextAddition)
  index_docs = list(enumerate(_TEST_DOCS))
  for i, doc in index_docs[0::2]:
    writer1.add_text(doc, i)
  bagz_path1 = tmp_path / f"{name}_index1.bagz"
  writer1.write(str(bagz_path1))

  writer2 = core.make_writer(config, core.SupportsTextAddition)
  for i, doc in index_docs[1::2]:
    writer2.add_text(doc, i)
  bagz_path2 = tmp_path / f"{name}_index2.bagz"
  writer2.write(str(bagz_path2))

  # Merge them
  merged_path = tmp_path / f"{name}_merged.bagz"
  core.merge_indices([str(bagz_path1), str(bagz_path2)], str(merged_path))

  _test_searches(config, core.make_reader(str(merged_path), core.SupportsTextSearch))
