import os
import pathlib
import sys
import tempfile
from collections.abc import Generator
from typing import ClassVar, Protocol

import google.protobuf.descriptor
import google.protobuf.message
import pytest

# Add the src directory to the path to allow importing modules
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

from bagz_index.generate_logic import (
  _compile_and_load_proto,
  _get_field_value,
  _matches_pattern,
  _parse_field_set,
  _yield_field_paths,
  expand_field_pattern,
  lookup_field_values,
  parse_pattern,
)

# Define a dummy proto content for testing
TEST_PROTO_CONTENT = """
syntax = "proto3";

message TestMessage {
  message SubMessage {
    string sub_id = 1;
    string sub_name = 2;
    int32 sub_value = 3;
  }
  string id = 1;
  string name = 2;
  int32 value = 3;
  SubMessage sub = 4;
  repeated string tags = 5;
  repeated SubMessage nested_subs = 6;
}

"""


class ISubMessage(Protocol):
  sub_id: str
  sub_name: str
  sub_value: int


class ITestMessage(Protocol):
  Submessage: ClassVar[type[ISubMessage]]
  id: str
  name: str
  value: int
  sub: ISubMessage
  tags: list[str]
  nested_subs: list[ISubMessage]


@pytest.fixture(scope="module")
def test_message_class() -> Generator[type[ITestMessage], None, None]:
  with tempfile.TemporaryDirectory() as temp_dir:
    proto_file_path = pathlib.Path(temp_dir) / "test.proto"
    proto_file_path.write_text(TEST_PROTO_CONTENT)

    # Use a unique module name to avoid conflicts across test runs
    module_name = f"test_module_{os.getpid()}_{id(test_message_class)}"

    # Compile and load the proto once for the module
    module = _compile_and_load_proto(
      TEST_PROTO_CONTENT,
      module_name,
      pathlib.Path(temp_dir),
      proto_include_paths=[pathlib.Path(temp_dir)],
    )
    yield module.TestMessage


@pytest.fixture
def sample_message(test_message_class: type[ITestMessage]) -> ITestMessage:
  return test_message_class(
    id="test_id",
    name="test_name",
    value=123,
    sub=test_message_class.SubMessage(sub_id="s1", sub_name="s_name", sub_value=456),
    tags=["tag1", "tag2"],
    nested_subs=[
      test_message_class.SubMessage(sub_id="ns1", sub_name="ns_name1"),
      test_message_class.SubMessage(sub_id="ns2", sub_name="ns_name2"),
    ],
  )


def test_compile_proto_and_import_record_type(
  test_message_class: type[ITestMessage],
) -> None:
  assert test_message_class is not None
  assert issubclass(test_message_class, google.protobuf.message.Message)
  assert test_message_class.__name__ == "TestMessage"
  assert hasattr(test_message_class, "SubMessage")
  assert issubclass(test_message_class.SubMessage, google.protobuf.message.Message)


def test_parse_field_set() -> None:
  assert _parse_field_set("field1") == ["field1"]
  assert _parse_field_set("{field1,field2}") == ["field1", "field2"]
  assert _parse_field_set("{ field1 , field2 }") == ["field1", "field2"]
  assert _parse_field_set("{}") == [""]


def test_yield_field_paths(test_message_class: type[ITestMessage]) -> None:
  paths = set()
  for path, _ in _yield_field_paths(test_message_class.DESCRIPTOR):
    paths.add(path)

  expected_paths = {
    ("id",),
    ("name",),
    ("value",),
    ("sub",),
    ("tags",),
    ("nested_subs",),
    ("sub", "sub_id"),
    ("sub", "sub_name"),
    ("sub", "sub_value"),
    ("nested_subs", "sub_id"),
    ("nested_subs", "sub_name"),
    ("nested_subs", "sub_value"),
  }
  assert paths == expected_paths


def test_matches_pattern_exact() -> None:
  assert _matches_pattern(("id",), parse_pattern("id"))
  assert _matches_pattern(("sub", "sub_id"), parse_pattern("sub.sub_id"))
  assert not _matches_pattern(("id",), parse_pattern("name"))
  assert not _matches_pattern(("sub", "sub_id"), parse_pattern("sub.sub_name"))


def test_matches_pattern_wildcard_star() -> None:
  assert _matches_pattern(("id",), parse_pattern("*"))
  assert _matches_pattern(("sub", "sub_id"), parse_pattern("sub.*"))
  assert _matches_pattern(("sub", "sub_id"), parse_pattern("*.sub_id"))


def test_matches_pattern_wildcard_double_star() -> None:
  assert _matches_pattern(("id",), parse_pattern("**"))
  assert _matches_pattern(("sub", "sub_id"), parse_pattern("**.sub_id"))
  assert _matches_pattern(("sub", "sub_id"), parse_pattern("sub.**"))
  assert _matches_pattern(("sub", "sub_id"), parse_pattern("**.**.sub_id"))
  assert _matches_pattern(("sub", "sub_id"), parse_pattern("**.*.sub_id"))
  assert not _matches_pattern(("sub", "sub_id"), parse_pattern("**.id"))


def test_matches_pattern_field_set() -> None:
  assert _matches_pattern(("id",), parse_pattern("{id,name}"))
  assert _matches_pattern(("name",), parse_pattern("{id,name}"))
  assert not _matches_pattern(("value",), parse_pattern("{id,name}"))
  assert _matches_pattern(("sub", "sub_id"), parse_pattern("sub.{sub_id,sub_name}"))


def test_expand_field_pattern_simple(test_message_class: type[ITestMessage]) -> None:
  expanded = expand_field_pattern(test_message_class, "id")
  assert expanded == {("id",)}


def test_expand_field_pattern_wildcard_star(
  test_message_class: type[ITestMessage],
) -> None:
  expanded = expand_field_pattern(test_message_class, "sub.*")
  assert expanded == {("sub", "sub_id"), ("sub", "sub_name"), ("sub", "sub_value")}


def test_expand_field_pattern_wildcard_double_star(
  test_message_class: type[ITestMessage],
) -> None:
  expanded = expand_field_pattern(test_message_class, "**.sub_id")
  assert expanded == {("sub", "sub_id"), ("nested_subs", "sub_id")}

  expanded_all_paths = expand_field_pattern(test_message_class, "**")
  expected_all_paths = {
    ("id",),
    ("name",),
    ("value",),
    ("sub",),
    ("tags",),
    ("nested_subs",),
    ("sub", "sub_id"),
    ("sub", "sub_name"),
    ("sub", "sub_value"),
    ("nested_subs", "sub_id"),
    ("nested_subs", "sub_name"),
    ("nested_subs", "sub_value"),
  }
  assert expanded_all_paths == expected_all_paths


def test_expand_field_pattern_field_set(test_message_class: type[ITestMessage]) -> None:
  expanded = expand_field_pattern(test_message_class, "{id,name}")
  assert expanded == {("id",), ("name",)}

  expanded = expand_field_pattern(test_message_class, "sub.{sub_id,sub_name}")
  assert expanded == {("sub", "sub_id"), ("sub", "sub_name")}


def test_expand_field_pattern_mixed(test_message_class: type[ITestMessage]) -> None:
  expanded = expand_field_pattern(test_message_class, "**.sub_id")
  assert expanded == {("sub", "sub_id"), ("nested_subs", "sub_id")}


def test_get_field_value(sample_message: ITestMessage) -> None:
  msg = sample_message
  assert list(_get_field_value(msg, ("id",))) == ["test_id"]
  assert list(_get_field_value(msg, ("sub", "sub_id"))) == ["s1"]
  assert set(_get_field_value(msg, ("tags",))) == {"tag1", "tag2"}
  assert set(_get_field_value(msg, ("nested_subs", "sub_id"))) == {"ns1", "ns2"}
  assert list(_get_field_value(msg, ("sub",))) == [msg.sub]


def test_lookup_field_values(sample_message: ITestMessage) -> None:
  msg = sample_message

  expanded_paths_single = {("id",)}
  assert lookup_field_values(msg, expanded_paths_single) == {"test_id"}

  expanded_paths_multiple = {("id",), ("name",)}
  assert lookup_field_values(msg, expanded_paths_multiple) == {"test_id", "test_name"}

  expanded_paths_repeated = {("tags",)}
  assert lookup_field_values(msg, expanded_paths_repeated) == {"tag1", "tag2"}

  expanded_paths_nested_repeated = {("nested_subs", "sub_id")}
  assert lookup_field_values(msg, expanded_paths_nested_repeated) == {"ns1", "ns2"}

  expanded_paths_mixed_types = {("id",), ("value",)}
  assert lookup_field_values(msg, expanded_paths_mixed_types) == {"test_id", 123}
