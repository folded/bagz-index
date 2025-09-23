import abc
import contextlib
import importlib
import importlib.resources
import importlib.util
import json
import pathlib
import re
import string
import sys
import tempfile
import types
from collections.abc import Generator, Sequence
from typing import Any, Generic, Self, TypeVar

import bagz
import google.protobuf.descriptor
import google.protobuf.message
import grpc_tools
from grpc_tools import protoc  # Import protoc from grpc_tools

from bagz_index import core

WriterT = TypeVar("WriterT", bound=core.IndexWriter)


class ShardedIndexBuilder(abc.ABC, Generic[WriterT]):
  def __init__(
    self,
    output_bagz_path: pathlib.Path,
    config: core.Config,
    shard_limit: int = 200_000,
  ) -> None:
    self.output_bagz_path = output_bagz_path
    self.config = config
    self.shard_limit = shard_limit

    self.temp_dir: pathlib.Path | None = None
    self._current_writer: WriterT | None = None
    self.current_shard_record_count = 0
    self.shard_paths: list[pathlib.Path] = []
    self.shard_id = 0
    self.cleanup_stack = contextlib.ExitStack()

  def __enter__(self) -> Self:
    temp_dir = self.cleanup_stack.enter_context(tempfile.TemporaryDirectory())
    self.temp_dir = pathlib.Path(temp_dir)
    self._new_shard_writer()
    return self

  @property
  def current_writer(self) -> WriterT:
    if self._current_writer is None:
      self._current_writer = self._new_shard_writer()
    return self._current_writer

  @abc.abstractmethod
  def _make_writer(self) -> WriterT: ...

  def _new_shard_writer(self) -> WriterT:
    if self.temp_dir is None:
      raise RuntimeError("ShardedIndexBuilder must be used as a context manager.")

    shard_file_name = (
      f"{self.output_bagz_path.stem}-{self.shard_id:05d}{self.output_bagz_path.suffix}"
    )
    shard_path = self.temp_dir / shard_file_name
    self.shard_paths.append(shard_path)
    writer = self._make_writer()
    self.current_shard_record_count = 0
    self.shard_id += 1
    return writer

  def _write_current_shard(self) -> None:
    if self._current_writer:
      self._current_writer.write(self.shard_paths[-1])
      self._current_writer = None

  def record_added(self) -> None:
    self.current_shard_record_count += 1
    if self.current_shard_record_count >= self.shard_limit:
      self._write_current_shard()

  def __exit__(self, *args) -> None:  # noqa: ANN002
    del args
    self._write_current_shard()  # Write the last shard
    if self.temp_dir and self.shard_paths:
      core.merge_indices([str(p) for p in self.shard_paths], str(self.output_bagz_path))
    self.cleanup_stack.close()


class ShardedKeyIndexBuilder(ShardedIndexBuilder[core.SupportsKeyAddition]):
  def _make_writer(self) -> core.SupportsKeyAddition:
    return core.make_writer(self.config, core.SupportsKeyAddition)

  def add_record(
    self,
    key: google.protobuf.message.Message,
    record_ids: Sequence[int],
  ) -> None:
    self.current_writer.add(key, record_ids)
    self.record_added()


class ShardedTextIndexBuilder(ShardedIndexBuilder[core.SupportsTextAddition]):
  def _make_writer(self) -> core.SupportsTextAddition:
    return core.make_writer(self.config, core.SupportsTextAddition)

  def add_record(self, text: str, record_id: int) -> None:
    self.current_writer.add_text(text, record_id)
    self.record_added()


def _compile_and_load_proto(
  proto_def: str,
  namespace: str,
  tmp_path: pathlib.Path,
  *,
  proto_include_paths: Sequence[pathlib.Path] = (),
) -> types.ModuleType:
  proto_path = tmp_path / (namespace.replace(".", "_") + ".proto")
  proto_path.write_text(proto_def)

  with importlib.resources.as_file(
    importlib.resources.files(grpc_tools) / "_proto",
  ) as grpc_tools_proto_path:
    proto_path_args = [
      f"--proto_path={include_path}" for include_path in proto_include_paths
    ]

    command = [
      "grpc_tools.protoc",
      f"--proto_path={tmp_path}",  # Primary proto_path
      f"--proto_path={grpc_tools_proto_path}",
      *proto_path_args,
      f"--python_out={tmp_path}",
      str(proto_path.relative_to(tmp_path)),  # Proto file relative to tmp_path
    ]
    # Add additional include paths if provided

    result_code = protoc.main(command)

  if result_code != 0:
    raise RuntimeError(f"protoc failed with exit code {result_code}")

  # Now, load the compiled module
  generated_file_path = tmp_path / (proto_path.stem + "_pb2.py")

  spec = importlib.util.spec_from_file_location(namespace, generated_file_path)
  if spec is None:
    raise ImportError(
      f"Could not find module spec for {namespace} at {generated_file_path}",
    )
  module = importlib.util.module_from_spec(spec)
  sys.modules[namespace] = module
  spec.loader.exec_module(module)
  return module


def _import_record_type(
  proto_file: str,
  proto_module_name: str,
  record_type_name: str,
) -> type[google.protobuf.message.Message]:
  """Compiles a .proto file and imports the specified record type."""
  with tempfile.TemporaryDirectory() as temp_dir:
    # Call _compile_proto to just compile the .proto file
    module = _compile_and_load_proto(
      pathlib.Path(proto_file).read_text(),
      proto_module_name,  # Namespace for the generated module
      pathlib.Path(temp_dir),
      proto_include_paths=[
        pathlib.Path(proto_file).parent,
      ],  # Pass the parent directory as include path
    )

    return getattr(module, record_type_name)


def _parse_field_set(field_str: str) -> list[str]:
  """Parses a field set string like '{a,b,c}' into a list of field names."""
  match = re.fullmatch(r"\{(.*)\}", field_str)
  if match:
    return [f.strip() for f in match.group(1).split(",")]
  return [field_str]


def _yield_field_paths(
  msg_descriptor: google.protobuf.descriptor.Descriptor,
) -> Generator[
  tuple[tuple[str, ...], google.protobuf.descriptor.FieldDescriptor],
  None,
  None,
]:
  for field_desc in msg_descriptor.fields:
    yield (field_desc.name,), field_desc
    if field_desc.type == field_desc.TYPE_MESSAGE:
      for new_path, nested_field_desc in _yield_field_paths(field_desc.message_type):
        yield (field_desc.name, *new_path), nested_field_desc


class Matcher(abc.ABC):
  @abc.abstractmethod
  def match(
    self,
    field_path: tuple[str, ...],
    pattern: "Pattern",
    pattern_index: int,
  ) -> bool: ...


class ExactMatcher(Matcher):
  def __init__(self, name: str) -> None:
    self.name = name

  def match(
    self,
    field_path: tuple[str, ...],
    pattern: "Pattern",
    pattern_index: int,
  ) -> bool:
    if not field_path:
      return False
    if self.name == field_path[0]:
      return pattern.match(field_path[1:], pattern_index + 1)
    return False


class WildcardMatcher(Matcher):
  def match(
    self,
    field_path: tuple[str, ...],
    pattern: "Pattern",
    pattern_index: int,
  ) -> bool:
    if not field_path:
      return False
    return pattern.match(field_path[1:], pattern_index + 1)


class DoubleWildcardMatcher(Matcher):
  def match(
    self,
    field_path: tuple[str, ...],
    pattern: "Pattern",
    pattern_index: int,
  ) -> bool:
    # Case 1: ** matches zero fields
    if pattern.match(field_path, pattern_index + 1):
      return True
    # Case 2: ** matches one or more fields
    if field_path:
      return self.match(field_path[1:], pattern, pattern_index)
    return False


class SetMatcher(Matcher):
  def __init__(self, field_names: list[str]) -> None:
    self.field_names = field_names

  def match(
    self,
    field_path: tuple[str, ...],
    pattern: "Pattern",
    pattern_index: int,
  ) -> bool:
    if not field_path:
      return False
    if field_path[0] in self.field_names:
      return pattern.match(field_path[1:], pattern_index + 1)
    return False


class Pattern:
  def __init__(self, matchers: Sequence[Matcher]) -> None:
    self.matchers = matchers

  def match(self, field_path: tuple[str, ...], pattern_index: int) -> bool:
    if pattern_index >= len(self.matchers):
      return not field_path
    if not field_path:
      return all(
        isinstance(m, DoubleWildcardMatcher) for m in self.matchers[pattern_index:]
      )

    matcher = self.matchers[pattern_index]
    return matcher.match(field_path, self, pattern_index)


def parse_pattern(pattern_str: str) -> Pattern:
  components = []
  current_component = ""
  brace_level = 0
  for char in pattern_str:
    if char == "{":
      brace_level += 1
      current_component += char
    elif char == "}":
      brace_level -= 1
      current_component += char
    elif char == "." and brace_level == 0:
      components.append(current_component)
      current_component = ""
    else:
      current_component += char
  if current_component:
    components.append(current_component)

  matchers: list[Matcher] = []
  for component in components:
    if component == "**":
      matchers.append(DoubleWildcardMatcher())
    elif component == "*":
      matchers.append(WildcardMatcher())
    elif component.startswith("{") and component.endswith("}"):
      matchers.append(SetMatcher(_parse_field_set(component)))
    else:
      matchers.append(ExactMatcher(component))
  return Pattern(matchers)


def _matches_pattern(field_path: tuple[str, ...], pattern: Pattern) -> bool:
  return pattern.match(field_path, 0)


def expand_field_pattern(
  msg_class: type[google.protobuf.message.Message],
  pattern_path: str,
) -> set[tuple[str, ...]]:
  expanded_paths: set[tuple[str, ...]] = set()
  pattern = parse_pattern(pattern_path)

  for field_path_tuple, _ in _yield_field_paths(msg_class.DESCRIPTOR):
    if _matches_pattern(field_path_tuple, pattern):
      expanded_paths.add(field_path_tuple)
  return expanded_paths


def _get_field_value(
  msg: google.protobuf.message.Message,
  path_components: tuple[str, ...],
) -> Generator[Any, None, None]:
  if not path_components:
    yield msg
    return

  current_component = path_components[0]
  remaining_path = path_components[1:]

  if current_component not in msg.DESCRIPTOR.fields_by_name:
    return

  field_desc = msg.DESCRIPTOR.fields_by_name[current_component]
  value = getattr(msg, field_desc.name)

  if field_desc.is_repeated:
    for item in value:
      yield from _get_field_value(item, remaining_path)
  else:
    yield from _get_field_value(value, remaining_path)


def lookup_field_values(
  pb: google.protobuf.message.Message,
  expanded_paths: set[tuple[str, ...]],
) -> set[Any]:
  result = set()
  for path_tuple in expanded_paths:
    for value in _get_field_value(pb, path_tuple):
      result.add(value)
  return result


def make_hashtable_index(
  reader: bagz.Reader,
  output_path: pathlib.Path,
  expanded_key_fields: set[tuple[str, ...]],  # Changed type
  record_type: type[google.protobuf.message.Message],
  key_proto_name: str,
) -> None:
  config = core.config_from_json(
    json.dumps(
      {
        "type": "hashbucket",
        "avg_bucket_size": 0.9,
        "key_proto_name": key_proto_name,
      },
    ),
  )
  with ShardedKeyIndexBuilder(
    output_path,
    config,
    shard_limit=200_000,
  ) as sharded_builder:
    for i, record in enumerate(reader):
      pb = record_type()
      pb.ParseFromString(record)
      for key in lookup_field_values(pb, expanded_key_fields):  # Changed call
        sharded_builder.add_record(
          sharded_builder.current_writer.key_proto(value=key),
          [i],
        )


def make_trigram_index(
  reader: bagz.Reader,
  output_path: pathlib.Path,
  expanded_key_fields: set[tuple[str, ...]],  # Changed type
  record_type: type[google.protobuf.message.Message],
) -> None:
  trigram_charset = string.ascii_lowercase + string.digits
  config_json = json.dumps(
    {
      "type": "trigram",
      "normalize": True,
      "store_positions": True,
      "character_set": trigram_charset,
      "delta_encode_record_ids": True,
    },
  )
  config = core.config_from_json(config_json)

  with ShardedTextIndexBuilder(
    output_path,
    config,
    shard_limit=200_000,
  ) as sharded_builder:
    for i, record in enumerate(reader):
      pb = record_type()
      pb.ParseFromString(record)
      for key in lookup_field_values(pb, expanded_key_fields):  # Changed call
        sharded_builder.add_record(key, i)


def _generate_matching_field_paths(
  record_type_class: type[google.protobuf.message.Message],
  key_field_patterns: list[str],
  exclude_field_patterns: list[str],
) -> set[tuple[str, ...]]:
  all_expanded_key_fields: set[tuple[str, ...]] = set()
  for pattern in key_field_patterns:
    all_expanded_key_fields.update(expand_field_pattern(record_type_class, pattern))
  for pattern in exclude_field_patterns:
    all_expanded_key_fields.difference_update(
      expand_field_pattern(record_type_class, pattern),
    )
  return all_expanded_key_fields


def _get_key_proto_name(
  all_expanded_key_fields: set[tuple[str, ...]],
  record_type_class: type[google.protobuf.message.Message],
) -> str:
  # Type checking for expanded fields
  first_field_type: int | None = None
  is_repeated_field: bool = False
  for path_tuple in all_expanded_key_fields:
    current_msg_class = record_type_class
    current_field_type: int | None = None
    for component in path_tuple:
      if component not in current_msg_class.DESCRIPTOR.fields_by_name:
        raise ValueError(
          f"Field '{component}' not found in message '{current_msg_class.__name__}' "
          f"for path '{'.'.join(path_tuple)}'",
        )
      field_desc = current_msg_class.DESCRIPTOR.fields_by_name[component]
      current_field_type = field_desc.type
      if field_desc.type == field_desc.TYPE_MESSAGE:
        current_msg_class = field_desc.message_type._concrete_class

    if first_field_type is None:
      first_field_type = current_field_type
    elif first_field_type != current_field_type:
      raise TypeError(
        "All key fields must be of the same protobuf type and repetition. "
        f"Found type {first_field_type} and {current_field_type}.",
      )

  match first_field_type:
    case google.protobuf.descriptor.FieldDescriptor.TYPE_STRING:
      key_proto_name = "bagz_index.keys.StringKey"
    case google.protobuf.descriptor.FieldDescriptor.TYPE_INT64:
      key_proto_name = "bagz_index.keys.Int64Key"
    case _:
      raise TypeError(
        f"Unsupported key field type: {first_field_type}"
        f" (repeated: {is_repeated_field})",
      )

  return key_proto_name


def generate_index(
  input_bagz_path: str,
  output_bagz_path: str,
  proto_file: str,
  record_type_name: str,
  key_field_patterns: list[str],
  exclude_field_patterns: list[str],
  is_trigram_index: bool,
) -> None:
  record_type_class = _import_record_type(proto_file, "index_proto", record_type_name)

  all_expanded_key_fields = _generate_matching_field_paths(
    record_type_class,
    key_field_patterns,
    exclude_field_patterns,
  )

  key_proto_name = _get_key_proto_name(all_expanded_key_fields, record_type_class)

  reader = bagz.Reader(pathlib.Path(input_bagz_path))
  output_path = pathlib.Path(output_bagz_path)

  if is_trigram_index:
    assert key_proto_name == "bagz_index.keys.StringKey"
    make_trigram_index(reader, output_path, all_expanded_key_fields, record_type_class)
  else:
    make_hashtable_index(
      reader,
      output_path,
      all_expanded_key_fields,
      record_type_class,
      key_proto_name,
    )
