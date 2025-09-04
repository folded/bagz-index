import pathlib

import bagz
import click

from bagz_index import core, hashtable, trigram
from bagz_index.generate_logic import generate_index  # Import the new function
from bagz_index.protos import messages_pb2


@click.group()
def main() -> None:
  pass


@main.command()
@click.argument("input_indices", nargs=-1, required=True)
@click.option(
  "-o",
  "--output",
  required=True,
  help="Path to the output merged index.",
)
def merge(input_indices: list[str], output: str) -> None:
  """Merge bagz indices."""
  core.merge_indices(list(input_indices), output)


@main.command()
@click.option("--input", type=str, required=True)
@click.option("--output", type=str, required=True)
@click.option("--proto-file", type=str, required=True, help="Path to the .proto file.")
@click.option(
  "--record-type",
  type=str,
  required=True,
  help="Name of the record message type.",
)
@click.option(
  "--key-field",
  type=str,
  multiple=True,
  help="""Field to use as key. Can be specified multiple times. Supports pattern matching:
- `*`: Matches any field in the current message.
- `**`: Matches a sequence of fields at any depth.
- `{a,b,c}`: Matches all of a set of fields (e.g., `{symbol,name}`).
Example: `--key-field 'location.*' --key-field '**.hgnc_id'`""",
)
@click.option(
  "--exclude-field",  # New option for exclusion patterns
  type=str,
  multiple=True,
  help="""Field to exclude from indexing. Can be specified multiple times. Supports pattern matching:
- `*`: Excludes any field in the current message.
- `**`: Excludes a sequence of fields at any depth.
- `{a,b,c}`: Excludes all of a set of fields (e.g., `{symbol,name}`).
Example: `--exclude-field 'location.end' --exclude-field '**.internal_id'`""",
)
@click.option(
  "--trigram",
  is_flag=True,
  default=False,
  help="Create a trigram index instead of a hashtable index.",
)
def generate(
  input: str,
  output: str,
  proto_file: str,
  record_type: str,
  key_field: list[str],
  exclude_field: list[str],
  trigram: bool,
) -> None:
  """Generate a bagz index from a bagz file."""
  generate_index(
    input, output, proto_file, record_type, key_field, exclude_field, trigram,
  )


@main.command()
@click.argument("index_file", type=str, required=True)
def dump(index_file: str) -> None:
  """Dump the contents of a bagz index file."""
  bagz_reader = bagz.Reader(pathlib.Path(index_file))
  config_json = bagz_reader[len(bagz_reader) - 1].decode("utf-8")
  config = core.config_from_json(config_json)

  print(f"Index Type: {config._get_type()}")
  print(f"Config: {config.to_json()}")
  print("--- Index Contents ---")

  if isinstance(config, hashtable.HashBucketConfig):
    key_proto_class = config.key_proto
    for i in range(len(bagz_reader) - 1):
      bucket_data = bagz_reader[i]
      if bucket_data:
        hash_bucket = messages_pb2.HashBucket()
        hash_bucket.ParseFromString(bucket_data)
        for record in hash_bucket.records:
          key_instance = key_proto_class()
          key_instance.ParseFromString(record.key)
          print(
            f"  Bucket {i}: Key: {key_instance.value}, Record IDs: {list(record.record_ids)}",
          )
  elif isinstance(config, trigram.TrigramConfig):
    for i in range(len(bagz_reader) - 1):
      posting_list_data = bagz_reader[i]
      if posting_list_data:
        posting_list = messages_pb2.PostingList()
        posting_list.ParseFromString(posting_list_data)
        if config.delta_encode_record_ids:
          trigram._delta_decode(posting_list)
        if config.store_positions:
          print(
            f"  Posting List {i}: Record IDs: {list(posting_list.record_ids)}, Offsets: {list(posting_list.record_offsets)}",
          )
        else:
          print(f"  Posting List {i}: Record IDs: {list(posting_list.record_ids)}")
  else:
    print(f"Unsupported index type for dumping: {config._get_type()}")


if __name__ == "__main__":
  main()
