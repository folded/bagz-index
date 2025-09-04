bagz-index is a library for building and reading static indices stored in bag(z)
files.

It is designed around the following Python API:

```python
import bagz_index.core as core
import bagz_index.hashtable as hashtable

# Example of building a hash index
config = hashtable.HashBucketConfig(
    avg_bucket_size=0.9,
    key_proto_name="bagz_index.keys.StringKey",
)

writer = core.make_writer(config)
writer.add_key([1, 2, 3], value="key1")
writer.write("my_index.bagz")

# Example of reading a hash index
reader = core.make_reader("my_index.bagz")
record_ids = reader.lookup_key(value="key1")
```

Internally, the `Writer` maintains an associative array from keys to a set of
record_ids.

Although `add_key()` does not need to be called with sorted record_ids, and may
be called multiple times for a given key, the hash table will store record_ids
in sorted order.

When `write()` is called, it writes this associative array out to a bagz file
with record protobufs:

```protobuf
message HashRecord {
    bytes key = 1;
    repeated int64 record_ids = 2;
}

message HashBucket {
    repeated HashRecord records = 1;
}
```

Empty hash cells are represented by zero length records.

The index is fixed sized (>= number of keys), and has a JSON configuration in
the last record of the bagz file that configures the indexing method. For the
`hashbucket` implementation, the bagz file records will be instances of
`HashBucket`. Within a hash bucket records will be sorted by key. So the JSON
configuration for a hash index will be:

```json
{
  "type": "hashbucket",
  "avg_bucket_size": 0.9,
  "key_proto_name": "bagz_index.keys.StringKey"
}
```

## Trigram Index

The trigram index is designed for efficient substring and text search. It breaks
down text into overlapping sequences of characters (trigrams) and indexes them.

### Configuration

The `TrigramConfig` allows you to control how the index is built and searched:

- `character_set` (str, required): The set of characters to consider for n-gram
  generation. Characters outside this set will be handled based on the
  `normalize` option.
- `ngram_size` (int, optional, default=3): The size of the n-grams to generate.
- `normalize` (bool, optional, default=False): If `True`:
  - Text is lowercased.
  - Runs of characters not in `character_set` are replaced by a single space.
  - Space is implicitly included in the `character_set` for index calculation.
- `store_positions` (bool, optional, default=False): If `True`, the index stores
  the character offsets of each n-gram within the document. This enables exact
  (false-positive-free) substring searches.

### Example Usage

```python
import bagz_index.core as core
import bagz_index.trigram as trigram
import string

# --- Simple Trigram Index (no normalization, no positions) ---
config_simple = trigram.TrigramConfig(
    character_set=string.ascii_lowercase,
    ngram_size=3,
)

writer_simple = core.make_writer(config_simple, core.SupportsTextAddition)
writer_simple.add_text("hello world", 0)
writer_simple.add_text("world of wonders", 1)
writer_simple.add_text("hello there", 2)
writer_simple.write("simple_trigram_index.bagz")

reader_simple = core.make_reader("simple_trigram_index.bagz", core.SupportsTextSearch)
# This index requires post-filtering, as it only provides candidate documents.
assert reader_simple.requires_post_filtering is True
results_simple = reader_simple.search("world")
print(f"Simple search for 'world': {results_simple}") # Example: [0, 1]

# --- Normalized Positional Trigram Index ---
config_pos = trigram.TrigramConfig(
    character_set=string.ascii_lowercase,
    ngram_size=3,
    normalize=True,
    store_positions=True,
)

writer_pos = core.make_writer(config_pos, core.SupportsTextAddition)
writer_pos.add_text("Hello, World!", 0)
writer_pos.add_text("World of... Wonders?", 1)
writer_pos.add_text("HELLO THERE", 2)
writer_pos.write("positional_trigram_index.bagz")

reader_pos = core.make_reader("positional_trigram_index.bagz", core.SupportsTextSearch)
# This index does NOT require post-filtering, as it returns exact matches.
assert reader_pos.requires_post_filtering is False

results_pos_exact = reader_pos.search("world")
print(f"Positional search for 'world': {results_pos_exact}") # Example: [0, 1]

results_pos_phrase = reader_pos.search("hello there")
print(f"Positional search for 'hello there': {results_pos_phrase}") # Example: [2]

# Search for a false positive (e.g., 'sea' and 'archers' are in the same doc but not 'search')
# This will not return any results from a positional index.
writer_pos.add_text("the sea archers", 3) # Add a document that would be a false positive
writer_pos.write("positional_trigram_index_updated.bagz") # Re-write the index
reader_pos_updated = core.make_reader("positional_trigram_index_updated.bagz", core.SupportsTextSearch)
results_false_pos = reader_pos_updated.search("search")
print(f"Positional search for 'search' (false positive test): {results_false_pos}") # Example: []
```

### JSON Configuration Examples

**Simple Trigram Index:**

```json
{
  "type": "trigram",
  "character_set": "abcdefghijklmnopqrstuvwxyz",
  "ngram_size": 3,
  "normalize": false,
  "store_positions": false
}
```

**Normalized Positional Trigram Index:**

```json
{
  "type": "trigram",
  "character_set": "abcdefghijklmnopqrstuvwxyz",
  "ngram_size": 3,
  "normalize": true,
  "store_positions": true
}
```
