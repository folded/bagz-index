from google.protobuf import descriptor_pool, message, message_factory

from bagz_index.protos import key_types_pb2  # noqa: F401


def get_key_message_class(key_proto_name: str) -> type[message.Message]:
  # Get the default descriptor pool
  pool = descriptor_pool.Default()
  message_descriptor = pool.FindMessageTypeByName(key_proto_name)
  msg = message_factory.GetMessageClass(message_descriptor)
  if msg is None:
    raise ValueError(f"Message '{key_proto_name}' not found in the descriptor pool.")
  return msg
