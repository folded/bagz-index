from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class HashRecord(_message.Message):
    __slots__ = ("key", "record_ids")
    KEY_FIELD_NUMBER: _ClassVar[int]
    RECORD_IDS_FIELD_NUMBER: _ClassVar[int]
    key: bytes
    record_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, key: _Optional[bytes] = ..., record_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class HashBucket(_message.Message):
    __slots__ = ("records",)
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[HashRecord]
    def __init__(self, records: _Optional[_Iterable[_Union[HashRecord, _Mapping]]] = ...) -> None: ...

class PostingList(_message.Message):
    __slots__ = ("record_ids", "record_offsets")
    RECORD_IDS_FIELD_NUMBER: _ClassVar[int]
    RECORD_OFFSETS_FIELD_NUMBER: _ClassVar[int]
    record_ids: _containers.RepeatedScalarFieldContainer[int]
    record_offsets: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, record_ids: _Optional[_Iterable[int]] = ..., record_offsets: _Optional[_Iterable[int]] = ...) -> None: ...
