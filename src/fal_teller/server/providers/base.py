from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Iterator, List, Optional, TypeVar

import pyarrow as arrow
from pyarrow import flight

PathT = TypeVar("PathT")


@dataclass
class Query:
    """A partial filter for what to get."""


@dataclass
class TableInfo(Generic[PathT]):
    schema: arrow.Schema
    path: PathT
    total_records: int = -1
    total_bytes: int = -1

    def to_flight(
        self,
        descriptor: flight.FlightDescriptor,
        endpoints: List[flight.FlightEndpoint],
    ) -> flight.FlightInfo:
        """Generate a new flight response for this table."""
        return flight.FlightInfo(
            self.schema,
            descriptor,
            endpoints,
            total_records=self.total_records,
            total_bytes=self.total_bytes,
        )


@dataclass
class ArrowProvider(Generic[PathT]):
    """An arrow provider that can stream data inwards or outwards."""

    def pack_path(self, *parts: str) -> PathT:
        raise NotImplementedError

    def unpack_path(self, path: PathT) -> str:
        raise NotImplementedError

    def write_to(self, path: PathT, stream: arrow.RecordBatchReader) -> None:
        raise NotImplementedError

    def read_from(self, path: PathT, query: Optional[Query]) -> arrow.RecordBatchReader:
        raise NotImplementedError

    def info(self, path: PathT) -> TableInfo[PathT]:
        raise NotImplementedError

    def list(self) -> Iterator[TableInfo[PathT]]:
        raise NotImplementedError
