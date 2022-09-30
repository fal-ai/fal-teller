from dataclasses import dataclass, field
from functools import cached_property
from typing import Any, Dict, Iterator, List, Literal, Optional

import fsspec
import pyarrow as arrow
from pyarrow import flight, fs, parquet
from pyarrow.fs import _resolve_filesystem_and_path


@dataclass
class Query:
    """A partial filter for what to list."""


@dataclass
class TableInfo:
    schema: arrow.Schema
    path: str
    total_records: int = -1
    total_bytes: int = -1

    def to_flight(
        self,
        descriptor: flight.FlightDescriptor,
        endpoints: List[flight.FlightEndpoint],
    ) -> flight.FlightInfo:
        return flight.FlightInfo(
            self.schema,
            descriptor,
            endpoints,
            total_records=self.total_records,
            total_bytes=self.total_bytes,
        )


@dataclass
class ArrowProvider:
    """An arrow provider that can stream data inwards or outwards."""

    def write_to(self, path: str, stream: arrow.RecordBatchReader) -> None:
        raise NotImplementedError

    def read_from(self, path: str, query: Optional[Query]) -> arrow.RecordBatchReader:
        raise NotImplementedError

    def info(self, path: str) -> TableInfo:
        raise NotImplementedError

    def list(self) -> Iterator[TableInfo]:
        raise NotImplementedError


@dataclass
class FileProvider(ArrowProvider):
    file_system: str
    config_options: Dict[str, Any] = field(default_factory=dict)
    root_path: Optional[None] = None
    file_type: Literal["parquet"] = "parquet"

    def __post_init__(self):
        assert self.file_type == "parquet"

    @cached_property
    def _file_system(self) -> fsspec.AbstractFileSystem:
        return fsspec.filesystem(self.file_system, **self.config_options)

    def read_from(self, path: str, query: Optional[Query]) -> arrow.RecordBatchReader:
        assert query is None
        filesystem, arrow_path = _resolve_filesystem_and_path(
            self._normalize_path(path), self._file_system
        )
        pq_file = parquet.ParquetFile(filesystem.open_input_file(arrow_path))
        return arrow.RecordBatchReader.from_batches(
            pq_file.schema.to_arrow_schema(), pq_file.iter_batches()
        )

    def write_to(self, path: str, stream: arrow.RecordBatchReader) -> None:
        path = self._normalize_path(path)
        with parquet.ParquetWriter(
            path, stream.schema, filesystem=self._file_system
        ) as writer:
            for batch in stream:
                writer.write_batch(batch)

    def info(self, path: str) -> TableInfo:
        filesystem, arrow_path = _resolve_filesystem_and_path(
            self._normalize_path(path), self._file_system
        )
        pq_file = parquet.ParquetFile(filesystem.open_input_file(arrow_path))
        return TableInfo(
            schema=pq_file.schema.to_arrow_schema(),
            path=arrow_path,
            total_records=pq_file.metadata.num_rows,
            total_bytes=pq_file.metadata.serialized_size,
        )

    def list(self, path: str) -> Iterator[TableInfo]:
        filesystem, arrow_path = _resolve_filesystem_and_path(
            self._normalize_path(path), self._file_system
        )
        for file_info in filesystem.get_file_info(
            fs.FileSelector(arrow_path, recursive=True)
        ):
            pq_file = parquet.ParquetFile(filesystem.open_input_file(file_info))
            yield TableInfo(
                schema=pq_file.schema.to_arrow_schema(),
                path=file_info.path,
                total_records=pq_file.metadata.num_rows,
                total_bytes=pq_file.metadata.serialized_size,
            )

    def _normalize_path(self, path: str) -> str:
        return self._file_system.sep.join((self.root_path, path))


def get_provider(provider_type: str, *args: Any, **kwargs: Any) -> ArrowProvider:
    if provider_type == "file":
        return FileProvider(*args, **kwargs)
    else:
        raise NotImplementedError(provider_type)
