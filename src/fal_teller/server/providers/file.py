from __future__ import annotations

from dataclasses import dataclass, field
from functools import cached_property
from typing import TYPE_CHECKING, Any, Dict, Iterator, Literal, Optional

import pyarrow as arrow
from pyarrow import fs, parquet
from pyarrow.fs import _resolve_filesystem_and_path

from fal_teller.server.providers.base import (
    ArrowProvider,
    PathT,
    Query,
    TableInfo,
)

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem


@dataclass
class FileProvider(ArrowProvider[str]):
    file_system: str
    config_options: Dict[str, Any] = field(default_factory=dict)
    root_path: Optional[None] = None
    file_type: Literal["parquet"] = "parquet"

    @cached_property
    def connection(self) -> AbstractFileSystem:
        import fsspec

        return fsspec.filesystem(self.file_system, **self.config_options)

    def pack_path(self, *parts: str) -> str:
        return self.connection.sep.join((self.root_path, *parts))

    def unpack_path(self, path: PathT) -> str:
        # We should probably take a relative against the root
        # path here.
        raise NotImplementedError
        return path

    def read_from(self, path: str, query: Optional[Query]) -> arrow.RecordBatchReader:
        assert query is None
        filesystem, arrow_path = _resolve_filesystem_and_path(path, self.connection)
        pq_file = parquet.ParquetFile(filesystem.open_input_file(arrow_path))
        arrow_schema = pq_file.schema.to_arrow_schema()
        arrow_records = pq_file.iter_batches()
        return arrow.RecordBatchReader.from_batches(arrow_schema, arrow_records)

    def write_to(self, path: str, stream: arrow.RecordBatchReader) -> None:
        with parquet.ParquetWriter(
            path, stream.schema, filesystem=self.connection
        ) as writer:
            for batch in stream:
                writer.write_batch(batch)

    def info(self, path: str) -> TableInfo[str]:
        filesystem, arrow_path = _resolve_filesystem_and_path(path, self.connection)
        pq_file = parquet.ParquetFile(filesystem.open_input_file(arrow_path))
        return TableInfo(
            schema=pq_file.schema.to_arrow_schema(),
            path=arrow_path,
            total_records=pq_file.metadata.num_rows,
            total_bytes=pq_file.metadata.serialized_size,
        )

    def list(self, path: str) -> Iterator[TableInfo[str]]:  # type: ignore[override]
        arrow_fs, arrow_path = _resolve_filesystem_and_path(path, self.connection)
        listing_agent = fs.FileSelector(arrow_path, recursive=True)
        for file_info in arrow_fs.get_file_info(listing_agent):
            pq_file = parquet.ParquetFile(arrow_fs.open_input_file(file_info))
            yield TableInfo(
                schema=pq_file.schema.to_arrow_schema(),
                path=file_info.path,
                total_records=pq_file.metadata.num_rows,
                total_bytes=pq_file.metadata.serialized_size,
            )
