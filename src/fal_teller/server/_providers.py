import itertools
from dataclasses import dataclass, field
from functools import cached_property
from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    Iterator,
    List,
    Literal,
    Optional,
    TypeVar,
)

import fsspec
import pyarrow as arrow
import snowflake.connector
from pyarrow import flight, fs, parquet
from pyarrow.fs import _resolve_filesystem_and_path
from snowflake.connector import SnowflakeConnection

from fal_teller.server._ddl import generate_ddl


@dataclass
class Query:
    """A partial filter for what to list."""


PathT = TypeVar("PathT")


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


@dataclass
class FileProvider(ArrowProvider[str]):
    file_system: str
    config_options: Dict[str, Any] = field(default_factory=dict)
    root_path: Optional[None] = None
    file_type: Literal["parquet"] = "parquet"

    @cached_property
    def connection(self) -> fsspec.AbstractFileSystem:
        return fsspec.filesystem(self.file_system, **self.config_options)

    def pack_path(self, *parts: List[str]) -> str:
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

    def list(self, path: str) -> Iterator[TableInfo[str]]:
        filesystem, arrow_path = _resolve_filesystem_and_path(path, self.connection)
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


@dataclass
class SnowflakeProvider(ArrowProvider[str]):
    _SQL_ALCHEMY_DIALECT: ClassVar["str"] = "snowflake://"

    config_options: Dict[str, Any]

    @cached_property
    def connection(self) -> SnowflakeConnection:
        return snowflake.connector.connect(**self.config_options)

    def pack_path(self, *path: List[str]) -> str:
        assert len(path) == 1
        return path[0]

    def unpack_path(self, path: PathT) -> str:
        return path

    def read_from(self, path: str, query: Optional[Query]) -> arrow.RecordBatchReader:
        assert query is None

        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {path}")

            # Snowflake connector already fetches the first batch for itself,
            # so we should be able to take a peek at it immediately (and use
            # it to infer the schema of the whole stream).
            batches = cursor.fetch_arrow_batches()
            batch = next(batches, None)
            assert batch is not None, "there should be at least one batch"

            # Add the batch we have just read to the beginning of the iterator.
            combined_batches = itertools.chain([batch], batches)
            return arrow.RecordBatchReader.from_batches(batch.schema, combined_batches)

    def write_to(self, path: str, stream: arrow.RecordBatchReader) -> None:
        from snowflake.connector.pandas_tools import write_pandas

        with self.connection.cursor() as cursor:
            create_stmt = generate_ddl(
                path,
                dialect=self._SQL_ALCHEMY_DIALECT,
                schema=stream.schema,
            )
            cursor.execute(create_stmt)

        # Since there is no write_arrow in snowflake yet, we can
        # leverage the existing write_pandas by converting each
        # batch one by one. This has some overhead.
        for should_append, batch in enumerate(stream):
            write_pandas(
                self.connection,
                batch.to_pandas(),
                path,
                # For the initial batch, we want write_pandas to
                # create a new table. But all the subsequent ones
                # will need to append to the existing one.
                auto_create_table=not should_append,
                overwrite=not should_append,
            )

    def info(self, path: str) -> TableInfo[str]:
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {path} LIMIT 0")

            # Even though this query did not return any data in,
            # snowflake still can construct an empty arrow table
            # with the schema of the whole table.
            [sf_batch] = cursor.get_result_batches()
            empty_arrow_table = sf_batch._create_empty_table()

        return TableInfo(
            schema=empty_arrow_table.schema,
            path=path,
            # For snowflake, we don't know these (maybe we can do count(*)?)
            total_records=-1,
            total_bytes=-1,
        )


def get_provider(provider_type: str, *args: Any, **kwargs: Any) -> ArrowProvider:
    if provider_type == "file":
        return FileProvider(*args, **kwargs)
    else:
        raise NotImplementedError(provider_type)
