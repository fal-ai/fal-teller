import itertools
from dataclasses import dataclass
from functools import cached_property
from typing import Any, ClassVar, Dict, Optional, cast

import pyarrow as arrow
import snowflake.connector
from snowflake.connector import SnowflakeConnection

from fal_teller.server.providers._ddl import generate_ddl
from fal_teller.server.providers.base import (
    ArrowProvider,
    PathT,
    Query,
    TableInfo,
)


@dataclass
class SnowflakeProvider(ArrowProvider[str]):
    _SQL_ALCHEMY_DIALECT: ClassVar["str"] = "snowflake://"

    config_options: Dict[str, Any]

    @cached_property
    def connection(self) -> SnowflakeConnection:
        return snowflake.connector.connect(**self.config_options)

    def pack_path(self, *path: str) -> str:
        assert len(path) == 1
        # Tables in snowflake should be capitalized?
        return path[0].upper()

    def unpack_path(self, path: PathT) -> str:
        return cast(str, path)

    def read_from(self, path: str, query: Optional[Query]) -> arrow.RecordBatchReader:
        assert query is None

        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {path}")

            # Snowflake connector already fetches the first batch for itself,
            # so we should be able to take a peek at it immediately (and use
            # it to infer the schema of the whole stream).
            table_batches = cursor.fetch_arrow_batches()
            fist_table = next(table_batches, None)
            assert fist_table is not None, "there should be at least one batch"

            # Snowflake returns an iterator of arrow tables, not record batches
            # so we need to convert them (but this has no overhead, since the underlying
            # conversion is zero-copy).
            return arrow.RecordBatchReader.from_batches(
                fist_table.schema,
                (
                    batch
                    for table in itertools.chain([fist_table], table_batches)
                    for batch in table.to_batches()
                ),
            )

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
