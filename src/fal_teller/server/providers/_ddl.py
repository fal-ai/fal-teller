from typing import Iterator

import pyarrow as arrow


def generate_ddl(path: str, dialect: str, schema: arrow.Schema) -> str:
    """Iterate some DDL statements (in the form of the specified SQL dialect) for
    initially dropping and then re-creating the given table."""

    # DDL generation is currently implemented in a generic fashion
    # using the pre-existing to_sql functionality from pandas and
    # sqlalchemy for code generation.

    from pandas.io import sql
    from sqlalchemy import create_mock_engine
    from sqlalchemy.sql.ddl import CreateTable, DropTable

    mock_engine = create_mock_engine(dialect, lambda *args, **kwargs: None)
    pandas_sql_db = sql.SQLDatabase(mock_engine)

    # Since we don't have the actual table, we can create a temporary one
    # from the schema we have and convert it to a dummy pandas table that
    # to_sql can use to generate the DDL.

    # Arrow -> Pandas -> Pandas SQL -> SQLAlchemy -> DDL
    dummy_arrow_table = arrow.table([[]] * len(schema), schema=schema)
    dummy_pandas_table = dummy_arrow_table.to_pandas()
    pandas_sql_table = sql.SQLTable(
        path, pandas_sql_db, dummy_pandas_table, index=False
    )

    create_stmt = CreateTable(pandas_sql_table.table, if_not_exists=True)
    return str(create_stmt.compile(mock_engine))
