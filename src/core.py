from typing import cast
import narwhals as nw
import duckdb
from hereutil import here
from pathlib import Path
from tqdm.auto import tqdm
import sqlglot

con = duckdb.connect(here("data/work/duckdb/duckdb.db"), config=dict(parquet_metadata_cache=True, preserve_insertion_order=False, enable_fsst_vectors=True))

con.sql("SET enable_progress_bar = true;")
con.sql("SET preserve_insertion_order = false;")
con.sql("SET enable_fsst_vectors = true;")

def to_narwhals(duckdb_relation: duckdb.DuckDBPyRelation) -> nw.LazyFrame[duckdb.DuckDBPyRelation]:
    return nw.from_native(duckdb_relation)

def to_duckdb(lnf: nw.LazyFrame[duckdb.DuckDBPyRelation]) -> duckdb.DuckDBPyRelation:
    return lnf.to_native()

def read_parquet(table_name: str, glob: Path) -> nw.LazyFrame[duckdb.DuckDBPyRelation]:
    con.sql(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{glob}');")
    return to_narwhals(con.view(table_name))

def to_table(table_name: str, lnf: nw.LazyFrame[duckdb.DuckDBPyRelation], temporary: bool = False, replace: bool = True) -> nw.LazyFrame[duckdb.DuckDBPyRelation]:
    if replace:
        con.sql(f"CREATE OR REPLACE {'TEMPORARY ' if temporary else ''}TABLE {table_name} AS {to_duckdb(lnf).sql_query()}")
    else:
        con.sql(f"CREATE {'TEMPORARY ' if temporary else ''}TABLE IF NOT EXISTS {table_name} AS {to_duckdb(lnf).sql_query()}")
    return to_narwhals(con.table(table_name))

def to_parquet(table_name: str, path: Path, lnf: nw.LazyFrame[duckdb.DuckDBPyRelation], *args, **kwargs) -> nw.LazyFrame[duckdb.DuckDBPyRelation]:
    to_duckdb(lnf).write_parquet(str(path), compression='zstd', *args, **kwargs)
    return read_parquet(table_name, path)

def to_df(lnf: nw.LazyFrame[duckdb.DuckDBPyRelation]):
    return to_duckdb(lnf).df()

def format_sql(query: str, read:str = 'duckdb', write:str = 'duckb') -> str:
    return sqlglot.transpile(query, read=read, write=write, pretty=True)[0]

c = nw.col
l = nw.lit

__all__ = ["c", "l", "con", "to_narwhals", "to_duckdb", "read_parquet", "to_table", "to_parquet", "to_df", "format_sql", "here", "tqdm", "duckdb", "nw", "cast"]