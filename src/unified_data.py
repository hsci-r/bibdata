import glob
from pathlib import Path

from duckdb import DuckDBPyRelation
from core import *

p_country_of_publication = cast(nw.LazyFrame[DuckDBPyRelation], None)
p_title = cast(nw.LazyFrame[DuckDBPyRelation], None)
p_year_of_publication = cast(nw.LazyFrame[DuckDBPyRelation], None)

for row in glob.glob(str(here("data/unified/*.parquet"))):
    name = Path(row).stem
    globals()[name] = read_parquet(name, here(f"data/unified/{name}.parquet"))
    print(f"{name} = cast(nw.LazyFrame[DuckDBPyRelation], None)")


__all__ = ["p_country_of_publication", "p_title", "p_year_of_publication"]