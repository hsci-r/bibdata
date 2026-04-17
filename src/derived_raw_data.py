import glob
from pathlib import Path
from duckdb import DuckDBPyRelation
from core import *

wd_collective_agent_types = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_collective_agents = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_geolocatable_entities = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_geolocatable_entity_types = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_people = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_people_types = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_preflabel = cast(nw.LazyFrame[DuckDBPyRelation], None)

for row in glob.glob(str(here("data/work/derived_raw/*.parquet"))):
    name = Path(row).stem
    globals()[name] = read_parquet(name, here(f"data/work/derived_raw/{name}.parquet"))
    #print(f"{name} = cast(nw.LazyFrame[DuckDBPyRelation], None)")

__all__ = ["wd_collective_agent_types", "wd_collective_agents", "wd_geolocatable_entities", "wd_geolocatable_entity_types", "wd_people", "wd_people_types", "wd_preflabel"]