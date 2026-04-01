import glob
from pathlib import Path
from duckdb import DuckDBPyRelation
from raw_refine.core import *
from raw_refine.raw_data import wd_claim_time, wd_entities

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

p_instance_of = wd_entities.filter(nw.col("id") == "P31").collect()['entity_id'][0]
p_is_subclass_of = wd_entities.filter(nw.col("id") == "P279").collect()['entity_id'][0]

p_date_of_birth = wd_entities.filter(nw.col("id") == "P569").collect()['entity_id'][0]
p_date_of_death = wd_entities.filter(nw.col("id") == "P570").collect()['entity_id'][0]
wd_birth_years = wd_claim_time.filter(nw.col("property_id") == p_date_of_birth).select('entity_id', dob_rank='rank', dob_time='time', dob_before='before', dob_after='after', dob_precision='precision', dob_calendarmodel_entity_id='calendarmodel_entity_id')
wd_death_years = wd_claim_time.filter(nw.col("property_id") == p_date_of_death).select('entity_id', dod_rank='rank', dod_time='time', dod_before='before', dod_after='after', dod_precision='precision', dod_calendarmodel_entity_id='calendarmodel_entity_id')
    

__all__ = ["wd_collective_agent_types", "wd_collective_agents", "wd_geolocatable_entities", "wd_geolocatable_entity_types", "wd_people", "wd_people_types", "wd_preflabel", "p_instance_of", "p_is_subclass_of", "p_date_of_birth", "p_date_of_death", "wd_birth_years", "wd_death_years"]