import shutil
import os

from core import *
from raw_data import wd_entities, wd_labels

shutil.rmtree(here("data/work/derived_raw"), ignore_errors=True)
os.makedirs(here("data/work/derived_raw"), exist_ok=True)

# derived wikidata tables
p_instance_of = wd_entities.filter(nw.col("id") == "P31").collect()['entity_id'][0]
p_is_subclass_of = wd_entities.filter(nw.col("id") == "P279").collect()['entity_id'][0]

#p_date_of_birth = wd_entities.filter(nw.col("id") == "P569").collect()['entity_id'][0]
#p_date_of_death = wd_entities.filter(nw.col("id") == "P570").collect()['entity_id'][0]
#wd_birth_years = wd_claim_time.filter(nw.col("property_id") == p_date_of_birth).select('entity_id', dob_rank='rank', dob_time='time', dob_before='before', dob_after='after', dob_precision='precision', dob_calendarmodel_entity_id='calendarmodel_entity_id')
#wd_death_years = wd_claim_time.filter(nw.col("property_id") == p_date_of_death).select('entity_id', dod_rank='rank', dod_time='time', dod_before='before', dod_after='after', dod_precision='precision', dod_calendarmodel_entity_id='calendarmodel_entity_id')

q_geolocatable_entity = wd_entities.filter(nw.col("id") == "Q123349660").collect()['entity_id'][0]

to_parquet("wd_geolocatable_entity_types", here("data/work/derived_raw/wd_geolocatable_entity_types.parquet"), to_narwhals(con.sql(f"""
WITH RECURSIVE rec_geolocatable_entities(subclass_id, superclass_id) AS (
    SELECT {q_geolocatable_entity}, {q_geolocatable_entity}
    UNION
    SELECT entity_id, value_entity_id
    FROM wd_claim_wikibase_entityid
    WHERE property_id = {p_is_subclass_of} AND value_entity_id = {q_geolocatable_entity}
    UNION
    SELECT c.entity_id, s.superclass_id
    FROM wd_claim_wikibase_entityid c
    JOIN rec_geolocatable_entities s ON c.value_entity_id = s.subclass_id
    WHERE c.property_id = {p_is_subclass_of}
)
SELECT DISTINCT subclass_id AS value_entity_id
FROM rec_geolocatable_entities
""")))


wd_geolocatable_entities = to_parquet("wd_geolocatable_entities", here("data/work/derived_raw/wd_geolocatable_entities.parquet"), to_narwhals(con.sql(f"""
SELECT DISTINCT entity_id, rank FROM 
wd_geolocatable_entity_types AS wpt
INNER JOIN
wd_claim_wikibase_entityid AS wspec USING (value_entity_id)
WHERE wspec.property_id = {p_instance_of}
"""
)))

q_collective_agent = wd_entities.filter(nw.col("id") == "Q131085629").collect()['entity_id'][0]
wd_collective_agent_types = to_parquet('wd_collective_agent_types', here("data/work/derived_raw/wd_collective_agent_types.parquet"), to_narwhals(con.sql(f"""
WITH RECURSIVE rec_collective_agents(subclass_id, superclass_id) AS (
    SELECT {q_collective_agent}, {q_collective_agent}
    UNION
    SELECT entity_id, value_entity_id
    FROM wd_claim_wikibase_entityid
    WHERE property_id = {p_is_subclass_of} AND value_entity_id = {q_collective_agent}
    UNION
    SELECT c.entity_id, s.superclass_id
    FROM wd_claim_wikibase_entityid c
    JOIN rec_collective_agents s ON c.value_entity_id = s.subclass_id
    WHERE c.property_id = {p_is_subclass_of}
)
SELECT DISTINCT subclass_id AS value_entity_id
FROM rec_collective_agents
""")))
wd_collective_agents = to_parquet('wd_collective_agents', here("data/work/derived_raw/wd_collective_agents.parquet"), to_narwhals(con.sql(f"""
SELECT DISTINCT entity_id, rank FROM 
wd_collective_agent_types AS wpt
INNER JOIN
wd_claim_wikibase_entityid AS wpec USING (value_entity_id)
WHERE wpec.property_id = {p_instance_of}
""")))

q_person = wd_entities.filter(nw.col("id") == "Q215627").collect()['entity_id'][0]
wd_people_types = to_parquet('wd_people_types', here("data/work/derived_raw/wd_people_types.parquet"), to_narwhals(con.sql(f"""
WITH RECURSIVE rec_people_types(subclass_id, superclass_id) AS (
    SELECT {q_person}, {q_person}
    UNION
    SELECT entity_id, value_entity_id
    FROM wd_claim_wikibase_entityid
    WHERE property_id = {p_is_subclass_of} AND value_entity_id = {q_person}
    UNION
    SELECT c.entity_id, s.superclass_id
    FROM wd_claim_wikibase_entityid c
    JOIN rec_people_types s ON c.value_entity_id = s.subclass_id
    WHERE c.property_id = {p_is_subclass_of}
)
SELECT DISTINCT subclass_id AS value_entity_id
FROM rec_people_types
""")))
wd_people = to_parquet('wd_people', here("data/work/derived_raw/wd_people.parquet"), to_narwhals(con.sql(f"""
SELECT entity_id, value_entity_id AS type_entity_id FROM 
wd_people_types AS wpt
INNER JOIN
wd_claim_wikibase_entityid AS wpec USING (value_entity_id)
WHERE wpec.property_id = {p_instance_of}
""")))

wd_preflabel = to_parquet('wd_preflabel', here("data/work/derived_raw/wd_preflabel.parquet"), nw.concat([
    wd_labels.filter(nw.col('language') == 'en'),
    to_narwhals(to_duckdb(wd_labels.join(wd_labels.filter(nw.col('language') == 'en'), 'entity_id', how='anti')).aggregate('entity_id, FIRST(language) AS language, FIRST(label) AS label','entity_id'))
]))