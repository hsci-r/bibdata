#%%
import shutil
import os

from core import *
from raw_data import *

#%%
shutil.rmtree(here("data/work/derived_raw"), ignore_errors=True)
os.makedirs(here("data/work/derived_raw"), exist_ok=True)

#%%

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
SELECT entity_id, MAX(rank) AS rank FROM 
wd_geolocatable_entity_types AS wpt
INNER JOIN
wd_claim_wikibase_entityid AS wspec USING (value_entity_id)
WHERE wspec.property_id = {p_instance_of}
GROUP BY entity_id
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
SELECT entity_id, MAX(rank) AS rank FROM 
wd_collective_agent_types AS wpt
INNER JOIN
wd_claim_wikibase_entityid AS wpec USING (value_entity_id)
WHERE wpec.property_id = {p_instance_of}
GROUP BY entity_id
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
SELECT entity_id, MAX(rank) AS rank FROM 
wd_people_types AS wpt
INNER JOIN
wd_claim_wikibase_entityid AS wpec USING (value_entity_id)
WHERE wpec.property_id = {p_instance_of}
GROUP BY entity_id
""")))

wd_preflabel = to_parquet('wd_preflabel', here("data/work/derived_raw/wd_preflabel.parquet"), nw.concat([
    wd_labels.filter(nw.col('language') == 'en'),
    to_narwhals(to_duckdb(wd_labels.join(wd_labels.filter(nw.col('language') == 'en'), 'entity_id', how='anti')).aggregate('entity_id, FIRST(language) AS language, FIRST(label) AS label','entity_id'))
]))

#%%

p_name = wd_entities.filter(nw.col("id") == "P2561").collect()['entity_id'][0]
p_subproperty_of = wd_entities.filter(nw.col("id") == "P1647").collect()['entity_id'][0]

wd_name_properties = to_table('wd_name_properties', to_narwhals(con.sql(f"""
WITH RECURSIVE rec_name_properties(subproperty_id, superproperty_id) AS (
    SELECT {p_name}, {p_name}
    UNION
    SELECT entity_id, value_entity_id
    FROM wd_claim_wikibase_entityid
    WHERE property_id = {p_subproperty_of} AND value_entity_id = {p_name}
    UNION
    SELECT c.entity_id, s.superproperty_id
    FROM wd_claim_wikibase_entityid c
    JOIN rec_name_properties s ON c.value_entity_id = s.subproperty_id
    WHERE c.property_id = {p_subproperty_of}
)
SELECT DISTINCT subproperty_id AS property_id
FROM rec_name_properties
""")), temporary = True, replace = True)

#%%
p_start_time = wd_entities.filter(nw.col("id") == "P580").collect()['entity_id'][0]
p_end_time = wd_entities.filter(nw.col("id") == "P582").collect()['entity_id'][0]


to_parquet('wd_names', here("data/work/derived_raw/wd_names.parquet"), wd_claim_monolingualtext
    .join(wd_name_properties, on='property_id')
    .join(wd_qualifier_time
          .filter(c('property_id')==p_start_time)
          .select(
              c('claim_id'), 
              start_time=c('time'), 
              start_precision=c('precision'), 
              start_calendarmodel_entity_id=c('calendarmodel_entity_id')
        )
          , on='claim_id', how='left'
    )
    .join(wd_qualifier_time
          .filter(c('property_id')==p_end_time)
          .select(
              c('claim_id'), 
              end_time=c('time'), 
              end_precision=c('precision'), 
              end_calendarmodel_entity_id=c('calendarmodel_entity_id')
        )
          , on='claim_id', how='left'
    )
)

#%%
all_stable_id_rows: list[nw.LazyFrame[duckdb.DuckDBPyRelation]] = []

def extract_stable_ids(dataset: str, standard: str):
    def emit_rows(
        frame: nw.LazyFrame[duckdb.DuckDBPyRelation],
        id_expr: nw.Expr,
        id_type: nw.Expr,
        id_extra_expr: nw.Expr | None = None,
    ):
        return frame.select(
            l(dataset).alias('dataset'),
            c('record_number').alias('record_number'),
            id_expr.alias('id'),
            id_type.alias('id_type'),
            (id_extra_expr if id_extra_expr is not None else l(None).cast(nw.String)).alias('id_extra'),
        )

    def extract_simple(
        source: nw.LazyFrame[duckdb.DuckDBPyRelation],
        field_code: str,
        id_type: str,
        subfield_code: str | None = None,
    ):
        predicates = [c('field_code') == field_code]
        if subfield_code is not None:
            predicates.append(c('subfield_code') == subfield_code)
        return emit_rows(source.filter(*predicates), c('value'), l(id_type))

    def extract_joined(
        source: nw.LazyFrame[duckdb.DuckDBPyRelation],
        left_field_code: str,
        left_subfield_code: str | None,
        right_subfield_code: str | None,
        id_type: str,
        right_field_code: str | None = None,
    ):
        left_predicates = [c('field_code') == left_field_code]
        if left_subfield_code is not None:
            left_predicates.append(c('subfield_code') == left_subfield_code)
        right_predicates = [c('field_code') == (right_field_code or left_field_code)]
        if right_subfield_code is not None:
            right_predicates.append(c('subfield_code') == right_subfield_code)
        joined = source.filter(*left_predicates).join(
            source.filter(*right_predicates),
            on=['record_number', 'field_number'],
            how='left',
        )
        return emit_rows(joined, c('value'), l(id_type), c('value_right'))

    dataset_rows: list[nw.LazyFrame[duckdb.DuckDBPyRelation]] = []
    source = f(dataset)

    if standard == 'pica':
        dataset_rows.extend([
            extract_simple(source, '003@', 'ppn'),
            extract_simple(source, '003@', 'oclc_number', '0'),
            extract_simple(source, '004A', 'isbn', '0'),
            extract_simple(source, '005A', 'issn', '0'),
            extract_simple(source, '006A', 'lccn_number', '0'),
            extract_simple(source, '006Y', 'general_id'),
            extract_joined(source, '006X', '0', 'i', 'other_id'),
            extract_joined(source, '007G', '0', 'i', 'original_id'),
        ])
    elif standard == 'marc21':
        dataset_rows.extend([
            extract_joined(source, '001', None, None, 'control_number', right_field_code='003'),
            extract_joined(source, '024', 'a', '2', 'standard_recording_code'),
            extract_simple(source, '010', 'lccn_number', 'a'),
            extract_simple(source, '020', 'isbn', 'a'),
            extract_simple(source, '022', 'issn', 'a'),
            extract_joined(source, '015', 'a', '2', 'national_bibliography_number'),
            extract_joined(source, '016', 'a', '2', 'national_bibliographic_agency_control_number'),
            extract_simple(source, '035', 'system_control_number', 'a'),
        ])
    elif standard == 'intermarc':
        dataset_rows.extend([
            extract_simple(source, '001', 'record_identification_number'),
            extract_simple(source, '003', 'permanent_url'),
        ])

    if dataset == 'vd17':
        dataset_rows.append(extract_simple(vd17, '006W', 'vd17_id'))
    elif dataset == 'vd18':
        dataset_rows.append(extract_simple(vd18, '006M', 'vd18_id'))
    elif dataset == 'melinda':
        dataset_rows.append(extract_joined(melinda, 'SID', 'b', 'c', 'sid'))
    elif dataset == 'cerl_thesaurus':
        dataset_rows.append(extract_simple(cerl_thesaurus, '001', 'cerl_id'))
        dataset_rows.append(emit_rows(
            source.filter(c('field_code') == '956', c('subfield_code') == 'y')
            .join(
                source.filter(c('field_code') == '956', c('subfield_code') == '0'),
                on=['record_number', 'field_number'],
            )
            .join(
                source.filter(c('field_code') == '956', c('subfield_code') == 'n'),
                on=['record_number', 'field_number'],
            ),
            c('value'),
            c('value_right'), 
            c('value_right_1')           
        ))
    elif dataset == 'viaf':
        dataset_rows.append(emit_rows(source.filter(c('field_code')=="700", c('subfield_code')=='0'), c('value').str.split(')').list.get(1), l('heading_linking_entry'), c('value').str.split(')').list.get(0).str.slice(1)))
    elif dataset in {"wikidata", "isni", "geonames", "viaf", "estc", "gnd", "bnf", "cnb", "dnb", "erb", "hpb"} or standard == "rdf":
        pass
    elif standard not in {'pica', 'marc21', 'intermarc'}:
        print(f"Skipping stable id extraction for dataset {dataset} with standard {standard}")

    if dataset_rows:
        all_stable_id_rows.append(nw.concat(dataset_rows))

iter_datasets(extract_stable_ids)

if all_stable_id_rows:
    to_parquet(
        'stable_ids',
        here("data/work/derived_raw/stable_ids.parquet"),
        nw.concat(all_stable_id_rows),
    )

# %%
