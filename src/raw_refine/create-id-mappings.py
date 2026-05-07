#%%
from core import *
from raw_data import *
from derived_raw_data import *

id_mappings = []


def append_mapping(
    frame: nw.LazyFrame[duckdb.DuckDBPyRelation],
    source_id_type: str,
    target_id_type: str,
    mapping_source: str,
):
    id_mappings.append(
        frame.select(
            c('source_id'),
            c('target_id'),
        ).with_columns(
            source_id_type=l(source_id_type),
            target_id_type=l(target_id_type),
            mapping_source=l(mapping_source),
        )
    )


#%%
# isni to wikidata on the wikidata side
p_isni = wd_entities.filter(c('id') == 'P213').collect()['entity_id'][0]

isni_to_wikidata_wikidata_query = (
    wd_claim_string
    .filter(c("property_id") == p_isni)
    .rename({'value': 'source_id'})
    .join(wd_entities, on='entity_id')
    .select(c('rank'), c('source_id'), target_id=c('id'))
)

append_mapping(
    isni_to_wikidata_wikidata_query.filter(c('rank') != 'deprecated'),
    'isni',
    'wikidata',
    'wikidata',
)

#%%
# isni to wikidata on the isni side
append_mapping(
    isni_same_as
    .filter(c('same_as').str.starts_with('wd:'))
    .join(isni_core, on='isni_n')
    .select(source_id=c('isni'), target_id=c('same_as').str.slice(3))
    .join(isni_to_wikidata_wikidata_query.filter(c('rank') == 'deprecated'), on=['source_id', 'target_id'], how='anti'),
    'isni',
    'wikidata',
    'isni',
)

#%%
# viaf to wikidata on the wikidata side
p_viaf_cluster_id = wd_entities.filter(c('id') == 'P214').collect()['entity_id'][0]

viaf_to_wikidata_wikidata_query = (
    wd_claim_string
    .filter(c("property_id") == p_viaf_cluster_id)
    .with_columns(value=nw.concat_str(l('viaf'), c('value')))
    .join(wd_entities, on='entity_id')
    .select(c('rank'), source_id=c('value'), target_id=c('id'))
)

append_mapping(
    viaf_to_wikidata_wikidata_query.filter(c('rank') != 'deprecated'),
    'viaf',
    'wikidata',
    'wikidata',
)

#%%
# viaf to wikidata on the viaf side
append_mapping(
    stable_ids.filter(c('dataset') == 'viaf', c('id_type') == 'control_number')
    .join(stable_ids.filter(c('dataset') == 'viaf', c('id_type') == 'heading_linking_entry', c('id_extra') == 'WKP'), on='record_number')
    .with_columns(target_id=c('id_right').str.slice(5), source_id=c('id'))
    .join(viaf_to_wikidata_wikidata_query.filter(c('rank') == 'deprecated'), on=['source_id', 'target_id'], how='anti'),
    'viaf',
    'wikidata',
    'viaf',
)

#%%
bad_viaf_isni_links_from_wikidata = (
    wd_claim_string
    .filter(c("property_id") == p_viaf_cluster_id)
    .with_columns(value=nw.concat_str(l('viaf'), c('value')))
    .join(wd_claim_string.filter(c("property_id") == p_isni), on='entity_id')
    .filter((c('rank') == 'deprecated') | (c('rank_right') == 'deprecated'))
    .select(source_id=c('value'), target_id=c('value_right'))
)

#%%
# viaf to isni on the viaf side
append_mapping(
    stable_ids.filter(c('dataset') == 'viaf', c('id_type') == 'control_number')
    .join(stable_ids.filter(c('dataset') == 'viaf', c('id_type') == 'heading_linking_entry', c('id_extra') == 'ISNI'), on='record_number')
    .with_columns(source_id=c('id'), target_id=c('id_right'))
    .join(bad_viaf_isni_links_from_wikidata, on=['source_id', 'target_id'], how='anti'),
    'viaf',
    'isni',
    'viaf',
)

#%%
# gnd to wikidata on the wikidata side
p_gnd_id = wd_entities.filter(c('id') == 'P227').collect()['entity_id'][0]

gnd_to_wikidata_wikidata_query = (
    wd_claim_string
    .filter(c("property_id") == p_gnd_id)
    .join(wd_entities, on='entity_id')
    .select(c('rank'), source_id=c('value'), target_id=c('id'))
)

append_mapping(
    gnd_to_wikidata_wikidata_query.filter(c('rank') != 'deprecated'),
    'gnd',
    'wikidata',
    'wikidata',
)

#%%
# gnd to wikidata on the gnd side
append_mapping(
    stable_ids.filter(c('dataset') == 'gnd', c('id_type') == 'control_number')
    .join(stable_ids.filter(c('dataset') == 'gnd', c('id_type') == 'standard_recording_code', c('id_extra') == 'wikidata'), on='record_number')
    .select(source_id=c('id'), target_id=c('id_right'))
    .join(gnd_to_wikidata_wikidata_query.filter(c('rank') == 'deprecated'), on=['source_id', 'target_id'], how='anti'),
    'gnd',
    'wikidata',
    'gnd',
)

#%%
# gnd to isni on the gnd side
bad_gnd_isni_links_from_wikidata = (
    wd_claim_string
    .filter(c("property_id") == p_gnd_id)
    .join(wd_claim_string.filter(c("property_id") == p_isni), on='entity_id')
    .filter((c('rank') == 'deprecated') | (c('rank_right') == 'deprecated'))
    .select(source_id=c('value'), target_id=c('value_right'))
)

append_mapping(
    stable_ids.filter(c('dataset') == 'gnd', c('id_type') == 'control_number')
    .join(stable_ids.filter(c('dataset') == 'gnd', c('id_type') == 'standard_recording_code', c('id_extra') == 'isni'), on='record_number')
    .select(source_id=c('id'),target_id=c('id_right').str.replace_all(' ', ''))
    .join(bad_gnd_isni_links_from_wikidata, on=['source_id', 'target_id'], how='anti'),
    'gnd',
    'isni',
    'gnd',
)

#%%
# gnd to viaf on the gnd side
bad_gnd_viaf_links_from_wikidata = (
    wd_claim_string
    .filter(c("property_id") == p_gnd_id)
    .join(wd_claim_string.filter(c("property_id") == p_viaf_cluster_id), on='entity_id')
    .with_columns(value_right=nw.concat_str(l('viaf'), c('value_right')))
    .filter((c('rank') == 'deprecated') | (c('rank_right') == 'deprecated'))
    .join(gnd.filter(c('field_code') == '001'), on='value')
    .join(viaf.filter(c('field_code') == '001'), left_on='value_right', right_on='value')
    .select(source_id=c('value'), target_id=c('value_right'))
)

append_mapping(
    stable_ids.filter(c('dataset') == 'gnd', c('id_type') == 'control_number')
    .join(stable_ids.filter(c('dataset') == 'gnd', c('id_type') == 'standard_recording_code', c('id_extra') == 'viaf'), on='record_number')
    .select(source_id=c('id'), target_id=nw.concat_str(l('viaf'), c('id_right')))
    .join(bad_gnd_viaf_links_from_wikidata, on=['source_id', 'target_id'], how='anti'),
    'gnd',
    'viaf',
    'gnd',
)

#%%
# cerl thesaurus to gnd on the cerl thesaurus side
p_cerl_thesaurus_id = wd_entities.filter(c('id') == 'P1871').collect()['entity_id'][0]

bad_cerl_thesaurus_gnd_links_from_wikidata = (
    wd_claim_string
    .filter(c("property_id") == p_cerl_thesaurus_id)
    .join(wd_claim_string.filter(c("property_id") == p_gnd_id), on='entity_id')
    .filter((c('rank') == 'deprecated') | (c('rank_right') == 'deprecated'))
    .join(cerl_thesaurus.filter(c('field_code') == '001'), on='value')
    .join(gnd.filter(c('field_code') == '001'), left_on='value_right', right_on='value')
    .select(source_id=c('value'), target_id=c('value_right'))
)

append_mapping(
    stable_ids.filter(c('dataset') == 'cerl_thesaurus', c('id_type') == 'cerl_id')
    .join(stable_ids.filter(c('dataset') == 'cerl_thesaurus', c('id_type') == 'same', c('id_extra') == 'DNBI'), on='record_number')
    .select(source_id=c('id'), target_id=c('id_right').str.slice(len('http://d-nb.info/gnd/')))
    .join(bad_cerl_thesaurus_gnd_links_from_wikidata, on=['source_id', 'target_id'], how='anti'),
    'cerl_thesaurus',
    'gnd',
    'cerl_thesaurus',
)

#%%
# ulan to wikidata on the wikidata side
p_ulan_id = wd_entities.filter(c('id') == 'P245').collect()['entity_id'][0]

ulan_to_wikidata_wikidata_query = (
    wd_claim_string
    .filter(c("property_id") == p_ulan_id)
    .join(ulan.filter(c('property') == 'dc:identifier'), left_on='value', right_on='object')
    .join(wd_entities, on='entity_id')
    .select(c('rank'), source_id=c('subject'), target_id=c('id'))
)

append_mapping(
    ulan_to_wikidata_wikidata_query.filter(c('rank') != 'deprecated'),
    'ulan',
    'wikidata',
    'wikidata',
)

#%%
# geonames to wikidata on the wikidata side
p_geonames_id = wd_entities.filter(c('id') == 'P1566').collect()['entity_id'][0]

geonames_to_wikidata_wikidata_query = (
    wd_claim_string
    .filter(c("property_id") == p_geonames_id)
    .join(wd_entities, on='entity_id')
    .select(c('rank'), source_id=c('value'), target_id=c('id'))
)

append_mapping(
    geonames_to_wikidata_wikidata_query.filter(c('rank') != 'deprecated'),
    'geonames',
    'wikidata',
    'wikidata',
)

#%%
# tgn to wikidata on the wikidata side
p_tgn_id = wd_entities.filter(c('id') == 'P1667').collect()['entity_id'][0]

tgn_to_wikidata_wikidata_query = (
    wd_claim_string
    .filter(c("property_id") == p_tgn_id)
    .join(tgn.filter(c('property') == 'dc:identifier'), left_on='value', right_on='object')
    .join(wd_entities, on='entity_id')
    .select(c('rank'), source_id=c('subject'), target_id=c('id'))
)

append_mapping(
    tgn_to_wikidata_wikidata_query.filter(c('rank') != 'deprecated'),
    'tgn',
    'wikidata',
    'wikidata',
)

#%%
id_mappings_table = nw.concat(id_mappings)

to_parquet('id_mappings', here("data/work/derived_raw/id_mappings.parquet"), id_mappings_table)
# %%
