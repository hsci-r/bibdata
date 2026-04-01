#%%
from raw_refine.core import *
from raw_refine.raw_data import *
from raw_refine.raw_id_mappings import *

id_mappings = []

#%%
#isni to wikidata on the wikidata side
p_isni = wd_entities.filter(nw.col('id')=='P213').collect()['entity_id'][0]

isni_to_wikidata_wikidata_query = (wd_claim_string
    .filter(
        nw.col("property_id")==p_isni, 
    )
    .join(isni_core, left_on='value', right_on='isni')
    .join(e_to_wd, on='entity_id')
    .join(e_to_isni, on='isni_n')
)

id_mappings.append(isni_to_wikidata_wikidata_query
    .filter(
        nw.col('rank') != 'deprecated'
    )                                  
    .select(nw.col('e_id'), nw.col('e_id_right'))
    .with_columns(mapping_source=nw.lit('wikidata'))
)


#%%
#isni to wikidata on the isni side
id_mappings.append(isni_same_as
    .filter(nw.col('same_as').str.starts_with('wd:'))
    .select(nw.col('isni_n'), nw.col('same_as').str.slice(3))
    .join(wd_entities, left_on='same_as', right_on='id')
    .join(e_to_wd, on='entity_id')
    .join(e_to_isni, on='isni_n')
    .join(isni_to_wikidata_wikidata_query.filter(nw.col('rank') == 'deprecated'), on=['e_id', 'e_id_right'], how='anti')
    .select(nw.col('e_id'), nw.col('e_id_right'))
    .with_columns(mapping_source=nw.lit('isni'))
)

#%%
#viaf to wikidata on the wikidata side
p_viaf_cluster_id = wd_entities.filter(nw.col('id')=='P214').collect()['entity_id'][0]

viaf_to_wikidata_wikidata_query = (wd_claim_string
    .filter(
        nw.col("property_id")==p_viaf_cluster_id, 
    ).with_columns(value=nw.concat_str(nw.lit('viaf'), nw.col('value')))
    .join(
        viaf
            .filter(nw.col('field_code') == '001')
        , on='value'
    )
    .join(e_to_wd, on='entity_id')
    .join(e_to_viaf, on='record_number')
)

id_mappings.append(viaf_to_wikidata_wikidata_query
    .filter(
        nw.col('rank') != 'deprecated'
    )                                  
    .select(nw.col('e_id'), nw.col('e_id_right'))
    .with_columns(mapping_source=nw.lit('wikidata'))
)

# %%
#viaf to wikidata on the viaf side
id_mappings.append(viaf
    .filter(
        nw.col('field_code') == '700', 
        nw.col('subfield_code') == '0', 
        nw.col('value').str.starts_with('(WKP)'))
    .with_columns(value=nw.col('value').str.slice(5))
    .join(wd_entities, left_on='value', right_on='id')
    .join(e_to_wd, on='entity_id')
    .join(e_to_viaf, on='record_number')
    .join(viaf_to_wikidata_wikidata_query.filter(nw.col('rank') == 'deprecated'), on=['e_id', 'e_id_right'], how='anti')
    .select(nw.col('e_id'), nw.col('e_id_right'))
    .with_columns(mapping_source=nw.lit('viaf'))
)

# %%

bad_viaf_isni_links_from_wikidata = (
    wd_claim_string.filter(
        nw.col("property_id")==p_viaf_cluster_id,
    )
    .with_columns(value=nw.concat_str(nw.lit('viaf'), nw.col('value')))
    .join(wd_claim_string.filter(
        nw.col("property_id")==p_isni,
    ), on='entity_id')
    .filter((nw.col('rank') == 'deprecated') | (nw.col('rank_right') == 'deprecated'))
    .join(
        viaf
            .filter(nw.col('field_code') == '001')
        , on='value'
    )
    .join(
        isni_core, left_on='value_right', right_on='isni'
    )
    .join(e_to_viaf, on='record_number')
    .join(e_to_isni, on='isni_n')
    .select(nw.col('e_id'), nw.col('e_id_right'))
)

# %%
#viaf to isni on the viaf side
id_mappings.append(viaf
    .filter(
        nw.col('field_code') == '700', 
        nw.col('subfield_code') == '0', 
        nw.col('value').str.starts_with('(ISNI)'))
    .with_columns(isni=nw.col('value').str.slice(6))
    .join(isni_core, on='isni')
    .join(e_to_isni, on='isni_n')
    .join(e_to_viaf, on='record_number')
    .join(bad_viaf_isni_links_from_wikidata, on=['e_id', 'e_id_right'], how='anti')
    .select(nw.col('e_id'), nw.col('e_id_right'))
    .with_columns(mapping_source=nw.lit('viaf'))
)



# %%
#gnd to wikidata on the wikidata side

p_gnd_id = wd_entities.filter(nw.col('id')=='P227').collect()['entity_id'][0]

gnd_to_wikidata_wikidata_query = (wd_claim_string
    .filter(
        nw.col("property_id")==p_gnd_id, 
    )
    .join(
        gnd
            .filter(nw.col('field_code') == '001')
        , on='value'
    )
    .join(e_to_wd, on='entity_id')
    .join(e_to_gnd, on='record_number')
)

id_mappings.append(gnd_to_wikidata_wikidata_query
    .filter(
        nw.col('rank') != 'deprecated'
    )                                  
    .select(nw.col('e_id'), nw.col('e_id_right'))
    .with_columns(mapping_source=nw.lit('wikidata'))
)
# %%
#gnd to wikidata on the gnd side
id_mappings.append(gnd
    .filter(
        nw.col('field_code') == '024', 
        nw.col('subfield_code') == '2', 
        nw.col('value') == 'wikidata'
    )
    .join(
        gnd.filter(
            nw.col('field_code') == '024',
            nw.col('subfield_code') == 'a',
        )
        , on=['record_number', 'field_number']
    )
    .join(wd_entities, left_on='value_right', right_on='id')
    .join(e_to_wd, on='entity_id')
    .join(e_to_gnd, on='record_number')
    .join(gnd_to_wikidata_wikidata_query.filter(nw.col('rank') == 'deprecated'), on=['e_id', 'e_id_right'], how='anti')
    .select(nw.col('e_id'), nw.col('e_id_right'))
    .with_columns(mapping_source=nw.lit('gnd'))
)

# %%
#gnd to isni on the gnd side

bad_gnd_isni_links_from_wikidata = (
    wd_claim_string.filter(
        nw.col("property_id")==p_gnd_id,
    )
    .join(wd_claim_string.filter(
        nw.col("property_id")==p_isni,
    ), on='entity_id')
    .filter((nw.col('rank') == 'deprecated') | (nw.col('rank_right') == 'deprecated'))
    .join(
        gnd
            .filter(nw.col('field_code') == '001')
        , on='value'
    )
    .join(
        isni_core, left_on='value_right', right_on='isni'
    )
    .join(e_to_gnd, on='record_number')
    .join(e_to_isni, on='isni_n')
    .select(nw.col('e_id'), nw.col('e_id_right'))
)

id_mappings.append(gnd
    .filter(
        nw.col('field_code') == '024', 
        nw.col('subfield_code') == '2', 
        nw.col('value') == 'isni'
    )
    .join(
        gnd.filter(
            nw.col('field_code') == '024',
            nw.col('subfield_code') == 'a',
        )
        , on=['record_number', 'field_number']
    )
    .join(isni_core, left_on='value_right', right_on='isni')
    .join(e_to_isni, on='isni_n')
    .join(e_to_gnd, on='record_number')
    .join(bad_gnd_isni_links_from_wikidata, on=['e_id', 'e_id_right'], how='anti')
    .select(nw.col('e_id'), nw.col('e_id_right'))
    .with_columns(mapping_source=nw.lit('gnd'))
)

#%%
#gnd to viaf on the gnd side

bad_gnd_viaf_links_from_wikidata = (
    wd_claim_string.filter(
        nw.col("property_id")==p_gnd_id,
    )
    .join(wd_claim_string.filter(
        nw.col("property_id")==p_viaf_cluster_id,
    ), on='entity_id')
    .with_columns(value_right=nw.concat_str(nw.lit('viaf'), nw.col('value_right')))
    .filter((nw.col('rank') == 'deprecated') | (nw.col('rank_right') == 'deprecated'))
    .join(
        gnd
            .filter(nw.col('field_code') == '001')
        , on='value'
    )
    .join(
        viaf
            .filter(nw.col('field_code') == '001')
        , left_on='value_right', right_on='value'
    )
    .join(e_to_gnd, on='record_number')
    .join(e_to_viaf, left_on='record_number_right', right_on='record_number')
    .select(nw.col('e_id'), nw.col('e_id_right'))
)

id_mappings.append(gnd
    .filter(
        nw.col('field_code') == '024', 
        nw.col('subfield_code') == '2', 
        nw.col('value') == 'viaf'
    )
    .join(
        gnd.filter(
            nw.col('field_code') == '024',
            nw.col('subfield_code') == 'a',
        )
        , on=['record_number', 'field_number']
    )
    .with_columns(value_right=nw.concat_str(nw.lit('viaf'), nw.col('value_right')))
    .join(viaf
        .filter(nw.col('field_code') == '001')
        , left_on='value_right', right_on='value'
    )
    .join(e_to_gnd, on='record_number')
    .join(e_to_viaf, left_on='record_number_right', right_on='record_number')
    .select(nw.col('e_id'), nw.col('e_id_right'))
    .join(bad_gnd_viaf_links_from_wikidata, on=['e_id', 'e_id_right'], how='anti')
    .with_columns(mapping_source=nw.lit('gnd'))
)

#%%
# ulan to wikidata on the wikidata side

p_ulan_id = wd_entities.filter(nw.col('id')=='P245').collect()['entity_id'][0]

ulan_to_wikidata_wikidata_query = (wd_claim_string
    .filter(
        nw.col("property_id")==p_ulan_id, 
    )
    .join(
        ulan.filter(nw.col('property') == 'dc:identifier'),
        left_on='value', right_on='object'
    )
    .join(e_to_wd, on='entity_id')
    .join(e_to_iri, left_on='subject', right_on='iri')
)

id_mappings.append(ulan_to_wikidata_wikidata_query
    .filter(
        nw.col('rank') != 'deprecated'
    )                                  
    .select(nw.col('e_id'), nw.col('e_id_right'))
    .with_columns(mapping_source=nw.lit('wikidata'))
)


#%%
# geonames to wikidata on the wikidata side

p_geonames_id = wd_entities.filter(nw.col('id')=='P1566').collect()['entity_id'][0]

geonames_to_wikidata_wikidata_query = (wd_claim_string
    .filter(
        nw.col("property_id")==p_geonames_id, 
    )
    .rename({'value': 'geonameid'})
    .join(
        geonames, on='geonameid'
    )
    .join(e_to_wd, on='entity_id')
    .join(e_to_geonames, on='geonameid')
)

id_mappings.append(geonames_to_wikidata_wikidata_query
    .filter(
        nw.col('rank') != 'deprecated'
    )                                  
    .select(nw.col('e_id'), nw.col('e_id_right'))
    .with_columns(mapping_source=nw.lit('wikidata'))
)

# %%
# tgn to wikidata on the wikidata side

p_tgn_id = wd_entities.filter(nw.col('id')=='P1667').collect()['entity_id'][0]

tgn_to_wikidata_wikidata_query = (wd_claim_string
    .filter(
        nw.col("property_id")==p_tgn_id, 
    )
    .join(
        tgn.filter(nw.col('property') == 'dc:identifier'),
        left_on='value', right_on='object'
    )
    .join(e_to_wd, on='entity_id')
    .join(e_to_iri, left_on='subject', right_on='iri')
)

id_mappings.append(tgn_to_wikidata_wikidata_query
    .filter(
        nw.col('rank') != 'deprecated'
    )                                  
    .select(nw.col('e_id'), nw.col('e_id_right'))
    .with_columns(mapping_source=nw.lit('wikidata'))
)

# %%
