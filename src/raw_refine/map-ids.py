#%%
from core import *
from raw_data import *
from raw_id_mappings import *

id_mappings = []

#%%
#isni to wikidata on the wikidata side
p_isni = wd_entities.filter(c('id')=='P213').collect()['entity_id'][0]

isni_to_wikidata_wikidata_query = (wd_claim_string
    .filter(
        c("property_id")==p_isni, 
    )
    .join(isni_core, left_on='value', right_on='isni')
    .join(e_id.filter(c('source')=='wikidata'), left_on='entity_id', right_on='i_id')
    .join(e_id.filter(c('source')=='isni'), left_on='isni_n', right_on='i_id')
)

id_mappings.append(isni_to_wikidata_wikidata_query
    .filter(
        c('rank') != 'deprecated'
    )                                  
    .select(c('e_id'), c('e_id_right'))
    .with_columns(source=l('isni'), target=l('wikidata'), mapping_source=l('wikidata'))
)


#%%
#isni to wikidata on the isni side
id_mappings.append(isni_same_as
    .filter(c('same_as').str.starts_with('wd:'))
    .select(c('isni_n'), c('same_as').str.slice(3))
    .join(wd_entities, left_on='same_as', right_on='id')
    .join(e_id.filter(c('source')=='wikidata'), left_on='entity_id', right_on='i_id')
    .join(e_id.filter(c('source')=='isni'), left_on='isni_n', right_on='i_id')
    .join(isni_to_wikidata_wikidata_query.filter(c('rank') == 'deprecated'), on=['e_id', 'e_id_right'], how='anti')
    .select(c('e_id'), c('e_id_right'))
    .with_columns(source=l('isni'), target=l('wikidata'), mapping_source=l('isni'))
)

#%%
#viaf to wikidata on the wikidata side
p_viaf_cluster_id = wd_entities.filter(c('id')=='P214').collect()['entity_id'][0]

viaf_to_wikidata_wikidata_query = (wd_claim_string
    .filter(
        c("property_id")==p_viaf_cluster_id, 
    ).with_columns(value=nw.concat_str(l('viaf'), c('value')))
    .join(
        viaf
            .filter(c('field_code') == '001')
        , on='value'
    )
    .join(e_id.filter(c('source')=='wikidata'), left_on='entity_id', right_on='i_id')
    .join(e_id.filter(c('source')=='viaf'), left_on='record_number', right_on='i_id')
)

id_mappings.append(viaf_to_wikidata_wikidata_query
    .filter(
        c('rank') != 'deprecated'
    )                                  
    .select(c('e_id'), c('e_id_right'))
    .with_columns(source=l('viaf'), target=l('wikidata'), mapping_source=l('wikidata'))
)

# %%
#viaf to wikidata on the viaf side
id_mappings.append(viaf
    .filter(
        c('field_code') == '700', 
        c('subfield_code') == '0', 
        c('value').str.starts_with('(WKP)'))
    .with_columns(value=c('value').str.slice(5))
    .join(wd_entities, left_on='value', right_on='id')
    .join(e_id.filter(c('source')=='wikidata'), left_on='entity_id', right_on='i_id')
    .join(e_id.filter(c('source')=='viaf'), left_on='record_number', right_on='i_id')
    .join(viaf_to_wikidata_wikidata_query.filter(c('rank') == 'deprecated'), on=['e_id', 'e_id_right'], how='anti')
    .select(c('e_id'), c('e_id_right'))
    .with_columns(source=l('viaf'), target=l('wikidata'), mapping_source=l('viaf'))
)

# %%

bad_viaf_isni_links_from_wikidata = (
    wd_claim_string.filter(
        c("property_id")==p_viaf_cluster_id,
    )
    .with_columns(value=nw.concat_str(l('viaf'), c('value')))
    .join(wd_claim_string.filter(
        c("property_id")==p_isni,
    ), on='entity_id')
    .filter((c('rank') == 'deprecated') | (c('rank_right') == 'deprecated'))
    .join(
        viaf
            .filter(c('field_code') == '001')
        , on='value'
    )
    .join(
        isni_core, left_on='value_right', right_on='isni'
    )
    .join(e_id.filter(c('source')=='viaf'), left_on='record_number', right_on='i_id')
    .join(e_id.filter(c('source')=='isni'), left_on='isni_n', right_on='i_id')
    .select(c('e_id'), c('e_id_right'))
)

# %%
#viaf to isni on the viaf side
id_mappings.append(viaf
    .filter(
        c('field_code') == '700', 
        c('subfield_code') == '0', 
        c('value').str.starts_with('(ISNI)'))
    .with_columns(isni=c('value').str.slice(6))
    .join(isni_core, on='isni')
    .join(e_id.filter(c('source')=='isni'), left_on='isni_n', right_on='i_id')
    .join(e_id.filter(c('source')=='viaf'), left_on='record_number', right_on='i_id')
    .join(bad_viaf_isni_links_from_wikidata, on=['e_id', 'e_id_right'], how='anti')
    .select(c('e_id'), c('e_id_right'))
    .with_columns(source=l('viaf'), target=l('isni'), mapping_source=l('viaf'))
)



# %%
#gnd to wikidata on the wikidata side

p_gnd_id = wd_entities.filter(c('id')=='P227').collect()['entity_id'][0]

gnd_to_wikidata_wikidata_query = (wd_claim_string
    .filter(
        c("property_id")==p_gnd_id, 
    )
    .join(
        gnd
            .filter(c('field_code') == '001')
        , on='value'
    )
    .join(e_id.filter(c('source')=='wikidata'), left_on='entity_id', right_on='i_id')
    .join(e_id.filter(c('source')=='gnd'), left_on='record_number', right_on='i_id')
)

id_mappings.append(gnd_to_wikidata_wikidata_query
    .filter(
        c('rank') != 'deprecated'
    )                                  
    .select(c('e_id'), c('e_id_right'))
    .with_columns(source=l('gnd'), target=l('wikidata'), mapping_source=l('wikidata'))
)
# %%
#gnd to wikidata on the gnd side
id_mappings.append(gnd
    .filter(
        c('field_code') == '024', 
        c('subfield_code') == '2', 
        c('value') == 'wikidata'
    )
    .join(
        gnd.filter(
            c('field_code') == '024',
            c('subfield_code') == 'a',
        )
        , on=['record_number', 'field_number']
    )
    .join(wd_entities, left_on='value_right', right_on='id')
    .join(e_id.filter(c('source')=='wikidata'), left_on='entity_id', right_on='i_id')
    .join(e_id.filter(c('source')=='gnd'), left_on='record_number', right_on='i_id')
    .join(gnd_to_wikidata_wikidata_query.filter(c('rank') == 'deprecated'), on=['e_id', 'e_id_right'], how='anti')
    .select(c('e_id'), c('e_id_right'))
    .with_columns(source=l('gnd'), target=l('wikidata'), mapping_source=l('gnd'))
)

# %%
#gnd to isni on the gnd side

bad_gnd_isni_links_from_wikidata = (
    wd_claim_string.filter(
        c("property_id")==p_gnd_id,
    )
    .join(wd_claim_string.filter(
        c("property_id")==p_isni,
    ), on='entity_id')
    .filter((c('rank') == 'deprecated') | (c('rank_right') == 'deprecated'))
    .join(
        gnd
            .filter(c('field_code') == '001')
        , on='value'
    )
    .join(
        isni_core, left_on='value_right', right_on='isni'
    )
    .join(e_id.filter(c('source')=='gnd'), left_on='record_number', right_on='i_id')
    .join(e_id.filter(c('source')=='isni'), left_on='isni_n', right_on='i_id')
    .select(c('e_id'), c('e_id_right'))
)

id_mappings.append(gnd
    .filter(
        c('field_code') == '024', 
        c('subfield_code') == '2', 
        c('value') == 'isni'
    )
    .join(
        gnd.filter(
            c('field_code') == '024',
            c('subfield_code') == 'a',
        )
        , on=['record_number', 'field_number']
    )
    .join(isni_core, left_on='value_right', right_on='isni')
    .join(e_id.filter(c('source')=='isni'), left_on='isni_n', right_on='i_id')
    .join(e_id.filter(c('source')=='gnd'), left_on='record_number', right_on='i_id')
    .join(bad_gnd_isni_links_from_wikidata, on=['e_id', 'e_id_right'], how='anti')
    .select(c('e_id'), c('e_id_right'))
    .with_columns(source=l('gnd'), target=l('isni'), mapping_source=l('gnd'))
)

#%%
#gnd to viaf on the gnd side

bad_gnd_viaf_links_from_wikidata = (
    wd_claim_string.filter(
        c("property_id")==p_gnd_id,
    )
    .join(wd_claim_string.filter(
        c("property_id")==p_viaf_cluster_id,
    ), on='entity_id')
    .with_columns(value_right=nw.concat_str(l('viaf'), c('value_right')))
    .filter((c('rank') == 'deprecated') | (c('rank_right') == 'deprecated'))
    .join(
        gnd
            .filter(c('field_code') == '001')
        , on='value'
    )
    .join(
        viaf
            .filter(c('field_code') == '001')
        , left_on='value_right', right_on='value'
    )
    .join(e_id.filter(c('source')=='gnd'), left_on='record_number', right_on='i_id')
    .join(e_id.filter(c('source')=='viaf'), left_on='record_number_right', right_on='i_id')
    .select(c('e_id'), c('e_id_right'))
)

id_mappings.append(gnd
    .filter(
        c('field_code') == '024', 
        c('subfield_code') == '2', 
        c('value') == 'viaf'
    )
    .join(
        gnd.filter(
            c('field_code') == '024',
            c('subfield_code') == 'a',
        )
        , on=['record_number', 'field_number']
    )
    .with_columns(value_right=nw.concat_str(l('viaf'), c('value_right')))
    .join(viaf
        .filter(c('field_code') == '001')
        , left_on='value_right', right_on='value'
    )
    .join(e_id.filter(c('source')=='gnd'), left_on='record_number', right_on='i_id')
    .join(e_id.filter(c('source')=='viaf'), left_on='record_number_right', right_on='i_id')
    .select(c('e_id'), c('e_id_right'))
    .join(bad_gnd_viaf_links_from_wikidata, on=['e_id', 'e_id_right'], how='anti')
    .with_columns(source=l('gnd'), target=l('viaf'), mapping_source=l('gnd'))
)

#%%
#cerl thesaurus to gnd on the cerl thesaurus side

p_cerl_thesaurus_id = wd_entities.filter(c('id')=='P1871').collect()['entity_id'][0]

bad_cerl_thesaurus_gnd_links_from_wikidata = (
    wd_claim_string.filter(
        c("property_id")==p_cerl_thesaurus_id,
    )
    .join(wd_claim_string.filter(
        c("property_id")==p_gnd_id,
    ), on='entity_id')
    .filter((c('rank') == 'deprecated') | (c('rank_right') == 'deprecated'))
    .join(
        cerl_thesaurus
            .filter(c('field_code') == '001')
        , on='value'
    )
    .join(
        gnd
            .filter(c('field_code') == '001')
        , left_on='value_right', right_on='value'
    )
    .join(e_id.filter(c('source')=='gnd'), left_on='record_number', right_on='i_id')
    .join(e_id.filter(c('source')=='viaf'), left_on='record_number_right', right_on='i_id')
    .select(c('e_id'), c('e_id_right'))
)

id_mappings.append(cerl_thesaurus
    .filter(
        c('field_code') == '956', 
        c('subfield_code') == 'n', 
        c('value') == 'DNBI'
    )
    .join(
        cerl_thesaurus
            .filter(
            c('field_code') == '956', 
            c('subfield_code') == 'y', 
        )
        , on=['record_number', 'field_number']
    )
    .join(gnd
        .filter(c('field_code') == '001')
        .with_columns(value=nw.concat_str(l('http://d-nb.info/gnd/'), c('value')))   
        , left_on='value_right', right_on='value'
    )
    .join(e_id.filter(c('source')=='cerl_thesaurus'), left_on='record_number', right_on='i_id')
    .join(e_id.filter(c('source')=='gnd'), left_on='record_number_right', right_on='i_id')
    .select(c('e_id'), c('e_id_right'))
    .join(bad_cerl_thesaurus_gnd_links_from_wikidata, on=['e_id', 'e_id_right'], how='anti')
    .with_columns(source=l('cerl_thesaurus'), target=l('gnd'), mapping_source=l('cerl_thesaurus'))
)

#%%
# ulan to wikidata on the wikidata side

p_ulan_id = wd_entities.filter(c('id')=='P245').collect()['entity_id'][0]

ulan_to_wikidata_wikidata_query = (wd_claim_string
    .filter(
        c("property_id")==p_ulan_id, 
    )
    .join(
        ulan.filter(c('property') == 'dc:identifier'),
        left_on='value', right_on='object'
    )
    .join(e_id.filter(c('source')=='wikidata'), left_on='entity_id', right_on='i_id')
    .join(e_id.filter(c('source')=='ulan'), left_on='subject', right_on='i_id')
)

id_mappings.append(ulan_to_wikidata_wikidata_query
    .filter(
        c('rank') != 'deprecated'
    )                                  
    .select(c('e_id'), c('e_id_right'))
    .with_columns(source=l('ulan'), target=l('wikidata'), mapping_source=l('wikidata'))
)


#%%
# geonames to wikidata on the wikidata side

p_geonames_id = wd_entities.filter(c('id')=='P1566').collect()['entity_id'][0]

geonames_to_wikidata_wikidata_query = (wd_claim_string
    .filter(
        c("property_id")==p_geonames_id, 
    )
    .rename({'value': 'geonameid'})
    .join(
        geonames, on='geonameid'
    )
    .join(e_id.filter(c('source')=='wikidata'), left_on='entity_id', right_on='i_id')
    .join(e_id.filter(c('source')=='geonames'), left_on='geonameid', right_on='i_id')
)

id_mappings.append(geonames_to_wikidata_wikidata_query
    .filter(
        c('rank') != 'deprecated'
    )                                  
    .select(c('e_id'), c('e_id_right'))
    .with_columns(source=l('geonames'), target=l('wikidata'), mapping_source=l('wikidata'))
)

# %%
# tgn to wikidata on the wikidata side

p_tgn_id = wd_entities.filter(c('id')=='P1667').collect()['entity_id'][0]

tgn_to_wikidata_wikidata_query = (wd_claim_string
    .filter(
        c("property_id")==p_tgn_id, 
    )
    .join(
        tgn.filter(c('property') == 'dc:identifier'),
        left_on='value', right_on='object'
    )
    .join(e_id.filter(c('source')=='wikidata'), left_on='entity_id', right_on='i_id')
    .join(e_id.filter(c('source')=='tgn'), left_on='subject', right_on='i_id')
)

id_mappings.append(tgn_to_wikidata_wikidata_query
    .filter(
        c('rank') != 'deprecated'
    )                                  
    .select(c('e_id'), c('e_id_right'))
    .with_columns(source=l('tgn'), target=l('wikidata'), mapping_source=l('wikidata'))
)

# %%
