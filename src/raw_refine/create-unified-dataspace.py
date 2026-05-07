#%%
import shutil
import os

from core import *
from raw_data import *
from derived_raw_data import *
from raw_id_mappings import *


subqueries: list[nw.LazyFrame] = []

#%%
shutil.rmtree(here("data/unified"), ignore_errors=True)
os.makedirs(here("data/unified"), exist_ok=True)

#%%
def extract_title(dataset: str, standard: str):
    if standard in {'marc21', 'intermarc', 'danmarc'} and dataset != 'viaf':
       q =  (globals()[dataset]
            .filter(c('field_code') == '245', c('subfield_code') == 'a')
       )
    elif standard == 'unimarc':
         q = (
             globals()[dataset]
                .filter(c('field_code') == '200', c('subfield_code') == 'a')
         )
    elif standard == 'pica':
         q = (
             globals()[dataset]
                .filter(c('field_code') == '021A', c('subfield_code') == 'a')
         )
    elif standard == 'istc':
         q = (
             globals()[dataset]
                .filter(c('field_code') == 'title')
         )
    else:
        print(f"Skipping title extraction for dataset {dataset} with standard {standard}")
        return
    subqueries.append(q
                        .join(e_id.filter(c('source') == dataset), left_on='record_number', right_on='i_id')
                        .select(c('e_id'), c('value').alias('main_title')))

subqueries.clear()
iter_datasets(extract_title)

to_parquet("p_title", here("data/unified/p_title.parquet"), nw.concat(subqueries).sort('e_id'))

#%%
def extract_country_of_publication(dataset: str, standard: str):
    if standard == 'marc21' and dataset != 'viaf':
        q = (globals()[dataset]
            .filter(c('field_code') == '008')
            .with_columns(value=c('value').str.slice(15, 3).str.strip_chars(' '))
        )
    elif standard == 'intermarc':
        q = (globals()[dataset]
            .filter(c('field_code') == '008')
            .with_columns(value=c('value').str.slice(29, 2))
        )
    elif standard == 'unimarc':
        q = (globals()[dataset]
            .filter(c('field_code') == '102', c('subfield_code') == 'a')
        )
    elif standard == 'pica':
        q = (globals()[dataset]
            .filter(c('field_code') == '019@', c('subfield_code') == 'a')
        )
    elif standard == 'danmarc':
        q = (globals()[dataset]
            .filter(c('field_code') == '008', c('subfield_code') == 'b')
        )
    elif standard == 'istc':
        q = (globals()[dataset]
            .filter(c('subfield_code') == 'geo_info_imprint_country_code')
        )
    else:
        print(f"Skipping country of publication extraction for dataset {dataset} with standard {standard}")
        return
    subqueries.append(q
                        .filter(c('value')!='')
                        .join(e_id.filter(c('source') == dataset), left_on='record_number', right_on='i_id')
                        .select(c('e_id'), c('value').alias('country_of_publication')))
    
subqueries.clear()
iter_datasets(extract_country_of_publication)

#mutate(value=value |> str_replace("[^-]*-", "") |> str_replace("-.*","") |> str_to_lower()) |>
to_parquet("p_country_of_publication", here("data/unified/p_country_of_publication.parquet"), nw.concat(subqueries).sort('e_id'))

#%%
def extract_year_of_publication(dataset: str, standard: str):
    if standard == 'marc21' and dataset != 'viaf':
        q = (globals()[dataset]
            .filter(c('field_code') == '008')
            .with_columns(value=c('value').str.slice(7, 4))
        )
    elif standard == 'intermarc':
        q = (globals()[dataset]
            .filter(c('field_code') == '008')
            .with_columns(value=c('value').str.slice(8, 4))
        )
    elif standard == 'unimarc':
        q = (globals()[dataset]
            .filter(c('field_code') == '100')
            .with_columns(value=c('value').str.slice(9, 4))
        )
    elif standard == 'danmarc':
        q = (globals()[dataset]
            .filter(c('field_code') == '008', c('subfield_code') == 'a')
        )
    elif standard == 'pica':
        q = (globals()[dataset]
            .filter(c('field_code') == '011@')
        )
    elif standard == 'istc':
        q = (globals()[dataset]
            .filter(c('subfield_code') == 'date_of_item_single_date')
        )
    else:
        print(f"Skipping year of publication extraction for dataset {dataset} with standard {standard}")
        return
    subqueries.append(q
                        .filter(c('value').str.contains(r'^\d{4}$'))
                        .join(e_id.filter(c('source') == dataset), left_on='record_number', right_on='i_id')
                        .select(c('e_id'), c('value').alias('year_of_publication').cast(nw.Int32)))
    
subqueries.clear()
iter_datasets(extract_year_of_publication)

to_parquet("p_year_of_publication", here("data/unified/p_year_of_publication.parquet"), nw.concat(subqueries).sort('e_id'))

#%%
def extract_primary_language(dataset: str, standard: str):
    if standard == 'marc21' and dataset != 'viaf':
        q = (globals()[dataset]
            .filter(c('field_code') == '008')
            .with_columns(value=c('value').str.slice(35, 3))
        )
    elif standard == 'intermarc':
        q = (globals()[dataset]
            .filter(c('field_code') == '008')
            .with_columns(value=c('value').str.slice(31, 3))
        )
    elif standard == 'unimarc':
        q = (globals()[dataset]
            .filter(c('field_code') == '100')
            .with_columns(value=c('value').str.slice(22, 3))
        )
    elif standard == 'danmarc':
        q = (globals()[dataset]
            .filter(c('field_code') == '008', c('subfield_code') == 'a')
        )
    elif standard == 'pica':
        q = (globals()[dataset]
            .filter(c('field_code') == '010@', c('subfield_code') == 'a')
        )
    else:
        print(f"Skipping primary language extraction for dataset {dataset} with standard {standard}")
        return
    subqueries.append(q
                        .with_columns(value=c('value').str.strip_chars(' |'))
                        .filter(c('value')!='', c('value')!='und')
                        .join(e_id.filter(c('source') == dataset), left_on='record_number', right_on='i_id')
                        .select(c('e_id'), c('value').alias('primary_language_code')))
    
subqueries.clear()
iter_datasets(extract_primary_language)

to_parquet("p_primary_language", here("data/unified/p_primary_language.parquet"), nw.concat(subqueries).sort('e_id'))

# %%

def extract_cataloguing_date(dataset: str, standard: str):
    if standard in {'marc21', 'intermarc'}:
        q = (globals()[dataset].filter(c('field_code')=='008'))
        q = to_narwhals(to_duckdb(q).select("record_number, substr(value, 0, 7) AS date_created_raw, try_cast(try_strptime(date_created_raw, '%y%m%d') AS DATE) AS date_created"))
    elif standard == 'unimarc':
        q = (globals()[dataset]
            .filter(c('field_code')=='100')
        )
        q = to_narwhals(to_duckdb(q).select("record_number, substr(value, 0, 9) AS date_created_raw, try_cast(try_strptime(date_created_raw, '%Y%m%d') AS DATE) AS date_created"))
    elif standard == 'danmarc':
        q = (globals()[dataset].filter(c('field_code')=='001', c('subfield_code') == 'd'))
        q = to_narwhals(to_duckdb(q).select("record_number, value AS date_created_raw, try_cast(try_strptime(date_created_raw, '%Y%m%d') AS DATE) AS date_created"))
    elif standard == 'pica':
        q = (globals()[dataset].filter(c('field_code')=='001A'))
        q = to_narwhals(to_duckdb(q).select("record_number, substr(value, 6, 11) AS date_created_raw, try_cast(try_strptime(date_created_raw, '%d-%m-%y') AS DATE) AS date_created"))
    else:
        print(f"Skipping cataloguing date extraction for dataset {dataset} with standard {standard}")
        return
    subqueries.append(q
                        .join(e_id.filter(c('source') == dataset), left_on='record_number', right_on='i_id')
                        .select(c('e_id'), c('date_created'), c('date_created_raw')))
    
subqueries.clear()
iter_datasets(extract_cataloguing_date)

to_parquet("p_cataloguing_date", here("data/unified/p_cataloguing_date.parquet"), nw.concat(subqueries).sort('e_id'))
# %%

def extract_last_modification_datetime(dataset: str, standard: str):
    if standard in {'marc21', 'unimarc'}:
        q = (globals()[dataset].filter(c('field_code')=='005'))
        q = to_narwhals(to_duckdb(q).select("record_number, value AS last_modification_datetime_raw, try_strptime(last_modification_datetime_raw, '%Y%m%d%H%M%S.%f') AS last_modification_datetime"))
    elif standard == 'danmarc':
        q = (globals()[dataset].filter(c('field_code')=='001', c('subfield_code') == 'c'))
        q = to_narwhals(to_duckdb(q).select("record_number, value AS last_modification_datetime_raw, try_strptime(last_modification_datetime_raw, '%Y%m%d%H%M%S') AS last_modification_datetime"))
    elif standard == 'pica':
        q = (
            globals()[dataset].filter(c('field_code')=='001B', c('subfield_code') == '0')
            .join(
                globals()[dataset].filter(c('field_code')=='001B', c('subfield_code') == 't'),
                on='record_number'
            )
        )
        q = to_narwhals(to_duckdb(q).select("record_number, concat(substr(value, 6, 11), 'T', value_right) AS last_modification_datetime_raw, try_strptime(last_modification_datetime_raw, '%d-%m-%yT%H:%M:%S.%f') AS last_modification_datetime"))
    else:
        print(f"Skipping last modification datetime extraction for dataset {dataset} with standard {standard}")
        return
    subqueries.append(q
                        .join(e_id.filter(c('source') == dataset), left_on='record_number', right_on='i_id')
                        .select(c('e_id'), c('last_modification_datetime'), c('last_modification_datetime_raw')))
    
subqueries.clear()
iter_datasets(extract_last_modification_datetime)

to_parquet("p_last_modification_datetime", here("data/unified/p_last_modification_datetime.parquet"), nw.concat(subqueries).sort('e_id'))
# %%

def extract_publication_place(dataset: str, standard: str):
    if standard == 'vd17':
        q = (vd17
            .filter(c('field_code') == '033D', c('subfield_code') == '7', c('value').str.starts_with('gnd/'))
            .select(c('record_number'), c('value').str.slice(len('gnd/')).alias('value'))
        )
    else:
        print(f"Skipping publication place extraction for dataset {dataset} with standard {standard}")
        return
    subqueries.append(q
                        .join(e_id.filter(c('source') == dataset), left_on='record_number', right_on='i_id')
                        .select(c('e_id'), c('value').alias('publication_place')))
    
subqueries.clear()
iter_datasets(extract_publication_place)

to_parquet("p_publication_place", here("data/unified/p_publication_place.parquet"), nw.concat(subqueries).sort('e_id'))

# %%

to_duckdb(vd17
            .filter(c('field_code') == '033D', c('subfield_code') == '7', c('value').str.starts_with('gnd/'))
            .join(
    vd17
            .filter(c('field_code') == '033D', c('subfield_code') == '7', c('value').str.starts_with('gnd/'))
            .select(c('record_number'), c('value').str.slice(len('gnd/')).alias('id'))
            .join(id_mappings.filter(c('source_id_type') == 'gnd', c('target_id_type') == 'wikidata'), left_on='id', right_on='source_id')
            .join(wd_entities, left_on='target_id', right_on='id')
            .join(e_id.filter(c('source') == 'vd17'), left_on='record_number', right_on='i_id')
            .join(e_id.filter(c('source') == 'wikidata'), left_on='entity_id', right_on='i_id')
            .select(c('e_id'), c('e_id_right'))            
            , on=["record_number", "field_number"], how="anti")
)
# %%
