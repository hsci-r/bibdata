#%%
import glob
import os
import re
import shutil

from core import *
from raw_data import *

#%%

shutil.rmtree(here("data/unified"), ignore_errors=True)
os.makedirs(here("data/unified"), exist_ok=True)

con.sql("DROP TABLE IF EXISTS e_id_tmp;")
con.sql(f"CREATE OR REPLACE TEMPORARY SEQUENCE e_id_seq START 1;")
con.sql("CREATE OR REPLACE TEMPORARY TABLE e_id_tmp (e_id BIGINT NOT NULL DEFAULT nextval('e_id_seq'), i_id BIGINT DEFAULT NULL, s_id STRING DEFAULT NULL, source STRING NOT NULL);")
e_id_tmp = to_narwhals(con.table("e_id_tmp"))

def map_ids(dataset: str, standard: str):
    if dataset == "wikidata":
        con.sql("INSERT INTO e_id_tmp (i_id, source) SELECT entity_id, 'wikidata' FROM wd_entities;")
    elif dataset == "isni":
        con.sql("INSERT INTO e_id_tmp (i_id, source) SELECT isni_n, 'isni' FROM isni_core;")
    elif dataset == "geonames":
        con.sql("INSERT INTO e_id_tmp (i_id, source) SELECT geonameid, 'geonames' FROM geonames;")
    elif standard == "rdf":
#        spo = tqdm(total=3, leave=False)
#        spo.set_description("p")
#        con.sql(f"INSERT INTO e_id_tmp (s_id, source) SELECT DISTINCT property, 'iri' FROM {dataset} ANTI JOIN e_id_tmp ON (property = s_id);")
#        spo.update(1)
#        spo.set_description("o")
#        con.sql(f"INSERT INTO e_id_tmp (s_id, source) SELECT DISTINCT object, 'iri' FROM {dataset} ANTI JOIN e_id_tmp ON (object = s_id) WHERE datatype_lang='xs:anyURI';")
#        spo.update(1)
#        spo.set_description("s")
        con.sql(f"INSERT INTO e_id_tmp (s_id, source) SELECT DISTINCT subject, 'iri' FROM {dataset} ANTI JOIN e_id_tmp ON (subject = s_id);")
#        spo.update(1)
#        spo.close()
    elif standard in {"intermarc", "marc21", "unimarc", "pica", "istc", "danmarc", 'ctmarc'}:
        con.sql(f"INSERT INTO e_id_tmp (i_id, source) SELECT DISTINCT record_number, '{dataset}' FROM {dataset};")
    else:
        raise ValueError(f"Unknown dataset standard {standard} for dataset {dataset}")

iter_datasets(map_ids)

os.makedirs(here("data/unified/.e_id_tmp"), exist_ok=True)
con.sql(f"COPY (SELECT * FROM e_id_tmp) TO '{here('data/unified/.e_id_tmp')}' (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 22, FILE_SIZE_BYTES 2_000_000_000)")
for file in sorted(glob.glob(str(here("data/unified/.e_id_tmp/data_*.parquet")))):
    m = cast(re.Match, re.search(r'data_(\d+)\.parquet$', file))
    suffix = "" if m.group(1) == "0" else f"_{m.group(1)}"
    shutil.move(file, here(f"data/unified/e_id{suffix}.parquet"))
os.rmdir(here("data/unified/.e_id_tmp"))
e_id_files = sorted(here("data/unified").glob("e_id*.parquet"))
e_id = read_parquet("e_id", *e_id_files)

# %%
e_id_tmp