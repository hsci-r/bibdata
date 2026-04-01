#%%
from raw_refine.core import *
from raw_refine.raw_data import *

#%%

max_e_id = 0

def e_to_x(e_to_x_name: str, q: nw.LazyFrame, select: nw.Expr, e_id_local: nw.Expr, sort: str, max_e_id: int) -> tuple[nw.LazyFrame, int]:
    e_to_x_tt = to_parquet(e_to_x_name, here(f"data/work/raw_id_mappings/{e_to_x_name}.parquet"), q.select(select, (e_id_local + nw.lit(max_e_id)).alias('e_id')).sort(sort))
    max_e_id = e_to_x_tt.select(nw.col("e_id").max()).collect()['e_id'][0]
    return e_to_x_tt, max_e_id

con.sql("DROP TABLE IF EXISTS e_to_iri_tmp;")
con.sql(f"CREATE OR REPLACE TEMPORARY SEQUENCE e_id_seq START {max_e_id + 1};")
con.sql("CREATE OR REPLACE TEMPORARY TABLE e_to_iri_tmp (iri STRING, e_id BIGINT NOT NULL DEFAULT nextval('e_id_seq'));")

for row in (pbar := tqdm(list(formats.collect(backend='polars').iter_rows(named=True)))):
    dataset = row['dataset']
    standard = row['standard']
    pbar.set_description(f"{dataset} ({standard})")
    if dataset == "wikidata":
        e_to_wd, max_e_id = e_to_x('e_to_wd', wd_entities, nw.col('entity_id'), nw.col('entity_id'), 'entity_id', max_e_id)
    elif dataset == "isni":
        e_to_isni, max_e_id = e_to_x('e_to_isni', isni_core, nw.col('isni_n'), nw.col('isni_n'), 'isni_n', max_e_id)
    elif dataset == "geonames":
        e_to_geonames, max_e_id = e_to_x('e_to_geonames', geonames, nw.col('geonameid'), nw.col('geonameid'), 'geonameid', max_e_id)
    elif standard == "rdf":
        table = to_duckdb(globals()[dataset]).sql_query() 
        spo = tqdm(total=3, leave=False)
        spo.set_description("p")
        con.sql(f"INSERT INTO e_to_iri_tmp (iri) SELECT DISTINCT property FROM ({table}) WHERE property NOT IN (SELECT iri FROM e_to_iri_tmp);")
        spo.update(1)
        spo.set_description("o")
        con.sql(f"INSERT INTO e_to_iri_tmp (iri) SELECT DISTINCT object FROM ({table}) WHERE datatype_lang='xs:anyURI' AND object NOT IN (SELECT iri FROM e_to_iri_tmp);")
        spo.update(1)
        spo.set_description("s")
        con.sql(f"INSERT INTO e_to_iri_tmp (iri) SELECT DISTINCT subject FROM ({table}) WHERE subject NOT IN (SELECT iri FROM e_to_iri_tmp);")
        spo.update(1)
        spo.close()
        e_to_iri_tmp = to_narwhals(con.table("e_to_iri_tmp"))
        max_e_id = e_to_iri_tmp.select(nw.col("e_id").max()).collect()['e_id'][0]
    elif standard in {"intermarc", "marc21", "unimarc", "pica", "istc", "danmarc2"}:
        e_to_gnd, max_e_id = e_to_x(f'e_to_{dataset}', globals()[dataset].group_by('record_number').agg(), nw.col('record_number'), nw.col('record_number'), 'record_number', max_e_id)
    else:
        raise ValueError(f"Unknown dataset standard {standard} for dataset {dataset}")

to_parquet("e_to_iri", here("data/work/raw_id_mappings/e_to_iri.parquet"), e_to_iri_tmp.sort('iri'))
# %%
