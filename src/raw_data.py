from typing import Callable

from sqlalchemy import func

from core import *
from duckdb import DuckDBPyRelation
import re

bnf = cast(nw.LazyFrame[DuckDBPyRelation], None)
cnb = cast(nw.LazyFrame[DuckDBPyRelation], None)
cerl_thesaurus = cast(nw.LazyFrame[DuckDBPyRelation], None)
dbnf = cast(nw.LazyFrame[DuckDBPyRelation], None)
dnb = cast(nw.LazyFrame[DuckDBPyRelation], None)
erb = cast(nw.LazyFrame[DuckDBPyRelation], None)
estc = cast(nw.LazyFrame[DuckDBPyRelation], None)
fennica = cast(nw.LazyFrame[DuckDBPyRelation], None)
geonames = cast(nw.LazyFrame[DuckDBPyRelation], None)
geonames_alternate_names = cast(nw.LazyFrame[DuckDBPyRelation], None)
gnd = cast(nw.LazyFrame[DuckDBPyRelation], None)
hpb = cast(nw.LazyFrame[DuckDBPyRelation], None)
idloc = cast(nw.LazyFrame[DuckDBPyRelation], None)
isni_same_as = cast(nw.LazyFrame[DuckDBPyRelation], None)
isni_deprecated_isnis = cast(nw.LazyFrame[DuckDBPyRelation], None)
isni_source_ids = cast(nw.LazyFrame[DuckDBPyRelation], None)
isni_authority_ids = cast(nw.LazyFrame[DuckDBPyRelation], None)
isni_names = cast(nw.LazyFrame[DuckDBPyRelation], None)
isni_core = cast(nw.LazyFrame[DuckDBPyRelation], None)
istc = cast(nw.LazyFrame[DuckDBPyRelation], None)
kbnl = cast(nw.LazyFrame[DuckDBPyRelation], None)
kbse = cast(nw.LazyFrame[DuckDBPyRelation], None)
melinda = cast(nw.LazyFrame[DuckDBPyRelation], None)
natdk = cast(nw.LazyFrame[DuckDBPyRelation], None)
natno = cast(nw.LazyFrame[DuckDBPyRelation], None)
plnb = cast(nw.LazyFrame[DuckDBPyRelation], None)
ptnb = cast(nw.LazyFrame[DuckDBPyRelation], None)
stcn = cast(nw.LazyFrame[DuckDBPyRelation], None)
stcv = cast(nw.LazyFrame[DuckDBPyRelation], None)
tgn = cast(nw.LazyFrame[DuckDBPyRelation], None)
ulan = cast(nw.LazyFrame[DuckDBPyRelation], None)
vd17 = cast(nw.LazyFrame[DuckDBPyRelation], None)
vd18 = cast(nw.LazyFrame[DuckDBPyRelation], None)
viaf = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_claim_quantity = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_descriptions = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_labels = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_qualifier_monolingualtext = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_reference_string = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_claim_no_value = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_aliases = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_entities = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_qualifier_quantity = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_qualifier_globecoordinate = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_claim_wikibase_entityid = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_claim_globecoordinate = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_reference_monolingualtext = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_claim_some_value = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_datatypes = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_sitelink_badges = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_qualifier_some_value = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_reference_some_value = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_claim_time = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_reference_quantity = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_reference_wikibase_entityid = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_reference_time = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_qualifier_no_value = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_qualifier_time = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_qualifier_string = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_reference_no_value = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_qualifier_wikibase_entityid = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_reference_globecoordinate = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_claim_string = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_claim_monolingualtext = cast(nw.LazyFrame[DuckDBPyRelation], None)
wd_sitelinks = cast(nw.LazyFrame[DuckDBPyRelation], None)

collection_info = nw.from_native(con.read_csv(here("data/schema-info/collection_info.tsv")))

def iter_datasets(func: Callable[[str, str], None]):
    for row in (pbar := tqdm(list(collection_info.collect(backend='polars').iter_rows(named=True)))):
        dataset = row['dataset']
        standard = row['standard']
        pbar.set_description(f"{dataset} ({standard})")
        func(dataset, standard)

def register_dataset(dataset: str, standard: str):
    groups: dict[str, list] = {}
    for file in sorted(here(f"data/{dataset}").glob("*.parquet")):
        table_name = re.sub(r"_\d+$", "", file.stem)
        groups.setdefault(table_name, []).append(file)
    for table_name, files in groups.items():
        globals()[table_name] = read_parquet(table_name, *files)

iter_datasets(register_dataset)

def f(dataset: str) -> nw.LazyFrame[duckdb.DuckDBPyRelation]:
    return cast(nw.LazyFrame[duckdb.DuckDBPyRelation], globals()[dataset] if dataset in globals() else None) 

__all__ = ["f", "bnf", "cnb", "cerl_thesaurus", "dbnf", "dnb", "erb", "estc", "fennica", "geonames", "geonames_alternate_names", "gnd", "hpb", "idloc", "isni_same_as", "isni_deprecated_isnis", "isni_source_ids", "isni_authority_ids", "isni_names", "isni_core", "istc", "kbnl", "kbse", "melinda", "natdk", "natno", "plnb", "ptnb", "stcn", "stcv", "tgn", "ulan", "vd17", "vd18", "viaf", "wd_claim_quantity", "wd_descriptions", "wd_labels", "wd_qualifier_monolingualtext", "wd_reference_string", "wd_claim_no_value", "wd_aliases", "wd_entities", "wd_qualifier_quantity", "wd_qualifier_globecoordinate", "wd_claim_wikibase_entityid", "wd_claim_globecoordinate", "wd_reference_monolingualtext", "wd_claim_some_value", "wd_datatypes", "wd_sitelink_badges", "wd_qualifier_some_value", "wd_reference_some_value", "wd_claim_time", "wd_reference_quantity", "wd_reference_wikibase_entityid", "wd_reference_time", "wd_qualifier_no_value", "wd_qualifier_time", "wd_qualifier_string", "wd_reference_no_value", "wd_qualifier_wikibase_entityid", "wd_reference_globecoordinate", "wd_claim_string", "wd_claim_monolingualtext", "wd_sitelinks", "collection_info", "iter_datasets"]