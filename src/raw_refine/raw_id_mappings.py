#%%
import glob
from pathlib import Path

from duckdb import DuckDBPyRelation
from raw_refine.core import *

e_to_natdk = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_stcv = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_dnb = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_istc = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_hpb = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_vd17 = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_cnb = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_gnd = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_wd = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_kbse = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_iri = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_fennica = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_ptnb = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_plnb = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_melinda = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_viaf = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_geonames = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_natno = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_bnf = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_isni = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_vd18 = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_erb = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_stcn = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_estc = cast(nw.LazyFrame[DuckDBPyRelation], None)
e_to_kbnl = cast(nw.LazyFrame[DuckDBPyRelation], None)

for row in glob.glob(str(here("data/work/raw_id_mappings/*.parquet"))):
    name = Path(row).stem
    globals()[name] = read_parquet(name, here(f"data/work/raw_id_mappings/{name}.parquet"))
    #print(f"{name} = cast(nw.LazyFrame[DuckDBPyRelation], None)")

__all__ = ["e_to_natdk", "e_to_stcv", "e_to_dnb", "e_to_istc", "e_to_hpb", "e_to_vd17", "e_to_cnb", "e_to_gnd", "e_to_wd", "e_to_kbse", "e_to_iri", "e_to_fennica", "e_to_ptnb", "e_to_plnb", "e_to_melinda", "e_to_viaf", "e_to_geonames", "e_to_natno", "e_to_bnf", "e_to_isni", "e_to_vd18", "e_to_erb", "e_to_stcn", "e_to_estc", "e_to_kbnl"]