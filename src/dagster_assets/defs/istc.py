

import uuid
import dagster as dg
import requests
from dagster_assets.utils import download_file, create_overview, get_date_from_last_modified_file, get_etag, get_parquet_glob_sha1sum, log_and_run

source_url =  "https://bl.iro.bl.uk/downloads/2b584243-64ee-49b5-ad69-5f7fb259cdf5?locale=en"
work_file = "data/work/istc_clean_1.0.yaml"
parquet_file = "data/istc/istc.parquet"

@dg.observable_source_asset
def istc_data() -> dg.DataVersion:
    return get_etag(source_url)
    
@dg.asset(deps=[istc_data], pool="download")
def istc_download(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    raise ValueError("ISTC requires manual downloading because it does not have a stable direct URL")
    return download_file(context, source_url, work_file)

@dg.asset(deps=[istc_download], pool="parquet")
def istc_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    cmd = f"python src/process-istc.py"
    log_and_run(cmd, context)
    return get_parquet_glob_sha1sum(parquet_file)

@dg.asset(deps=[istc_parquet], pool="overview")
def istc_overview(context: dg.AssetExecutionContext):
    create_overview(
        context,
        name="Incunabula Short Title Catalogue",
        data_glob=parquet_file,
        date_modified=get_date_from_last_modified_file(work_file),
        fields_file="data/schema-info/k10_pica_fields.tsv",
        subfields_file="data/schema-info/k10_pica_subfields.tsv",
        output_file="data/istc/istc-overview.html"
    )
