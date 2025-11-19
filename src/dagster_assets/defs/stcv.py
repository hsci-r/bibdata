

import dagster as dg
from dagster_assets.utils import download_file, get_date_from_file_modification_time, get_etag, log_and_run, create_bib_overview, run_bibxml2

source_url = "https://anet.be/opendata/stcv/stcv_marc.xml.gz"
work_file = "data/work/stcv_marc.xml.gz"
parquet_file = "data/stcv/stcv.parquet"

@dg.observable_source_asset
def stcv_data() -> dg.DataVersion:
    return get_etag(source_url)

@dg.asset(deps=[stcv_data], pool="download")
def stcv_download(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return download_file(context, source_url, work_file)

@dg.asset(deps=[stcv_download], pool="parquet")
def stcv_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, work_file, 'marc')

@dg.asset(deps=[stcv_parquet], pool="overview")
def stcv_overview(context: dg.AssetExecutionContext):
    create_bib_overview(
        context,
        name="Bibliography of the Hand Press Books in Flanders",
        data_glob=parquet_file,
        date_modified=get_date_from_file_modification_time(work_file),
        fields_file="data/schema-info/marc_fields.tsv",
        subfields_file="data/schema-info/marc_subfields.tsv",
        output_file="data/stcv/stcv-overview.html"
    )

