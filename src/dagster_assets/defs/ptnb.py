
import dagster as dg
from dagster_assets.utils import get_date_from_last_modified_file, get_etag, create_overview, download_file, run_bibxml2

source_url = "https://opendata.bnportugal.gov.pt/docs/catalogo.marcxchange.zip"
work_file = "data/work/ptnb/catalogo.marcxchange.zip"
parquet_file = "data/ptnb/ptnb.parquet"

@dg.observable_source_asset
def ptnb_data() -> dg.DataVersion:
    return get_etag(source_url)

@dg.asset(deps=[ptnb_data], pool="download")
def ptnb_download(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return download_file(context, source_url, work_file)

@dg.asset(deps=[ptnb_download], pool="parquet")
def ptnb_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, f"zip://*::{work_file}", 'marc', no_input_glob=True)
    

@dg.asset(deps=[ptnb_parquet], pool="overview")
def ptnb_overview(context: dg.AssetExecutionContext):
    create_overview(
        context,
        name="Portuguese National Bibliography",
        data_glob=parquet_file,
        date_modified=get_date_from_last_modified_file(work_file),
        fields_file=None,
        subfields_file=None,
        output_file="data/ptnb/ptnb-overview.html"
    )
    
