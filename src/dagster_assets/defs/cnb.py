
import dagster as dg
from dagster_assets.utils import get_etag, create_overview, download_file, run_bibxml2, get_date_from_last_modified_file

source_url = "https://aleph.nkp.cz/data/cnb.xml.gz"
work_file = "data/work/cnb.xml.gz"
parquet_file = "data/cnb/cnb.parquet"

@dg.observable_source_asset
def cnb_data() -> dg.DataVersion:
    return get_etag(source_url)

@dg.asset(deps=[cnb_data], pool="download")
def cnb_download(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return download_file(context, source_url, work_file)

@dg.asset(deps=[cnb_download], pool="parquet")
def cnb_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, work_file, 'marc')

@dg.asset(deps=[cnb_parquet], pool="overview")
def cnb_overview(context: dg.AssetExecutionContext):
    create_overview(
        context,
        name="Czech National Bibliography",
        data_glob=parquet_file,
        date_modified=get_date_from_last_modified_file(work_file),
        fields_file="data/schema-info/marc_fields.tsv",
        subfields_file="data/schema-info/marc_subfields.tsv",
        output_file="data/cnb/cnb-overview.html"
    )
