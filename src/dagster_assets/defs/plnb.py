
import dagster as dg
from dagster_assets.utils import get_date_from_last_modified_file, get_etag, log_and_run, create_overview, download_file, run_bibxml2

source_url = "https://data.bn.org.pl/db/institutions/bibs-all.marc"
work_dir = "data/work/plnb"
parquet_file = "data/plnb/plnb.parquet"

@dg.observable_source_asset
def plnb_data() -> dg.DataVersion:
    return get_etag(source_url)

@dg.asset(deps=[plnb_data], pool="download")
def plnb_download(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return download_file(context, source_url, work_dir + "/bibs-all.marc")

@dg.asset(deps=[plnb_download], pool="parquet")
def plnb_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, f"{work_dir}/bibs-all.marc", 'marc')

@dg.asset(deps=[plnb_parquet], pool="overview")
def plnb_overview(context: dg.AssetExecutionContext):
    create_overview(
        context,
        name="Polish National Bibliography",
        data_glob=parquet_file,
        date_modified=get_date_from_last_modified_file(work_dir + "/bibs-all.marc"),
        fields_file="data/schema-info/marc_fields.tsv",
        subfields_file="data/schema-info/marc_subfields.tsv",
        output_file="data/plnb/plnb-overview.html"
    )
