
import dagster as dg
from dagster_assets.utils import create_rdf_overview, get_date_from_last_modified_file, get_etag, download_file, get_parquet_glob_sha1sum, log_and_run

source_url = "http://tgndownloads.getty.edu/VocabData/explicit.zip"
work_file = "data/work/tgn/explicit.zip"
parquet_file = "data/tgn/tgn.parquet"

@dg.observable_source_asset
def tgn_data() -> dg.DataVersion:
    return get_etag(source_url)

@dg.asset(deps=[tgn_data], pool="download")
def tgn_download(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return download_file(context, source_url, work_file)

@dg.asset(deps=[tgn_download], pool="parquet")
def tgn_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    cmd = f"python src/process-ntriples.py -o {parquet_file} -p data/schema-info/lod_prefixes.tsv zip://*::{work_file}"
    log_and_run(cmd, context)
    return get_parquet_glob_sha1sum(parquet_file)

@dg.asset(deps=[tgn_parquet], pool="overview")
def tgn_overview(context: dg.AssetExecutionContext):
    create_rdf_overview(
        context,
        name="Getty Thesaurus of Geographic Names",
        data_glob=parquet_file,
        date_modified=get_date_from_last_modified_file(work_file),
        properties_file="data/schema-info/rdf_properties.tsv",
        output_file="data/tgn/tgn-overview.html"
    )