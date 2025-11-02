
import dagster as dg
from dagster_assets.utils import get_etag, download_file, get_parquet_glob_sha1sum, log_and_run

source_url = "http://ulandownloads.getty.edu/VocabData/explicit.zip"
work_file = "data/work/ulan/explicit.zip"
parquet_file = "data/ulan/ulan.parquet"

@dg.observable_source_asset
def ulan_data() -> dg.DataVersion:
    return get_etag(source_url)

@dg.asset(deps=[ulan_data], pool="download")
def ulan_download(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return download_file(context, source_url, work_file)

@dg.asset(deps=[ulan_download], pool="parquet")
def ulan_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    cmd = f"python src/process-ntriples.py -o {parquet_file} -p data/schema-info/lod_prefixes.tsv zip://*::{work_file}"
    log_and_run(cmd, context)
    return get_parquet_glob_sha1sum(parquet_file)