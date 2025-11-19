
import dagster as dg
from dagster_assets.utils import get_etag, create_bib_overview, download_file, get_parquet_glob_sha1sum, log_and_run, run_bibxml2, get_date_from_last_modified_file

source_url = "https://download.geonames.org/export/dump/allCountries.zip"
work_file = "data/work/geonames/allCountries.zip"
altnames_url =  "https://download.geonames.org/export/dump/alternateNamesV2.zip"
altnames_work_file = "data/work/geonames/alternateNamesV2.zip"
parquet_file = "data/geonames/.parquet"

@dg.observable_source_asset
def geonames_data() -> dg.DataVersion:
    return get_etag(source_url)

@dg.asset(deps=[geonames_data], pool="download")
def geonames_download(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return download_file(context, source_url, work_file)

@dg.observable_source_asset
def altnames_data() -> dg.DataVersion:
    return get_etag(altnames_url)

@dg.asset(deps=[altnames_data], pool="download")
def altnames_download(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return download_file(context, altnames_url, altnames_work_file)

@dg.asset(deps=[geonames_download, altnames_download], pool="parquet")
def geonames_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    cmd = f"python src/process-geonames.py"
    log_and_run(cmd, context)
    return get_parquet_glob_sha1sum(parquet_file)