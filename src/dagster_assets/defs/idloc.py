
import dagster as dg
from dagster_assets.defs.lod import lod_prefixes
from dagster_assets.utils import create_rdf_overview, get_date_from_last_modified_file, get_etag, download_file, get_parquet_glob_sha1sum, log_and_run

source_url = "https://lds-downloads.s3.amazonaws.com/authorities/names.madsrdf.nt.gz"
work_file = "data/work/idloc/names.madsrdf.nt.gz"
parquet_file = "data/idloc/idloc.parquet"

@dg.observable_source_asset
def idloc_data() -> dg.DataVersion:
    return get_etag(source_url)

@dg.asset(deps=[idloc_data], pool="download")
def idloc_download(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return download_file(context, source_url, work_file)

@dg.asset(deps=[idloc_download, lod_prefixes], pool="parquet")
def idloc_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    cmd = f"python src/raw_to_parquet/process-ntriples.py -k idloc -o {parquet_file} -p data/schema-info/lod_prefixes.tsv {work_file}"
    log_and_run(cmd, context)
    return get_parquet_glob_sha1sum(parquet_file)

@dg.asset(deps=[idloc_parquet], pool="overview")
def idloc_overview(context: dg.AssetExecutionContext):
    create_rdf_overview(
        context,
        name="Library of Congress Identities Linked Data",
        data_glob=parquet_file,
        date_modified=get_date_from_last_modified_file(work_file),
        properties_file="data/schema-info/rdf_properties.tsv",
        output_file="data/idloc/idloc-overview.html"
    )
