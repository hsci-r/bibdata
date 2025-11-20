
import dagster as dg
from hereutil import here
from dagster_assets.utils import get_etag, download_file, get_parquet_glob_sha1sum, log_and_run

source_url_people = "https://isni.oclc.org:2443/isni/public_export/ISNI_persons.jsonld.gz"
source_url_organisations = "https://isni.oclc.org:2443/isni/public_export/ISNI_organizations.jsonld.gz"
work_file_people = "data/work/isni/ISNI_persons.jsonld.gz"
work_file_organisations = "data/work/isni/ISNI_organizations.jsonld.gz"
parquet_file = "data/isni/"

@dg.observable_source_asset
def isni_people_data() -> dg.DataVersion:
    return get_etag(source_url_people)

@dg.asset(deps=[isni_people_data], pool="download")
def isni_people_download(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return download_file(context, source_url_people, work_file_people)

@dg.observable_source_asset
def isni_organisations_data() -> dg.DataVersion:
    return get_etag(source_url_organisations)

@dg.asset(deps=[isni_organisations_data], pool="download")
def isni_organisations_download(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return download_file(context, source_url_organisations, work_file_organisations)

@dg.asset(deps=[isni_people_download, isni_organisations_download], pool="parquet")
def isni_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    cmd = f"python src/process-isni.py -o {parquet_file} -p data/schema-info/lod_prefixes.tsv {work_file_people} {work_file_organisations}"
    log_and_run(cmd, context)
    return get_parquet_glob_sha1sum("data/isni/.parquet")

@dg.asset(deps=[isni_parquet], pool="overview")
def isni_overview(context: dg.AssetExecutionContext):
    cmd = f"Rscript -e \"rmarkdown::render('src/isni-overview.Rmd', output_file = '../data/isni/isni-overview.html')\""
    log_and_run(cmd, context)