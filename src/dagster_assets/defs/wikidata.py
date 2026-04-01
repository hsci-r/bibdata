import os
import dagster as dg
from hereutil import here
from dagster_assets.utils import create_bib_overview, get_parquet_glob_sha1sum, log_and_run, run_bibxml2, get_etag, download_file

source_url = "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2"
input_file = "data/work/wikidata/latest-all.json.bz2"
work_dir = "data/work/wikidata"
parquet_dir = "data/wikidata"

@dg.observable_source_asset()
def wikidata_data() -> dg.DataVersion:
    return get_etag(source_url)

@dg.asset(deps=[wikidata_data], pool="download")
def wikidata_download(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return download_file(context, source_url, input_file)

@dg.asset(deps=[wikidata_download])
def wikidata_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    processors = os.cpu_count() or 8
    lbzcat_processors = max(1, processors * 2 // 5)
    process_processors = max(1, processors - lbzcat_processors)
    log_and_run(f"lbzcat -n {lbzcat_processors} {input_file} | pv -r -a -m 3600 -s 118704633 -p -b -l -e -t -v | target/release/process-wikidata -b 16384 -t {process_processors} -o {work_dir}", context)
    return get_parquet_glob_sha1sum("data/wikidata/.parquet")
    
@dg.asset(deps=[wikidata_parquet], pool="overview")
def wikidata_overview(context: dg.AssetExecutionContext):
    cmd = f"Rscript -e \"rmarkdown::render('src/raw_overview/wikidata-overview.Rmd', output_file = '../../data/wikidata/wikidata-overview.html')\""
    log_and_run(cmd, context)
