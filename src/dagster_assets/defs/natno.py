

import dagster as dg
from dagster_assets.utils import get_date_from_file_modification_time, log_and_run, create_bib_overview, run_bibxml2

work_file = "data/work/natno.mrcx.gz"
parquet_file = "data/natno/natno.parquet"


@dg.asset(pool="download")
def natno_crawl(context: dg.AssetExecutionContext):
    # No curl call here, skipping refactor
    cmd = f"python src/raw_procure/crawl-oai-pmh.py -e https://bibsys.alma.exlibrisgroup.com/view/oai/47BIBSYS_NETWORK/request -o {work_file} -p marc21 -s nasjonalbibliografien"
    log_and_run(cmd, context)


@dg.asset(deps=[natno_crawl], pool="parquet")
def natno_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, work_file, 'marc')


@dg.asset(deps=[natno_parquet], pool="overview")
def natno_overview(context: dg.AssetExecutionContext):
    create_bib_overview(
        context,
        name="Norwegian National Bibliography",
        data_glob=parquet_file,
        date_modified=get_date_from_file_modification_time(work_file),
        fields_file="data/schema-info/marc_fields.tsv",
        subfields_file="data/schema-info/marc_subfields.tsv",
        output_file="data/natno/natno-overview.html"
    )
