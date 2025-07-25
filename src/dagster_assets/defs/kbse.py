

import dagster as dg
from dagster_assets.utils import get_date_from_file_modification_time, log_and_run, create_overview, run_bibxml2

work_file = "data/work/kbse.mrcx.gz"
parquet_file = "data/kbse/kbse.parquet"


@dg.asset(pool="download")
def kbse_crawl(context: dg.AssetExecutionContext):
    cmd = f"python src/crawl-oai-pmh.py -e https://libris.kb.se/api/oaipmh/ -o {work_file} -p marcxml_includehold_expanded"
    log_and_run(cmd, context)


@dg.asset(deps=[kbse_crawl], pool="parquet")
def kbse_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, work_file, 'marc')


@dg.asset(deps=[kbse_parquet], pool="overview")
def kbse_overview(context: dg.AssetExecutionContext):
    create_overview(
        context,
        name="Dutch National Library",
        data_glob=parquet_file,
        date_modified=get_date_from_file_modification_time(work_file),
        fields_file="data/schema-info/gcc_pica_fields.tsv",
        subfields_file="data/schema-info/gcc_pica_subfields.tsv",
        output_file="data/kbse/kbse-overview.html"
    )

