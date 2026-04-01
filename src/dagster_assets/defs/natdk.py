

import dagster as dg
from dagster_assets.utils import get_date_from_file_modification_time, log_and_run, create_bib_overview, run_bibxml2

work_file = "data/work/natdk.mrcx.gz"
parquet_file = "data/natdk/natdk.parquet"


@dg.asset(pool="download")
def natdk_crawl(context: dg.AssetExecutionContext):
    # No curl call here, skipping refactor
    cmd = f"python src/raw_procure/crawl-oai-pmh.py -e https://oai.addi.dk/oai -o {work_file} -p marcx -s nat"
    log_and_run(cmd, context)


@dg.asset(deps=[natdk_crawl], pool="parquet")
def natdk_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, work_file, 'marc')


@dg.asset(deps=[natdk_parquet], pool="overview")
def natdk_overview(context: dg.AssetExecutionContext):
    create_bib_overview(
        context,
        name="Danish National Bibliography",
        data_glob=parquet_file,
        date_modified=get_date_from_file_modification_time(work_file),
        fields_file=None,
        subfields_file=None,
        output_file="data/natdk/natdk-overview.html"
    )
