

import dagster as dg
from dagster_assets.utils import get_date_from_last_modified_file, log_and_run, create_overview, run_bibxml2

work_file = "data/work/erb.mrcx.gz"
parquet_file = "data/erb/erb.parquet"


@dg.asset(pool="download")
def erb_crawl(context: dg.AssetExecutionContext):
    # No curl call here, skipping refactor
    cmd = f"python src/crawl-oai-pmh.py -e https://data.digar.ee/repox/OAIHandler -o {work_file} -p marc21xml -s erb"
    log_and_run(cmd, context)


@dg.asset(deps=[erb_crawl], pool="parquet")
def erb_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, work_file, 'marc')


@dg.asset(deps=[erb_parquet], pool="overview")
def erb_overview(context: dg.AssetExecutionContext):
    create_overview(
        context,
        name="Estonian National Bibliography",
        data_glob=parquet_file,
        date_modified=get_date_from_last_modified_file(work_file),
        fields_file="data/schema-info/marc_fields.tsv",
        subfields_file="data/schema-info/marc_subfields.tsv",
        output_file="data/erb/erb-overview.html"
    )

