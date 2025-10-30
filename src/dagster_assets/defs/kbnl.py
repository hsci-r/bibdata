

import dagster as dg
from dagster_assets.utils import get_date_from_file_modification_time, log_and_run, create_overview, run_bibxml2

work_file = "data/work/kbnl.mrcx.gz"
parquet_file = "data/kbnl/kbnl.parquet"


@dg.asset(pool="download")
def kbnl_crawl(context: dg.AssetExecutionContext):
    cmd = f"python src/crawl-oai-pmh.py -e https://services.kb.nl/mdo/oai -o {work_file} -p picaplus -s GGC -f 2013-01-01"
    log_and_run(cmd, context)


@dg.asset(deps=[kbnl_crawl], pool="parquet")
def kbnl_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, work_file, 'marc')


@dg.asset(deps=[kbnl_parquet], pool="overview")
def kbnl_overview(context: dg.AssetExecutionContext):
    create_overview(
        context,
        name="Dutch National Library",
        data_glob=parquet_file,
        date_modified=get_date_from_file_modification_time(work_file),
        fields_file="data/schema-info/gcc_pica_fields.tsv",
        subfields_file="data/schema-info/gcc_pica_subfields.tsv",
        output_file="data/kbnl/kbnl-overview.html"
    )

