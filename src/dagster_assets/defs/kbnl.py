

import dagster as dg
from dagster_assets.utils import get_date_from_file_modification_time, log_and_run, create_bib_overview, run_bibxml2

work_dir = "data/work/kbnl"
parquet_file = "data/kbnl/kbnl.parquet"


@dg.asset(backfill_policy=dg.BackfillPolicy.multi_run(1), partitions_def=dg.WeeklyPartitionsDefinition(start_date="2013-01-01", day_offset=2), pool="kbnl_api")
def kbnl_crawl(context: dg.AssetExecutionContext):
    start = context.partition_time_window.start.strftime("%Y-%m-%d")
    end = context.partition_time_window.end.strftime("%Y-%m-%d")
    cmd = f"python src/crawl-oai-pmh.py -e https://services.kb.nl/mdo/oai -o {work_dir}/{start}.mrcx.zst -p picaplus -s GGC  -f {start if start != '2013-01-01' else '0000-01-01'} -u {end}"
    log_and_run(cmd, context)


@dg.asset(deps=[kbnl_crawl], pool="parquet")
def kbnl_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, f"{work_dir}/*.mrcx.zst", 'pica')


@dg.asset(deps=[kbnl_parquet], pool="overview")
def kbnl_overview(context: dg.AssetExecutionContext):
    create_bib_overview(
        context,
        name="Dutch National Library",
        data_glob=parquet_file,
        date_modified=get_date_from_file_modification_time(f"{work_dir}/*.mrcx.zst"),
        fields_file="data/schema-info/gcc_pica_fields.tsv",
        subfields_file="data/schema-info/gcc_pica_subfields.tsv",
        output_file="data/kbnl/kbnl-overview.html"
    )

