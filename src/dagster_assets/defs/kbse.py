import dagster as dg
from dagster_assets.utils import get_date_from_file_modification_time, log_and_run, create_bib_overview, run_bibxml2

work_dir = "data/work/kbse/"
parquet_file = "data/kbse/kbse.parquet"


#@dg.asset(pool="download")
#def kbse_crawl(context: dg.AssetExecutionContext):
#    cmd = f"python src/raw_procure/crawl-oai-pmh.py -e https://libris.kb.se/api/oaipmh/ -o {work_file} -p marcxml_includehold_expanded"
#    log_and_run(cmd, context)

#@dg.asset(backfill_policy=dg.BackfillPolicy.multi_run(1), partitions_def=dg.MonthlyPartitionsDefinition(start_date="2002-01", fmt="%Y-%m"), pool="kbse_api")
@dg.asset(backfill_policy=dg.BackfillPolicy.multi_run(1), partitions_def=dg.WeeklyPartitionsDefinition(start_date="2002-01-01", day_offset=2), pool="kbse_api")
def kbse_crawl(context: dg.AssetExecutionContext):
    start = context.partition_time_window.start.strftime("%Y-%m-%d")
    end = context.partition_time_window.end.strftime("%Y-%m-%d")
    cmd = f"python src/raw_procure/crawl-oai-pmh.py -e https://libris.kb.se/api/oaipmh/ -o {work_dir}/{start}.mrcx.zst -p marcxml_includehold_expanded -f {start if start != '2002-01-01' else '0000-01-01'} -u {end}"
    log_and_run(cmd, context)


@dg.asset(deps=[kbse_crawl], pool="parquet")
def kbse_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, f"{work_dir}/*.mrcx.zst", 'marc')


@dg.asset(deps=[kbse_parquet], pool="overview")
def kbse_overview(context: dg.AssetExecutionContext):
    create_bib_overview(
        context,
        name="Swedish National Library",
        data_glob=parquet_file,
        date_modified=get_date_from_file_modification_time(f"{work_dir}/*.mrcx.zst"),
        fields_file="data/schema-info/gcc_pica_fields.tsv",
        subfields_file="data/schema-info/gcc_pica_subfields.tsv",
        output_file="data/kbse/kbse-overview.html"
    )
