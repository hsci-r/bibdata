


import os
import dagster as dg
from dagster_assets.utils import get_date_from_file_modification_time, log_and_run, create_overview, run_bibxml2

work_dir = "data/work/fennica"
parquet_file = "data/fennica/fennica.parquet"

#@dg.asset(backfill_policy=dg.BackfillPolicy.multi_run(1), partitions_def=dg.MonthlyPartitionsDefinition(start_date="2019-01", fmt="%Y-%m"), pool="fennica_api")
#def fennica_crawl(context: dg.AssetExecutionContext):
#    start = context.partition_time_window.start.strftime("%Y-%m")
#    end = context.partition_time_window.end.strftime("%Y-%m")
#    cmd = f"python src/crawl-oai-pmh.py -e https://oai-pmh.api.melinda.kansalliskirjasto.fi/bib -o {work_dir}/{start}.mrcx.zst -p melinda_marc -s fennica -f {start if start != '2019-01' else '0000-01'}-01 -u {end}-01"
#    log_and_run(cmd, context)

@dg.asset(pool="fennica_api")
def fennica_crawl(context: dg.AssetExecutionContext):
#    start = context.partition_time_window.start.strftime("%Y-%m")
#    end = context.partition_time_window.end.strftime("%Y-%m")
    cmd = f"python src/crawl-oai-pmh.py -e https://oai-pmh.api.melinda.kansalliskirjasto.fi/bib -o {work_dir}/fennica.mrcx.zst -p melinda_marc -s fennica"
    log_and_run(cmd, context)    

#@dg.asset(backfill_policy=dg.BackfillPolicy.multi_run(1), partitions_def=dg.WeeklyPartitionsDefinition(start_date="2025-01-01", day_offset=3), pool="fennica_api")
#def fennica_latest_crawl(context: dg.AssetExecutionContext):
#    start = context.partition_time_window.start.strftime("%Y-%m-%d")
#    end = context.partition_time_window.end.strftime("%Y-%m-%d")
#    cmd = f"python src/crawl-oai-pmh.py -e https://oai-pmh.api.melinda.kansalliskirjasto.fi/bib -o {work_dir}/{start}.mrcx.zst -p melinda_marc -s fennica -f {start} -u {end}"
#    log_and_run(cmd, context)

@dg.asset(deps=[fennica_crawl], pool="parquet")
def fennica_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, f"{work_dir}/*.mrcx.zst", 'marc')
    

@dg.asset(deps=[fennica_parquet], pool="overview")
def fennica_overview(context: dg.AssetExecutionContext):
    create_overview(
        context,
        name="Fennica",
        data_glob=parquet_file,
        date_modified=get_date_from_file_modification_time(f"{work_dir}/*.mrcx.zst"),
        fields_file="data/schema-info/marc_fields.tsv",
        subfields_file="data/schema-info/marc_subfields.tsv",
        output_file="data/fennica/fennica-overview.html"
    )
    
