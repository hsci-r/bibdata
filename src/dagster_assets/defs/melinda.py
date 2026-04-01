


import os
import dagster as dg
from dagster_assets.utils import get_date_from_file_modification_time, log_and_run, create_bib_overview, run_bibxml2

work_dir = "data/work/melinda"
parquet_file = "data/melinda/melinda.parquet"

#@dg.asset(backfill_policy=dg.BackfillPolicy.multi_run(1), partitions_def=dg.MonthlyPartitionsDefinition(start_date="2019-01", fmt="%Y-%m"), pool="melinda_api")
#def melinda_crawl(context: dg.AssetExecutionContext):
#    start = context.partition_time_window.start.strftime("%Y-%m")
#    end = context.partition_time_window.end.strftime("%Y-%m")
#    cmd = f"python src/raw_procure/crawl-oai-pmh.py -e https://oai-pmh.api.melinda.kansalliskirjasto.fi/bib -o {work_dir}/{start}.mrcx.zst -p melinda_marc -s melinda -f {start if start != '2019-01' else '0000-01'}-01 -u {end}-01"
#    log_and_run(cmd, context)

#@dg.asset(pool="melinda_api")
#def melinda_crawl(context: dg.AssetExecutionContext):
#    start = context.partition_time_window.start.strftime("%Y-%m")
#    end = context.partition_time_window.end.strftime("%Y-%m")
#    cmd = f"python src/raw_procure/crawl-oai-pmh.py -e https://oai-pmh.api.melinda.kansalliskirjasto.fi/bib -o {work_dir}/melinda.mrcx.zst -p melinda_marc -s melinda"
#    log_and_run(cmd, context)    

@dg.asset(backfill_policy=dg.BackfillPolicy.multi_run(1), partitions_def=dg.WeeklyPartitionsDefinition(start_date="2019-01-01", day_offset=2), pool="melinda_api")
def melinda_crawl(context: dg.AssetExecutionContext):
    start = context.partition_time_window.start.strftime("%Y-%m-%d")
    end = context.partition_time_window.end.strftime("%Y-%m-%d")
    cmd = f"python src/raw_procure/crawl-oai-pmh.py -e https://oai-pmh.api.melinda.kansalliskirjasto.fi/bib -o {work_dir}/{start}.mrcx.zst -p melinda_marc -f {start} -u {end}"
    log_and_run(cmd, context)

@dg.asset(deps=[melinda_crawl], pool="parquet")
def melinda_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, f"{work_dir}/*.mrcx.zst", 'marc')
    

@dg.asset(deps=[melinda_parquet], pool="overview")
def melinda_overview(context: dg.AssetExecutionContext):
    create_bib_overview(
        context,
        name="melinda",
        data_glob=parquet_file,
        date_modified=get_date_from_file_modification_time(f"{work_dir}/*.mrcx.zst"),
        fields_file="data/schema-info/marc_fields.tsv",
        subfields_file="data/schema-info/marc_subfields.tsv",
        output_file="data/melinda/melinda-overview.html"
    )
    
