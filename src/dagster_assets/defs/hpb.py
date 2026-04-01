
import dagster as dg
from dagster_assets.utils import create_bib_overview, get_date_from_file_modification_time, log_and_run, run_bibxml2

#input_glob = "data/work/hpb/*.mrcx.gz"
work_dir = "data/work/hpb"
parquet_file = "data/hpb/hpb.parquet"

@dg.asset(pool="download", backfill_policy=dg.BackfillPolicy.multi_run(1), partitions_def=dg.StaticPartitionsDefinition(list('0'+str(i) for i in range(10)) + list(str(i) for i in range(1,10))))
def hpb_crawl(context: dg.AssetExecutionContext):
    cmd = (
        "python src/raw_procure/crawl-sru.py "
        "-v 2.0 "
        "-e https://sru.k10plus.de/hpb "
        f"-o {work_dir} "
        "-r picaxml "
        f"-i hpb_ppn_{context.partition_key} "
        f"-q 'pica.ppn={context.partition_key}*'"
    )
    log_and_run(cmd, context)

@dg.asset(deps=[hpb_crawl], pool="parquet")
def hpb_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
#    return run_bibxml2(context, parquet_file, input_glob, 'pica')
    return run_bibxml2(context, parquet_file, f"{work_dir}/*.xml.gz", 'pica')

@dg.asset(deps=[hpb_parquet], pool="overview")
def hpb_overview(context: dg.AssetExecutionContext):
    create_bib_overview(
        context,
        name="Heritage of the Printed Book",
        data_glob=parquet_file,
        date_modified=get_date_from_file_modification_time(work_dir + "/*.xml.gz"),
        fields_file="data/schema-info/pica_fields.tsv",
        subfields_file="data/schema-info/pica_subfields.tsv",
        output_file="data/hpb/hpb-overview.html",
        start_year="1400",
        end_year="1800"
    )
