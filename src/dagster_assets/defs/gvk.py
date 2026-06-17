import dagster as dg
from dagster_assets.utils import get_date_from_file_modification_time, log_and_run, create_bib_overview, run_bibxml2

work_dir = "data/work/gvk"
parquet_file = "data/gvk/gvk.parquet"

@dg.asset(pool="download", backfill_policy=dg.BackfillPolicy.multi_run(1), partitions_def=dg.StaticPartitionsDefinition(list("165"+str(i) for i in range(10)) + list(str(i)+str(j)+str(k) for i in range(10) for j in range(10) for k in range(10) if not (i == 1 and j == 6 and k == 5))))
def gvk_crawl(context: dg.AssetExecutionContext):
    cmd = (
        "python src/raw_procure/crawl-sru.py "
        "-v 2.0 "
        "-e https://sru.k10plus.de/gvk "
        f"-o {work_dir} "
        "-r picaxml "
        f"-i gvk_{context.partition_key} "
        f"-q 'pica.ppn={context.partition_key}*'"
    )
    log_and_run(cmd, context)

@dg.asset(deps=[gvk_crawl], pool="parquet")
def gvk_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, f"{work_dir}/*.xml.gz", 'pica')

@dg.asset(deps=[gvk_parquet], pool="overview")
def gvk_overview(context: dg.AssetExecutionContext):
    create_bib_overview(
        context,
        name="gvk",
        data_glob=parquet_file,
        date_modified=get_date_from_file_modification_time(f"{work_dir}/*.xml.gz"),
        fields_file="data/schema-info/k10_pica_fields.tsv",
        subfields_file="data/schema-info/k10_pica_subfields.tsv",
        output_file="data/gvk/gvk-overview.html",
    )
