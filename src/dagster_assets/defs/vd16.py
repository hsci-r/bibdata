import dagster as dg
from dagster_assets.utils import get_date_from_file_modification_time, log_and_run, create_bib_overview, run_bibxml2

work_dir = "data/work/vd16"
parquet_file = "data/vd16/vd16.parquet"

@dg.asset(pool="download")
def vd16_crawl(context: dg.AssetExecutionContext):
    cmd = (
        "python src/raw_procure/crawl-sru.py "
        "-v 2.0 "
        "-e https://sru.k10plus.de/gvk "
        f"-o {work_dir} "
        "-i vd16 "
        "-r picaxml "
        "-q 'pica.vdr=V*'"
    )
    log_and_run(cmd, context)

@dg.asset(deps=[vd16_crawl], pool="parquet")
def vd16_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, f"{work_dir}/*.xml.gz", 'pica')

@dg.asset(deps=[vd16_parquet], pool="overview")
def vd16_overview(context: dg.AssetExecutionContext):
    create_bib_overview(
        context,
        name="vd16",
        data_glob=parquet_file,
        date_modified=get_date_from_file_modification_time(f"{work_dir}/*.xml.gz"),
        fields_file="data/schema-info/k10_pica_fields.tsv",
        subfields_file="data/schema-info/k10_pica_subfields.tsv",
        output_file="data/vd16/vd16-overview.html",
        start_year="1501",
        end_year="1600"
    )
