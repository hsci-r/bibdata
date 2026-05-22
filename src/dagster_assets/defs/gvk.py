import dagster as dg
from dagster_assets.utils import get_date_from_file_modification_time, log_and_run, create_bib_overview, run_bibxml2

work_dir = "data/work/gvk"
parquet_file = "data/gvk/gvk.parquet"

@dg.asset(pool="download")
def gvk_crawl(context: dg.AssetExecutionContext):
    cmd = (
        "python src/raw_procure/crawl-sru.py "
        "-v 2.0 "
        "-e https://sru.k10plus.de/gvk "
        f"-o {work_dir} "
        "-i gvk "
        "-r picaxml "
        "-q 'pica.ppn=0* or pica.ppn=1* or pica.ppn=2* or pica.ppn=3* or pica.ppn=4* or pica.ppn=5* or pica.ppn=6* or pica.ppn=7* or pica.ppn=8* or pica.ppn=9*'"
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
