import dagster as dg
from dagster_assets.utils import get_date_from_file_modification_time, log_and_run, create_bib_overview, run_bibxml2

work_dir = "data/work/bnf"
parquet_file = "data/bnf/bnf.parquet"

@dg.asset(pool="download")
def bnf_crawl(context: dg.AssetExecutionContext):
    cmd = (
        "python src/crawl-sru.py "
        "-v 1.2 "
        "-e https://catalogue.bnf.fr/api/SRU " 
        "-o data/work/bnf "
        "-r intermarcXchange-anl "
        "-q 'bib.status=validated' "
    )
    log_and_run(cmd, context)

@dg.asset(deps=[bnf_crawl], pool="parquet")
def bnf_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, f"{work_dir}/*.xml.gz", 'marc')

@dg.asset(deps=[bnf_parquet], pool="overview")
def bnf_overview(context: dg.AssetExecutionContext):
    create_bib_overview(
        context,
        name="French National Bibliography (BNF)",
        data_glob=parquet_file,
        date_modified=get_date_from_file_modification_time(f"{work_dir}/*.xml.gz"),
        fields_file=None,
        subfields_file=None,
        output_file="data/bnf/bnf-overview.html"
    )
