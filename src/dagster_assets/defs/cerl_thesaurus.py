
import dagster as dg
from dagster_assets.utils import create_bib_overview, get_date_from_file_modification_time, log_and_run, run_bibxml2

#input_glob = "data/work/cerl_thesaurus/*.mrcx.gz"
work_dir = "data/work/cerl_thesaurus"
parquet_file = "data/cerl_thesaurus/cerl_thesaurus.parquet"

@dg.asset(pool="download", backfill_policy=dg.BackfillPolicy.multi_run(1), partitions_def=dg.StaticPartitionsDefinition([a+b+c for a in ('cnc000', 'cni000', 'cnl000', 'cnp000') for b in (str(b) for b in range(10)) for c in (str(c) for c in range(10))]))
def cerl_thesaurus_crawl(context: dg.AssetExecutionContext):
    cmd = (
        "python src/raw_procure/crawl-sru.py "
        "-v 1.2 "
        "-e https://data.cerl.org/thesaurus/_sru "
        f"-o {work_dir} "
        "-r marcxml "
        "-bs 100 "
        f"-i cerl_thesaurus_{context.partition_key} "
        f"-q 'dc.identifier={context.partition_key}*'"
    )
    log_and_run(cmd, context)

@dg.asset(deps=[cerl_thesaurus_crawl], pool="parquet")
def cerl_thesaurus_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
#    return run_bibxml2(context, parquet_file, input_glob, 'pica')
    return run_bibxml2(context, parquet_file, f"{work_dir}/*.xml.gz", 'marc')

@dg.asset(deps=[cerl_thesaurus_parquet])
def cerl_thesaurus_deduplicate(context: dg.AssetExecutionContext):
    import duckdb
    con = duckdb.connect()
    con.sql(f"""
COPY (
    SELECT * FROM read_parquet('{parquet_file}')
    INNER JOIN (
        SELECT MIN(record_number) AS record_number 
        FROM read_parquet('{parquet_file}') 
        WHERE field_code=='001' GROUP BY value
    ) USING (record_number)
    ORDER BY field_code, record_number, field_number, subfield_number
) TO 'cerl_thesaurus.parquet' (FORMAT parquet, COMPRESSION zstd)""")


@dg.asset(deps=[cerl_thesaurus_deduplicate], pool="overview")
def cerl_thesaurus_overview(context: dg.AssetExecutionContext):
    create_bib_overview(
        context,
        name="CERL thesaurus",
        data_glob=parquet_file,
        date_modified=get_date_from_file_modification_time(work_dir + "/*.xml.gz"),
        fields_file=None,
        subfields_file=None,
        output_file="data/cerl_thesaurus/cerl_thesaurus-overview.html"
    )
