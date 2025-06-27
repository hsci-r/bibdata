

import dagster as dg
from dagster_assets.utils import get_etag, create_overview, download_file, run_bibxml2, get_date_from_last_modified_file

work_dir = "data/work/dnb"
parquet_file = "data/dnb/dnb.parquet"

pd = dg.StaticPartitionsDefinition(list(str(i+1) for i in range(5)))

@dg.observable_source_asset(partitions_def=pd)
def dnb_data(context: dg.AssetExecutionContext) -> dg.DataVersionsByPartition:
    if not context.has_partition_key and not context.has_partition_key_range:
        return dg.DataVersionsByPartition({i: get_etag(f"https://data.dnb.de/DNB/dnb_all_dnbmarc.{i}.mrc.xml.gz") for i in pd.get_partition_keys()})
    return dg.DataVersionsByPartition({i:get_etag(f"https://data.dnb.de/DNB/dnb_all_dnbmarc.{i}.mrc.xml.gz") for i in context.partition_keys})

@dg.asset(backfill_policy=dg.BackfillPolicy.multi_run(1), deps=[dnb_data], partitions_def=pd, pool="download")
def dnb_download(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
#    for i in context.partition_keys:
    out = f"dnb_all_dnbmarc.{context.partition_key}.mrc.xml.gz"
    url = f"https://data.dnb.de/DNB/{out}"
    return download_file(context, url, f"{work_dir}/{out}")

@dg.asset(deps=[dnb_download], pool="parquet")
def dnb_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, f"{work_dir}/*.mrc.xml.gz", 'marc')

@dg.asset(deps=[dnb_parquet], pool="overview")
def dnb_overview(context: dg.AssetExecutionContext):
    create_overview(
        context,
        name="German National Bibliography",
        data_glob=parquet_file,
        date_modified=get_date_from_last_modified_file(work_dir + "/dnb_all_dnbmarc.1.mrc.xml.gz"),
        fields_file="data/schema-info/marc_fields.tsv",
        subfields_file="data/schema-info/marc_subfields.tsv",
        output_file="data/dnb/dnb-overview.html"
    )
