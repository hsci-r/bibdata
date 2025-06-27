

import dagster as dg
from dagster_assets.utils import get_date_from_last_modified_file, get_etag, create_overview, download_file, run_bibxml2

work_dir = "data/work/gnd"
parquet_file = "data/gnd/gnd.parquet"

pd = dg.StaticPartitionsDefinition(["authorities-gnd-geografikum_dnbmarc.mrc.xml.gz", "authorities-gnd-koerperschaft_dnbmarc.mrc.xml.gz", "authorities-gnd-kongress_dnbmarc.mrc.xml.gz", "authorities-gnd-person_dnbmarc.mrc.xml.gz", "authorities-gnd-sachbegriff_dnbmarc.mrc.xml.gz", "authorities-gnd-werk_dnbmarc.mrc.xml.gz", "authorities-gnd_umlenk_loesch.mrc.xml.gz"])

@dg.observable_source_asset(partitions_def=pd)
def gnd_data(context: dg.AssetExecutionContext) -> dg.DataVersionsByPartition:
    if not context.has_partition_key and not context.has_partition_key_range:
        return dg.DataVersionsByPartition({filename: get_etag(f"https://data.dnb.de/GND/{filename}") for filename in pd.get_partition_keys()})
    return dg.DataVersionsByPartition({filename:get_etag(f"https://data.dnb.de/GND/{filename}") for filename in context.partition_keys})

@dg.asset(backfill_policy=dg.BackfillPolicy.multi_run(1), deps=[gnd_data], partitions_def=pd, pool="download")
def gnd_download(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
#    for i in context.partition_keys:
    out = context.partition_key
    url = f"https://data.dnb.de/GND/{out}"
    return download_file(context, url, f"{work_dir}/{out}")

@dg.asset(deps=[gnd_download], pool="parquet")
def gnd_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, f"{work_dir}/*.mrc.xml.gz", 'marc')

@dg.asset(deps=[gnd_parquet], pool="overview")
def gnd_overview(context: dg.AssetExecutionContext):
    create_overview(
        context,
        name="German Integrated Authority File (Gemeinsame Normdatei)",
        data_glob=parquet_file,
        date_modified=get_date_from_last_modified_file(f"{work_dir}/authorities-gnd-*.mrc.xml.gz"),
        fields_file="data/schema-info/marc_fields.tsv",
        subfields_file="data/schema-info/marc_subfields.tsv",
        output_file="data/gnd/gnd-overview.html"
    )
