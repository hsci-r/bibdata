import glob
import dagster as dg
from dagster_assets.defs.lod import lod_prefixes
from dagster_assets.utils import create_rdf_overview, download_file, get_date_from_file_modification_time, get_date_from_last_modified_file, get_etag, get_parquet_glob_sha1sum, log_and_run, create_bib_overview, run_bibxml2

work_dir = "data/work/dbnf"
urls = dg.StaticPartitionsDefinition([ # from https://api.bnf.fr/index.php/fr/node/270
    "https://transfert.bnf.fr/link/1ccac8d2-817b-44fb-9e5a-ae008d661b7a",
    "https://transfert.bnf.fr/link/51a873c1-54b6-46e6-aa66-914a3a241307",
    "https://transfert.bnf.fr/link/56f62e2e-a034-458b-98cc-1ad83cdd7cfd",
    "https://transfert.bnf.fr/link/6fb5f804-2e42-4c9e-839d-c5d7f6f11df8",
    "https://transfert.bnf.fr/link/1a2c5a67-5f8a-4a7b-87b5-47b5307f2d0d",
    "https://transfert.bnf.fr/link/9c2e947f-2c7b-4636-8616-f8436469235d",
    "https://transfert.bnf.fr/link/35fb4868-3a87-43f6-a56f-829442b6a41b",
    "https://transfert.bnf.fr/link/d9bd036c-9309-4c66-b228-403937ef327a",
    "https://transfert.bnf.fr/link/4213453c-850b-4bf5-b24c-440eab4b0bc2",
    "https://transfert.bnf.fr/link/3fa7b908-d701-4608-b891-1cfd4e93fb15",
    "https://transfert.bnf.fr/link/856d3769-7113-4f17-bfcc-49bfbc8fe73e",
    "https://transfert.bnf.fr/link/a37251ef-1519-4c05-8f8d-e1781e1519a4",
    "https://transfert.bnf.fr/link/c3b7cb94-3721-4475-b82a-be38eb7b42c3",
    "https://transfert.bnf.fr/link/19e57a68-0332-4985-90c5-486fe70a443e",
    "https://transfert.bnf.fr/link/2a27bddc-2a22-4bfe-b86e-3083d38abfa7",
    "https://transfert.bnf.fr/link/d0bee774-62d7-4e7e-8186-f298f8cdfc6f",
    "https://transfert.bnf.fr/link/75c11061-1698-4d0f-ad6b-535e7ee42ede",
    "https://transfert.bnf.fr/link/7da54f6e-34e0-48c5-b7c3-f3912cdcf355",
    "https://transfert.bnf.fr/link/76496b60-e4f9-4fb2-a729-fd90c6a5970a",
    "https://transfert.bnf.fr/link/522274cd-c279-4d97-9b73-a290c91984ed",
    "https://transfert.bnf.fr/link/84642237-e75a-4610-a653-248487393a5d",
    "https://transfert.bnf.fr/link/36f17534-0569-4720-9663-29d82b630c20"
])
parquet_file = "data/dbnf/dbnf.parquet"

@dg.observable_source_asset(partitions_def=urls)
def dbnf_data() -> dg.DataVersionsByPartition:
    return dg.DataVersionsByPartition(
        {url: get_etag(url) for url in urls.get_partition_keys()}
    )

@dg.asset(deps=[dbnf_data], pool="download", partitions_def=urls, backfill_policy=dg.BackfillPolicy.multi_run(1))
def dbnf_download(context: dg.AssetExecutionContext):
    return download_file(context, context.partition_key, f"{work_dir}/{context.partition_key[context.partition_key.rfind('/') + 1:]}.tar.gz")

@dg.asset(deps=[dbnf_download, lod_prefixes], pool="parquet")
def dbnf_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    files = ['tar://*.nt::' + file for file in glob.glob(f"{work_dir}/*.tar.gz")]
    cmd = f"python src/raw_to_parquet/process-ntriples.py -k dbnf -o {parquet_file} -p data/schema-info/lod_prefixes.tsv {' '.join(files)}"
    log_and_run(cmd, context)
    return get_parquet_glob_sha1sum(parquet_file)

@dg.asset(deps=[dbnf_parquet], pool="overview")
def dbnf_overview(context: dg.AssetExecutionContext):
    create_rdf_overview(
        context,
        name="French National Library Linked Data",
        data_glob=parquet_file,
        date_modified=get_date_from_file_modification_time(work_dir + "/*.tar.gz"),
        properties_file="data/schema-info/rdf_properties.tsv",
        output_file="data/dbnf/dbnf-overview.html"
    )
