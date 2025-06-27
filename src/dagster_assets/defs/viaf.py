import dagster as dg
from dagster_assets.utils import create_overview, get_parquet_glob_sha1sum, log_and_run, run_bibxml2, get_etag, download_file

source_url = "https://downloads.viaf.org/download/yearly/viaf-20240804-clusters.xml.gz"
input_file = "data/work/viaf/viaf.xml.gz"
work_dir = "data/work/viaf"
parquet_file = "data/viaf/viaf.parquet"

@dg.observable_source_asset()
def viaf_data() -> dg.DataVersion:
    return get_etag(source_url)

@dg.asset(deps=[viaf_data], pool="download")
def viaf_download(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return download_file(context, source_url, input_file)

@dg.asset(deps=[viaf_download])
def viaf_convert(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    log_and_run(f"sh -c \"echo '<foo>' | zstd > {work_dir}/viaf.mrcx.zst; gzcat {input_file} | sed 's|^[^<]*||' | zstd >> {work_dir}/viaf.mrcx.zst; echo '</foo>' | zstd >> {work_dir}/viaf.mrcx.zst\"", context)
    return get_parquet_glob_sha1sum(f"{work_dir}/viaf.mrcx.zst")

@dg.asset(deps=[viaf_convert], pool="parquet")
def viaf_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, f"{work_dir}/viaf.mrcx.zst", 'marc')

@dg.asset(deps=[viaf_parquet], pool="overview")
def viaf_overview(context: dg.AssetExecutionContext):
    create_overview(
        context,
        name="Virtual International Authority File",
        data_glob=parquet_file,
        date_modified="2024-08-04",
        fields_file="data/schema-info/marc_fields.tsv",
        subfields_file="data/schema-info/marc_subfields.tsv",
        output_file="data/viaf/viaf-overview.html"
    )
