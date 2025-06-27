

import dagster as dg
from dagster_assets.utils import create_overview, run_bibxml2

input_glob = "data/work/estc/ESTC0920.xml.gz"
parquet_file = "data/estc/estc.parquet"

@dg.asset(pool="download")
def estc_download() -> dg.MaterializeResult:
    raise ValueError("ESTC data is not publicly available for download.")

@dg.asset(deps=[estc_download], pool="parquet")
def estc_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, input_glob, 'marc')

@dg.asset(deps=[estc_parquet], pool="overview")
def estc_overview(context: dg.AssetExecutionContext):
    create_overview(
        context,
        name="English Short Title Catalogue",
        data_glob=parquet_file,
        date_modified="2020-09",
        fields_file="data/schema-info/marc_fields.tsv",
        subfields_file="data/schema-info/marc_subfields.tsv",
        output_file="data/estc/estc-overview.html",
        start_year="1400",
        end_year="1800"
    )
