

import dagster as dg
from dagster_assets.utils import create_overview, run_bibxml2

input_glob = "data/work/hpb/*.mrcx.gz"
parquet_file = "data/hpb/hpb.parquet"

@dg.asset(pool="download")
def hpb_download() -> dg.MaterializeResult:
    raise ValueError("HPB data is not publicly available for download.")

@dg.asset(deps=[hpb_download], pool="parquet")
def hpb_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, input_glob, 'pica')

@dg.asset(deps=[hpb_parquet], pool="overview")
def hpb_overview(context: dg.AssetExecutionContext):
    create_overview(
        context,
        name="Heritage of the Printed Book",
        data_glob=parquet_file,
        date_modified="2025-03",
        fields_file="data/schema-info/pica_fields.tsv",
        subfields_file="data/schema-info/pica_subfields.tsv",
        output_file="data/hpb/hpb-overview.html",
        start_year="1400",
        end_year="1800"
    )
