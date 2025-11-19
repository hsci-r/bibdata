

import dagster as dg
from dagster_assets.utils import get_date_from_file_modification_time, log_and_run, create_bib_overview, run_bibxml2

work_file = "data/work/stcn.zip"
parquet_file = "data/stcn/stcn.parquet"

@dg.asset(pool="download")
def stcn_download() -> dg.MaterializeResult:
    raise ValueError("STCN data is not publicly available for download.")

@dg.asset(deps=[stcn_download], pool="parquet")
def stcn_parquet(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_bibxml2(context, parquet_file, f"zip://*::{work_file}", 'pica', no_input_glob=True)


@dg.asset(deps=[stcn_parquet], pool="overview")
def stcn_overview(context: dg.AssetExecutionContext):
    create_bib_overview(
        context,
        name="Short-Title Catalogue Netherlands",
        data_glob=parquet_file,
        date_modified=get_date_from_file_modification_time(work_file),
        fields_file="data/schema-info/gcc_pica_fields.tsv",
        subfields_file="data/schema-info/gcc_pica_subfields.tsv",
        output_file="data/stcn/stcn-overview.html"
    )

