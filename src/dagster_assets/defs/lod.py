import dagster as dg
from hereutil import here
from dagster_assets.utils import get_date_from_file_modification_time

@dg.observable_source_asset
def lod_prefixes() -> dg.DataVersion:
    return dg.DataVersion(get_date_from_file_modification_time(str(here("data/schema-info/lod_prefixes.tsv"))))