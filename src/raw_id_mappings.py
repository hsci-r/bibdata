#%%
from core import *

e_id = read_parquet('e_id', here("data/work/raw_id_mappings/e_id.parquet"))

__all__ = ["e_id"]