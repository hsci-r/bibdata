#%%
from core import *

e_id_files = sorted(here("data/unified").glob("e_id*.parquet"))
e_id = read_parquet('e_id', *e_id_files)

__all__ = ["e_id"]