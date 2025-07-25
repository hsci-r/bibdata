import os
import duckdb
import fsspec
from tqdm.auto import tqdm
import yaml
from bib2 import schema, parquet_writer
import pyarrow.parquet as pq
import pyarrow as pa



def process_istc():
    with open('data/work/istc_clean_1.0.yaml', 'r') as file, parquet_writer('data/istc/istc.tmp.parquet', 'zstd', None, False) as pw:
        batch = []
        data = yaml.safe_load_all(file)
        for record_number, item in enumerate(tqdm(data), start=1):
            batch.append((record_number, 1, 1, 'id', '', item['_id']))
            field_number = 2
            for attribute, value in item['data'].items():
                if isinstance(value, list):
                    for v in value:
                        if isinstance(v, dict):
                            subfield_number = 1
                            for subfield_attribute, subfield_value in v.items():
                                if isinstance(subfield_value, list):
                                    for subfield_value_item in subfield_value:
                                        if isinstance(subfield_value_item, dict):
                                            for key, subfield_value_item in subfield_value_item.items():
                                                batch.append((record_number, field_number, subfield_number, attribute, subfield_attribute + "_" + key, str(subfield_value_item)))
                                                subfield_number += 1
                                            field_number += 1
                                            subfield_number = 1
                                        else:
                                            batch.append((record_number, field_number, subfield_number, attribute, subfield_attribute, str(subfield_value_item)))
                                            subfield_number += 1
                                else:
                                    batch.append((record_number, field_number, subfield_number, attribute, subfield_attribute, str(subfield_value)))
                                    subfield_number += 1
                        else:
                            batch.append((record_number, field_number, 1, attribute, '', str(v)))
                        field_number += 1
                else:
                    batch.append((record_number, field_number, 1, attribute, '', str(value)))
                    field_number += 1
        pw.write_batch(pa.record_batch(list(zip(*batch)), schema=schema), row_group_size=1024*1024)
    duckdb.query(f"COPY 'data/istc/istc.tmp.parquet' TO 'data/istc/istc.parquet' (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 22)")
    os.remove('data/istc/istc.tmp.parquet')

if __name__ == "__main__":
    process_istc()
