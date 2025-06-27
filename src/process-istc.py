import fsspec
from tqdm.auto import tqdm
import yaml
from bibxml2 import schema
import pyarrow.parquet as pq
import pyarrow as pa

def parquet_writer(of, parquet_compression: str, parquet_compression_level: int) -> pq.ParquetWriter:
    return pq.ParquetWriter(of, 
            schema=schema, 
            compression=parquet_compression, 
            compression_level=parquet_compression_level,
#            use_byte_stream_split=['record_number', 'field_number', 'subfield_number'], # type: ignore pyarrow import complains: BYTE_STREAM_SPLIT encoding is only supported for FLOAT or DOUBLE data
            write_page_index=True, 
            use_dictionary=['field_code', 'subfield_code'], # type: ignore
            store_decimal_as_integer=True,
            sorting_columns=[pq.SortingColumn(0), pq.SortingColumn(1), pq.SortingColumn(2)],
        ) 

def process_istc():
    with open('data/work/istc_clean_1.0.yaml', 'r') as file, fsspec.open('data/istc/istc.parquet', 'wb') as parquet_file, parquet_writer(parquet_file, 'zstd', 22) as pw:
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
        pw.write_batch(pa.record_batch(list(zip(*batch)), schema=schema), row_group_size=64*1024*1024)

if __name__ == "__main__":
    process_istc()
