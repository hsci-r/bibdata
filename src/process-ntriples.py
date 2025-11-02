#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
from functools import reduce
import glob
from io import TextIOWrapper
import logging
import re
import shutil
from typing import Callable, Iterator

import click
import duckdb
import fsspec
from fsspec.core import OpenFile, compr, infer_compression
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.compute as pc
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)

schema: pa.Schema = pa.schema([ # R compatibility schema
    pa.field('subject', pa.string(), nullable=False),
    pa.field('property', pa.string(), nullable=False),
    pa.field('object', pa.string(), nullable=False),
    pa.field('datatype_lang', pa.string(), nullable=False),
])

def yield_rows(inputs: list[str], replace_prefix: Callable[[str], str]) -> Iterator[tuple[str,str,str,str]]:
    input_files = [of for input in inputs for of in fsspec.open_files(input, 'rb')]
    tsize = reduce(lambda tsize, inf: tsize + inf.fs.size(inf.path), input_files, 0)
    pbar = tqdm(total=tsize, unit='b', smoothing=0, unit_scale=True, unit_divisor=1024, dynamic_ncols=True)
    processed_files_tsize = 0
    for input_file in input_files:
        pbar.set_description(f"Processing {input_file.path}")
        with input_file as oinf:
            compression = infer_compression(input_file.path)
            if compression is not None:
                inf = compr[compression](oinf, mode='rb') # type: ignore
            else:
                inf = oinf
            with TextIOWrapper(inf, encoding='utf-8') as tinf:
                for line in tinf:
                    s, p, o = line.split(' ', 2)
                    o = o[:-3] if o.endswith(' .\n') else o[:-2]
                    object_is_literal = o.startswith('"')
                    if not object_is_literal:
                        d = 'xs:anyURI'
                    else:
                        m = re.search(r'(?<!\\)"\^\^(.+)>', o)
                        if m is not None:
                            d = m.group(1)
                        else:
                            m = re.search(r'(?<!\\)"(@.+)', o)
                            if m is not None:
                                d = m.group(1)
                            else:
                                d = 'xs:string'
                        o = o[1:o.rfind('"')]
                    s = replace_prefix(s)
                    p = replace_prefix(p)
                    o = replace_prefix(o) if not object_is_literal else o
                    d = replace_prefix(d)
                    yield (s, p, o, d)
                    pbar.n = processed_files_tsize + oinf.tell()
                    pbar.update(0)
        processed_files_tsize += input_file.fs.size(input_file.path)
   

def yield_batches(input: list[str], replace_prefix: Callable[[str], str], parquet_batch_size: int, schema: pa.Schema) -> Iterator[pa.RecordBatch]:
    batch = []
    for row in yield_rows(input, replace_prefix):
        batch.append(row)
        if len(batch) == parquet_batch_size:
            yield pa.record_batch(list(zip(*batch)), schema=schema)
            batch = []
    if batch:
        yield pa.record_batch(list(zip(*batch)), schema=schema)

@click.command()
@click.option("-p", "--prefixes", help="prefix TSV file", required=False, type=click.Path(dir_okay=False, readable=True))
@click.option("-o", "--output", help="output parquet file", required=True, type=click.Path(dir_okay=False, writable=True))
@click.option("-s", "--max-file-size", help="Maximum size of parquet files in bytes (default 4,000,000,000)", type=int, default=4_000_000_000)
@click.argument('input', nargs=-1)
def convert_ntriples(input: list[str], prefixes: str, output: str, max_file_size: int) -> None:
    """Convert N-Triples files to Parquet format line-by-line"""
    if prefixes is not None:
        with open(prefixes, 'r') as pf:
            pr = csv.reader(pf, delimiter='\t')
            prefix_map = {'<' + row[1]: row[0]+':' for row in pr}
    else:
        prefix_map = {}
    def replace_prefix(value: str) -> str:
        cbp = value.rfind('#') + 1
        if cbp > 0 and value[:cbp] in prefix_map:
            return prefix_map[value[:cbp]] + value[cbp:-1]
        cbp = len(value)
        while cbp > 0:
            if value[:cbp] in prefix_map:
                return prefix_map[value[:cbp]] + value[cbp:-1]
            cbp = value.rfind('/', 0, cbp - 1) + 1
        return value
    print("Writing to temporary parquet dataset to split data by property:")
    ds.write_dataset(yield_batches(input, replace_prefix, 1024*1024, schema), output+".tmp", format='parquet', partitioning_flavor="hive", partitioning=["property"], schema=schema, min_rows_per_group=2**16, file_options=ds.ParquetFileFormat().make_write_options(compression='zstd'))
    duckdb.query("SET enable_progress_bar_print=TRUE")
    duckdb.query("SET progress_bar_time=0")
    duckdb.query("SET threads=1")
    print("Coalescing and optimising into unified parquet(s):")
    duckdb.query(f"COPY (SELECT * FROM parquet_scan('{output}.tmp/*/*.parquet', hive_partitioning=TRUE)) TO '{output}.tmp.2' (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 22, STRING_DICTIONARY_PAGE_SIZE_LIMIT 100_000, FILE_SIZE_BYTES {max_file_size})")
    shutil.rmtree(output+".tmp", ignore_errors=True)
    print("Renaming final parquet files:")
    shutil.rmtree(output, ignore_errors=True)
    for file in tqdm(glob.glob(f"{output}.tmp.2/data_*.parquet")):
        part = re.search(r'data(_\d+).parquet', file).group(1)
        if part == "_0":
            part = ""
        shutil.move(file, output.replace('.parquet', part+'.parquet'))
    shutil.rmtree(output+".tmp.2", ignore_errors=True)


if __name__ == '__main__':
    convert_ntriples()
