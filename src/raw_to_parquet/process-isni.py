#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
from functools import reduce
import glob
from io import TextIOWrapper
import json
import logging
import os
import re
import shutil
from typing import Iterator, cast

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

core: pa.Schema = pa.schema([
    pa.field('isni_n', pa.int64(), nullable=False),
    pa.field('isni', pa.string(), nullable=False),
    pa.field('type', pa.string(), nullable=False),
    pa.field('birthdate', pa.string(), nullable=True),
    pa.field('deathdate', pa.string(), nullable=True),
])

names: pa.Schema = pa.schema([
    pa.field('isni_n', pa.int64(), nullable=False),
    pa.field('name', pa.string(), nullable=False),
])
deprecated_isnis: pa.Schema = pa.schema([
    pa.field('isni_n', pa.int64(), nullable=False),
    pa.field('deprecated_isni', pa.string(), nullable=False),
])
same_as: pa.Schema = pa.schema([
    pa.field('isni_n', pa.int64(), nullable=False),
    pa.field('same_as', pa.string(), nullable=False),
])
authority_ids: pa.Schema = pa.schema([
    pa.field('isni_n', pa.int64(), nullable=False),
    pa.field('authority_id', pa.string(), nullable=False),
])
source_ids: pa.Schema = pa.schema([
    pa.field('isni_n', pa.int64(), nullable=False),
    pa.field('source_id', pa.string(), nullable=False),
])

@click.command()
@click.option("-p", "--prefixes", help="prefix TSV file", required=False, type=click.Path(dir_okay=False, readable=True))
@click.option("-o", "--output", help="output parquet file", required=True, type=click.Path(file_okay=False, writable=True))
@click.option("-s", "--max-file-size", help="Maximum size of parquet files in bytes (default 4,000,000,000)", type=int, default=4_000_000_000)
@click.argument('input', nargs=-1)
def convert_isni(input: list[str], prefixes: str, output: str, max_file_size: int) -> None:
    """Convert ISNI jsonld files to Parquet format line-by-line"""
    if prefixes is not None:
        with open(prefixes, 'r') as pf:
            pr = csv.reader(pf, delimiter='\t')
            prefix_map = {row[1]: row[0]+':' for row in pr}
    else:
        prefix_map = {}
    def replace_prefix(value: str) -> str:
        cbp = value.rfind('#') + 1
        if cbp > 0 and value[:cbp] in prefix_map:
            return prefix_map[value[:cbp]] + value[cbp:]
        cbp = len(value)
        while cbp > 0:
            if value[:cbp] in prefix_map:
                return prefix_map[value[:cbp]] + value[cbp:]
            cbp = value.rfind('/', 0, cbp - 1) + 1
        return value
    input_files = fsspec.open_files(input, 'rb')
    print("Writing to temporary parquet dataset to split data by property:")
    tsize = reduce(lambda tsize, inf: tsize + inf.fs.size(inf.path), input_files, 0)
    pbar = tqdm(total=tsize, unit='b', smoothing=0, unit_scale=True, unit_divisor=1024, dynamic_ncols=True)
    processed_files_tsize = 0
    os.makedirs(f"{output}.tmp", exist_ok=True)
    with pq.ParquetWriter(f"{output}.tmp/core.parquet", core, compression='zstd') as core_writer, \
         pq.ParquetWriter(f"{output}.tmp/names.parquet", names, compression='zstd') as names_writer, \
         pq.ParquetWriter(f"{output}.tmp/deprecated_isnis.parquet", deprecated_isnis, compression='zstd') as deprecated_isnis_writer, \
         pq.ParquetWriter(f"{output}.tmp/same_as.parquet", same_as, compression='zstd') as same_as_writer, \
         pq.ParquetWriter(f"{output}.tmp/authority_ids.parquet", authority_ids, compression='zstd') as authority_ids_writer, \
         pq.ParquetWriter(f"{output}.tmp/source_ids.parquet", source_ids, compression='zstd') as source_ids_writer:
        core_batch = []
        names_batch = []
        deprecated_isnis_batch = []
        same_as_batch = []
        authority_ids_batch = []
        source_ids_batch = []
        batch_size = 122_880
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
                        if not line.startswith('{'):
                            continue
                        line = re.sub(r'\u001e', '', line, flags=re.UNICODE)
                        o = json.loads(line)["@graph"][0]
                        isni_n = int(o['@id'][22:-1])  # strip < and >http://isni.org/isni/
                        isni_s = o['@id'][22:]
                        if 'schema:birthDate' in o:
                            birthdate = o['schema:birthDate']
                        else:
                            birthdate = None
                        if 'schema:deathDate' in o:
                            deathdate = o['schema:deathDate']
                        else:
                            deathdate = None
                        core_batch.append( (isni_n, isni_s, o['@type'], birthdate, deathdate) )
                        for name in o.get('schema:alternateName', []):
                            names_batch.append( (isni_n, name) )
                        for deprecated in o.get('isni:hasDeprecatedISNI', []):
                            deprecated_isnis_batch.append( (isni_n, deprecated['@id']) )
                        for same in o.get('owl:sameAs', []):
                            same_as_batch.append( (isni_n, replace_prefix(same['@id']) ))
                        for authority in o.get('madsrdf:isIdentifiedByAuthority', []):
                            authority_ids_batch.append( (isni_n, replace_prefix(authority['@id'])) )
                        for source in o.get('dcterms:source', []):
                            source_ids_batch.append( (isni_n, replace_prefix(source['@id'])) )
                        if len(core_batch) >= batch_size:
                            core_writer.write_batch(pa.record_batch(list(zip(*core_batch)), schema=core))
                            core_batch = []
                        if len(names_batch) >= batch_size:
                            names_writer.write_batch(pa.record_batch(list(zip(*names_batch)), schema=names))
                            names_batch = []
                        if len(deprecated_isnis_batch) >= batch_size:
                            deprecated_isnis_writer.write_batch(pa.record_batch(list(zip(*deprecated_isnis_batch)), schema=deprecated_isnis))
                            deprecated_isnis_batch = []
                        if len(same_as_batch) >= batch_size:
                            same_as_writer.write_batch(pa.record_batch(list(zip(*same_as_batch)), schema=same_as))
                            same_as_batch = []
                        if len(authority_ids_batch) >= batch_size:
                            authority_ids_writer.write_batch(pa.record_batch(list(zip(*authority_ids_batch)), schema=authority_ids))
                            authority_ids_batch = []
                        if len(source_ids_batch) >= batch_size:
                            source_ids_writer.write_batch(pa.record_batch(list(zip(*source_ids_batch)), schema=source_ids))
                            source_ids_batch = []
                        pbar.n = processed_files_tsize + oinf.tell()
                        pbar.update(0)
            processed_files_tsize += input_file.fs.size(input_file.path)
        if core_batch:
            core_writer.write_table(pa.table(list(zip(*core_batch)), schema=core))
        if names_batch:
            names_writer.write_table(pa.table(list(zip(*names_batch)), schema=names))
        if deprecated_isnis_batch:
            deprecated_isnis_writer.write_table(pa.table(list(zip(*deprecated_isnis_batch)), schema=deprecated_isnis))
        if same_as_batch:
            same_as_writer.write_table(pa.table(list(zip(*same_as_batch)), schema=same_as))
        if authority_ids_batch:
            authority_ids_writer.write_table(pa.table(list(zip(*authority_ids_batch)), schema=authority_ids))
        if source_ids_batch:
            source_ids_writer.write_table(pa.table(list(zip(*source_ids_batch)), schema=source_ids))
    pbar.close()
    duckdb.query("SET enable_progress_bar_print=TRUE")
    duckdb.query("SET progress_bar_time=0")
    duckdb.query("SET threads=1")
    print("Coalescing and optimising into unified parquet(s):")
    os.makedirs(f"{output}", exist_ok=True)
    for part in ['core', 'names', 'deprecated_isnis', 'same_as', 'authority_ids', 'source_ids']:
        print(f"Processing {part}")
        os.makedirs(f"{output}.tmp.2/{part}", exist_ok=True)
        duckdb.query(f"COPY (SELECT * FROM parquet_scan('{output}.tmp/{part}.parquet')) TO '{output}.tmp.2/{part}' (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 22, STRING_DICTIONARY_PAGE_SIZE_LIMIT 100_000, FILE_SIZE_BYTES {max_file_size})")
        os.remove(f"{output}.tmp/{part}.parquet")
        for file in tqdm(glob.glob(f"{output}.tmp.2/{part}/data_*.parquet")):
            part2 = cast(re.Match, re.search(r'data(_\d+).parquet', file)).group(1)
            if part2 == "_0":
                part2 = ""
            shutil.move(file, f"{output}/{part}{part2}.parquet")
        os.rmdir(f"{output}.tmp.2/{part}")
    os.rmdir(f"{output}.tmp.2")
    os.rmdir(f"{output}.tmp")
#    shutil.rmtree(f"{output}.tmp.2", ignore_errors=True)

if __name__ == '__main__':
    convert_isni()
