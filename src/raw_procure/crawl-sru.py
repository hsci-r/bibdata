#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import glob
import gzip
import logging
import os
import re
import shutil
import time
from typing import cast

import click
import requests
from requests.adapters import HTTPAdapter
from tqdm.auto import tqdm
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO)

def get_session_with_retries(retries=5):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        status=retries,
        status_forcelist=[500, 502, 503, 504],
        backoff_factor=0.1,
        allowed_methods=["HEAD", "GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


@click.command()
@click.option("-v", "--version", help="SRU endpoint version", default="2.0")
@click.option("-e", "--endpoint", help="SRU endpoint url", required=True)
@click.option("-o", "--output", help="output directory", required=True, type=click.Path(file_okay=False, writable=True))
@click.option("-r", "--record-schema", help="record schema")
@click.option("-bs", "--batch-size", help="batch size", default=1000, type=int)
@click.option('-q', '--query', help='query', required=True)
@click.option('-i', '--id', help='dataset id', required=True)
def crawl_sru(endpoint: str, query: str, version: str, record_schema: str, output: str, batch_size: int, id: str):
    """Fetch a dataset from an SRU endpoint"""
    for file in glob.glob(f"{output}/{id}_*.xml.gz"):
        os.remove(file)
    os.makedirs(output, exist_ok=True)
    session = get_session_with_retries()
    params = dict(
        version=version,
        operation="searchRetrieve",
        query=query,
        maximumRecords=batch_size,
        startRecord=1,
        recordSchema=record_schema
    )
    response = session.get(endpoint, params=params).text
    total_records = int(cast(re.Match, re.search(
        "numberOfRecords>([0-9]*)</", response)).group(1))
    logging.info(f"Going to download {total_records:,} records in batches of {batch_size:,}.")
    for start in tqdm(range(1, total_records, batch_size)):
        with gzip.open(output  + os.sep + f'{id}_{str(start)}.xml.gz', 'wb') as f:
            # logging.info(f"Processing batch {start} of {total_records}.")
            params['startRecord'] = start
            response = session.get(endpoint, params=params)
            response.raise_for_status()
            f.write(response.content)


if __name__ == '__main__':
    crawl_sru()
