
import email.utils
import glob
from pathlib import Path
import shlex
import subprocess
import time
from typing import Optional
import uuid
import dagster as dg
import requests
import os

def log_and_run(cmd: str, context: dg.AssetExecutionContext, check_returncode: bool = True) -> None:
    context.log.info(f"Running: {cmd}")
    result = subprocess.run(shlex.split(cmd))
    if check_returncode:
        result.check_returncode()


# New utility for bibxml2
def run_bibxml2(
    context: dg.AssetExecutionContext,
    output_file: str,
    input_files: str,
    fmt: str,
    no_input_glob: bool = False,
) -> dg.MaterializeResult:
    for path in glob.glob(output_file.replace(".parquet","*.parquet")):
        os.remove(path)
    cmd = f"bib2 -f {fmt} -o {output_file} {' '.join(glob.glob(input_files)) if not no_input_glob else input_files}"
    log_and_run(cmd, context)
    return get_parquet_glob_sha1sum(output_file)

def download_file(
    context: dg.AssetExecutionContext,
    url: str,
    output_file: str,
) -> dg.MaterializeResult:
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    cmd = f"python src/download-file.py {shlex.quote(url)} {shlex.quote(output_file)}"
    log_and_run(cmd, context)
    with open(f"{output_file}.etag", "r") as f:
        etag = f.read().strip()
        return  dg.MaterializeResult(data_version=dg.DataVersion(etag))

def get_etag(url: str) -> dg.DataVersion:
    etag = requests.head(url).headers.get("ETag")
    if etag:
        return dg.DataVersion(etag)
    else:
        return dg.DataVersion("Unknown " + uuid.uuid4().hex)
    
def get_date_from_last_modified_file(file_glob: str) -> str:
    mtime = ""
    for file in glob.glob(file_glob):
        with open(file+".last_modified", "r") as f:
            date_str = f.read().strip()
            if date_str > mtime:
                mtime = date_str
    return mtime

def get_date_from_file_modification_time(file_glob: str) -> str:
    mtime = max(os.path.getmtime(f) for f in glob.glob(file_glob))
    return time.strftime("%Y-%m-%d", time.gmtime(mtime))
    
def get_parquet_glob_sha1sum(file_path: str) -> dg.MaterializeResult:
    import hashlib
    sha1 = hashlib.sha1()
    for file_path in glob.glob(file_path.replace(".parquet", "*.parquet")):
        with open(file_path, "rb") as f:
            while chunk := f.read(1024*128):
                sha1.update(chunk)
    return dg.MaterializeResult(data_version=dg.DataVersion(sha1.hexdigest()))

def create_overview(
    context: dg.AssetExecutionContext,
    name: str,
    data_glob: str,
    date_modified: Optional[str],
    fields_file: Optional[str],
    subfields_file: Optional[str],
    output_file: str,
    start_year: Optional[str] = None,
    end_year: Optional[str] = None,
) -> None:
    log_and_run(
        f"Rscript src/create-overview.R "
        f"-n '{name}' " +
        (f"-y {start_year} " if start_year else "") +
        (f"-z {end_year} " if end_year else "") +
        f"-d {data_glob.replace('.parquet','*.parquet')} " +
        (f"-f {fields_file} " if fields_file else "") +
        (f"-s {subfields_file} " if subfields_file else "") +
        (f"-m {date_modified} " if date_modified else "") +
        f"-o {output_file}", context
    )