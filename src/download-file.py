import email.utils
import os
import shlex
import subprocess
import uuid
from pathlib import Path
import click
import requests


@click.command()
@click.argument('url')
@click.argument('output_file')
def download_file(
    url: str,
    output_file: str,
):
    head_response = requests.head(url)
    head_response.raise_for_status()
    remote_size = int(head_response.headers['Content-Length']) if 'Content-Length' in head_response.headers else None
    if 'ETag' not in head_response.headers:
        print(f"No ETag found for {url}. This may lead to unnecessary downloads.")
        etag = "Unknown " + uuid.uuid4().hex
    else:
        etag = head_response.headers["ETag"]
    if os.path.exists(f"{output_file}.etag"):
        with open(f"{output_file}.etag", "r") as f:
            old_etag = f.read().strip()
    else:
        old_etag = None
        Path(output_file).unlink(missing_ok=True)
    if etag != old_etag:
        print(f"ETag changed for {url} ({etag}!={old_etag})")
        Path(output_file).unlink(missing_ok=True)
    for _ in range(20):
        if os.path.exists(output_file) and os.path.getsize(output_file) == remote_size:
            print(f"File {output_file} is up to date (ETag {etag}, size {remote_size}).")
            if etag:
                with open(f"{output_file}.etag", "w") as f:
                    f.write(etag)
            if 'Last-Modified' in head_response.headers:
                last_modified = email.utils.parsedate_to_datetime(head_response.headers['Last-Modified']).strftime("%Y-%m-%d")
                with open(f"{output_file}.last_modified", "w") as f:
                    f.write(last_modified)
            return
        cmd = f"curl -C - {url} -o {output_file}"
        print(f"Running: {cmd}")
        subprocess.run(shlex.split(cmd))
    raise RuntimeError("Failed to download file after 20 attempts. Please check the URL or network connection.")

if __name__ == "__main__":
    download_file()