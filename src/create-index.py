import glob
import os
from pathlib import Path
import re
from typing import cast
import click
import humanize

@click.command()
def create_index():
    with open("data/index.html", "wt") as f:
        f.write("<html><body><h1>Index</h1><ul>")
        for file in glob.glob("data/*/*-overview.html"):
            with open(file, "rt") as inf:
                content = inf.read()
                title = re.search(r"<title>.*?Composition Analysis of the (.*?)</title>", content, re.DOTALL)
            name = cast(re.Match, title).group(1).strip()
            f.write(f"<li><a href='{Path(file).relative_to('data')}'>{name}</a></li>\n")
#            with open(Path(file).parent / "index.html", "wt") as index_file:
#                index_file.write("<html><body><h1>Index</h1><ul>")
#                index_file.write(f"<li><a href='{Path(file).name}'>Overview of {name}</a></li>\n")
#                for file2 in glob.glob(str(Path(file).parent / "*.parquet")):
#                    index_file.write(f"<li><a href='{file2}'>{file2} ({humanize.naturalsize(os.path.getsize(file2))})</a></li>\n")
#                index_file.write("</ul></body></html>")
        f.write("</ul></body></html>")

if __name__ == "__main__":
    create_index()