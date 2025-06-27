import glob
from pathlib import Path
import re
from typing import cast
import click

@click.command()
def create_index():
    with open("data/index.html", "wt") as f:
        f.write("<html><body><h1>Index</h1><ul>")
        for file in glob.glob("data/*/*-overview.html"):
            with open(file, "rt") as inf:
                content = inf.read()
                title = re.search(r"<title>.*?Composition Analysis of the (.*?)</title>", content, re.DOTALL)
                f.write(f"<li><a href='{Path(file).parent.relative_to('data')}'>{cast(re.Match, title).group(1).strip()}</a></li>\n")
        f.write("</ul></body></html>")

if __name__ == "__main__":
    create_index()