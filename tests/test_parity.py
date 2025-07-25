import os
import sys
import shutil
import subprocess
import tempfile
import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
INPUT_BZ2 = REPO_ROOT / "tests" / "test.json.bz2"
PY_SCRIPT = REPO_ROOT / "src" / "process-wikidata.py"


def has_module(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def have_python_deps() -> bool:
    return all(has_module(m) for m in ["duckdb", "pyarrow", "fsspec", "msgspec", "click"])  # type: ignore


def have_cargo() -> bool:
    return shutil.which("cargo") is not None


def rust_binary_path() -> Path:
    # Prefer an already built debug binary to avoid long builds during tests
    cand = REPO_ROOT / "target" / "debug" / ("process-wikidata.exe" if os.name == "nt" else "process-wikidata")
    return cand


def test_parity_python_vs_rust():
    # Pre-flight checks — fail if prerequisites are missing
    if not INPUT_BZ2.exists():
        pytest.fail(f"Missing input dump: {INPUT_BZ2}")
    if not PY_SCRIPT.exists():
        pytest.fail(f"Missing Python script: {PY_SCRIPT}")
    if not have_cargo():
        pytest.fail("Rust cargo not found in PATH")
    if not have_python_deps():
        pytest.fail("Missing required Python packages: duckdb, pyarrow, fsspec, msgspec, click")

    import duckdb  # lazy import after skip checks

    # Allow preserving outputs for manual inspection via env vars.
    # - PARITY_OUTPUT_DIR: if set, write outputs under this directory and do not delete afterwards
    # - PARITY_KEEP: if truthy (1/true/yes), keep a temporary directory and print its path
    env_out = os.getenv("PARITY_OUTPUT_DIR")
    keep_flag = os.getenv("PARITY_KEEP", "").lower() in ("1", "true", "yes")

    if env_out:
        base_dir = Path(env_out)
        base_dir.mkdir(parents=True, exist_ok=True)
        tmp_cm = None
    elif keep_flag:
        base_dir = Path(tempfile.mkdtemp(prefix="parity_"))
        tmp_cm = None
    else:
        tmp_cm = tempfile.TemporaryDirectory()
        base_dir = Path(tmp_cm.name)

    try:
        out_rust = Path(base_dir) / "out_rust"
        out_py = Path(base_dir) / "out_py"
        out_rust.mkdir(parents=True, exist_ok=True)
        out_py.mkdir(parents=True, exist_ok=True)

        # Run Rust using prebuilt binary if available; otherwise build quickly
        subprocess.run([
            "cargo", "build", "--quiet"
        ], check=True, cwd=REPO_ROOT, timeout=600)            
        # Build (may take long on first run). Build from repo root.

        cmd_bzo = ['lbzcat', str(INPUT_BZ2)]
        bzo = subprocess.Popen(cmd_bzo, cwd=REPO_ROOT, stdout=subprocess.PIPE)
        cmd_rust = [
            str(rust_binary_path()),
            "--output", str(out_rust),
            "--batch-size", "500",
        ]
        subprocess.run(cmd_rust, cwd=REPO_ROOT, stdin=bzo.stdout, timeout=300, check=True)
        if bzo.wait(timeout=300):
            raise subprocess.CalledProcessError(bzo.returncode, cmd_bzo)

        # Run Python script with same input
        cmd_py = [
            sys.executable,
            str(PY_SCRIPT),
            "--input",
            str(INPUT_BZ2),
            "--output",
            str(out_py),
            "--batch-size",
            "500",
        ]
        subprocess.run(cmd_py, check=True, cwd=REPO_ROOT, timeout=300)

        con = duckdb.connect(database=":memory:")
        con.execute("SET threads=1")

    # Compare datasets while disregarding integer entity IDs by mapping them back to string IDs.
        left_entities_glob = str(out_rust / "entities*.parquet")
        right_entities_glob = str(out_py / "entities*.parquet")

        # 1) Entities: compare only the string id column
        for name in ["entities"]:
            left_glob = str(out_rust / f"{name}*.parquet")
            right_glob = str(out_py / f"{name}*.parquet")
            left_any = list(Path(out_rust).glob(f"{name}*.parquet"))
            right_any = list(Path(out_py).glob(f"{name}*.parquet"))
            if not left_any and not right_any:
                pytest.fail(f"No output files produced for dataset '{name}' on either side")
            assert left_any, f"missing {left_glob}"
            assert right_any, f"missing {right_glob}"
            row = con.execute(f"SELECT COUNT(*) FROM parquet_scan('{left_glob}')").fetchone()
            assert row is not None
            lc = row[0]
            row = con.execute(f"SELECT COUNT(*) FROM parquet_scan('{right_glob}')").fetchone()
            assert row is not None
            rc = row[0]
            # Fail if both sides are empty (entities must not be empty)
            if lc == 0 and rc == 0:
                pytest.fail(f"No rows found for dataset '{name}' on either side")
            assert lc == rc, f"Row count differs for {name}"
            row = con.execute(
                f"WITH l AS (SELECT id FROM parquet_scan('{left_glob}')), r AS (SELECT id FROM parquet_scan('{right_glob}')) SELECT COUNT(*) FROM (SELECT * FROM l EXCEPT SELECT * FROM r)"
            ).fetchone()
            assert row is not None
            d1 = row[0]
            row = con.execute(
                f"WITH l AS (SELECT id FROM parquet_scan('{left_glob}')), r AS (SELECT id FROM parquet_scan('{right_glob}')) SELECT COUNT(*) FROM (SELECT * FROM r EXCEPT SELECT * FROM l)"
            ).fetchone()
            assert row is not None
            d2 = row[0]
            assert d1 + d2 == 0, f"Dataset {name} differs"

        # 2) Simple datasets that reference entity IDs: project to string IDs via join and compare
        simple_map = [
            "labels",  # entity_id -> id
            "aliases",  # entity_id -> id
            "descriptions",  # entity_id -> id
            "datatypes",  # entity_id -> id
            "sitelinks",  # entity_id -> id
            "sitelink_badges",  # entity_id -> id, badge_entity_id -> id
        ]
        for name in simple_map:
            left_glob = str(out_rust / f"{name}*.parquet")
            right_glob = str(out_py / f"{name}*.parquet")
            left_any = list(Path(out_rust).glob(f"{name}*.parquet"))
            right_any = list(Path(out_py).glob(f"{name}*.parquet"))
            if not left_any and not right_any:
                pytest.fail(f"No output files produced for dataset '{name}' on either side")
            assert left_any, f"missing {left_glob}"
            assert right_any, f"missing {right_glob}"

            row = con.execute(f"SELECT COUNT(*) FROM parquet_scan('{left_glob}')").fetchone()
            assert row is not None
            lc = row[0]
            row = con.execute(f"SELECT COUNT(*) FROM parquet_scan('{right_glob}')").fetchone()
            assert row is not None
            rc = row[0]
            # Allow datatypes to be empty (test data has no properties), otherwise require non-empty on both sides.
            if name != "datatypes" and lc == 0 and rc == 0:
                pytest.fail(f"No rows found for dataset '{name}' on either side")
            assert lc == rc, f"Row count differs for {name}"

            if name == "sitelinks":
                lsql = f"""
                    WITH l AS (SELECT * FROM parquet_scan('{left_glob}')),
                         le AS (SELECT entity_id, id AS entity FROM parquet_scan('{left_entities_glob}'))
                    SELECT le.entity, l.site, l.title FROM l JOIN le ON l.entity_id = le.entity_id
                """
                rsql = f"""
                    WITH r AS (SELECT * FROM parquet_scan('{right_glob}')),
                         re AS (SELECT entity_id, id AS entity FROM parquet_scan('{right_entities_glob}'))
                    SELECT re.entity, r.site, r.title FROM r JOIN re ON r.entity_id = re.entity_id
                """
            elif name == "sitelink_badges":
                lsql = f"""
                    WITH l AS (SELECT * FROM parquet_scan('{left_glob}')),
                         le AS (SELECT entity_id, id AS entity FROM parquet_scan('{left_entities_glob}')),
                         be AS (SELECT entity_id, id AS badge FROM parquet_scan('{left_entities_glob}'))
                    SELECT le.entity, l.site, be.badge
                    FROM l JOIN le ON l.entity_id = le.entity_id
                          LEFT JOIN be ON l.badge_entity_id = be.entity_id
                """
                rsql = f"""
                    WITH r AS (SELECT * FROM parquet_scan('{right_glob}')),
                         re AS (SELECT entity_id, id AS entity FROM parquet_scan('{right_entities_glob}')),
                         be AS (SELECT entity_id, id AS badge FROM parquet_scan('{right_entities_glob}'))
                    SELECT re.entity, r.site, be.badge
                    FROM r JOIN re ON r.entity_id = re.entity_id
                          LEFT JOIN be ON r.badge_entity_id = be.entity_id
                """
            elif name == "datatypes":
                lsql = f"""
                    WITH l AS (SELECT * FROM parquet_scan('{left_glob}')),
                         le AS (SELECT entity_id, id AS entity FROM parquet_scan('{left_entities_glob}'))
                    SELECT le.entity, l.datatype FROM l JOIN le ON l.entity_id = le.entity_id
                """
                rsql = f"""
                    WITH r AS (SELECT * FROM parquet_scan('{right_glob}')),
                         re AS (SELECT entity_id, id AS entity FROM parquet_scan('{right_entities_glob}'))
                    SELECT re.entity, r.datatype FROM r JOIN re ON r.entity_id = re.entity_id
                """
            else:
                # labels, aliases, descriptions
                value_col = {"labels": "label", "aliases": "alias", "descriptions": "description"}[name]
                lsql = f"""
                    WITH l AS (SELECT * FROM parquet_scan('{left_glob}')),
                         le AS (SELECT entity_id, id AS entity FROM parquet_scan('{left_entities_glob}'))
                    SELECT le.entity, l.language, l.{value_col} FROM l JOIN le ON l.entity_id = le.entity_id
                """
                rsql = f"""
                    WITH r AS (SELECT * FROM parquet_scan('{right_glob}')),
                         re AS (SELECT entity_id, id AS entity FROM parquet_scan('{right_entities_glob}'))
                    SELECT re.entity, r.language, r.{value_col} FROM r JOIN re ON r.entity_id = re.entity_id
                """

            row = con.execute(f"WITH l AS ({lsql}), r AS ({rsql}) SELECT COUNT(*) FROM (SELECT * FROM l EXCEPT SELECT * FROM r)").fetchone()
            assert row is not None
            d1 = row[0]
            row = con.execute(f"WITH l AS ({lsql}), r AS ({rsql}) SELECT COUNT(*) FROM (SELECT * FROM r EXCEPT SELECT * FROM l)").fetchone()
            assert row is not None
            d2 = row[0]
            assert d1 + d2 == 0, f"Dataset {name} differs"

        # Claim/qualifier/reference datasets
        ctypes = ["claim", "qualifier", "reference"]
        vkinds = [
            "no_value",
            "some_value",
            "string",
            "wikibase-entityid",
            "time",
            "globecoordinate",
            "monolingualtext",
            "quantity",
        ]
        for c in ctypes:
            for v in vkinds:
                dir_name = f"{c}_{v}"
                # Rust compacts to parts under <dir_name>
                left_glob = out_rust / f"{dir_name}*.parquet"
                # Python may produce single file <dir_name>.parquet or a directory tree under <dir_name>/...
                right_glob = out_py / f"{dir_name}*.parquet"

                # If both sides empty/missing, skip
                try:
                    row = con.execute(f"SELECT COUNT(*) FROM parquet_scan('{left_glob}')").fetchone()
                    lc = int(row[0]) if row is not None else 0
                except Exception:
                    lc = 0
                try:
                    row = con.execute(f"SELECT COUNT(*) FROM parquet_scan('{right_glob}')").fetchone()
                    rc = int(row[0]) if row is not None else 0
                except Exception:
                    rc = 0
                if lc == 0 and rc == 0:
                    if c == 'reference' and v == 'globecoordinate': # this doesn't exist
                        continue
                    pytest.fail(f"No rows found for dataset '{dir_name}' on either side")
                assert lc == rc, f"Row count differs for {dir_name}"

                # Build normalized projections that map integer IDs back to string IDs before comparison
                if c == "claim":
                    if v in ("no_value", "some_value"):
                        lproj = f"""
                            WITH l AS (SELECT * FROM parquet_scan('{left_glob}')),
                                 le AS (SELECT entity_id, id AS entity FROM parquet_scan('{left_entities_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{left_entities_glob}'))
                            SELECT l.rank, le.entity, pe.property, l.datatype FROM l
                            JOIN le ON l.entity_id = le.entity_id
                            JOIN pe ON l.property_id = pe.entity_id
                        """
                        rproj = f"""
                            WITH r AS (SELECT * FROM parquet_scan('{right_glob}')),
                                 re AS (SELECT entity_id, id AS entity FROM parquet_scan('{right_entities_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{right_entities_glob}'))
                            SELECT r.rank, re.entity, pe.property, r.datatype FROM r
                            JOIN re ON r.entity_id = re.entity_id
                            JOIN pe ON r.property_id = pe.entity_id
                        """
                    elif v == "string":
                        lproj = f"""
                            WITH l AS (SELECT * FROM parquet_scan('{left_glob}')),
                                 le AS (SELECT entity_id, id AS entity FROM parquet_scan('{left_entities_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{left_entities_glob}'))
                            SELECT l.rank, le.entity, pe.property, l.datatype, l.value FROM l
                            JOIN le ON l.entity_id = le.entity_id
                            JOIN pe ON l.property_id = pe.entity_id
                        """
                        rproj = f"""
                            WITH r AS (SELECT * FROM parquet_scan('{right_glob}')),
                                 re AS (SELECT entity_id, id AS entity FROM parquet_scan('{right_entities_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{right_entities_glob}'))
                            SELECT r.rank, re.entity, pe.property, r.datatype, r.value FROM r
                            JOIN re ON r.entity_id = re.entity_id
                            JOIN pe ON r.property_id = pe.entity_id
                        """
                    elif v == "wikibase-entityid":
                        lproj = f"""
                            WITH l AS (SELECT * FROM parquet_scan('{left_glob}')),
                                 le AS (SELECT entity_id, id AS entity FROM parquet_scan('{left_entities_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{left_entities_glob}')),
                                 ve AS (SELECT entity_id, id AS value_entity FROM parquet_scan('{left_entities_glob}'))
                            SELECT l.rank, le.entity, pe.property, l.datatype, ve.value_entity FROM l
                            JOIN le ON l.entity_id = le.entity_id
                            JOIN pe ON l.property_id = pe.entity_id
                            LEFT JOIN ve ON l.value_entity_id = ve.entity_id
                        """
                        rproj = f"""
                            WITH r AS (SELECT * FROM parquet_scan('{right_glob}')),
                                 re AS (SELECT entity_id, id AS entity FROM parquet_scan('{right_entities_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{right_entities_glob}')),
                                 ve AS (SELECT entity_id, id AS value_entity FROM parquet_scan('{right_entities_glob}'))
                            SELECT r.rank, re.entity, pe.property, r.datatype, ve.value_entity FROM r
                            JOIN re ON r.entity_id = re.entity_id
                            JOIN pe ON r.property_id = pe.entity_id
                            LEFT JOIN ve ON r.value_entity_id = ve.entity_id
                        """
                    elif v == "time":
                        lproj = f"""
                            WITH l AS (SELECT * FROM parquet_scan('{left_glob}')),
                                 le AS (SELECT entity_id, id AS entity FROM parquet_scan('{left_entities_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{left_entities_glob}')),
                                 ce AS (SELECT entity_id, id AS calendarmodel FROM parquet_scan('{left_entities_glob}'))
                            SELECT l.rank, le.entity, pe.property, l.datatype,
                                   l.time, l.timezone, l.before, l.after, l.precision, ce.calendarmodel
                            FROM l
                            JOIN le ON l.entity_id = le.entity_id
                            JOIN pe ON l.property_id = pe.entity_id
                            LEFT JOIN ce ON l.calendarmodel_entity_id = ce.entity_id
                        """
                        rproj = f"""
                            WITH r AS (SELECT * FROM parquet_scan('{right_glob}')),
                                 re AS (SELECT entity_id, id AS entity FROM parquet_scan('{right_entities_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{right_entities_glob}')),
                                 ce AS (SELECT entity_id, id AS calendarmodel FROM parquet_scan('{right_entities_glob}'))
                            SELECT r.rank, re.entity, pe.property, r.datatype,
                                   r.time, r.timezone, r.before, r.after, r.precision, ce.calendarmodel
                            FROM r
                            JOIN re ON r.entity_id = re.entity_id
                            JOIN pe ON r.property_id = pe.entity_id
                            LEFT JOIN ce ON r.calendarmodel_entity_id = ce.entity_id
                        """
                    elif v == "globecoordinate":
                        lproj = f"""
                            WITH l AS (SELECT * FROM parquet_scan('{left_glob}')),
                                 le AS (SELECT entity_id, id AS entity FROM parquet_scan('{left_entities_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{left_entities_glob}')),
                                 ge AS (SELECT entity_id, id AS globe FROM parquet_scan('{left_entities_glob}'))
                            SELECT l.rank, le.entity, pe.property, l.datatype,
                                   ROUND(l.latitude, 9) AS latitude, ROUND(l.longitude, 9) AS longitude, ROUND(l.precision, 12) AS precision, ge.globe
                            FROM l
                            JOIN le ON l.entity_id = le.entity_id
                            JOIN pe ON l.property_id = pe.entity_id
                            LEFT JOIN ge ON l.globe_entity_id = ge.entity_id
                        """
                        rproj = f"""
                            WITH r AS (SELECT * FROM parquet_scan('{right_glob}')),
                                 re AS (SELECT entity_id, id AS entity FROM parquet_scan('{right_entities_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{right_entities_glob}')),
                                 ge AS (SELECT entity_id, id AS globe FROM parquet_scan('{right_entities_glob}'))
                            SELECT r.rank, re.entity, pe.property, r.datatype,
                                   ROUND(r.latitude, 9) AS latitude, ROUND(r.longitude, 9) AS longitude, ROUND(r.precision, 12) AS precision, ge.globe
                            FROM r
                            JOIN re ON r.entity_id = re.entity_id
                            JOIN pe ON r.property_id = pe.entity_id
                            LEFT JOIN ge ON r.globe_entity_id = ge.entity_id
                        """
                    elif v == "monolingualtext":
                        lproj = f"""
                            WITH l AS (SELECT * FROM parquet_scan('{left_glob}')),
                                 le AS (SELECT entity_id, id AS entity FROM parquet_scan('{left_entities_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{left_entities_glob}'))
                            SELECT l.rank, le.entity, pe.property, l.datatype,
                                   l.language, l.text
                            FROM l
                            JOIN le ON l.entity_id = le.entity_id
                            JOIN pe ON l.property_id = pe.entity_id
                        """
                        rproj = f"""
                            WITH r AS (SELECT * FROM parquet_scan('{right_glob}')),
                                 re AS (SELECT entity_id, id AS entity FROM parquet_scan('{right_entities_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{right_entities_glob}'))
                            SELECT r.rank, re.entity, pe.property, r.datatype,
                                   r.language, r.text
                            FROM r
                            JOIN re ON r.entity_id = re.entity_id
                            JOIN pe ON r.property_id = pe.entity_id
                        """
                    elif v == "quantity":
                        lproj = f"""
                            WITH l AS (SELECT * FROM parquet_scan('{left_glob}')),
                                 le AS (SELECT entity_id, id AS entity FROM parquet_scan('{left_entities_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{left_entities_glob}')),
                                 ue AS (SELECT entity_id, id AS unit FROM parquet_scan('{left_entities_glob}'))
                            SELECT l.rank, le.entity, pe.property, l.datatype,
                                   ROUND(l.amount, 12) AS amount, ROUND(l.lower_bound, 12) AS lower_bound, ROUND(l.upper_bound, 12) AS upper_bound, ue.unit
                            FROM l
                            JOIN le ON l.entity_id = le.entity_id
                            JOIN pe ON l.property_id = pe.entity_id
                            LEFT JOIN ue ON l.unit_entity_id = ue.entity_id
                        """
                        rproj = f"""
                            WITH r AS (SELECT * FROM parquet_scan('{right_glob}')),
                                 re AS (SELECT entity_id, id AS entity FROM parquet_scan('{right_entities_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{right_entities_glob}')),
                                 ue AS (SELECT entity_id, id AS unit FROM parquet_scan('{right_entities_glob}'))
                            SELECT r.rank, re.entity, pe.property, r.datatype,
                                   ROUND(r.amount, 12) AS amount, ROUND(r.lower_bound, 12) AS lower_bound, ROUND(r.upper_bound, 12) AS upper_bound, ue.unit
                            FROM r
                            JOIN re ON r.entity_id = re.entity_id
                            JOIN pe ON r.property_id = pe.entity_id
                            LEFT JOIN ue ON r.unit_entity_id = ue.entity_id
                        """
                    else:
                        raise AssertionError(f"Unhandled vkind {v}")
                else:
                    # qualifier or reference: base columns differ (no entity column)
                    if v in ("no_value", "some_value"):
                        lproj = f"""
                            WITH l AS (SELECT * FROM parquet_scan('{left_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{left_entities_glob}'))
                            SELECT l."order", pe.property, l.datatype FROM l
                            JOIN pe ON l.property_id = pe.entity_id
                        """
                        rproj = f"""
                            WITH r AS (SELECT * FROM parquet_scan('{right_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{right_entities_glob}'))
                            SELECT r."order", pe.property, r.datatype FROM r
                            JOIN pe ON r.property_id = pe.entity_id
                        """
                    elif v == "string":
                        lproj = f"""
                            WITH l AS (SELECT * FROM parquet_scan('{left_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{left_entities_glob}'))
                            SELECT l."order", pe.property, l.datatype, l.value FROM l
                            JOIN pe ON l.property_id = pe.entity_id
                        """
                        rproj = f"""
                            WITH r AS (SELECT * FROM parquet_scan('{right_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{right_entities_glob}'))
                            SELECT r."order", pe.property, r.datatype, r.value FROM r
                            JOIN pe ON r.property_id = pe.entity_id
                        """
                    elif v == "wikibase-entityid":
                        lproj = f"""
                            WITH l AS (SELECT * FROM parquet_scan('{left_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{left_entities_glob}')),
                                 ve AS (SELECT entity_id, id AS value_entity FROM parquet_scan('{left_entities_glob}'))
                            SELECT l."order", pe.property, l.datatype, ve.value_entity FROM l
                            JOIN pe ON l.property_id = pe.entity_id
                            LEFT JOIN ve ON l.value_entity_id = ve.entity_id
                        """
                        rproj = f"""
                            WITH r AS (SELECT * FROM parquet_scan('{right_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{right_entities_glob}')),
                                 ve AS (SELECT entity_id, id AS value_entity FROM parquet_scan('{right_entities_glob}'))
                            SELECT r."order", pe.property, r.datatype, ve.value_entity FROM r
                            JOIN pe ON r.property_id = pe.entity_id
                            LEFT JOIN ve ON r.value_entity_id = ve.entity_id
                        """
                    elif v == "time":
                        lproj = f"""
                            WITH l AS (SELECT * FROM parquet_scan('{left_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{left_entities_glob}')),
                                 ce AS (SELECT entity_id, id AS calendarmodel FROM parquet_scan('{left_entities_glob}'))
                            SELECT l."order", pe.property, l.datatype,
                                   l.time, l.timezone, l.before, l.after, l.precision, ce.calendarmodel
                            FROM l
                            JOIN pe ON l.property_id = pe.entity_id
                            LEFT JOIN ce ON l.calendarmodel_entity_id = ce.entity_id
                        """
                        rproj = f"""
                            WITH r AS (SELECT * FROM parquet_scan('{right_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{right_entities_glob}')),
                                 ce AS (SELECT entity_id, id AS calendarmodel FROM parquet_scan('{right_entities_glob}'))
                            SELECT r."order", pe.property, r.datatype,
                                   r.time, r.timezone, r.before, r.after, r.precision, ce.calendarmodel
                            FROM r
                            JOIN pe ON r.property_id = pe.entity_id
                            LEFT JOIN ce ON r.calendarmodel_entity_id = ce.entity_id
                        """
                    elif v == "globecoordinate":
                        lproj = f"""
                            WITH l AS (SELECT * FROM parquet_scan('{left_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{left_entities_glob}')),
                                 ge AS (SELECT entity_id, id AS globe FROM parquet_scan('{left_entities_glob}'))
                            SELECT l."order", pe.property, l.datatype,
                                   ROUND(l.latitude, 9) AS latitude, ROUND(l.longitude, 9) AS longitude, ROUND(l.precision, 12) AS precision, ge.globe
                            FROM l
                            JOIN pe ON l.property_id = pe.entity_id
                            LEFT JOIN ge ON l.globe_entity_id = ge.entity_id
                        """
                        rproj = f"""
                            WITH r AS (SELECT * FROM parquet_scan('{right_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{right_entities_glob}')),
                                 ge AS (SELECT entity_id, id AS globe FROM parquet_scan('{right_entities_glob}'))
                            SELECT r."order", pe.property, r.datatype,
                                   ROUND(r.latitude, 9) AS latitude, ROUND(r.longitude, 9) AS longitude, ROUND(r.precision, 12) AS precision, ge.globe
                            FROM r
                            JOIN pe ON r.property_id = pe.entity_id
                            LEFT JOIN ge ON r.globe_entity_id = ge.entity_id
                        """
                    elif v == "monolingualtext":
                        lproj = f"""
                            WITH l AS (SELECT * FROM parquet_scan('{left_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{left_entities_glob}'))
                            SELECT l."order", pe.property, l.datatype,
                                   l.language, l.text
                            FROM l
                            JOIN pe ON l.property_id = pe.entity_id
                        """
                        rproj = f"""
                            WITH r AS (SELECT * FROM parquet_scan('{right_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{right_entities_glob}'))
                            SELECT r."order", pe.property, r.datatype,
                                   r.language, r.text
                            FROM r
                            JOIN pe ON r.property_id = pe.entity_id
                        """
                    elif v == "quantity":
                        lproj = f"""
                            WITH l AS (SELECT * FROM parquet_scan('{left_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{left_entities_glob}')),
                                 ue AS (SELECT entity_id, id AS unit FROM parquet_scan('{left_entities_glob}'))
                            SELECT l."order", pe.property, l.datatype,
                                   ROUND(l.amount, 12) AS amount, ROUND(l.lower_bound, 12) AS lower_bound, ROUND(l.upper_bound, 12) AS upper_bound, ue.unit
                            FROM l
                            JOIN pe ON l.property_id = pe.entity_id
                            LEFT JOIN ue ON l.unit_entity_id = ue.entity_id
                        """
                        rproj = f"""
                            WITH r AS (SELECT * FROM parquet_scan('{right_glob}')),
                                 pe AS (SELECT entity_id, id AS property FROM parquet_scan('{right_entities_glob}')),
                                 ue AS (SELECT entity_id, id AS unit FROM parquet_scan('{right_entities_glob}'))
                            SELECT r."order", pe.property, r.datatype,
                                   ROUND(r.amount, 12) AS amount, ROUND(r.lower_bound, 12) AS lower_bound, ROUND(r.upper_bound, 12) AS upper_bound, ue.unit
                            FROM r
                            JOIN pe ON r.property_id = pe.entity_id
                            LEFT JOIN ue ON r.unit_entity_id = ue.entity_id
                        """
                    else:
                        raise AssertionError(f"Unhandled vkind {v}")

                # Compare grouped counts over projected columns to preserve multiplicity while ignoring entity_id and claim_id
                lwrapped = f"SELECT *, COUNT(*) AS cnt FROM ({lproj}) GROUP BY ALL"
                rwrapped = f"SELECT *, COUNT(*) AS cnt FROM ({rproj}) GROUP BY ALL"
                row = con.execute(f"WITH l AS ({lwrapped}), r AS ({rwrapped}) SELECT COUNT(*) FROM (SELECT * FROM l EXCEPT SELECT * FROM r)").fetchone()
                assert row is not None
                d1 = row[0]
                row = con.execute(f"WITH l AS ({lwrapped}), r AS ({rwrapped}) SELECT COUNT(*) FROM (SELECT * FROM r EXCEPT SELECT * FROM l)").fetchone()
                assert row is not None
                d2 = row[0]
                assert d1 + d2 == 0, f"Dataset {dir_name} differs"

    finally:
        # If outputs are preserved, print the location to aid manual inspection.
        if env_out or keep_flag:
            print(f"Parity outputs preserved at: {base_dir}")
        # If we used a TemporaryDirectory context manager, ensure cleanup by letting it go out of scope.
        if tmp_cm is not None:
            # Explicit cleanup for clarity (also happens on context exit)
            tmp_cm.cleanup()
