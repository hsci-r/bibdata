import io
import os
import importlib.util
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

MODULE_PATH = SRC_PATH / "crawl-oai-pmh.py"

_spec = importlib.util.spec_from_file_location("_crawl_oai_pmh_module", MODULE_PATH)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Unable to load crawl_oai_pmh module from {MODULE_PATH}")
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)

crawl_oai_pmh = getattr(_module, "crawl_oai_pmh")

ENDPOINT = "https://oai-pmh.api.melinda.kansalliskirjasto.fi/bib"
CACHE_PATH = Path(__file__).resolve().parents[0] / "data" / "oai_melinda_marc_2020-05-11_2020-05-12.xml"


class _DummyRaw(io.BytesIO):
    def __init__(self, data: bytes) -> None:
        super().__init__(data)
        self.decode_content = False


class _DummyResponse:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self.raw = _DummyRaw(data)
        self._closed = False

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int = 1) -> Iterator[bytes]:
        for start in range(0, len(self._data), chunk_size):
            yield self._data[start:start + chunk_size]

    def close(self) -> None:
        self._closed = True


class _DummySession:
    def __init__(self, responses: list[_DummyResponse]) -> None:
        if not responses:
            raise ValueError("Dummy session requires at least one response")
        self._responses = responses
        self._calls = 0
        self.headers: dict[str, str] = {}

    def get(self, *_args: object, **_kwargs: object) -> _DummyResponse:
        self._calls += 1
        try:
            return self._responses[self._calls - 1]
        except IndexError as exc:  # pragma: no cover - defensive to surface unexpected extra calls
            raise AssertionError("Unexpected additional request in dummy session") from exc

    def close(self) -> None:
        for response in self._responses:
            response.close()

    @property
    def calls(self) -> int:
        return self._calls


def _extract_payloads_from_doc(path: Path) -> list[_module.etree._Element]:  # type: ignore[attr-defined]
    doc = _module.etree.parse(str(path))
    root = doc.getroot()
    strip_tag = getattr(_module, "_strip_tag")
    qualify = getattr(_module, "_qualify")
    get_namespace = getattr(_module, "_get_namespace")
    select_payload = getattr(_module, "_select_record_payload")

    root_tag = strip_tag(root.tag)
    if root_tag == "records":
        return list(root)

    if root_tag == "OAI-PMH":
        namespace = get_namespace(root)
        payloads = []
        for record_elem in root.findall(f'.//{qualify("record", namespace)}'):
            payload = select_payload(record_elem, full_record=False)
            if payload is not None:
                payloads.append(payload)
        return payloads

    raise AssertionError(f"Unexpected root tag: {root.tag}")


def _run_crawler(tmp_path: Path) -> Path:
    output_path = tmp_path / "melinda.xml"
    runner = CliRunner()
    result = runner.invoke(
        crawl_oai_pmh,
        [
            "--endpoint",
            ENDPOINT,
            "--metadata-prefix",
            "melinda_marc",
            "--from-timestamp",
            "2020-05-11",
            "--until-timestamp",
            "2020-05-12",
            "--output",
            str(output_path),
        ],
        catch_exceptions=False,
    )
    if result.exit_code != 0:
        raise RuntimeError(result.output)
    return output_path


@pytest.mark.skipif(not CACHE_PATH.exists(), reason="Cached OAI response missing")
def test_stream_parser_emits_records_from_cache() -> None:
    content = CACHE_PATH.read_bytes()
    response = _DummyResponse(content)
    empty_followup = _DummyResponse(
        b"""<?xml version='1.0' encoding='UTF-8'?><OAI-PMH xmlns='http://www.openarchives.org/OAI/2.0/'><responseDate>2020-05-12T00:00:00Z</responseDate><ListRecords/></OAI-PMH>"""
    )
    session = _DummySession([response, empty_followup])

    records = list(
        _module.stream_oai_records(
            session,
            ENDPOINT,
            "melinda_marc",
            None,
            "2020-05-11",
            "2020-05-12",
            strip_xml=False,
            full_record=False,
        )
    )

    assert records, "Expected at least one record in cached OAI response"
    assert response.raw.decode_content is True
    assert session.calls >= 1


@pytest.mark.skipif(not CACHE_PATH.exists(), reason="Cached OAI response missing")
def test_strip_namespaces_on_payload() -> None:
    payloads = _extract_payloads_from_doc(CACHE_PATH)
    assert payloads, "Cached payloads missing"
    payload = payloads[0]
    payload_copy = _module.etree.fromstring(_module.etree.tostring(payload))

    _module.strip_namespaces(payload_copy)
    assert '{' not in payload_copy.tag
    for element in payload_copy.iter():
        assert '{' not in element.tag


@pytest.mark.skipif(not CACHE_PATH.exists(), reason="Cached OAI response missing")
def test_cached_oai_sample(tmp_path: Path) -> None:
    cached_copy = tmp_path / "cached.xml"
    cached_copy.write_bytes(CACHE_PATH.read_bytes())
    payloads = _extract_payloads_from_doc(cached_copy)
    assert payloads, "Cached OAI response should contain at least one record"


@pytest.mark.skipif(bool(os.environ.get("NO_NETWORK")), reason="Network-access tests disabled via NO_NETWORK")
def test_crawl_oai_pmh_live(tmp_path: Path) -> None:
    output_path = _run_crawler(tmp_path)
    payloads = _extract_payloads_from_doc(output_path)
    assert payloads, "API response should contain at least one record"
