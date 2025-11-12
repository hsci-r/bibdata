import logging
import time
from collections.abc import Iterator
from typing import Optional, cast

import click
import fsspec
import requests
from xml.etree import ElementTree
from lxml import etree  # pyright: ignore[reportAttributeAccessIssue]
from requests import HTTPError, Response
from requests.exceptions import RequestException
from tqdm import tqdm

DEFAULT_MAX_RETRIES = 20
DEFAULT_TIMEOUT = (600, 600)
BACKOFF_SECONDS = 60
CHUNK_SIZE = 128 * 1024
OAI_NAMESPACE = "http://www.openarchives.org/OAI/2.0/"


class OAINoRecordsMatch(RuntimeError):
    """Raised when the server responds with a noRecordsMatch error."""


def _strip_namespaces(doc: etree._Element) -> None:
    """Remove all namespaces from an lxml element tree."""
    for el in doc.iter():
        if isinstance(el.tag, str) and el.tag.startswith('{'):
            el.tag = el.tag.split('}', 1)[1]
        # Loop on element attributes also
        attrib_keys = list(el.attrib.keys())
        for an in attrib_keys:
            if an.startswith('{'):
                el.attrib[an.split('}', 1)[1]] = el.attrib.pop(an)
    etree.cleanup_namespaces(doc, top_nsmap=None, keep_ns_prefixes=False)


def _qualify(tag: str, namespace: Optional[str]) -> str:
    return f'{{{namespace}}}{tag}' if namespace else tag


def _strip_tag(tag: str) -> str:
    if tag.startswith('{'):
        return tag.split('}', 1)[1]
    return tag


def _cleanup_element(elem: etree._Element) -> None:
    parent = elem.getparent()
    elem.clear()
    prev = elem.getprevious()
    while prev is not None:
        prev_parent = prev.getparent()
        if prev_parent is not None:
            prev_parent.remove(prev)
        prev = elem.getprevious()
    if parent is not None:
        parent.remove(elem)


def _request_with_retry(
    session: requests.Session,
    endpoint: str,
    params: dict[str, str],
    max_retries: int,
    timeout: tuple[int, int],
) -> Response:
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            response = session.get(endpoint, params=params, timeout=timeout, stream=True)
            response.raise_for_status()
            return response
        except (HTTPError, RequestException) as exc:
            last_exc = exc
            logging.warning("Retrying after exception (%s/%s): %s", attempt + 1, max_retries, exc)
            if attempt + 1 == max_retries:
                break
            time.sleep(BACKOFF_SECONDS * (attempt + 1))
    assert last_exc is not None
    raise last_exc

def stream_oai_records(
    session: requests.Session,
    endpoint: str,
    metadata_prefix: str,
    set_spec: Optional[str],
    from_timestamp: Optional[str],
    until_timestamp: Optional[str],
    *,
    strip_xml: bool,
    full_record: bool,
    max_retries: int = DEFAULT_MAX_RETRIES,
    timeout: tuple[int, int] = DEFAULT_TIMEOUT,
) -> Iterator[str]:
    stats: dict[str, int] = {'reqs': 0, 'deleted': 0, 'accepted': 0}
    resumption_token: Optional[str] = None
    with tqdm(unit="records", smoothing=0, total=None, leave=True, dynamic_ncols=True) as progress_bar:
        while True:
            if resumption_token:
                params = {'verb': 'ListRecords', 'resumptionToken': resumption_token}
            else:
                params = {'verb': 'ListRecords', 'metadataPrefix': metadata_prefix}
                if set_spec:
                    params['set'] = set_spec
                if from_timestamp:
                    params['from'] = from_timestamp
                if until_timestamp:
                    params['until'] = until_timestamp

            response = _request_with_retry(session, endpoint, params, max_retries=max_retries, timeout=timeout)
            stats['reqs'] += 1
            progress_bar.set_postfix(stats, refresh=False)
            parser = etree.XMLPullParser(events=('end',), recover=True, huge_tree=True, resolve_entities=False)
            response.raw.decode_content = True
            next_token: Optional[str] = None
            try:
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if not chunk:
                        continue
                    parser.feed(chunk)
                    for _, elem in parser.read_events():
                        if any(_get_namespace(parent) == OAI_NAMESPACE and _strip_tag(parent.tag) == 'record' for parent in elem.iterancestors()):
                            continue
                        if _get_namespace(elem) != OAI_NAMESPACE:
                            logging.warning("Skipping element with unexpected namespace: %s", etree.tostring(elem))
                            continue
                        tag = _strip_tag(elem.tag)
                        if tag == 'record':
                            record_xml: Optional[str] = None
                            if _is_deleted_record(elem):
                                stats['deleted'] += 1
                            else:
                                payload = elem
                                if not full_record:
                                    payload = elem.find(_qualify('metadata', OAI_NAMESPACE))[0]
                                if payload is None:
                                    logging.warning("Skipping record with missing payload: %s", etree.tostring(elem))
                                else:
                                    if strip_xml:
                                        _strip_namespaces(payload)
                                    record_xml = ElementTree.tostring(payload, encoding='unicode', method='xml')
                                    stats['accepted'] += 1
                            progress_bar.set_postfix(stats, refresh=False)
                            progress_bar.update(1)
                            if record_xml:
                                yield record_xml
                        elif tag == 'resumptionToken':
                            complete_list_size = elem.get('completeListSize')
                            if complete_list_size:
                                try:
                                    progress_bar.total = int(complete_list_size)
                                    progress_bar.refresh()
                                except ValueError:
                                    logging.debug("Invalid completeListSize: %s", complete_list_size)
                            next_token = elem.text
                        elif tag == 'error':
                            code = elem.get('code', 'unknown')
                            message = elem.text
                            if code == 'noRecordsMatch':
                                raise OAINoRecordsMatch(message or 'No records match the request')
                            raise RuntimeError(f"OAI error {code}: {message}")
                        elif not tag in {'OAI-PMH','request','responseDate','ListRecords'}:
                            logging.warning("Skipping unexpected element: %s", etree.tostring(elem))
                        _cleanup_element(elem)
            finally:
                response.close()

            if not next_token:
                break
            resumption_token = next_token


def _list_metadata_formats(
    session: requests.Session,
    endpoint: str,
    max_retries: int = DEFAULT_MAX_RETRIES,
    timeout: tuple[int, int] = DEFAULT_TIMEOUT,
) -> list[str]:
    response = _request_with_retry(session, endpoint, {'verb': 'ListMetadataFormats'}, max_retries=max_retries, timeout=timeout)
    try:
        content = response.content
    finally:
        response.close()
    root = etree.fromstring(content)
    xpath = _qualify('metadataPrefix', OAI_NAMESPACE)
    return [node.text.strip() for node in root.findall(f'.//{xpath}') if node.text]


def _list_sets(
    session: requests.Session,
    endpoint: str,
    max_retries: int = DEFAULT_MAX_RETRIES,
    timeout: tuple[int, int] = DEFAULT_TIMEOUT,
) -> list[tuple[str, str]]:
    response = _request_with_retry(session, endpoint, {'verb': 'ListSets'}, max_retries=max_retries, timeout=timeout)
    try:
        content = response.content
    finally:
        response.close()
    root = etree.fromstring(content)
    spec_xpath = f'.//{_qualify("setSpec", OAI_NAMESPACE)}'
    name_xpath = f'.//{_qualify("setName", OAI_NAMESPACE)}'
    specs = [node.text.strip() for node in root.findall(spec_xpath) if node.text]
    names = [node.text.strip() for node in root.findall(name_xpath) if node.text]
    return list(zip(specs, names))


def _get_namespace(elem: etree._Element) -> Optional[str]:
    if isinstance(elem.tag, str) and elem.tag.startswith('{'):
        return elem.tag.split('}', 1)[0][1:]
    return None


def _is_deleted_record(record_elem: etree._Element) -> bool:
    header = record_elem.find(_qualify('header', OAI_NAMESPACE))
    return header is not None and header.get('status') == 'deleted'

@click.command()
@click.option('-p', '--metadata-prefix', help="Metadata prefix to query")
@click.option('-e', '--endpoint', help="OAI-PMH endpoint to query", required=True)
@click.option('-s', '--set', 'set_spec', help="Set to query")
@click.option('-f', '--from-timestamp', help="Date from which to query records")
@click.option('-u', '--until-timestamp', help="Date until which to query records")
@click.option('-sx/-nsx', '--strip-xml/--no-strip-xml', default=True,
              help="whether to strip XML namespaces from XML output (default is to strip)")
@click.option('-fr/-nfr', '--full-record/--no-full-record', default=False,
              help="whether to output the record in full or only the main content of it without the OAI/PMH metadata (default is to output only the main content)")
@click.option('-o', '--output', help="output (gz/bz2/xz/zst) file in which to write records", required=True)
def crawl_oai_pmh(endpoint: str, metadata_prefix: Optional[str], output: str, set_spec: Optional[str], from_timestamp: Optional[str], until_timestamp: Optional[str], strip_xml: bool, full_record: bool) -> None:
    """Download metadata and records from an OAI-PMH endpoint from the desired metadata prefix."""

    with requests.Session() as session:
        session.headers.update({'User-Agent': 'foo'})
        if metadata_prefix is None:
            prefixes = _list_metadata_formats(session, endpoint)
            sets = _list_sets(session, endpoint)
            if prefixes:
                print("Available prefixes (specify with -p or --metadata-prefix): " + ', '.join(prefixes))
            else:
                print("No metadata prefixes returned by the endpoint.")
            if sets:
                formatted_sets = '\n'.join(f"{spec}: {name}" for spec, name in sets)
                print("Available sets (optionally specify with -s or --set):\n" + formatted_sets)
            else:
                print("No sets returned by the endpoint.")
            return

        with cast(fsspec.core.OpenFile, fsspec.open(output, 'wt', compression='infer')) as of:
            of.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            of.write('<records>\n')
            try:
                for record_xml in stream_oai_records(
                    session,
                    endpoint,
                    metadata_prefix,
                    set_spec,
                    from_timestamp,
                    until_timestamp,
                    strip_xml=strip_xml,
                    full_record=full_record,
                ):
                    of.write(record_xml)
                    of.write('\n')
            except OAINoRecordsMatch:
                logging.warning("No records found.")
            of.write('</records>\n')


if __name__ == '__main__':
    crawl_oai_pmh()
