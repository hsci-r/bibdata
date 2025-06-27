import logging
import time
from typing import Optional, cast
from xml.etree import ElementTree

import click
import fsspec
from lxml import etree
from requests import HTTPError
from sickle import Sickle, OAIResponse, oaiexceptions
from tqdm import tqdm

_huge_tree_xml_parser = etree.XMLParser(remove_blank_text=True, huge_tree=True, recover=True, resolve_entities=False)


class HugeTreeOAIResponse(OAIResponse):

    def __init__(self, http_response, params):
        super().__init__(http_response, params)

    @property
    def xml(self):
        """The server's response as parsed XML."""
        return etree.XML(self.http_response.content,
                         parser=_huge_tree_xml_parser)


class HugeTreeSickle(Sickle):
    def __init__(self, endpoint, **kwargs):
        super().__init__(endpoint, **kwargs)

    def harvest(self, **kwargs):
        for i in range(self.max_retries - 1):
            try:
                response = super().harvest(**kwargs)
                return HugeTreeOAIResponse(response.http_response, response.params)
            except Exception as e:
                logging.warning(f"Retrying after exception: {e}")
                time.sleep(60*i)
        response = super().harvest(**kwargs)
        return HugeTreeOAIResponse(response.http_response, response.params)


def strip_namespaces(doc):
    """Remove all namespaces"""
    for el in doc.getiterator():
        if el.tag.startswith('{'):
            el.tag = el.tag.split('}', 1)[1]
        # loop on element attributes also
        for an in el.attrib.keys():
            if an.startswith('{'):
                el.attrib[an.split('}', 1)[1]] = el.attrib.pop(an)
    # remove namespace declarations
    etree.cleanup_namespaces(doc, top_nsmap=None, keep_ns_prefixes=False)


@click.command()
@click.option('-p', '--metadata-prefix', help="Metadata prefix to query")
@click.option('-e', '--endpoint', help="OAI-PMH endpoint to query", required=True)
@click.option('-s', '--set', help="Set to query")
@click.option('-f', '--from-timestamp', help="Date from which to query records")
@click.option('-u', '--until-timestamp', help="Date until which to query records")
@click.option('-sx/-nsx', '--strip-xml/--no-strip-xml', default=True,
              help="whether to strip XML namespaces from XML output (default is to strip)")
@click.option('-fr/-nfr', '--full-record/--no-full-record', default=False,
              help="whether to output the record in full or only the main content of it without the OAI/PMH metadata (default is to output only the main content)")
@click.option('-o', '--output', help="output (gz/bz2/xz/zst) file in which to write records", required=True)
def crawl_oai_pmh(endpoint: str, metadata_prefix: Optional[str], output: str, set: Optional[str], from_timestamp: Optional[str], until_timestamp: Optional[str], strip_xml: bool, full_record: bool):
    """Download metadata and records from an OAI-PMH endpoint from the desired metadata prefix"""
    sickle = HugeTreeSickle(endpoint, retry_status_codes=[429, 500, 502, 503], max_retries=20,
                            headers={'User-Agent': 'foo'})
    if metadata_prefix is None:
        print(
            f"Available prefixes (specify with -p or --metadata-prefix): {', '.join([prefix.metadataPrefix for prefix in sickle.ListMetadataFormats()])}")
        print(
            f"Available sets (optionally specify with -s or --set):\n{'\n'.join([set.setSpec+': '+set.setName for set in sickle.ListSets()])}")
        return
    with cast(fsspec.core.OpenFile, fsspec.open(output, 'wt', compression='infer')) as of:
        of.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        of.write('<records>\n')
        try:
            records = sickle.ListRecords(metadataPrefix=metadata_prefix, set=set, ignore_deleted=True, **{'from':from_timestamp}, until=until_timestamp)
        except oaiexceptions.NoRecordsMatch as e:
            logging.warning(f"No records found.")
            of.write('</records>\n')
            return
        total = int(records.resumption_token.complete_list_size) if records.resumption_token is not None and records.resumption_token.complete_list_size else None
        for record in tqdm(records, unit="records", smoothing=0, 
                            total=total, leave=True, dynamic_ncols=True):
            if full_record:
                elem = record.xml
            else:
                elem = record.xml.find('.//' + record._oai_namespace + 'metadata').getchildren()[0]
            if strip_xml:
                strip_namespaces(elem)
            of.write(ElementTree.tostring(elem, encoding='unicode', method='xml'))
            of.write('\n')
        of.write('</records>\n')


if __name__ == '__main__':
    crawl_oai_pmh()
