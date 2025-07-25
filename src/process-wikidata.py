import asyncio
import glob
from io import TextIOWrapper
import os
import re
import shutil
from threading import current_thread
from typing import Literal, Optional, cast
import asyncclick as click
import duckdb
import fsspec
import pyarrow as pa
import pyarrow.parquet as pq

from fsspec.core import compr, infer_compression
from tqdm.auto import tqdm
import msgspec

"""hive (wmf)> describe wikidata_entity;
OK
col_name	data_type	comment
id                  	string              	The id of the entity, P31 or Q32753077 for instance
typ                 	string              	The type of the entity, property or item for instance
datatype            	string              	The data type of the entity when a property
labels              	map<string,string>  	The language/label map of the entity
descriptions        	map<string,string>  	The language/description map of the entity
aliases             	map<string,array<string>>	The language/List-of-aliases map of the entity
claims              	array<struct<id:string,mainSnak:struct<typ:string,property:string,dataType:string,dataValue:struct<typ:string,value:string>,hash:string>,typ:string,rank:string,qualifiers:array<struct<typ:string,property:string,dataType:string,dataValue:struct<typ:string,value:string>,hash:string>>,qualifiersOrder:array<string>,references:array<struct<snaks:array<struct<typ:string,property:string,dataType:string,dataValue:struct<typ:string,value:string>,hash:string>>,snaksOrder:array<string>,hash:string>>>>	The claim array of the entity
sitelinks           	array<struct<site:string,title:string,badges:array<string>,url:string>>	The siteLinks array of the entity
lastrevid           	bigint              	The latest revision id of the entity
snapshot            	string              	Versioning information to keep multiple datasets (YYYY-MM-DD for regular weekly imports)"""

# entity namespaces: Q, P, L, E, L-S, L-F

entity_schema = pa.schema([
    pa.field('entity_id', pa.int64(), nullable=False),
    pa.field('id', pa.string(), nullable=False),
#    pa.field('type', pa.string(), nullable=False),
])

label_schema = pa.schema([pa.field('entity_id', pa.int64(), nullable=False), 
    pa.field('language', pa.string(), nullable=False), 
    pa.field('label', pa.string(), nullable=False)
])

alias_schema = pa.schema([pa.field('entity_id', pa.int64(), nullable=False), 
    pa.field('language', pa.string(), nullable=False),
    pa.field('alias', pa.string(), nullable=False)
])

description_schema = pa.schema([pa.field('entity_id', pa.int64(), nullable=False),
    pa.field('language', pa.string(), nullable=False),
    pa.field('description', pa.string(), nullable=False)
])

datatype_schema = pa.schema([
    pa.field('entity_id', pa.int64(), nullable=False),
    pa.field('datatype', pa.string(), nullable=False)
])

common_claim_fields = (
    pa.field('claim_id', pa.int64(), nullable=False),
    pa.field('rank', pa.string(), nullable=False),
    pa.field('entity_id', pa.int64(), nullable=False),
    pa.field('property_id', pa.int64(), nullable=False),
    pa.field('datatype', pa.string(), nullable=False)
)

common_qualifier_fields = (
    pa.field('order', pa.int32(), nullable=False),
    pa.field('claim_id', pa.int64(), nullable=False),
    pa.field('property_id', pa.int64(), nullable=False),
    pa.field('datatype', pa.string(), nullable=False)
)

def make_claim_schemas(*fields: pa.Field) -> tuple[pa.Schema, pa.Schema]:
    return (pa.schema([
        *common_claim_fields,
        *fields
    ]), pa.schema([
        *common_qualifier_fields, 
        *fields
    ]))

value_schemas={
    'no_value':make_claim_schemas(),
    'some_value':make_claim_schemas(),
    'string':make_claim_schemas(
        pa.field('value', pa.string(), nullable=False),
    ),
    'wikibase-entityid':make_claim_schemas(
        pa.field('value_entity_id', pa.int64(), nullable=False),
    ),
    'time': make_claim_schemas(
        pa.field('time', pa.string(), nullable=False),
        pa.field('timezone', pa.int32(), nullable=False),
        pa.field('before', pa.int32(), nullable=False),
        pa.field('after', pa.int32(), nullable=False),
        pa.field('precision', pa.int32(), nullable=False),
        pa.field('calendarmodel_entity_id', pa.int64(), nullable=False)
    ),
    'globecoordinate': make_claim_schemas(
        pa.field('latitude', pa.float64(), nullable=False),
        pa.field('longitude', pa.float64(), nullable=False),
        pa.field('precision', pa.float64(), nullable=True),
        pa.field('globe_entity_id', pa.int64(), nullable=False),
    ),
    'monolingualtext': make_claim_schemas(
        pa.field('language', pa.string(), nullable=False),
        pa.field('text', pa.string(), nullable=False)
    ),
    'quantity': make_claim_schemas(
        pa.field('amount', pa.float64(), nullable=False),
        pa.field('lower_bound', pa.float64(), nullable=True),
        pa.field('upper_bound', pa.float64(), nullable=True),
        pa.field('unit_entity_id', pa.int64(), nullable=True)
    )
}

sitelink_schema = pa.schema([
    pa.field('entity_id', pa.int64(), nullable=False),
    pa.field('site', pa.string(), nullable=False),
    pa.field('title', pa.string(), nullable=False)
])

sitelink_badge_schema = pa.schema([
    pa.field('entity_id', pa.int64(), nullable=False),
    pa.field('site', pa.string(), nullable=False),
    pa.field('badge_entity_id', pa.int64(), nullable=False)
])

@click.command()
@click.option('-i', '--input', type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True))
@click.option('-o', '--output', type=click.Path(file_okay=False, dir_okay=True, writable=True))
@click.option('-b', '--batch-size', type=int, default=122_880, show_default=True, help='Number of entities to process in each batch.')
async def process_wikidata(input, output, batch_size: int):
    os.makedirs(output+"/tmp", exist_ok=True)
    with cast(fsspec.core.OpenFile, fsspec.open(input, 'rb')) as infile:
        pbar = tqdm(total=infile.fs.size(infile.path), unit='b', smoothing=0, unit_scale=True, unit_divisor=1024, dynamic_ncols=True)
        compression = infer_compression(infile.path)
        if compression is not None:
            uc_infile = compr[compression](infile, mode='rb') # type: ignore
        else:
            uc_infile = infile
        uc_infile = TextIOWrapper(uc_infile, encoding='utf-8')
        batches=dict(
            entities=[],
            labels=[],
            aliases=[],
            descriptions=[],
            datatypes=[],
            sitelinks=[],
            sitelink_badges=[]
        )
        entity_id_map = dict[str, int]()
        def get_entity_id(id: str) -> int:
            entity_id = entity_id_map.get(id, None)
            if entity_id is None:
                entity_id = len(entity_id_map) + 1
                entity_id_map[id] = entity_id
                batches['entities'].append((entity_id, id))
            return entity_id
        def write(writer: pq.ParquetWriter, batch: pa.RecordBatch) -> None:
            writer.write_batch(batch, row_group_size=batch_size)
        async def worker(writer: pq.ParquetWriter, schema: pa.Schema, queue: asyncio.Queue[Optional[list]]) -> None:
            while True:
                batch = await queue.get()
                if batch is None:
                    break
                else:
                    await asyncio.to_thread(write, writer, pa.record_batch(list(zip(*batch)), schema=schema))
                    queue.task_done()
            writer.close()
            queue.task_done()
        def create_writer_thread(name: str, schema: pa.Schema) -> asyncio.Queue[Optional[list]]:
            q = asyncio.Queue()
            writer = pq.ParquetWriter(output+f"/tmp/{name}", schema=schema, compression="zstd")
            asyncio.create_task(worker(writer, schema, q))
            return q
        writers: dict[str, asyncio.Queue[Optional[list]]] = dict(
            entities=create_writer_thread("entities.parquet", entity_schema),
            labels=create_writer_thread("labels.parquet", label_schema),
            aliases=create_writer_thread("aliases.parquet", alias_schema),
            descriptions=create_writer_thread("descriptions.parquet", description_schema),
            datatypes=create_writer_thread("datatypes.parquet", datatype_schema),
            sitelinks=create_writer_thread("sitelinks.parquet", sitelink_schema),
            sitelink_badges=create_writer_thread("sitelink_badges.parquet", sitelink_badge_schema)
        )
        claim_batches_dict = dict()
        def ensure_claim_batch(claim_type: Literal['claim','qualifier','reference'], rank: Literal['normal','preferred','deprecated'], property_id: int, value_type: Literal['no_value', 'some_value', 'string', 'wikibase-entityid', 'time', 'globecoordinate', 'monolingualtext', 'quantity']) -> list:
            key = (claim_type, rank, property_id, value_type)
            if key not in claim_batches_dict:
                claim_batches_dict[key] = []
            return claim_batches_dict[key]
        claim_writers_dict: dict[tuple[str, str, int, str], asyncio.Queue[Optional[list]]] = dict()
        def ensure_claim_writer(claim_type: Literal['claim','qualifier','reference'], rank: Literal['normal','preferred','deprecated'], property_id: int, value_type: Literal['no_value', 'some_value', 'string', 'wikibase-entityid', 'time', 'globecoordinate', 'monolingualtext', 'quantity']) -> asyncio.Queue[Optional[list]]:
            key = (claim_type, rank, property_id, value_type)
            if key not in claim_writers_dict:
                schema_index = 0 if claim_type == 'claim' else 1
                os.makedirs(f"{output}/tmp/{claim_type}_{value_type}/{rank}/{property_id}", exist_ok=True)
                claim_writers_dict[key] = create_writer_thread(f"{claim_type}_{value_type}/{rank}/{property_id}/data.parquet", schema=value_schemas[value_type][schema_index])
            return claim_writers_dict[key]
        def process_claim_value(claim_type: Literal['claim','qualifier','reference'], rank: Literal['normal','preferred','deprecated'], property_id: int, snak: dict, *base_fields) -> None:
            snaktype = snak['snaktype']
            if snaktype == 'novalue':
                ensure_claim_batch(claim_type, rank, property_id, 'no_value').append(base_fields)
            elif snaktype == 'somevalue':
                ensure_claim_batch(claim_type, rank, property_id, 'some_value').append(base_fields)
            else:
                value = snak['datavalue']['value']
                value_type = snak['datavalue']['type']
                if value_type == 'string':
                    ensure_claim_batch(claim_type, rank, property_id, 'string').append((*base_fields, value))
                elif value_type == 'wikibase-entityid':
                    value_id = get_entity_id(value['id'])
                    ensure_claim_batch(claim_type, rank, property_id, 'wikibase-entityid').append((*base_fields, value_id))
                elif value_type == 'time':
                    time_value = value['time']
                    timezone = value['timezone']
                    before = value['before']
                    after = value['after']
                    precision = value['precision']
                    calendarmodel = value['calendarmodel'].replace("http://www.wikidata.org/entity/", "")
                    calendarmodel_entity_id = get_entity_id(calendarmodel)
                    ensure_claim_batch(claim_type, rank, property_id, 'time').append((*base_fields, time_value, timezone, before, after, precision, calendarmodel_entity_id))
                elif value_type == 'globecoordinate':
                    latitude = float(value['latitude'])
                    longitude = float(value['longitude'])
                    precision = float(value['precision']) if value['precision'] is not None else None
                    globe = value['globe'].replace("http://www.wikidata.org/entity/", "")
                    globe_entity_id = get_entity_id(globe)
                    ensure_claim_batch(claim_type, rank, property_id, 'globecoordinate').append((*base_fields, latitude, longitude, precision, globe_entity_id))
                elif value_type == 'monolingualtext':
                    language = value['language']
                    text = value['text']
                    ensure_claim_batch(claim_type, rank, property_id, 'monolingualtext').append((*base_fields, language, text))
                elif value_type == 'quantity':
                    amount = float(value['amount'])
                    lower_bound = float(value['lowerBound']) if 'lowerBound' in value else None
                    upper_bound = float(value['upperBound']) if 'upperBound' in value else None
                    unit = value['unit']
                    if unit == "1":
                        unit_entity_id = None
                    else:
                        unit_entity_id = get_entity_id(unit.replace("http://www.wikidata.org/entity/", ""))
                    ensure_claim_batch(claim_type, rank, property_id, 'quantity').append((*base_fields, amount, lower_bound, upper_bound, unit_entity_id))
        claim_id = 1
        decoder = msgspec.json.Decoder()
        #encoder = msgspec.json.Encoder()
        next(uc_infile)
        for line in uc_infile:
            if line=="]\n":
                break
            elif line.endswith(",\n"):
                line = line[:-2]
            object = decoder.decode(line)
            entity_id = get_entity_id(object['id'])
            for label in object.get('labels', {}).values():
                batches['labels'].append((entity_id, label['language'], label['value']))
            for aliases in object.get('aliases', {}).values():
                for alias in aliases:
                    batches['aliases'].append((entity_id, alias['language'], alias['value']))
            for description in object.get('descriptions', {}).values():
                batches['descriptions'].append((entity_id, description['language'], description['value']))
            datatype = object.get('datatype', None)
            if datatype:
                batches['datatypes'].append((entity_id, datatype))
            for claims in object.get('claims', {}).values():
                for claim in claims:
                    #claim_id_str = claim['id']
                    #claim_type = claim['type'] # always 'statement'
                    property = claim['mainsnak']['property']
                    property_id = get_entity_id(property)
                    rank = claim['rank']
                    datatype = claim['mainsnak'].get('datatype', None)
                    process_claim_value('claim', rank, property_id, claim['mainsnak'], claim_id, rank, entity_id, property_id, datatype)
                    claim_id += 1
                    for qualifiers in claim.get('qualifiers', {}).values():
                        for qualifier in qualifiers:
                            property = qualifier['property']
                            datatype = qualifier.get('datatype', None)
                            order = claim['qualifiers-order'].index(property) + 1
                            process_claim_value('qualifier', 'normal', property_id, qualifier, order, claim_id, property_id, datatype)
                    for references in claim.get('references', []):
                        for reference_snaks in references.get('snaks', {}).values():
                            for reference_snak in reference_snaks:
                                property = reference_snak['property']
                                datatype = reference_snak.get('datatype', None)
                                order = references['snaks-order'].index(property) + 1
                                process_claim_value('reference', 'normal', property_id, reference_snak, order, claim_id, property_id, datatype)
            for sitelink in object.get('sitelinks', {}).values():
                site = sitelink['site']
                title = sitelink['title']
                batches['sitelinks'].append((entity_id, site, title))
                for badge in sitelink.get('badges', []):
                    badge_entity_id = get_entity_id(badge)
                    batches['sitelink_badges'].append((entity_id, site, badge_entity_id))
            for batch_type, batch in batches.items():
                if len(batch) >= batch_size:
                    await writers[batch_type].put(batch.copy())
                    batch.clear()
            for key, batch in claim_batches_dict.items():
                if len(batch) >= batch_size:
                    claim_type, rank, property_id, value_type = key
                    q = ensure_claim_writer(claim_type, rank, property_id, value_type)
                    await q.put(batch.copy())
                    batch.clear()
            pbar.n = infile.tell()
            pbar.update(0)
        pbar.close()
    for batch_type, batch in batches.items():
        if batch:
            await writers[batch_type].put(batch.copy())
            batch.clear()
    for key, batch in claim_batches_dict.items():
        if batch:
            claim_type, rank, property_id, value_type = key
            q = ensure_claim_writer(claim_type, rank, property_id, value_type)
            await q.put(batch.copy())
            batch.clear()
    duckdb.query("SET enable_progress_bar_print=TRUE")
    duckdb.query("SET progress_bar_time=0")
    duckdb.query("SET threads=1")
    os.makedirs(output+"/tmp2", exist_ok=True)
    con = duckdb.connect()
    def finalise_single_file_output(key: str):
        in_path = output+f"/tmp/{key}.parquet"
        mid_path = re.sub(r"/tmp(?!.*/tmp)", "/tmp2", in_path)
        out_path = re.sub(r"/tmp(?!.*/tmp)", "", in_path)
        shutil.rmtree(mid_path, ignore_errors=True)
        con.cursor().execute(f"COPY (SELECT * FROM parquet_scan('{in_path}')) TO '{mid_path}' (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 22, STRING_DICTIONARY_PAGE_SIZE_LIMIT 100_000, FILE_SIZE_BYTES 5_000_000_000)")
        os.remove(in_path)
        for file in tqdm(glob.glob(f"{mid_path}/data_*.parquet")):
            part = cast(re.Match, re.search(r'data(_\d+).parquet', file)).group(1)
            if part == "_0":
                part = ""
            shutil.move(file, out_path.replace('.parquet', part+'.parquet'))
    tasks = []
    for key, writer in writers.items():
        await writer.put(None)
        await writer.join()
        tasks.append(asyncio.to_thread(finalise_single_file_output, key))
    for writer in claim_writers_dict.values():
        await writer.put(None)
        await writer.join()
    def finalise_multifile_output(dataset: str):
        in_path = f"{output}/tmp/{dataset}"
        mid_path = re.sub(r"/tmp(?!.*/tmp)", "/tmp2", in_path)
        shutil.rmtree(mid_path, ignore_errors=True)
        con.cursor().execute(f"COPY (SELECT * FROM parquet_scan('{in_path}/*/*/*.parquet')) TO '{mid_path}' (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 22, STRING_DICTIONARY_PAGE_SIZE_LIMIT 100_000, FILE_SIZE_BYTES 5_000_000_000)")
        shutil.rmtree(in_path, ignore_errors=True)
        for file in tqdm(glob.glob(f"{mid_path}/data_*.parquet")):
            out_path = re.sub(r"/data(_0)?", "", file.replace("/tmp2",""))
            shutil.move(file, out_path)
    for dataset in os.listdir(output+"/tmp"):
        if 'claim_' in dataset or 'qualifier_' in dataset or 'reference_' in dataset:
            tasks.append(asyncio.to_thread(finalise_multifile_output, dataset))
    for task in tasks:
        await task
    shutil.rmtree(output+"/tmp", ignore_errors=True)
    shutil.rmtree(output+"/tmp2", ignore_errors=True)

if __name__ == "__main__":
    process_wikidata()