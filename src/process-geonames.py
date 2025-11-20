import csv
import os
import duckdb
import fsspec
from tqdm.auto import tqdm
import yaml
import pyarrow.parquet as pq
import pyarrow as pa

"""geonameid         : integer id of record in geonames database
name              : name of geographical point (utf8) varchar(200)
asciiname         : name of geographical point in plain ascii characters, varchar(200)
alternatenames    : alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
latitude          : latitude in decimal degrees (wgs84)
longitude         : longitude in decimal degrees (wgs84)
feature class     : see http://www.geonames.org/export/codes.html, char(1)
feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
country code      : ISO-3166 2-letter country code, 2 characters
cc2               : alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
admin1 code       : fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
admin2 code       : code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80) 
admin3 code       : code for third level administrative division, varchar(20)
admin4 code       : code for fourth level administrative division, varchar(20)
population        : bigint (8 byte int) 
elevation         : in meters, integer
dem               : digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
timezone          : the iana timezone id (see file timeZone.txt) varchar(40)
modification date : date of last modification in yyyy-MM-dd format"""
"""
Nullable:
  1. a: False
  2. b: True
  3. c: True
  4. d: True
  5. e: False
  6. f: False
  7. g: True
  8. h: True
  9. i: True
 10. j: True
 11. k: True
 12. l: True
 13. m: True
 14. n: True
 15. o: False
 16. p: True
 17. q: False
 18. r: True
 19. s: False"""
schema = pa.schema([
    pa.field('geonameid', pa.int64(), nullable=False),
    pa.field('name', pa.string(), nullable=True),
    pa.field('asciiname', pa.string(), nullable=True),
#    pa.field('alternatenames', pa.string(), nullable=True),
    pa.field('latitude', pa.float64(), nullable=False),
    pa.field('longitude', pa.float64(), nullable=False),
    pa.field('feature_class', pa.string(), nullable=True),
    pa.field('feature_code', pa.string(), nullable=True),
    pa.field('country_code', pa.string(), nullable=True),
    pa.field('cc2', pa.string(), nullable=True),
    pa.field('admin1_code', pa.string(), nullable=True),
    pa.field('admin2_code', pa.string(), nullable=True),
    pa.field('admin3_code', pa.string(), nullable=True),
    pa.field('admin4_code', pa.string(), nullable=True),
    pa.field('population', pa.int64(), nullable=True),
    pa.field('elevation', pa.int32(), nullable=True),
    pa.field('dem', pa.int32(), nullable=False),
    pa.field('timezone', pa.string(), nullable=True),
    pa.field('modification_date', pa.string(), nullable=False)
])

"""-----------------------------
alternateNameId   : the id of this alternate name, int
geonameid         : geonameId referring to id in table 'geoname', int
isolanguage       : iso 639 language code 2- or 3-characters, optionally followed by a hyphen and a countrycode for country specific variants (ex:zh-CN) or by a variant name (ex: zh-Hant); 4-characters 'post' for postal codes and 'iata','icao' and faac for airport codes, fr_1793 for French Revolution names,  abbr for abbreviation, link to a website (mostly to wikipedia), wkdt for the wikidataid, varchar(7)
alternate name    : alternate name or name variant, varchar(400)
isPreferredName   : '1', if this alternate name is an official/preferred name
isShortName       : '1', if this is a short name like 'California' for 'State of California'
isColloquial      : '1', if this alternate name is a colloquial or slang term. Example: 'Big Apple' for 'New York'.
isHistoric        : '1', if this alternate name is historic and was used in the past. Example 'Bombay' for 'Mumbai'.
from		  : from period when the name was used
to		  : to period when the name was used"""

alternate_names_schema = pa.schema([
    pa.field('alternate_name_id', pa.int64(), nullable=False),
    pa.field('geonameid', pa.int64(), nullable=False),
    pa.field('isolanguage', pa.string(), nullable=True),
    pa.field('alternate_name', pa.string(), nullable=True),
    pa.field('is_preferred_name', pa.bool_(), nullable=False),
    pa.field('is_short_name', pa.bool_(), nullable=False),
    pa.field('is_colloquial', pa.bool_(), nullable=False),
    pa.field('is_historic', pa.bool_(), nullable=False),
    pa.field('from', pa.int32(), nullable=True),
    pa.field('to', pa.int32(), nullable=True),
])

def process_geonames():
    with fsspec.open('zip://allCountries.txt::data/work/geonames/allCountries.zip', 'rt') as file, pq.ParquetWriter("data/geonames/geonames.tmp.parquet", schema=schema, compression="zstd") as pw:
        batch = []
        r = csv.reader(file, delimiter='\t')
        for row in tqdm(r):
            batch.append((
                int(row[0]),  # geonameid
                row[1] if row[1] else None,  # name
                row[2] if row[2] else None,  # asciiname
#                row[3] if row[3] else None,  # alternatenames
                float(row[4]),  # latitude
                float(row[5]),  # longitude
                row[6] if row[6] else None,  # feature_class
                row[7] if row[7] else None,  # feature_code
                row[8] if row[8] else None,  # country_code
                row[9] if row[9] else None,  # cc2
                row[10] if row[10] else None,  # admin1_code
                row[11] if row[11] else None,  # admin2_code
                row[12] if row[12] else None,  # admin3_code
                row[13] if row[13] else None,  # admin4_code
                int(row[14]) if row[14].isdigit() and row[14]!=0 else None,  # population
                int(row[15]) if row[15].isdigit() else None,  # elevation
                int(row[16]),  # dem
                row[17],  # timezone
                row[18],  # modification_date
            ))
        pw.write_batch(pa.record_batch(list(zip(*batch)), schema=schema), row_group_size=1024*1024)
    duckdb.query(f"COPY 'data/geonames/geonames.tmp.parquet' TO 'data/geonames/geonames.parquet' (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 22)")
    os.remove('data/geonames/geonames.tmp.parquet')
    with fsspec.open('zip://alternateNamesV2.txt::data/work/geonames/alternateNamesV2.zip', 'rt') as file, pq.ParquetWriter("data/geonames/alternate_names.tmp.parquet", schema=alternate_names_schema, compression="zstd") as pw:
        batch = []
        r = csv.reader(file, delimiter='\t')
        for row in tqdm(r):
            batch.append((
                int(row[0]),  # alternate_name_id
                int(row[1]),  # geonameid
                row[2] if row[2] else None,  # isolanguage
                row[3],  # alternate_name
                row[4] == '1',  # is_preferred_name
                row[5] == '1',  # is_short_name
                row[6] == '1',  # is_colloquial
                row[7] == '1',  # is_historic
                int(row[8]) if row[8].isdigit() else None,  # from
                int(row[9]) if row[9].isdigit() else None,  # to
            ))
        pw.write_batch(pa.record_batch(list(zip(*batch)), schema=alternate_names_schema), row_group_size=1024*1024)
    duckdb.query(f"COPY 'data/geonames/alternate_names.tmp.parquet' TO 'data/geonames/alternate_names.parquet' (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 22)")
    os.remove('data/geonames/alternate_names.tmp.parquet')

if __name__ == "__main__":
    process_geonames()
