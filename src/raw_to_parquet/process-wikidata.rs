use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::{Context, Result};
use clap::Parser;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde_json::Value;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::{self, JoinHandle};

use dashmap::DashMap;

const URI_PREFIX: &str = "http://www.wikidata.org/entity/";
const READY_BATCHES_BUFFER: usize = 2;
const LINE_CHANNEL_CAPACITY: usize = 5_000;
const DEFAULT_BATCH_SIZE: usize = 122_880;
const DEFAULT_MAX_FILE_SIZE: u64 = 4_000_000_000;

// Small helpers for clarity
#[inline]
fn json_snippet(v: &Value) -> String {
    // Render a compact snippet of the JSON value for error messages
    let s = v.to_string();
    const MAX: usize = 600;
    let clipped: String = s.chars().take(MAX).collect();
    if clipped.len() < s.len() {
        format!("{}…", clipped)
    } else {
        clipped
    }
}

#[inline]
fn strip_uri(s: &str) -> &str {
    s.strip_prefix(URI_PREFIX).unwrap_or(s)
}

#[inline]
fn intern_uri<'a>(interner: &Interner, simple: &mut SimpleBatchers, s: &'a str) -> i64 {
    interner.get_or_insert(simple, strip_uri(s))
}

#[inline]
fn order_of<S: AsRef<str>>(list: &[S], prop: &str) -> i32 {
    list.iter()
        .position(|x| x.as_ref() == prop)
        .map(|i| (i as i32) + 1)
        .unwrap()
}

// Tiny helpers to keep code terse and consistent
#[inline]
fn push_opt_str(builder: &mut StringBuilder, val: Option<&str>) {
    if let Some(s) = val {
        builder.append_value(s)
    } else {
        builder.append_null()
    }
}

#[inline]
fn push_opt_f64(builder: &mut Float64Builder, val: Option<f64>) {
    if let Some(v) = val {
        builder.append_value(v)
    } else {
        builder.append_null()
    }
}

#[inline]
fn push_opt_i64(builder: &mut Int64Builder, val: Option<i64>) {
    if let Some(v) = val {
        builder.append_value(v)
    } else {
        builder.append_null()
    }
}

// Compact JSON access helpers with consistent error context
#[inline]
fn req_str_opt<'a>(v: Option<&'a str>, msg: &str, ctx: &Value) -> Result<&'a str> {
    v.with_context(|| format!("{}; snak={}", msg, json_snippet(ctx)))
}

#[inline]
fn req_i64_opt(v: Option<i64>, msg: &str, ctx: &Value) -> Result<i64> {
    v.with_context(|| format!("{}; snak={}", msg, json_snippet(ctx)))
}

#[inline]
fn req_f64_opt(v: Option<f64>, msg: &str, ctx: &Value) -> Result<f64> {
    v.with_context(|| format!("{}; snak={}", msg, json_snippet(ctx)))
}

#[inline]
fn make_batch(schema: SchemaRef, arrays: Vec<Arc<dyn arrow::array::Array>>) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(schema, arrays)?)
}

// Macros for concise repeated builder appends
macro_rules! push_claim_header {
    ($claim_id:expr, $rank:expr, $entity_id:expr, $property_id:expr, $datatype:expr, $b:expr) => {{
        $claim_id.append_value($b.claim_id);
        $rank.append_value($b.rank.as_str());
        $entity_id.append_value($b.entity_id);
        $property_id.append_value($b.property_id);
        push_opt_str($datatype, $b.datatype.as_deref());
    }};
}

macro_rules! push_qual_header {
    ($order:expr, $claim_id:expr, $property_id:expr, $datatype:expr, $b:expr) => {{
        $order.append_value($b.order);
        $claim_id.append_value($b.claim_id);
        $property_id.append_value($b.property_id);
        push_opt_str($datatype, $b.datatype.as_deref());
    }};
}
use push_claim_header;
use push_qual_header;

// Small macro to finish builders, reset counts, and send a batch for SimpleBatchers
macro_rules! flush_simple_async {
    ($self:ident, $cond:expr, $count:ident, $schema_fn:ident, [$($col:ident),+], $tx:ident) => {
        if $cond {
            let batch = make_batch(
                $schema_fn(),
                vec![ $( Arc::new($self.$col.finish()) ),+ ],
            ).unwrap();
            $self.$count = 0;
            $self.$tx.send(batch).await.ok();
        }
    };
}

// Macro to finish a claim batch: reset count, finish header arrays, then finish value arrays
macro_rules! finish_claim_batch {
    // No value columns
    ($schema:expr, $is_claim:expr, $header:ident, $count:ident) => {{
        *$count = 0;
        let cols = $header.finish_to_arrays($is_claim);
        make_batch($schema, cols)
    }};
    // One or more value columns
    ($schema:expr, $is_claim:expr, $header:ident, $count:ident, $( $val:ident ),+ ) => {{
        *$count = 0;
        let mut cols = $header.finish_to_arrays($is_claim);
        $( cols.push(Arc::new($val.finish())); )+
        make_batch($schema, cols)
    }};
}

// Macro to append header and increment count, with custom body for value columns
macro_rules! append_with_header {
    ($h:ident, $header:ident, $count:ident, $body:block) => {{
        $h.append_to($header);
        {
            $body
        }
        *$count += 1;
    }};
}

#[derive(Parser, Debug)]
#[command(
    name = "process-wikidata",
    version,
    about = "Rewrite of process-wikidata.py in Rust"
)]
struct Args {
    /// Output directory (will create tmp/ and tmp2/)
    #[arg(short, long)]
    output: PathBuf,
    /// Number of entities to buffer in memory before flushing to parquet
    #[arg(short = 'b', long, default_value_t = DEFAULT_BATCH_SIZE)]
    batch_size: usize,
    /// Number of concurrent parse workers (default: number of CPUs)
    #[arg(short = 't', long)]
    parse_threads: Option<usize>,
    /// Maximum final parquet file size in bytes
    #[arg(short = 's', long, default_value_t = DEFAULT_MAX_FILE_SIZE)]
    max_file_size: u64,
}

// Arrow schemas
fn schema_entities() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Int64, false),
        Field::new("id", DataType::Utf8, false),
    ]))
}
fn schema_labels() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Int64, false),
        Field::new("language", DataType::Utf8, false),
        Field::new("label", DataType::Utf8, false),
    ]))
}
fn schema_aliases() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Int64, false),
        Field::new("language", DataType::Utf8, false),
        Field::new("alias", DataType::Utf8, false),
    ]))
}
fn schema_descriptions() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Int64, false),
        Field::new("language", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
    ]))
}
fn schema_datatypes() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Int64, false),
        Field::new("datatype", DataType::Utf8, false),
    ]))
}
fn schema_sitelinks() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Int64, false),
        Field::new("site", DataType::Utf8, false),
        Field::new("title", DataType::Utf8, false),
    ]))
}
fn schema_sitelink_badges() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Int64, false),
        Field::new("site", DataType::Utf8, false),
        Field::new("badge_entity_id", DataType::Int64, false),
    ]))
}

fn mk_writer(path: &Path, schema: SchemaRef) -> Result<ArrowWriter<File>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let f = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    Ok(ArrowWriter::try_new(f, schema, Some(props))?)
}

struct Interner {
    map: DashMap<String, i64>,
    next: AtomicI64,
}

impl Interner {
    fn new() -> Self {
        Self {
            map: DashMap::new(),
            next: AtomicI64::new(1),
        }
    }

    #[inline]
    fn get_or_insert(&self, simple: &mut SimpleBatchers, id: &str) -> i64 {
        if let Some(v) = self.map.get(id) {
            return *v;
        }
        // Fast path missed; insert new
        let next = self.next.fetch_add(1, Ordering::Relaxed);
        let id_owned = id.to_owned();
        if let Some(existing) = self.map.insert(id_owned.clone(), next) {
            // Another thread inserted concurrently; use existing and roll back counter usage is benign
            existing
        } else {
            // Emit into entities via this thread's batcher
            simple.entities_append(next, &id_owned);
            next
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum ClaimType {
    Claim,
    Qualifier,
    Reference,
}
impl ClaimType {
    fn as_str(&self) -> &'static str {
        match self {
            ClaimType::Claim => "claim",
            ClaimType::Qualifier => "qualifier",
            ClaimType::Reference => "reference",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Rank {
    Normal,
    Preferred,
    Deprecated,
}
impl Rank {
    fn as_str(&self) -> &'static str {
        match self {
            Rank::Normal => "normal",
            Rank::Preferred => "preferred",
            Rank::Deprecated => "deprecated",
        }
    }
}
impl From<&str> for Rank {
    fn from(s: &str) -> Self {
        match s {
            "preferred" => Rank::Preferred,
            "deprecated" => Rank::Deprecated,
            _ => Rank::Normal,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum ValueKind {
    NoValue,
    SomeValue,
    String,
    EntityId,
    Time,
    GlobeCoordinate,
    MonolingualText,
    Quantity,
}
impl ValueKind {
    fn as_str(&self) -> &'static str {
        match self {
            ValueKind::NoValue => "no_value",
            ValueKind::SomeValue => "some_value",
            ValueKind::String => "string",
            ValueKind::EntityId => "wikibase-entityid",
            ValueKind::Time => "time",
            ValueKind::GlobeCoordinate => "globecoordinate",
            ValueKind::MonolingualText => "monolingualtext",
            ValueKind::Quantity => "quantity",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ClaimKey {
    ctype: ClaimType,
    rank: Rank,
    property_id: i64,
    vkind: ValueKind,
}

impl ClaimKey {
    #[inline]
    fn new(ctype: ClaimType, rank: Rank, property_id: i64, vkind: ValueKind) -> Self {
        Self {
            ctype,
            rank,
            property_id,
            vkind,
        }
    }
}

#[derive(Clone, Debug)]
struct BaseClaim {
    claim_id: i64,
    rank: Rank,
    entity_id: i64,
    property_id: i64,
    datatype: Option<String>,
}
#[derive(Clone, Debug)]
struct BaseQual {
    order: i32,
    claim_id: i64,
    property_id: i64,
    datatype: Option<String>,
}

#[derive(Clone, Debug)]
enum ValuePayload {
    NoSome,
    String(String),
    EntityId(i64),
    Time {
        time: String,
        tz: i32,
        before: i32,
        after: i32,
        precision: i32,
        cal_id: i64,
    },
    Globe {
        lat: f64,
        lon: f64,
        precision: Option<f64>,
        globe_id: i64,
    },
    Mono {
        lang: String,
        text: String,
    },
    Quantity {
        amount: f64,
        lower: Option<f64>,
        upper: Option<f64>,
        unit: Option<i64>,
    },
}

#[allow(clippy::large_enum_variant)]
struct Row {
    header: HeaderIn,
    payload: ValuePayload,
}

fn schema_for_value(vk: ValueKind, is_claim: bool) -> SchemaRef {
    let mut fields = Vec::new();
    if is_claim {
        fields.push(Field::new("claim_id", DataType::Int64, false));
        fields.push(Field::new("rank", DataType::Utf8, false));
        fields.push(Field::new("entity_id", DataType::Int64, false));
        fields.push(Field::new("property_id", DataType::Int64, false));
        fields.push(Field::new("datatype", DataType::Utf8, true));
    } else {
        fields.push(Field::new("order", DataType::Int32, false));
        fields.push(Field::new("claim_id", DataType::Int64, false));
        fields.push(Field::new("property_id", DataType::Int64, false));
        fields.push(Field::new("datatype", DataType::Utf8, true));
    }
    match vk {
        ValueKind::NoValue | ValueKind::SomeValue => {}
        ValueKind::String => fields.push(Field::new("value", DataType::Utf8, false)),
        ValueKind::EntityId => fields.push(Field::new("value_entity_id", DataType::Int64, false)),
        ValueKind::Time => {
            fields.push(Field::new("time", DataType::Utf8, false));
            fields.push(Field::new("timezone", DataType::Int32, false));
            fields.push(Field::new("before", DataType::Int32, false));
            fields.push(Field::new("after", DataType::Int32, false));
            fields.push(Field::new("precision", DataType::Int32, false));
            fields.push(Field::new(
                "calendarmodel_entity_id",
                DataType::Int64,
                false,
            ));
        }
        ValueKind::GlobeCoordinate => {
            fields.push(Field::new("latitude", DataType::Float64, false));
            fields.push(Field::new("longitude", DataType::Float64, false));
            fields.push(Field::new("precision", DataType::Float64, true));
            fields.push(Field::new("globe_entity_id", DataType::Int64, false));
        }
        ValueKind::MonolingualText => {
            fields.push(Field::new("language", DataType::Utf8, false));
            fields.push(Field::new("text", DataType::Utf8, false));
        }
        ValueKind::Quantity => {
            fields.push(Field::new("amount", DataType::Float64, false));
            fields.push(Field::new("lower_bound", DataType::Float64, true));
            fields.push(Field::new("upper_bound", DataType::Float64, true));
            fields.push(Field::new("unit_entity_id", DataType::Int64, true));
        }
    }
    Arc::new(Schema::new(fields))
}

fn mk_claim_writer(base_dir: &Path, key: &ClaimKey) -> Result<ArrowWriter<File>> {
    let claim_part = format!("{}_{}", key.ctype.as_str(), key.vkind.as_str());
    let dir = base_dir
        .join(&claim_part)
        .join(key.rank.as_str())
        .join(format!("{}", key.property_id));
    fs::create_dir_all(&dir)?;
    let path = dir.join("data.parquet");
    let is_claim = matches!(key.ctype, ClaimType::Claim);
    let schema = schema_for_value(key.vkind, is_claim);
    mk_writer(&path, schema)
}

// Header input that is independent from value type
#[derive(Clone, Debug)]
enum HeaderIn {
    Claim(BaseClaim),
    Sub(BaseQual),
}

impl HeaderIn {
    fn append_to(&self, header: &mut ClaimHeadBuilder) {
        match self {
            HeaderIn::Claim(b) => header.append_claim(b),
            HeaderIn::Sub(b) => header.append_sub(b),
        }
    }

    fn to_row(&self, payload: ValuePayload) -> Row {
        Row {
            header: self.clone(),
            payload,
        }
    }
}

async fn process_snak_value(
    interner: &Interner,
    simple: &mut SimpleBatchers,
    builders: &mut HashMap<ClaimKey, ClaimBatchHolder>,
    batch_size: usize,
    ctype: ClaimType,
    rank: Rank,
    header: HeaderIn,
    property_id: i64,
    snak: &Value,
) -> Result<()> {
    // Small helper to append a row for a given value kind
    let mut push_row = |vkind: ValueKind, payload: ValuePayload| {
        let key = ClaimKey::new(ctype, rank, property_id, vkind);
        let entry = builders
            .entry(key.clone())
            .or_insert_with(|| ClaimBatchHolder::new(&key, batch_size));
        entry.append(header.to_row(payload));
    };
    let snaktype = req_str_opt(
        snak.get("snaktype").and_then(|v| v.as_str()),
        "snak missing 'snaktype'",
        snak,
    )?;
    match snaktype {
        "novalue" | "somevalue" => {
            let vkind = if snaktype == "novalue" {
                ValueKind::NoValue
            } else {
                ValueKind::SomeValue
            };
            push_row(vkind, ValuePayload::NoSome);
        }
        _ => {
            let dv = snak.get("datavalue").with_context(|| {
                format!(
                    "snak missing 'datavalue' for value type; snak={}",
                    json_snippet(snak)
                )
            })?;
            let vtyp = req_str_opt(
                dv.get("type").and_then(|v| v.as_str()),
                "datavalue missing 'type'",
                snak,
            )?;
            let v = dv.get("value").with_context(|| {
                format!("datavalue missing 'value'; snak={}", json_snippet(snak))
            })?;
            match vtyp {
                "string" => {
                    let s =
                        req_str_opt(v.as_str(), "string datavalue missing string 'value'", snak)?
                            .to_string();
                    push_row(ValueKind::String, ValuePayload::String(s));
                }
                "wikibase-entityid" => {
                    let id = req_str_opt(
                        v.get("id").and_then(|vv| vv.as_str()),
                        "entityid datavalue missing 'id'",
                        snak,
                    )?;
                    let eid = interner.get_or_insert(simple, id);
                    push_row(ValueKind::EntityId, ValuePayload::EntityId(eid));
                }
                "time" => {
                    let time = req_str_opt(
                        v.get("time").and_then(|vv| vv.as_str()),
                        "time datavalue missing 'time'",
                        snak,
                    )?
                    .to_string();
                    let tz = req_i64_opt(
                        v.get("timezone").and_then(|vv| vv.as_i64()),
                        "time datavalue missing 'timezone'",
                        snak,
                    )? as i32;
                    let before = req_i64_opt(
                        v.get("before").and_then(|vv| vv.as_i64()),
                        "time datavalue missing 'before'",
                        snak,
                    )? as i32;
                    let after = req_i64_opt(
                        v.get("after").and_then(|vv| vv.as_i64()),
                        "time datavalue missing 'after'",
                        snak,
                    )? as i32;
                    let precision = req_i64_opt(
                        v.get("precision").and_then(|vv| vv.as_i64()),
                        "time datavalue missing 'precision'",
                        snak,
                    )? as i32;
                    let cal = req_str_opt(
                        v.get("calendarmodel")
                            .and_then(|vv| vv.as_str())
                            .map(strip_uri),
                        "time datavalue missing 'calendarmodel'",
                        snak,
                    )?;
                    let cal_id = intern_uri(interner, simple, cal);
                    push_row(
                        ValueKind::Time,
                        ValuePayload::Time {
                            time,
                            tz,
                            before,
                            after,
                            precision,
                            cal_id,
                        },
                    );
                }
                "globecoordinate" => {
                    let lat = req_f64_opt(
                        v.get("latitude").and_then(|vv| vv.as_f64()),
                        "globecoordinate missing 'latitude'",
                        snak,
                    )?;
                    let lon = req_f64_opt(
                        v.get("longitude").and_then(|vv| vv.as_f64()),
                        "globecoordinate missing 'longitude'",
                        snak,
                    )?;
                    let prec = v.get("precision").and_then(|vv| vv.as_f64());
                    let globe = req_str_opt(
                        v.get("globe").and_then(|vv| vv.as_str()).map(strip_uri),
                        "globecoordinate missing 'globe'",
                        snak,
                    )?;
                    let globe_id = intern_uri(interner, simple, globe);
                    push_row(
                        ValueKind::GlobeCoordinate,
                        ValuePayload::Globe {
                            lat,
                            lon,
                            precision: prec,
                            globe_id,
                        },
                    );
                }
                "monolingualtext" => {
                    let lang = req_str_opt(
                        v.get("language").and_then(|vv| vv.as_str()),
                        "monolingualtext missing 'language'",
                        snak,
                    )?
                    .to_string();
                    let text = req_str_opt(
                        v.get("text").and_then(|vv| vv.as_str()),
                        "monolingualtext missing 'text'",
                        snak,
                    )?
                    .to_string();
                    push_row(
                        ValueKind::MonolingualText,
                        ValuePayload::Mono { lang, text },
                    );
                }
                "quantity" => {
                    let amount = req_str_opt(
                        v.get("amount").and_then(|vv| vv.as_str()),
                        "quantity missing 'amount'",
                        snak,
                    )?
                    .parse::<f64>()
                    .with_context(|| {
                        format!(
                            "quantity 'amount' not a number'; snak={}",
                            json_snippet(snak)
                        )
                    })?;
                    let lower = v
                        .get("lowerBound")
                        .and_then(|vv| vv.as_str())
                        .and_then(|s| s.parse::<f64>().ok());
                    let upper = v
                        .get("upperBound")
                        .and_then(|vv| vv.as_str())
                        .and_then(|s| s.parse::<f64>().ok());
                    let unit = req_str_opt(
                        v.get("unit").and_then(|vv| vv.as_str()),
                        "quantity missing 'unit'",
                        snak,
                    )?;
                    let unit_id = if unit == "1" {
                        None
                    } else {
                        Some(intern_uri(interner, simple, unit))
                    };
                    push_row(
                        ValueKind::Quantity,
                        ValuePayload::Quantity {
                            amount,
                            lower,
                            upper,
                            unit: unit_id,
                        },
                    );
                }
                _ => {}
            }
        }
    }
    Ok(())
}

struct ClaimIdGen(AtomicI64);
impl ClaimIdGen {
    fn new() -> Self {
        Self(AtomicI64::new(1))
    }
    fn next(&self) -> i64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

async fn process_entity(
    json_line: &str,
    interner: &Interner,
    simple: &mut SimpleBatchers,
    builders: &mut HashMap<ClaimKey, ClaimBatchHolder>,
    claim_writers: &ClaimWriters,
    batch_size: usize,
    claim_ids: &ClaimIdGen,
) -> Result<()> {
    let obj: Value = serde_json::from_str(json_line).with_context(|| {
        format!(
            "failed to parse entity json: {}",
            json_snippet(&Value::String(json_line.chars().take(200).collect()))
        )
    })?;
    let id = obj
        .get("id")
        .and_then(|v| v.as_str())
        .with_context(|| format!("entity missing 'id'; entity={}", json_line))?;
    let entity_id = interner.get_or_insert(simple, id);

    if let Some(labels) = obj.get("labels").and_then(|v| v.as_object()) {
        for (_k, v) in labels {
            if let (Some(lang), Some(val)) = (
                v.get("language").and_then(|x| x.as_str()),
                v.get("value").and_then(|x| x.as_str()),
            ) {
                simple.labels_append(entity_id, lang, val);
            }
        }
    }
    if let Some(aliases) = obj.get("aliases").and_then(|v| v.as_object()) {
        for (_k, list) in aliases {
            if let Some(arr) = list.as_array() {
                for v in arr {
                    if let (Some(lang), Some(val)) = (
                        v.get("language").and_then(|x| x.as_str()),
                        v.get("value").and_then(|x| x.as_str()),
                    ) {
                        simple.aliases_append(entity_id, lang, val);
                    }
                }
            }
        }
    }
    if let Some(descriptions) = obj.get("descriptions").and_then(|v| v.as_object()) {
        for (_k, v) in descriptions {
            if let (Some(lang), Some(val)) = (
                v.get("language").and_then(|x| x.as_str()),
                v.get("value").and_then(|x| x.as_str()),
            ) {
                simple.descriptions_append(entity_id, lang, val);
            }
        }
    }
    if let Some(datatype) = obj.get("datatype").and_then(|v| v.as_str()) {
        simple.datatypes_append(entity_id, datatype);
    }

    if let Some(claims) = obj.get("claims").and_then(|v| v.as_object()) {
        for (_p, arr) in claims {
            if let Some(claim_list) = arr.as_array() {
                for claim in claim_list {
                    let rank_str =
                        claim
                            .get("rank")
                            .and_then(|v| v.as_str())
                            .with_context(|| {
                                format!("claim missing 'rank'; claim={}", json_snippet(claim))
                            })?;
                    let rank = Rank::from(rank_str);
                    let mainsnak = claim.get("mainsnak").with_context(|| {
                        format!("claim missing 'mainsnak'; claim={}", json_snippet(claim))
                    })?;
                    let property = mainsnak
                        .get("property")
                        .and_then(|v| v.as_str())
                        .with_context(|| {
                            format!(
                                "mainsnak missing 'property'; mainsnak={}",
                                json_snippet(mainsnak)
                            )
                        })?;
                    let property_id = interner.get_or_insert(simple, property);
                    let datatype = mainsnak
                        .get("datatype")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());

                    let claim_id_for_claim = claim_ids.next();
                    let base = BaseClaim {
                        claim_id: claim_id_for_claim,
                        rank,
                        entity_id,
                        property_id,
                        datatype,
                    };
                    process_snak_value(
                        interner,
                        simple,
                        builders,
                        batch_size,
                        ClaimType::Claim,
                        rank,
                        HeaderIn::Claim(base),
                        property_id,
                        mainsnak,
                    )
                    .await?;
                    // A second id shared by all qualifiers/references of this claim
                    let claim_id_for_subs = claim_ids.next();

                    if let Some(quals) = claim.get("qualifiers").and_then(|v| v.as_object()) {
                        let order_list: Vec<&str> = claim
                            .get("qualifiers-order")
                            .and_then(|v| v.as_array())
                            .map(|a| a.iter().filter_map(|x| x.as_str()).collect())
                            .unwrap();
                        for (prop, list) in quals {
                            if let Some(arr) = list.as_array() {
                                for qualifier in arr {
                                    let order = order_of(&order_list, prop);
                                    let qdatatype = qualifier
                                        .get("datatype")
                                        .and_then(|v| v.as_str())
                                        .map(|s| s.to_string());
                                    let qprop_id = property_id;
                                    let base = BaseQual {
                                        order,
                                        claim_id: claim_id_for_subs,
                                        property_id: qprop_id,
                                        datatype: qdatatype,
                                    };
                                    process_snak_value(
                                        interner,
                                        simple,
                                        builders,
                                        batch_size,
                                        ClaimType::Qualifier,
                                        Rank::Normal,
                                        HeaderIn::Sub(base),
                                        property_id,
                                        qualifier,
                                    )
                                    .await?;
                                }
                            }
                        }
                    }

                    if let Some(refs) = claim.get("references").and_then(|v| v.as_array()) {
                        for r in refs {
                            let snaks_order: Vec<&str> = r
                                .get("snaks-order")
                                .and_then(|v| v.as_array())
                                .map(|a| a.iter().filter_map(|x| x.as_str()).collect())
                                .unwrap();
                            if let Some(snaks) = r.get("snaks").and_then(|v| v.as_object()) {
                                for (prop, arr) in snaks {
                                    if let Some(ar) = arr.as_array() {
                                        for snak in ar {
                                            let order = order_of(&snaks_order, prop);
                                            let qdatatype = snak
                                                .get("datatype")
                                                .and_then(|v| v.as_str())
                                                .map(|s| s.to_string());
                                            let base = BaseQual {
                                                order,
                                                claim_id: claim_id_for_subs,
                                                property_id,
                                                datatype: qdatatype,
                                            };
                                            process_snak_value(
                                                interner,
                                                simple,
                                                builders,
                                                batch_size,
                                                ClaimType::Reference,
                                                Rank::Normal,
                                                HeaderIn::Sub(base),
                                                property_id,
                                                snak,
                                            )
                                            .await?;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if let Some(sitelinks) = obj.get("sitelinks").and_then(|v| v.as_object()) {
        for (_k, v) in sitelinks {
            if let Some(site) = v.get("site").and_then(|v| v.as_str()) {
                if let Some(title) = v.get("title").and_then(|v| v.as_str()) {
                    simple.sitelinks_append(entity_id, site, title);
                }
            }
            if let Some(badges) = v.get("badges").and_then(|v| v.as_array()) {
                for b in badges {
                    if let Some(badge) = b.as_str() {
                        let bid = interner.get_or_insert(simple, badge);
                        if let Some(site) = v.get("site").and_then(|v| v.as_str()) {
                            simple.sitelink_badges_append(entity_id, site, bid);
                        }
                    }
                }
            }
        }
    }

    for (_key, entry) in builders.iter_mut() {
        entry.flush_if_needed_send(claim_writers).await?;
    }
    simple.flush_if_needed_async().await?;

    Ok(())
}

// Unified header builders reused across claim/qualifier/reference
enum ClaimHeadBuilder {
    Claim {
        claim_id: Int64Builder,
        rank: StringBuilder,
        entity_id: Int64Builder,
        property_id: Int64Builder,
        datatype: StringBuilder,
    },
    Sub {
        order: Int32Builder,
        claim_id: Int64Builder,
        property_id: Int64Builder,
        datatype: StringBuilder,
    },
}

impl ClaimHeadBuilder {
    fn new(ctype: ClaimType) -> Self {
        match ctype {
            ClaimType::Claim => ClaimHeadBuilder::Claim {
                claim_id: Int64Builder::new(),
                rank: StringBuilder::new(),
                entity_id: Int64Builder::new(),
                property_id: Int64Builder::new(),
                datatype: StringBuilder::new(),
            },
            _ => ClaimHeadBuilder::Sub {
                order: Int32Builder::new(),
                claim_id: Int64Builder::new(),
                property_id: Int64Builder::new(),
                datatype: StringBuilder::new(),
            },
        }
    }

    fn append_claim(&mut self, b: &BaseClaim) {
        match self {
            ClaimHeadBuilder::Claim {
                claim_id,
                rank,
                entity_id,
                property_id,
                datatype,
            } => {
                push_claim_header!(claim_id, rank, entity_id, property_id, datatype, b);
            }
            ClaimHeadBuilder::Sub { .. } => unreachable!("header kind mismatch: expected Claim"),
        }
    }

    fn append_sub(&mut self, b: &BaseQual) {
        match self {
            ClaimHeadBuilder::Sub {
                order,
                claim_id,
                property_id,
                datatype,
            } => {
                push_qual_header!(order, claim_id, property_id, datatype, b);
            }
            ClaimHeadBuilder::Claim { .. } => unreachable!("header kind mismatch: expected Sub"),
        }
    }

    fn finish_to_arrays(&mut self, is_claim: bool) -> Vec<Arc<dyn arrow::array::Array>> {
        // Note: The order of arrays returned here MUST match schema_for_value's
        // header column ordering for the respective path (claim vs sub). Value
        // columns are appended by ClaimBatchBuilders::finish_to_batch afterwards.
        match (is_claim, self) {
            (
                true,
                ClaimHeadBuilder::Claim {
                    claim_id,
                    rank,
                    entity_id,
                    property_id,
                    datatype,
                },
            ) => {
                vec![
                    Arc::new(claim_id.finish()),
                    Arc::new(rank.finish()),
                    Arc::new(entity_id.finish()),
                    Arc::new(property_id.finish()),
                    Arc::new(datatype.finish()),
                ]
            }
            (
                false,
                ClaimHeadBuilder::Sub {
                    order,
                    claim_id,
                    property_id,
                    datatype,
                },
            ) => {
                vec![
                    Arc::new(order.finish()),
                    Arc::new(claim_id.finish()),
                    Arc::new(property_id.finish()),
                    Arc::new(datatype.finish()),
                ]
            }
            (true, ClaimHeadBuilder::Sub { .. }) => {
                unreachable!("header type mismatch: sub used for claim schema")
            }
            (false, ClaimHeadBuilder::Claim { .. }) => {
                unreachable!("header type mismatch: claim used for sub schema")
            }
        }
    }
}

// Single set of value builders, with shared header builders
enum ClaimBatchBuilders {
    NoSome {
        header: ClaimHeadBuilder,
        count: usize,
    },
    String {
        header: ClaimHeadBuilder,
        value: StringBuilder,
        count: usize,
    },
    EntityId {
        header: ClaimHeadBuilder,
        value_entity_id: Int64Builder,
        count: usize,
    },
    Time {
        header: ClaimHeadBuilder,
        time: StringBuilder,
        timezone: Int32Builder,
        before: Int32Builder,
        after: Int32Builder,
        precision: Int32Builder,
        calendarmodel_entity_id: Int64Builder,
        count: usize,
    },
    Globe {
        header: ClaimHeadBuilder,
        latitude: Float64Builder,
        longitude: Float64Builder,
        precision: Float64Builder,
        globe_entity_id: Int64Builder,
        count: usize,
    },
    Mono {
        header: ClaimHeadBuilder,
        language: StringBuilder,
        text: StringBuilder,
        count: usize,
    },
    Quantity {
        header: ClaimHeadBuilder,
        amount: Float64Builder,
        lower_bound: Float64Builder,
        upper_bound: Float64Builder,
        unit_entity_id: Int64Builder,
        count: usize,
    },
}

impl ClaimBatchBuilders {
    fn new(key: &ClaimKey) -> Self {
        let header = ClaimHeadBuilder::new(key.ctype);
        match key.vkind {
            ValueKind::NoValue | ValueKind::SomeValue => Self::NoSome { header, count: 0 },
            ValueKind::String => Self::String {
                header,
                value: StringBuilder::new(),
                count: 0,
            },
            ValueKind::EntityId => Self::EntityId {
                header,
                value_entity_id: Int64Builder::new(),
                count: 0,
            },
            ValueKind::Time => Self::Time {
                header,
                time: StringBuilder::new(),
                timezone: Int32Builder::new(),
                before: Int32Builder::new(),
                after: Int32Builder::new(),
                precision: Int32Builder::new(),
                calendarmodel_entity_id: Int64Builder::new(),
                count: 0,
            },
            ValueKind::GlobeCoordinate => Self::Globe {
                header,
                latitude: Float64Builder::new(),
                longitude: Float64Builder::new(),
                precision: Float64Builder::new(),
                globe_entity_id: Int64Builder::new(),
                count: 0,
            },
            ValueKind::MonolingualText => Self::Mono {
                header,
                language: StringBuilder::new(),
                text: StringBuilder::new(),
                count: 0,
            },
            ValueKind::Quantity => Self::Quantity {
                header,
                amount: Float64Builder::new(),
                lower_bound: Float64Builder::new(),
                upper_bound: Float64Builder::new(),
                unit_entity_id: Int64Builder::new(),
                count: 0,
            },
        }
    }

    fn append(&mut self, row: Row) {
        let Row { header: h, payload } = row;
        match self {
            Self::NoSome { header, count } => match payload {
                ValuePayload::NoSome => {
                    append_with_header!(h, header, count, {});
                }
                _ => unreachable!("payload type mismatch for NoSome batch"),
            },
            Self::String {
                header,
                value,
                count,
            } => match payload {
                ValuePayload::String(v) => {
                    append_with_header!(h, header, count, {
                        value.append_value(&v);
                    });
                }
                _ => unreachable!("payload type mismatch for String batch"),
            },
            Self::EntityId {
                header,
                value_entity_id,
                count,
            } => match payload {
                ValuePayload::EntityId(v) => {
                    append_with_header!(h, header, count, {
                        value_entity_id.append_value(v);
                    });
                }
                _ => unreachable!("payload type mismatch for EntityId batch"),
            },
            Self::Time {
                header,
                time,
                timezone,
                before,
                after,
                precision,
                calendarmodel_entity_id,
                count,
            } => match payload {
                ValuePayload::Time {
                    time: t,
                    tz,
                    before: bf,
                    after: af,
                    precision: pr,
                    cal_id: cal,
                } => {
                    append_with_header!(h, header, count, {
                        time.append_value(&t);
                        timezone.append_value(tz);
                        before.append_value(bf);
                        after.append_value(af);
                        precision.append_value(pr);
                        calendarmodel_entity_id.append_value(cal);
                    });
                }
                _ => unreachable!("payload type mismatch for Time batch"),
            },
            Self::Globe {
                header,
                latitude,
                longitude,
                precision,
                globe_entity_id,
                count,
            } => match payload {
                ValuePayload::Globe {
                    lat,
                    lon,
                    precision: prec,
                    globe_id: globe,
                } => {
                    append_with_header!(h, header, count, {
                        latitude.append_value(lat);
                        longitude.append_value(lon);
                        push_opt_f64(precision, prec);
                        globe_entity_id.append_value(globe);
                    });
                }
                _ => unreachable!("payload type mismatch for Globe batch"),
            },
            Self::Mono {
                header,
                language,
                text,
                count,
            } => match payload {
                ValuePayload::Mono { lang, text: t } => {
                    append_with_header!(h, header, count, {
                        language.append_value(&lang);
                        text.append_value(&t);
                    });
                }
                _ => unreachable!("payload type mismatch for Mono batch"),
            },
            Self::Quantity {
                header,
                amount,
                lower_bound,
                upper_bound,
                unit_entity_id,
                count,
            } => match payload {
                ValuePayload::Quantity {
                    amount: a,
                    lower: lb,
                    upper: ub,
                    unit,
                } => {
                    append_with_header!(h, header, count, {
                        amount.append_value(a);
                        push_opt_f64(lower_bound, lb);
                        push_opt_f64(upper_bound, ub);
                        push_opt_i64(unit_entity_id, unit);
                    });
                }
                _ => unreachable!("payload type mismatch for Quantity batch"),
            },
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::NoSome { count, .. }
            | Self::String { count, .. }
            | Self::EntityId { count, .. }
            | Self::Time { count, .. }
            | Self::Globe { count, .. }
            | Self::Mono { count, .. }
            | Self::Quantity { count, .. } => *count == 0,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::NoSome { count, .. }
            | Self::String { count, .. }
            | Self::EntityId { count, .. }
            | Self::Time { count, .. }
            | Self::Globe { count, .. }
            | Self::Mono { count, .. }
            | Self::Quantity { count, .. } => *count,
        }
    }

    fn finish_to_batch(&mut self, key: &ClaimKey) -> Result<RecordBatch> {
        let is_claim = matches!(key.ctype, ClaimType::Claim);
        let schema = schema_for_value(key.vkind, is_claim);
        let batch = match self {
            Self::NoSome { header, count } => finish_claim_batch!(schema, is_claim, header, count)?,
            Self::String {
                header,
                value,
                count,
            } => finish_claim_batch!(schema, is_claim, header, count, value)?,
            Self::EntityId {
                header,
                value_entity_id,
                count,
            } => finish_claim_batch!(schema, is_claim, header, count, value_entity_id)?,
            Self::Time {
                header,
                time,
                timezone,
                before,
                after,
                precision,
                calendarmodel_entity_id,
                count,
            } => finish_claim_batch!(
                schema,
                is_claim,
                header,
                count,
                time,
                timezone,
                before,
                after,
                precision,
                calendarmodel_entity_id
            )?,
            Self::Globe {
                header,
                latitude,
                longitude,
                precision,
                globe_entity_id,
                count,
            } => finish_claim_batch!(
                schema,
                is_claim,
                header,
                count,
                latitude,
                longitude,
                precision,
                globe_entity_id
            )?,
            Self::Mono {
                header,
                language,
                text,
                count,
            } => finish_claim_batch!(schema, is_claim, header, count, language, text)?,
            Self::Quantity {
                header,
                amount,
                lower_bound,
                upper_bound,
                unit_entity_id,
                count,
            } => finish_claim_batch!(
                schema,
                is_claim,
                header,
                count,
                amount,
                lower_bound,
                upper_bound,
                unit_entity_id
            )?,
        };
        Ok(batch)
    }
}

// Claim writer actor per key (one task per ClaimKey) writing ready-made RecordBatches
async fn run_claim_writer(
    base_dir: PathBuf,
    key: ClaimKey,
    mut rx: Receiver<RecordBatch>,
    _batch_size: usize,
) -> Result<()> {
    let mut writer = mk_claim_writer(&base_dir, &key)?;
    while let Some(batch) = rx.recv().await {
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(())
}

struct ClaimBatchEvent {
    key: ClaimKey,
    batch: RecordBatch,
}

struct ClaimWritersInner {
    base_dir: PathBuf,
    batch_size: usize,
    senders: HashMap<ClaimKey, Sender<RecordBatch>>,
    handles: Vec<JoinHandle<Result<()>>>,
}

#[derive(Clone)]
struct ClaimWriters(Arc<Mutex<ClaimWritersInner>>);

// Generic batch writer for simple datasets: writes incoming RecordBatches and closes
async fn run_batch_writer(
    path: PathBuf,
    mut rx: Receiver<RecordBatch>,
    schema: SchemaRef,
) -> Result<()> {
    let mut writer = mk_writer(&path, schema)?;
    while let Some(batch) = rx.recv().await {
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(())
}

fn spawn_simple_writer(
    tasks: &mut Vec<JoinHandle<Result<()>>>,
    base_dir: &Path,
    name: &str,
    rx: Receiver<RecordBatch>,
    schema: SchemaRef,
) {
    tasks.push(task::spawn(run_batch_writer(
        base_dir.join(format!("{}.parquet", name)),
        rx,
        schema,
    )));
}

// Parser-side simple dataset batchers: build batches and send only when checked
struct SimpleBatchers {
    batch_size: usize,
    // senders
    entities_tx: Sender<RecordBatch>,
    labels_tx: Sender<RecordBatch>,
    aliases_tx: Sender<RecordBatch>,
    descriptions_tx: Sender<RecordBatch>,
    datatypes_tx: Sender<RecordBatch>,
    sitelinks_tx: Sender<RecordBatch>,
    sitelink_badges_tx: Sender<RecordBatch>,
    // builders + counts
    ent_id: Int64Builder,
    ent_str: StringBuilder,
    ent_count: usize,

    lab_id: Int64Builder,
    lab_lang: StringBuilder,
    lab_val: StringBuilder,
    lab_count: usize,

    ali_id: Int64Builder,
    ali_lang: StringBuilder,
    ali_val: StringBuilder,
    ali_count: usize,

    des_id: Int64Builder,
    des_lang: StringBuilder,
    des_val: StringBuilder,
    des_count: usize,

    dt_id: Int64Builder,
    dt_str: StringBuilder,
    dt_count: usize,

    sl_id: Int64Builder,
    sl_site: StringBuilder,
    sl_title: StringBuilder,
    sl_count: usize,

    slb_id: Int64Builder,
    slb_site: StringBuilder,
    slb_badge: Int64Builder,
    slb_count: usize,
}

impl SimpleBatchers {
    fn new(
        batch_size: usize,
        entities_tx: Sender<RecordBatch>,
        labels_tx: Sender<RecordBatch>,
        aliases_tx: Sender<RecordBatch>,
        descriptions_tx: Sender<RecordBatch>,
        datatypes_tx: Sender<RecordBatch>,
        sitelinks_tx: Sender<RecordBatch>,
        sitelink_badges_tx: Sender<RecordBatch>,
    ) -> Self {
        Self {
            batch_size,
            entities_tx,
            labels_tx,
            aliases_tx,
            descriptions_tx,
            datatypes_tx,
            sitelinks_tx,
            sitelink_badges_tx,
            ent_id: Int64Builder::with_capacity(batch_size),
            ent_str: StringBuilder::with_capacity(batch_size, batch_size * 8),
            ent_count: 0,
            lab_id: Int64Builder::with_capacity(batch_size),
            lab_lang: StringBuilder::with_capacity(batch_size, batch_size * 6),
            lab_val: StringBuilder::with_capacity(batch_size, batch_size * 12),
            lab_count: 0,
            ali_id: Int64Builder::with_capacity(batch_size),
            ali_lang: StringBuilder::with_capacity(batch_size, batch_size * 6),
            ali_val: StringBuilder::with_capacity(batch_size, batch_size * 12),
            ali_count: 0,
            des_id: Int64Builder::with_capacity(batch_size),
            des_lang: StringBuilder::with_capacity(batch_size, batch_size * 6),
            des_val: StringBuilder::with_capacity(batch_size, batch_size * 12),
            des_count: 0,
            dt_id: Int64Builder::with_capacity(batch_size),
            dt_str: StringBuilder::with_capacity(batch_size, batch_size * 6),
            dt_count: 0,
            sl_id: Int64Builder::with_capacity(batch_size),
            sl_site: StringBuilder::with_capacity(batch_size, batch_size * 6),
            sl_title: StringBuilder::with_capacity(batch_size, batch_size * 12),
            sl_count: 0,
            slb_id: Int64Builder::with_capacity(batch_size),
            slb_site: StringBuilder::with_capacity(batch_size, batch_size * 6),
            slb_badge: Int64Builder::with_capacity(batch_size),
            slb_count: 0,
        }
    }

    fn entities_append(&mut self, id: i64, s: &str) {
        self.ent_id.append_value(id);
        self.ent_str.append_value(s);
        self.ent_count += 1;
    }

    fn labels_append(&mut self, id: i64, lang: &str, val: &str) {
        self.lab_id.append_value(id);
        self.lab_lang.append_value(lang);
        self.lab_val.append_value(val);
        self.lab_count += 1;
    }

    fn aliases_append(&mut self, id: i64, lang: &str, val: &str) {
        self.ali_id.append_value(id);
        self.ali_lang.append_value(lang);
        self.ali_val.append_value(val);
        self.ali_count += 1;
    }

    fn descriptions_append(&mut self, id: i64, lang: &str, val: &str) {
        self.des_id.append_value(id);
        self.des_lang.append_value(lang);
        self.des_val.append_value(val);
        self.des_count += 1;
    }

    fn datatypes_append(&mut self, id: i64, dt: &str) {
        self.dt_id.append_value(id);
        self.dt_str.append_value(dt);
        self.dt_count += 1;
    }

    fn sitelinks_append(&mut self, id: i64, site: &str, title: &str) {
        self.sl_id.append_value(id);
        self.sl_site.append_value(site);
        self.sl_title.append_value(title);
        self.sl_count += 1;
    }

    fn sitelink_badges_append(&mut self, id: i64, site: &str, badge: i64) {
        self.slb_id.append_value(id);
        self.slb_site.append_value(site);
        self.slb_badge.append_value(badge);
        self.slb_count += 1;
    }

    // Flush only if thresholds reached; invoked at end of each entity
    async fn flush_if_needed_async(&mut self) -> Result<()> {
        flush_simple_async!(
            self,
            self.ent_count >= self.batch_size,
            ent_count,
            schema_entities,
            [ent_id, ent_str],
            entities_tx
        );
        flush_simple_async!(
            self,
            self.lab_count >= self.batch_size,
            lab_count,
            schema_labels,
            [lab_id, lab_lang, lab_val],
            labels_tx
        );
        flush_simple_async!(
            self,
            self.ali_count >= self.batch_size,
            ali_count,
            schema_aliases,
            [ali_id, ali_lang, ali_val],
            aliases_tx
        );
        flush_simple_async!(
            self,
            self.des_count >= self.batch_size,
            des_count,
            schema_descriptions,
            [des_id, des_lang, des_val],
            descriptions_tx
        );
        flush_simple_async!(
            self,
            self.dt_count >= self.batch_size,
            dt_count,
            schema_datatypes,
            [dt_id, dt_str],
            datatypes_tx
        );
        flush_simple_async!(
            self,
            self.sl_count >= self.batch_size,
            sl_count,
            schema_sitelinks,
            [sl_id, sl_site, sl_title],
            sitelinks_tx
        );
        flush_simple_async!(
            self,
            self.slb_count >= self.batch_size,
            slb_count,
            schema_sitelink_badges,
            [slb_id, slb_site, slb_badge],
            sitelink_badges_tx
        );
        Ok(())
    }

    async fn flush_all_async(&mut self) -> Result<()> {
        flush_simple_async!(
            self,
            self.ent_count > 0,
            ent_count,
            schema_entities,
            [ent_id, ent_str],
            entities_tx
        );
        flush_simple_async!(
            self,
            self.lab_count > 0,
            lab_count,
            schema_labels,
            [lab_id, lab_lang, lab_val],
            labels_tx
        );
        flush_simple_async!(
            self,
            self.ali_count > 0,
            ali_count,
            schema_aliases,
            [ali_id, ali_lang, ali_val],
            aliases_tx
        );
        flush_simple_async!(
            self,
            self.des_count > 0,
            des_count,
            schema_descriptions,
            [des_id, des_lang, des_val],
            descriptions_tx
        );
        flush_simple_async!(
            self,
            self.dt_count > 0,
            dt_count,
            schema_datatypes,
            [dt_id, dt_str],
            datatypes_tx
        );
        flush_simple_async!(
            self,
            self.sl_count > 0,
            sl_count,
            schema_sitelinks,
            [sl_id, sl_site, sl_title],
            sitelinks_tx
        );
        flush_simple_async!(
            self,
            self.slb_count > 0,
            slb_count,
            schema_sitelink_badges,
            [slb_id, slb_site, slb_badge],
            sitelink_badges_tx
        );
        Ok(())
    }
}

// Per-key claim buffering: accumulate builders and only flush when checked
struct ClaimBatchHolder {
    key: ClaimKey,
    batch_size: usize,
    current: ClaimBatchBuilders,
}

impl ClaimBatchHolder {
    fn new(key: &ClaimKey, batch_size: usize) -> Self {
        Self {
            key: key.clone(),
            batch_size,
            current: ClaimBatchBuilders::new(key),
        }
    }

    fn append(&mut self, row: Row) {
        self.current.append(row);
    }

    async fn flush_if_needed_send(&mut self, writers: &ClaimWriters) -> Result<()> {
        if self.current.len() >= self.batch_size {
            let batch = self.current.finish_to_batch(&self.key)?;
            writers
                .send(ClaimBatchEvent {
                    key: self.key.clone(),
                    batch,
                })
                .await?;
        }
        Ok(())
    }

    async fn finalize_and_send_all(self, writers: &ClaimWriters) -> Result<()> {
        let this = self;
        // send remaining partial if any
        if !this.current.is_empty() {
            let mut cur = this.current;
            let batch = cur.finish_to_batch(&this.key)?;
            writers
                .send(ClaimBatchEvent {
                    key: this.key.clone(),
                    batch,
                })
                .await?;
        }
        Ok(())
    }
}

impl ClaimWriters {
    fn new(base_dir: PathBuf, batch_size: usize) -> Self {
        Self(Arc::new(Mutex::new(ClaimWritersInner {
            base_dir,
            batch_size,
            senders: HashMap::new(),
            handles: Vec::new(),
        })))
    }

    async fn send(&self, ev: ClaimBatchEvent) -> Result<()> {
        let mut inner = self.0.lock().await;
        let tx = if let Some(tx) = inner.senders.get(&ev.key) {
            tx.clone()
        } else {
            let (tx_new, rx_new) = mpsc::channel::<RecordBatch>(READY_BATCHES_BUFFER);
            let base = inner.base_dir.clone();
            let key = ev.key.clone();
            let h = task::spawn(run_claim_writer(
                base,
                key.clone(),
                rx_new,
                inner.batch_size,
            ));
            inner.handles.push(h);
            inner.senders.insert(key.clone(), tx_new.clone());
            tx_new
        };
        drop(inner);
        let _ = tx.send(ev.batch).await;
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        let handles = {
            let mut inner = self.0.lock().await;
            inner.senders.clear();
            std::mem::take(&mut inner.handles)
        };
        for h in handles {
            h.await??;
        }
        Ok(())
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    fs::create_dir_all(args.output.join("tmp"))?;

    // Async writer actor setup
    let base_simple_dir = args.output.join("tmp");

    // capacities sized to absorb bursts but apply backpressure under sustained load
    let (entities_tx, entities_rx) = mpsc::channel::<RecordBatch>(READY_BATCHES_BUFFER);
    let (labels_tx, labels_rx) = mpsc::channel::<RecordBatch>(READY_BATCHES_BUFFER);
    let (aliases_tx, aliases_rx) = mpsc::channel::<RecordBatch>(READY_BATCHES_BUFFER);
    let (descriptions_tx, descriptions_rx) = mpsc::channel::<RecordBatch>(READY_BATCHES_BUFFER);
    let (datatypes_tx, datatypes_rx) = mpsc::channel::<RecordBatch>(READY_BATCHES_BUFFER);
    let (sitelinks_tx, sitelinks_rx) = mpsc::channel::<RecordBatch>(READY_BATCHES_BUFFER);
    let (sitelink_badges_tx, sitelink_badges_rx) =
        mpsc::channel::<RecordBatch>(READY_BATCHES_BUFFER);

    let mut writer_tasks: Vec<JoinHandle<Result<()>>> = Vec::new();
    spawn_simple_writer(
        &mut writer_tasks,
        &base_simple_dir,
        "entities",
        entities_rx,
        schema_entities(),
    );
    spawn_simple_writer(
        &mut writer_tasks,
        &base_simple_dir,
        "labels",
        labels_rx,
        schema_labels(),
    );
    spawn_simple_writer(
        &mut writer_tasks,
        &base_simple_dir,
        "aliases",
        aliases_rx,
        schema_aliases(),
    );
    spawn_simple_writer(
        &mut writer_tasks,
        &base_simple_dir,
        "descriptions",
        descriptions_rx,
        schema_descriptions(),
    );
    spawn_simple_writer(
        &mut writer_tasks,
        &base_simple_dir,
        "datatypes",
        datatypes_rx,
        schema_datatypes(),
    );
    spawn_simple_writer(
        &mut writer_tasks,
        &base_simple_dir,
        "sitelinks",
        sitelinks_rx,
        schema_sitelinks(),
    );
    spawn_simple_writer(
        &mut writer_tasks,
        &base_simple_dir,
        "sitelink_badges",
        sitelink_badges_rx,
        schema_sitelink_badges(),
    );

    // Reader task (blocking) will be spawned after worker channels are created

    // Shared interner and claim writers
    let interner = Arc::new(Interner::new());
    let claim_writers = ClaimWriters::new(args.output.join("tmp"), args.batch_size);
    let claim_ids = Arc::new(ClaimIdGen::new());

    // Create worker channels
    let number_of_entity_parser_threads = args.parse_threads.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    });
    let mut entity_parser_txs = Vec::with_capacity(number_of_entity_parser_threads);
    let mut entity_parser_threads: Vec<JoinHandle<Result<()>>> =
        Vec::with_capacity(number_of_entity_parser_threads);
    for _ in 0..number_of_entity_parser_threads {
        let (tx, mut rx) =
            mpsc::channel::<String>(LINE_CHANNEL_CAPACITY / number_of_entity_parser_threads.max(1));
        entity_parser_txs.push(tx);
        // Clone resources for worker
        let entities_tx_c = entities_tx.clone();
        let labels_tx_c = labels_tx.clone();
        let aliases_tx_c = aliases_tx.clone();
        let descriptions_tx_c = descriptions_tx.clone();
        let datatypes_tx_c = datatypes_tx.clone();
        let sitelinks_tx_c = sitelinks_tx.clone();
        let sitelink_badges_tx_c = sitelink_badges_tx.clone();
        let interner_c = interner.clone();
        let claim_writers_c = claim_writers.clone();
        let claim_ids_c = claim_ids.clone();
        let batch_size = args.batch_size;
        let h = task::spawn(async move {
            // Per-worker batchers and builders
            let mut simple = SimpleBatchers::new(
                batch_size,
                entities_tx_c,
                labels_tx_c,
                aliases_tx_c,
                descriptions_tx_c,
                datatypes_tx_c,
                sitelinks_tx_c,
                sitelink_badges_tx_c,
            );
            let mut claim_builders: HashMap<ClaimKey, ClaimBatchHolder> = HashMap::new();
            while let Some(line) = rx.recv().await {
                process_entity(
                    &line,
                    &interner_c,
                    &mut simple,
                    &mut claim_builders,
                    &claim_writers_c,
                    batch_size,
                    &claim_ids_c,
                )
                .await?;
            }
            // Flush remaining claim batches
            for (_key, entry) in claim_builders.into_iter() {
                entry.finalize_and_send_all(&claim_writers_c).await?;
            }
            // Flush simple batchers
            simple.flush_all_async().await?;
            Ok::<_, anyhow::Error>(())
        });
        entity_parser_threads.push(h);
    }
    // Now that worker_txs is initialized, spawn the reader that directly dispatches to workers
    let reader_thread: JoinHandle<Result<()>> = {
        let mut txs = entity_parser_txs;
        task::spawn_blocking(move || -> Result<()> {
            let mut br = BufReader::new(std::io::stdin().lock());
            let mut line_buf = String::new();
            br.read_line(&mut line_buf)?; // skip initial '['
            let mut idx: usize = 0;
            loop {
                line_buf.clear();
                let n = br.read_line(&mut line_buf)?;
                if n == 0 {
                    break;
                }
                let mut line = line_buf.trim_end().to_string();
                if line == "]" || line == "]," {
                    break;
                }
                if line.ends_with(',') {
                    line.pop();
                }
                if line.is_empty() {
                    continue;
                }
                // Round-robin dispatch with backpressure. If a worker channel is closed, drop it.
                loop {
                    if txs.is_empty() {
                        // No workers available; nothing more we can do
                        break;
                    }
                    let i = idx % txs.len();
                    match txs[i].blocking_send(line.clone()) {
                        Ok(()) => {
                            idx = idx.wrapping_add(1);
                            break;
                        }
                        Err(_e) => {
                            // This worker closed; remove and retry with remaining
                            txs.remove(i);
                            // do not advance idx; try current index with new vec
                            continue;
                        }
                    }
                }
            }
            // Drop all senders to signal EOF to workers
            drop(txs);
            Ok(())
        })
    };
    // Ensure reader completed successfully
    reader_thread.await??;

    // Wait for workers
    for h in entity_parser_threads {
        h.await??;
    }

    // Drop senders to finish writer tasks cleanly
    drop(entities_tx);
    drop(labels_tx);
    drop(aliases_tx);
    drop(descriptions_tx);
    drop(datatypes_tx);
    drop(sitelinks_tx);
    drop(sitelink_badges_tx);
    // Close claim writers and wait for them
    claim_writers.close().await?;

    // Close simple dataset writers
    for t in writer_tasks {
        t.await??;
    }

    finalize_output_parallel(&args.output, args.max_file_size).await?;

    Ok(())
}

async fn finalize_output_parallel(out_root: &Path, max_file_size: u64) -> Result<()> {
    let datasets = vec![
        "entities",
        "labels",
        "aliases",
        "descriptions",
        "datatypes",
        "sitelinks",
        "sitelink_badges",
    ];
    let tmp_root = out_root.join("tmp");
    let tmp2_root = out_root.join("tmp2");
    fs::create_dir_all(&tmp2_root).ok();

    let mut handles: Vec<JoinHandle<Result<()>>> = Vec::new();

    // Helper: collect output parts from DuckDB COPY temp directory
    fn collect_duckdb_parts(dir: &Path) -> Result<Vec<(i64, PathBuf)>> {
        let mut parts: Vec<(i64, PathBuf)> = Vec::new();
        for e in fs::read_dir(dir)? {
            let p = e?.path();
            if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                if name == "data.parquet" {
                    parts.push((0, p));
                } else if let Some(stem) = name.strip_suffix(".parquet") {
                    if let Some(suf) = stem.strip_prefix("data_") {
                        let idx: i64 = suf.parse().unwrap_or(0);
                        parts.push((idx, p));
                    }
                }
            }
        }
        parts.sort_by_key(|(i, _)| *i);
        Ok(parts)
    }

    // Simple dataset files -> one blocking task each
    for name in datasets {
        let in_path = out_root.join(format!("tmp/{}.parquet", name));
        if !in_path.exists() {
            continue;
        }
        let mid_dir = out_root.join(format!("tmp2/{}", name));
        let out_single = out_root.join(format!("{}.parquet", name));
        let h = task::spawn_blocking(move || -> Result<()> {
            use duckdb::Connection;
            fs::create_dir_all(&mid_dir).ok();
            let conn = Connection::open_in_memory()?;
            conn.execute_batch(
                "SET enable_progress_bar_print=TRUE; SET progress_bar_time=0; SET threads=1;",
            )?;
            let sql = format!(
                "COPY (SELECT * FROM parquet_scan('{}')) TO '{}' (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 22, STRING_DICTIONARY_PAGE_SIZE_LIMIT 100_000, FILE_SIZE_BYTES {})",
                in_path.display(),
                mid_dir.display(),
                max_file_size
            );
            conn.execute_batch(&sql)?;
            let _ = fs::remove_file(&in_path);
            let parts = collect_duckdb_parts(&mid_dir)?;
            for (i, f) in parts {
                let suffix = if i == 0 {
                    String::new()
                } else {
                    format!("_{}", i)
                };
                let dest = out_single
                    .with_file_name(format!(
                        "{}{}",
                        out_single.file_stem().unwrap().to_string_lossy(),
                        suffix
                    ))
                    .with_extension("parquet");
                fs::rename(&f, &dest)?;
            }
            let _ = fs::remove_dir_all(&mid_dir);
            Ok(())
        });
        handles.push(h);
    }

    // claim/qual/reference directories
    let mut claim_dirs: Vec<String> = Vec::new();
    if tmp_root.exists() {
        for entry in fs::read_dir(&tmp_root)? {
            let p = entry?.path();
            if let Some(dname) = p.file_name().and_then(|s| s.to_str()) {
                if dname.starts_with("claim_")
                    || dname.starts_with("qualifier_")
                    || dname.starts_with("reference_")
                {
                    claim_dirs.push(dname.to_string());
                }
            }
        }
    }
    for dname in claim_dirs {
        let tmp_root_c = tmp_root.clone();
        let out_root_c = out_root.to_path_buf();
        let dname_c = dname.clone();
        let h = task::spawn_blocking(move || -> Result<()> {
            use duckdb::Connection;
            let glob = format!("{}/{}/*/*/*.parquet", tmp_root_c.display(), dname_c);
            let mid_dir = out_root_c.join(format!("tmp2/{}", dname_c));
            fs::create_dir_all(&mid_dir).ok();
            let conn = Connection::open_in_memory()?;
            conn.execute_batch(
                "SET enable_progress_bar_print=TRUE; SET progress_bar_time=0; SET threads=1;",
            )?;
            let sql = format!(
                "COPY (SELECT * FROM parquet_scan('{}')) TO '{}' (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 22, STRING_DICTIONARY_PAGE_SIZE_LIMIT 100_000, FILE_SIZE_BYTES {})",
                glob,
                mid_dir.display(),
                max_file_size
            );
            conn.execute_batch(&sql)?;
            let _ = fs::remove_dir_all(out_root_c.join(format!("tmp/{}", dname_c)));
            let parts = collect_duckdb_parts(&mid_dir)?;
            for (i, f) in parts {
                let dest = if i == 0 {
                    out_root_c.join(format!("{}.parquet", dname_c))
                } else {
                    out_root_c.join(format!("{}_{}.parquet", dname_c, i))
                };
                fs::rename(&f, &dest)?;
            }
            let _ = fs::remove_dir_all(&mid_dir);
            Ok(())
        });
        handles.push(h);
    }

    for h in handles {
        h.await??;
    }
    let _ = fs::remove_dir_all(&tmp_root);
    let _ = fs::remove_dir_all(&tmp2_root);
    Ok(())
}
