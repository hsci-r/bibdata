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
const SIMPLE_DATASETS: &[&str] = &[
    "entities",
    "labels",
    "aliases",
    "descriptions",
    "datatypes",
    "sitelinks",
    "sitelink_badges",
];

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
fn order_of<S: AsRef<str>>(list: &[S], prop: &str) -> i32 {
    list.iter()
        .position(|x| x.as_ref() == prop)
        .map(|i| (i as i32) + 1)
        .unwrap()
}

// Compact JSON access helper with consistent error context
#[inline]
fn req_opt<T>(v: Option<T>, msg: &str, ctx: &Value) -> Result<T> {
    v.with_context(|| format!("{}; snak={}", msg, json_snippet(ctx)))
}

#[inline]
fn make_batch(schema: SchemaRef, arrays: Vec<Arc<dyn arrow::array::Array>>) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(schema, arrays)?)
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
            simple.entities.append(next, &id_owned);
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

enum ParsedValue<'a> {
    NoSome,
    String(&'a str),
    EntityId(i64),
    Time {
        time: &'a str,
        tz: i32,
        before: i32,
        after: i32,
        precision: i32,
        cal_id: i64,
    },
    Globe {
        lat: f64,
        lon: f64,
        prec: Option<f64>,
        globe_id: i64,
    },
    Mono {
        lang: &'a str,
        text: &'a str,
    },
    Quantity {
        amount: f64,
        lower: Option<f64>,
        upper: Option<f64>,
        unit_id: Option<i64>,
    },
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

struct ClaimIdGen(AtomicI64);
impl ClaimIdGen {
    fn new() -> Self {
        Self(AtomicI64::new(1))
    }
    fn next(&self) -> i64 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

fn ordered_props<'a>(value: &'a Value, key: &str) -> Vec<&'a str> {
    value
        .get(key)
        .and_then(|v| v.as_array())
        .map(|items| items.iter().filter_map(|item| item.as_str()).collect())
        .unwrap_or_default()
}

fn visit_lang_values(section: &serde_json::Map<String, Value>, mut visit: impl FnMut(&str, &str)) {
    for value in section.values() {
        if let (Some(lang), Some(text)) = (
            value.get("language").and_then(|v| v.as_str()),
            value.get("value").and_then(|v| v.as_str()),
        ) {
            visit(lang, text);
        }
    }
}

fn visit_lang_value_lists(
    section: &serde_json::Map<String, Value>,
    mut visit: impl FnMut(&str, &str),
) {
    for list in section.values() {
        if let Some(items) = list.as_array() {
            for value in items {
                if let (Some(lang), Some(text)) = (
                    value.get("language").and_then(|v| v.as_str()),
                    value.get("value").and_then(|v| v.as_str()),
                ) {
                    visit(lang, text);
                }
            }
        }
    }
}

struct WorkerState {
    interner: Arc<Interner>,
    simple: SimpleBatchers,
    claim_builders: HashMap<ClaimKey, ClaimBatchBuilders>,
    claim_writers: ClaimWriters,
    batch_size: usize,
    claim_ids: Arc<ClaimIdGen>,
}

impl WorkerState {
    fn new(
        batch_size: usize,
        senders: SimpleWriterSenders,
        interner: Arc<Interner>,
        claim_writers: ClaimWriters,
        claim_ids: Arc<ClaimIdGen>,
    ) -> Self {
        Self {
            interner,
            simple: SimpleBatchers::new(batch_size, senders),
            claim_builders: HashMap::new(),
            claim_writers,
            batch_size,
            claim_ids,
        }
    }

    fn intern(&mut self, id: &str) -> i64 {
        self.interner.get_or_insert(&mut self.simple, id)
    }

    fn intern_uri(&mut self, id: &str) -> i64 {
        self.intern(strip_uri(id))
    }

    fn get_claim_builders(
        &mut self,
        ctype: ClaimType,
        rank: Rank,
        property_id: i64,
        vkind: ValueKind,
    ) -> &mut ClaimBatchBuilders {
        let key = ClaimKey::new(ctype, rank, property_id, vkind);
        let batch_size = self.batch_size;
        self.claim_builders
            .entry(key.clone())
            .or_insert_with(|| ClaimBatchBuilders::new(&key, batch_size))
    }

    fn parse_snak<'a>(&mut self, snak: &'a Value) -> Result<Option<(ValueKind, ParsedValue<'a>)>> {
        let snaktype = req_opt(
            snak.get("snaktype").and_then(|v| v.as_str()),
            "snak missing 'snaktype'",
            snak,
        )?;
        match snaktype {
            "novalue" => Ok(Some((ValueKind::NoValue, ParsedValue::NoSome))),
            "somevalue" => Ok(Some((ValueKind::SomeValue, ParsedValue::NoSome))),
            _ => {
                let dv = snak.get("datavalue").with_context(|| {
                    format!(
                        "snak missing 'datavalue' for value type; snak={}",
                        json_snippet(snak)
                    )
                })?;
                let value_type = req_opt(
                    dv.get("type").and_then(|v| v.as_str()),
                    "datavalue missing 'type'",
                    snak,
                )?;
                let value = dv.get("value").with_context(|| {
                    format!("datavalue missing 'value'; snak={}", json_snippet(snak))
                })?;
                match value_type {
                    "string" => {
                        let s = req_opt(
                            value.as_str(),
                            "string datavalue missing string 'value'",
                            snak,
                        )?;
                        Ok(Some((ValueKind::String, ParsedValue::String(s))))
                    }
                    "wikibase-entityid" => {
                        let id = req_opt(
                            value.get("id").and_then(|v| v.as_str()),
                            "entityid datavalue missing 'id'",
                            snak,
                        )?;
                        let eid = self.intern(id);
                        Ok(Some((ValueKind::EntityId, ParsedValue::EntityId(eid))))
                    }
                    "time" => {
                        let time = req_opt(
                            value.get("time").and_then(|v| v.as_str()),
                            "time datavalue missing 'time'",
                            snak,
                        )?;
                        let tz = req_opt(
                            value.get("timezone").and_then(|v| v.as_i64()),
                            "time datavalue missing 'timezone'",
                            snak,
                        )? as i32;
                        let before = req_opt(
                            value.get("before").and_then(|v| v.as_i64()),
                            "time datavalue missing 'before'",
                            snak,
                        )? as i32;
                        let after = req_opt(
                            value.get("after").and_then(|v| v.as_i64()),
                            "time datavalue missing 'after'",
                            snak,
                        )? as i32;
                        let precision = req_opt(
                            value.get("precision").and_then(|v| v.as_i64()),
                            "time datavalue missing 'precision'",
                            snak,
                        )? as i32;
                        let cal_id = self.intern(req_opt(
                            value
                                .get("calendarmodel")
                                .and_then(|v| v.as_str())
                                .map(strip_uri),
                            "time datavalue missing 'calendarmodel'",
                            snak,
                        )?);
                        Ok(Some((
                            ValueKind::Time,
                            ParsedValue::Time {
                                time,
                                tz,
                                before,
                                after,
                                precision,
                                cal_id,
                            },
                        )))
                    }
                    "globecoordinate" => {
                        let lat = req_opt(
                            value.get("latitude").and_then(|v| v.as_f64()),
                            "globecoordinate missing 'latitude'",
                            snak,
                        )?;
                        let lon = req_opt(
                            value.get("longitude").and_then(|v| v.as_f64()),
                            "globecoordinate missing 'longitude'",
                            snak,
                        )?;
                        let prec = value.get("precision").and_then(|v| v.as_f64());
                        let globe_id = self.intern(req_opt(
                            value.get("globe").and_then(|v| v.as_str()).map(strip_uri),
                            "globecoordinate missing 'globe'",
                            snak,
                        )?);
                        Ok(Some((
                            ValueKind::GlobeCoordinate,
                            ParsedValue::Globe {
                                lat,
                                lon,
                                prec,
                                globe_id,
                            },
                        )))
                    }
                    "monolingualtext" => {
                        let lang = req_opt(
                            value.get("language").and_then(|v| v.as_str()),
                            "monolingualtext missing 'language'",
                            snak,
                        )?;
                        let text = req_opt(
                            value.get("text").and_then(|v| v.as_str()),
                            "monolingualtext missing 'text'",
                            snak,
                        )?;
                        Ok(Some((
                            ValueKind::MonolingualText,
                            ParsedValue::Mono { lang, text },
                        )))
                    }
                    "quantity" => {
                        let amount = req_opt(
                            value.get("amount").and_then(|v| v.as_str()),
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
                        let lower = value
                            .get("lowerBound")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<f64>().ok());
                        let upper = value
                            .get("upperBound")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<f64>().ok());
                        let unit = req_opt(
                            value.get("unit").and_then(|v| v.as_str()),
                            "quantity missing 'unit'",
                            snak,
                        )?;
                        let unit_id = if unit == "1" {
                            None
                        } else {
                            Some(self.intern_uri(unit))
                        };
                        Ok(Some((
                            ValueKind::Quantity,
                            ParsedValue::Quantity {
                                amount,
                                lower,
                                upper,
                                unit_id,
                            },
                        )))
                    }
                    _ => Ok(None),
                }
            }
        }
    }

    fn process_snak_value(
        &mut self,
        ctype: ClaimType,
        rank: Rank,
        claim_id: i64,
        entity_id: i64,
        order: i32,
        property_id: i64,
        datatype: Option<&str>,
        snak: &Value,
    ) -> Result<()> {
        let Some((vkind, parsed)) = self.parse_snak(snak)? else {
            return Ok(());
        };
        let holder = self.get_claim_builders(ctype, rank, property_id, vkind);
        holder.append_header(claim_id, entity_id, order, property_id, datatype);
        holder.values_mut().append_parsed(&parsed);
        Ok(())
    }

    fn process_sub_snaks(
        &mut self,
        ctype: ClaimType,
        claim_id: i64,
        order_list: &[&str],
        snaks: &serde_json::Map<String, Value>,
    ) {
        for (prop, arr) in snaks {
            let Some(items) = arr.as_array() else {
                continue;
            };
            let order = order_of(order_list, prop);
            let property_id = self.intern(prop);
            for snak in items {
                let dt = snak.get("datatype").and_then(|v| v.as_str());
                if let Err(e) = self.process_snak_value(
                    ctype,
                    Rank::Normal,
                    claim_id,
                    0,
                    order,
                    property_id,
                    dt,
                    snak,
                ) {
                    eprintln!(
                        "Warning: skipping {} snak due to error: {:#}",
                        ctype.as_str(),
                        e
                    );
                }
            }
        }
    }

    fn process_sitelinks(&mut self, entity_id: i64, sitelinks: &serde_json::Map<String, Value>) {
        for value in sitelinks.values() {
            let site = value.get("site").and_then(|v| v.as_str());
            if let (Some(site), Some(title)) = (site, value.get("title").and_then(|v| v.as_str())) {
                self.simple.sitelinks.append(entity_id, site, title);
            }
            if let (Some(site), Some(badges)) =
                (site, value.get("badges").and_then(|v| v.as_array()))
            {
                for badge in badges.iter().filter_map(|badge| badge.as_str()) {
                    let badge_id = self.intern(badge);
                    self.simple
                        .sitelink_badges
                        .append(entity_id, site, badge_id);
                }
            }
        }
    }

    async fn process_entity(&mut self, json_line: &str) -> Result<()> {
        let obj: Value = match serde_json::from_str(json_line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "Warning: skipping unparseable entity JSON: {}; line={}",
                    e,
                    json_snippet(&Value::String(json_line.chars().take(200).collect()))
                );
                return Ok(());
            }
        };
        let Some(id) = obj.get("id").and_then(|v| v.as_str()) else {
            eprintln!(
                "Warning: skipping entity missing 'id': {}",
                json_snippet(&obj)
            );
            return Ok(());
        };
        let entity_id = self.intern(id);

        if let Some(labels) = obj.get("labels").and_then(|v| v.as_object()) {
            visit_lang_values(labels, |lang, value| {
                self.simple.labels.append(entity_id, lang, value)
            });
        }
        if let Some(aliases) = obj.get("aliases").and_then(|v| v.as_object()) {
            visit_lang_value_lists(aliases, |lang, value| {
                self.simple.aliases.append(entity_id, lang, value);
            });
        }
        if let Some(descriptions) = obj.get("descriptions").and_then(|v| v.as_object()) {
            visit_lang_values(descriptions, |lang, value| {
                self.simple.descriptions.append(entity_id, lang, value);
            });
        }
        if let Some(datatype) = obj.get("datatype").and_then(|v| v.as_str()) {
            self.simple.datatypes.append(entity_id, datatype);
        }

        if let Some(claims) = obj.get("claims").and_then(|v| v.as_object()) {
            for claim_list in claims.values().filter_map(|v| v.as_array()) {
                for claim in claim_list {
                    let Some(rank_str) = claim.get("rank").and_then(|v| v.as_str()) else {
                        eprintln!(
                            "Warning: skipping claim missing 'rank': {}",
                            json_snippet(claim)
                        );
                        continue;
                    };
                    let rank = Rank::from(rank_str);
                    let Some(mainsnak) = claim.get("mainsnak") else {
                        eprintln!(
                            "Warning: skipping claim missing 'mainsnak': {}",
                            json_snippet(claim)
                        );
                        continue;
                    };
                    let Some(property) = mainsnak.get("property").and_then(|v| v.as_str()) else {
                        eprintln!(
                            "Warning: skipping claim with mainsnak missing 'property': {}",
                            json_snippet(mainsnak)
                        );
                        continue;
                    };
                    let claim_id = self.claim_ids.next();
                    let prop_id = self.intern(property);
                    let dt = mainsnak.get("datatype").and_then(|v| v.as_str());
                    if let Err(e) = self.process_snak_value(
                        ClaimType::Claim,
                        rank,
                        claim_id,
                        entity_id,
                        0,
                        prop_id,
                        dt,
                        mainsnak,
                    ) {
                        eprintln!("Warning: skipping claim snak due to error: {:#}", e);
                    }

                    if let Some(qualifiers) = claim.get("qualifiers").and_then(|v| v.as_object()) {
                        let order_list = ordered_props(claim, "qualifiers-order");
                        self.process_sub_snaks(
                            ClaimType::Qualifier,
                            claim_id,
                            &order_list,
                            qualifiers,
                        );
                    }

                    if let Some(references) = claim.get("references").and_then(|v| v.as_array()) {
                        for reference in references {
                            if let Some(snaks) = reference.get("snaks").and_then(|v| v.as_object())
                            {
                                let order_list = ordered_props(reference, "snaks-order");
                                self.process_sub_snaks(
                                    ClaimType::Reference,
                                    claim_id,
                                    &order_list,
                                    snaks,
                                );
                            }
                        }
                    }
                }
            }
        }

        if let Some(sitelinks) = obj.get("sitelinks").and_then(|v| v.as_object()) {
            self.process_sitelinks(entity_id, sitelinks);
        }

        for entry in self.claim_builders.values_mut() {
            entry.flush_if_needed_send(&self.claim_writers).await?;
        }
        self.simple.flush_async(false).await?;
        Ok(())
    }

    async fn finish(mut self) -> Result<()> {
        for entry in std::mem::take(&mut self.claim_builders).into_values() {
            entry.finalize_and_send_all(&self.claim_writers).await?;
        }
        self.simple.flush_async(true).await?;
        Ok(())
    }
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

    fn finish_to_arrays(&mut self) -> Vec<Arc<dyn arrow::array::Array>> {
        match self {
            ClaimHeadBuilder::Claim {
                claim_id,
                rank,
                entity_id,
                property_id,
                datatype,
            } => {
                vec![
                    Arc::new(claim_id.finish()),
                    Arc::new(rank.finish()),
                    Arc::new(entity_id.finish()),
                    Arc::new(property_id.finish()),
                    Arc::new(datatype.finish()),
                ]
            }
            ClaimHeadBuilder::Sub {
                order,
                claim_id,
                property_id,
                datatype,
            } => {
                vec![
                    Arc::new(order.finish()),
                    Arc::new(claim_id.finish()),
                    Arc::new(property_id.finish()),
                    Arc::new(datatype.finish()),
                ]
            }
        }
    }

    fn append(
        &mut self,
        claim_id: i64,
        rank: &str,
        entity_id: i64,
        order: i32,
        property_id: i64,
        datatype: Option<&str>,
    ) {
        match self {
            ClaimHeadBuilder::Claim {
                claim_id: c,
                rank: r,
                entity_id: e,
                property_id: p,
                datatype: d,
            } => {
                c.append_value(claim_id);
                r.append_value(rank);
                e.append_value(entity_id);
                p.append_value(property_id);
                d.append_option(datatype);
            }
            ClaimHeadBuilder::Sub {
                order: o,
                claim_id: c,
                property_id: p,
                datatype: d,
            } => {
                o.append_value(order);
                c.append_value(claim_id);
                p.append_value(property_id);
                d.append_option(datatype);
            }
        }
    }
}

// Value-specific builders only
enum ValueBuilders {
    NoSome,
    String {
        value: StringBuilder,
    },
    EntityId {
        value_entity_id: Int64Builder,
    },
    Time {
        time: StringBuilder,
        timezone: Int32Builder,
        before: Int32Builder,
        after: Int32Builder,
        precision: Int32Builder,
        calendarmodel_entity_id: Int64Builder,
    },
    Globe {
        latitude: Float64Builder,
        longitude: Float64Builder,
        precision: Float64Builder,
        globe_entity_id: Int64Builder,
    },
    Mono {
        language: StringBuilder,
        text: StringBuilder,
    },
    Quantity {
        amount: Float64Builder,
        lower_bound: Float64Builder,
        upper_bound: Float64Builder,
        unit_entity_id: Int64Builder,
    },
}

impl ValueBuilders {
    fn append_parsed(&mut self, pv: &ParsedValue) {
        match (self, pv) {
            (ValueBuilders::NoSome, ParsedValue::NoSome) => {}
            (ValueBuilders::String { value }, ParsedValue::String(s)) => {
                value.append_value(s);
            }
            (ValueBuilders::EntityId { value_entity_id }, ParsedValue::EntityId(eid)) => {
                value_entity_id.append_value(*eid);
            }
            (
                ValueBuilders::Time {
                    time,
                    timezone,
                    before,
                    after,
                    precision,
                    calendarmodel_entity_id,
                },
                ParsedValue::Time {
                    time: t,
                    tz,
                    before: bf,
                    after: af,
                    precision: pr,
                    cal_id,
                },
            ) => {
                time.append_value(t);
                timezone.append_value(*tz);
                before.append_value(*bf);
                after.append_value(*af);
                precision.append_value(*pr);
                calendarmodel_entity_id.append_value(*cal_id);
            }
            (
                ValueBuilders::Globe {
                    latitude,
                    longitude,
                    precision,
                    globe_entity_id,
                },
                ParsedValue::Globe {
                    lat,
                    lon,
                    prec,
                    globe_id,
                },
            ) => {
                latitude.append_value(*lat);
                longitude.append_value(*lon);
                precision.append_option(*prec);
                globe_entity_id.append_value(*globe_id);
            }
            (ValueBuilders::Mono { language, text }, ParsedValue::Mono { lang, text: t }) => {
                language.append_value(lang);
                text.append_value(t);
            }
            (
                ValueBuilders::Quantity {
                    amount,
                    lower_bound,
                    upper_bound,
                    unit_entity_id,
                },
                ParsedValue::Quantity {
                    amount: a,
                    lower,
                    upper,
                    unit_id,
                },
            ) => {
                amount.append_value(*a);
                lower_bound.append_option(*lower);
                upper_bound.append_option(*upper);
                unit_entity_id.append_option(*unit_id);
            }
            _ => unreachable!("value type mismatch"),
        }
    }

    fn finish_to_arrays(&mut self) -> Vec<Arc<dyn arrow::array::Array>> {
        match self {
            ValueBuilders::NoSome => vec![],
            ValueBuilders::String { value } => {
                vec![Arc::new(value.finish())]
            }
            ValueBuilders::EntityId { value_entity_id } => {
                vec![Arc::new(value_entity_id.finish())]
            }
            ValueBuilders::Time {
                time,
                timezone,
                before,
                after,
                precision,
                calendarmodel_entity_id,
            } => {
                vec![
                    Arc::new(time.finish()),
                    Arc::new(timezone.finish()),
                    Arc::new(before.finish()),
                    Arc::new(after.finish()),
                    Arc::new(precision.finish()),
                    Arc::new(calendarmodel_entity_id.finish()),
                ]
            }
            ValueBuilders::Globe {
                latitude,
                longitude,
                precision,
                globe_entity_id,
            } => {
                vec![
                    Arc::new(latitude.finish()),
                    Arc::new(longitude.finish()),
                    Arc::new(precision.finish()),
                    Arc::new(globe_entity_id.finish()),
                ]
            }
            ValueBuilders::Mono { language, text } => {
                vec![Arc::new(language.finish()), Arc::new(text.finish())]
            }
            ValueBuilders::Quantity {
                amount,
                lower_bound,
                upper_bound,
                unit_entity_id,
            } => {
                vec![
                    Arc::new(amount.finish()),
                    Arc::new(lower_bound.finish()),
                    Arc::new(upper_bound.finish()),
                    Arc::new(unit_entity_id.finish()),
                ]
            }
        }
    }
}

// Claim batch builders with shared header and count
struct ClaimBatchBuilders {
    key: ClaimKey,
    schema: SchemaRef,
    batch_size: usize,
    header: ClaimHeadBuilder,
    count: usize,
    values: ValueBuilders,
}

impl ClaimBatchBuilders {
    fn new(key: &ClaimKey, batch_size: usize) -> Self {
        let header = ClaimHeadBuilder::new(key.ctype);
        let values = match key.vkind {
            ValueKind::NoValue | ValueKind::SomeValue => ValueBuilders::NoSome,
            ValueKind::String => ValueBuilders::String {
                value: StringBuilder::new(),
            },
            ValueKind::EntityId => ValueBuilders::EntityId {
                value_entity_id: Int64Builder::new(),
            },
            ValueKind::Time => ValueBuilders::Time {
                time: StringBuilder::new(),
                timezone: Int32Builder::new(),
                before: Int32Builder::new(),
                after: Int32Builder::new(),
                precision: Int32Builder::new(),
                calendarmodel_entity_id: Int64Builder::new(),
            },
            ValueKind::GlobeCoordinate => ValueBuilders::Globe {
                latitude: Float64Builder::new(),
                longitude: Float64Builder::new(),
                precision: Float64Builder::new(),
                globe_entity_id: Int64Builder::new(),
            },
            ValueKind::MonolingualText => ValueBuilders::Mono {
                language: StringBuilder::new(),
                text: StringBuilder::new(),
            },
            ValueKind::Quantity => ValueBuilders::Quantity {
                amount: Float64Builder::new(),
                lower_bound: Float64Builder::new(),
                upper_bound: Float64Builder::new(),
                unit_entity_id: Int64Builder::new(),
            },
        };
        let is_claim = matches!(key.ctype, ClaimType::Claim);
        let schema = schema_for_value(key.vkind, is_claim);
        Self {
            key: key.clone(),
            schema,
            batch_size,
            header,
            count: 0,
            values,
        }
    }

    fn append_header(
        &mut self,
        claim_id: i64,
        entity_id: i64,
        order: i32,
        property_id: i64,
        datatype: Option<&str>,
    ) {
        self.count += 1;
        self.header.append(
            claim_id,
            self.key.rank.as_str(),
            entity_id,
            order,
            property_id,
            datatype,
        );
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn finish_to_batch(&mut self) -> Result<RecordBatch> {
        self.count = 0;
        let mut cols = self.header.finish_to_arrays();
        cols.extend(self.values.finish_to_arrays());
        Ok(RecordBatch::try_new(self.schema.clone(), cols)?)
    }

    fn values_mut(&mut self) -> &mut ValueBuilders {
        &mut self.values
    }

    async fn flush_if_needed_send(&mut self, writers: &ClaimWriters) -> Result<()> {
        if self.count >= self.batch_size {
            let batch = self.finish_to_batch()?;
            writers.send(self.key.clone(), batch).await?;
        }
        Ok(())
    }

    async fn finalize_and_send_all(mut self, writers: &ClaimWriters) -> Result<()> {
        if !self.is_empty() {
            let batch = self.finish_to_batch()?;
            writers.send(self.key, batch).await?;
        }
        Ok(())
    }
}

// Claim writer actor per key (one task per ClaimKey) writing ready-made RecordBatches
async fn run_claim_writer(
    base_dir: PathBuf,
    key: ClaimKey,
    mut rx: Receiver<RecordBatch>,
) -> Result<()> {
    let mut writer = mk_claim_writer(&base_dir, &key)?;
    while let Some(batch) = rx.recv().await {
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(())
}

struct ClaimWritersInner {
    base_dir: PathBuf,
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

fn make_simple_writer(
    tasks: &mut Vec<JoinHandle<Result<()>>>,
    base_dir: &Path,
    name: &str,
    schema: fn() -> SchemaRef,
) -> Sender<RecordBatch> {
    let (tx, rx) = mpsc::channel::<RecordBatch>(READY_BATCHES_BUFFER);
    spawn_simple_writer(tasks, base_dir, name, rx, schema());
    tx
}

#[derive(Clone)]
struct SimpleWriterSenders {
    entities: Sender<RecordBatch>,
    labels: Sender<RecordBatch>,
    aliases: Sender<RecordBatch>,
    descriptions: Sender<RecordBatch>,
    datatypes: Sender<RecordBatch>,
    sitelinks: Sender<RecordBatch>,
    sitelink_badges: Sender<RecordBatch>,
}

macro_rules! define_simple_batch {
    (
        $name:ident {
            new($batch_size:ident $(, $cap_param:ident : $cap_ty:ty)*);
            $($field:ident : $builder_ty:ty = $init:expr, $val_ty:ty;)*
        }
    ) => {
        struct $name {
            tx: Sender<RecordBatch>,
            schema: fn() -> SchemaRef,
            $($field: $builder_ty,)*
            count: usize,
        }

        impl $name {
            fn new($batch_size: usize, $($cap_param: $cap_ty,)* tx: Sender<RecordBatch>, schema: fn() -> SchemaRef) -> Self {
                Self {
                    tx,
                    schema,
                    $($field: $init,)*
                    count: 0,
                }
            }

            fn append(&mut self, $($field: $val_ty),*) {
                $(self.$field.append_value($field);)*
                self.count += 1;
            }

            async fn flush(&mut self, min: usize) -> Result<()> {
                if self.count >= min {
                    let batch = make_batch(
                        (self.schema)(),
                        vec![$(Arc::new(self.$field.finish())),*],
                    )?;
                    self.count = 0;
                    let _ = self.tx.send(batch).await;
                }
                Ok(())
            }
        }
    };
}

define_simple_batch!(IntStrBatch {
    new(batch_size, string_capacity: usize);
    id: Int64Builder = Int64Builder::with_capacity(batch_size), i64;
    value: StringBuilder = StringBuilder::with_capacity(batch_size, string_capacity), &str;
});

define_simple_batch!(IntStrStrBatch {
    new(batch_size, left_capacity: usize, right_capacity: usize);
    id: Int64Builder = Int64Builder::with_capacity(batch_size), i64;
    left: StringBuilder = StringBuilder::with_capacity(batch_size, left_capacity), &str;
    right: StringBuilder = StringBuilder::with_capacity(batch_size, right_capacity), &str;
});

define_simple_batch!(IntStrIntBatch {
    new(batch_size, string_capacity: usize);
    id: Int64Builder = Int64Builder::with_capacity(batch_size), i64;
    text: StringBuilder = StringBuilder::with_capacity(batch_size, string_capacity), &str;
    value: Int64Builder = Int64Builder::with_capacity(batch_size), i64;
});

struct SimpleBatchers {
    batch_size: usize,
    entities: IntStrBatch,
    labels: IntStrStrBatch,
    aliases: IntStrStrBatch,
    descriptions: IntStrStrBatch,
    datatypes: IntStrBatch,
    sitelinks: IntStrStrBatch,
    sitelink_badges: IntStrIntBatch,
}

impl SimpleBatchers {
    fn new(batch_size: usize, senders: SimpleWriterSenders) -> Self {
        Self {
            batch_size,
            entities: IntStrBatch::new(
                batch_size,
                batch_size * 8,
                senders.entities,
                schema_entities,
            ),
            labels: IntStrStrBatch::new(
                batch_size,
                batch_size * 6,
                batch_size * 12,
                senders.labels,
                schema_labels,
            ),
            aliases: IntStrStrBatch::new(
                batch_size,
                batch_size * 6,
                batch_size * 12,
                senders.aliases,
                schema_aliases,
            ),
            descriptions: IntStrStrBatch::new(
                batch_size,
                batch_size * 6,
                batch_size * 12,
                senders.descriptions,
                schema_descriptions,
            ),
            datatypes: IntStrBatch::new(
                batch_size,
                batch_size * 6,
                senders.datatypes,
                schema_datatypes,
            ),
            sitelinks: IntStrStrBatch::new(
                batch_size,
                batch_size * 6,
                batch_size * 12,
                senders.sitelinks,
                schema_sitelinks,
            ),
            sitelink_badges: IntStrIntBatch::new(
                batch_size,
                batch_size * 6,
                senders.sitelink_badges,
                schema_sitelink_badges,
            ),
        }
    }

    async fn flush_async(&mut self, force: bool) -> Result<()> {
        let min = if force { 1 } else { self.batch_size };
        self.entities.flush(min).await?;
        self.labels.flush(min).await?;
        self.aliases.flush(min).await?;
        self.descriptions.flush(min).await?;
        self.datatypes.flush(min).await?;
        self.sitelinks.flush(min).await?;
        self.sitelink_badges.flush(min).await?;
        Ok(())
    }
}

impl ClaimWriters {
    fn new(base_dir: PathBuf) -> Self {
        Self(Arc::new(Mutex::new(ClaimWritersInner {
            base_dir,
            senders: HashMap::new(),
            handles: Vec::new(),
        })))
    }

    async fn send(&self, key: ClaimKey, batch: RecordBatch) -> Result<()> {
        let mut inner = self.0.lock().await;
        let tx = if let Some(tx) = inner.senders.get(&key) {
            tx.clone()
        } else {
            let (tx_new, rx_new) = mpsc::channel::<RecordBatch>(READY_BATCHES_BUFFER);
            let base = inner.base_dir.clone();
            let h = task::spawn(run_claim_writer(base, key.clone(), rx_new));
            inner.handles.push(h);
            inner.senders.insert(key, tx_new.clone());
            tx_new
        };
        drop(inner);
        let _ = tx.send(batch).await;
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

    let base_simple_dir = args.output.join("tmp");
    let mut writer_tasks: Vec<JoinHandle<Result<()>>> = Vec::new();
    let simple_senders = SimpleWriterSenders {
        entities: make_simple_writer(
            &mut writer_tasks,
            &base_simple_dir,
            "entities",
            schema_entities,
        ),
        labels: make_simple_writer(&mut writer_tasks, &base_simple_dir, "labels", schema_labels),
        aliases: make_simple_writer(
            &mut writer_tasks,
            &base_simple_dir,
            "aliases",
            schema_aliases,
        ),
        descriptions: make_simple_writer(
            &mut writer_tasks,
            &base_simple_dir,
            "descriptions",
            schema_descriptions,
        ),
        datatypes: make_simple_writer(
            &mut writer_tasks,
            &base_simple_dir,
            "datatypes",
            schema_datatypes,
        ),
        sitelinks: make_simple_writer(
            &mut writer_tasks,
            &base_simple_dir,
            "sitelinks",
            schema_sitelinks,
        ),
        sitelink_badges: make_simple_writer(
            &mut writer_tasks,
            &base_simple_dir,
            "sitelink_badges",
            schema_sitelink_badges,
        ),
    };

    // Shared interner and claim writers
    let interner = Arc::new(Interner::new());
    let claim_writers = ClaimWriters::new(args.output.join("tmp"));
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
        let senders_c = simple_senders.clone();
        let interner_c = interner.clone();
        let claim_writers_c = claim_writers.clone();
        let claim_ids_c = claim_ids.clone();
        let batch_size = args.batch_size;
        let h = task::spawn(async move {
            let mut worker = WorkerState::new(
                batch_size,
                senders_c,
                interner_c,
                claim_writers_c,
                claim_ids_c,
            );
            while let Some(line) = rx.recv().await {
                worker.process_entity(&line).await?;
            }
            worker.finish().await?;
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
    drop(simple_senders);
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

    fn run_duckdb_copy(scan: &str, out_dir: &Path, max_file_size: u64) -> Result<()> {
        use duckdb::Connection;
        fs::create_dir_all(out_dir).ok();
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "SET enable_progress_bar_print=TRUE; SET progress_bar_time=0; SET threads=1;",
        )?;
        let sql = format!(
            "COPY (SELECT * FROM parquet_scan('{}')) TO '{}' (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 22, STRING_DICTIONARY_PAGE_SIZE_LIMIT 100_000, FILE_SIZE_BYTES {})",
            scan,
            out_dir.display(),
            max_file_size
        );
        conn.execute_batch(&sql)?;
        Ok(())
    }

    fn move_parts(dir: &Path, mut dest_for_index: impl FnMut(i64) -> PathBuf) -> Result<()> {
        for (index, file) in collect_duckdb_parts(dir)? {
            fs::rename(&file, dest_for_index(index))?;
        }
        Ok(())
    }

    fn is_claim_dir(name: &str) -> bool {
        name.starts_with("claim_")
            || name.starts_with("qualifier_")
            || name.starts_with("reference_")
    }

    // Simple dataset files -> one blocking task each
    for name in SIMPLE_DATASETS {
        let in_path = out_root.join(format!("tmp/{}.parquet", name));
        if !in_path.exists() {
            continue;
        }
        let mid_dir = out_root.join(format!("tmp2/{}", name));
        let out_single = out_root.join(format!("{}.parquet", name));
        let h = task::spawn_blocking(move || -> Result<()> {
            run_duckdb_copy(&in_path.display().to_string(), &mid_dir, max_file_size)?;
            let _ = fs::remove_file(&in_path);
            move_parts(&mid_dir, |index| {
                let suffix = if index == 0 {
                    String::new()
                } else {
                    format!("_{}", index)
                };
                out_single
                    .with_file_name(format!(
                        "{}{}",
                        out_single.file_stem().unwrap().to_string_lossy(),
                        suffix
                    ))
                    .with_extension("parquet")
            })?;
            let _ = fs::remove_dir_all(&mid_dir);
            Ok(())
        });
        handles.push(h);
    }

    // claim/qual/reference directories
    let claim_dirs: Vec<String> = if tmp_root.exists() {
        fs::read_dir(&tmp_root)?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| is_claim_dir(name))
            .collect()
    } else {
        Vec::new()
    };
    for dname in claim_dirs {
        let tmp_root_c = tmp_root.clone();
        let out_root_c = out_root.to_path_buf();
        let dname_c = dname.clone();
        let h = task::spawn_blocking(move || -> Result<()> {
            let glob = format!("{}/{}/*/*/*.parquet", tmp_root_c.display(), dname_c);
            let mid_dir = out_root_c.join(format!("tmp2/{}", dname_c));
            run_duckdb_copy(&glob, &mid_dir, max_file_size)?;
            let _ = fs::remove_dir_all(out_root_c.join(format!("tmp/{}", dname_c)));
            move_parts(&mid_dir, |index| {
                if index == 0 {
                    out_root_c.join(format!("{}.parquet", dname_c))
                } else {
                    out_root_c.join(format!("{}_{}.parquet", dname_c, index))
                }
            })?;
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
