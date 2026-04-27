//! In-memory `fetch` / `fetchOne`: scan → order → limit, plus basic `.include()` via intron refs.

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use crate::encoding::{decode_codons_to_bytes, EncodedPayload};
use crate::model::Intron;
use crate::model::Strand;

use super::ast::SortDirection;
use super::guide::{Clause, GuidePattern, RangeOp};
use super::index::{build_exact_hash_index, exact_lookup_signatures, GlobalRangeIndex};
use super::planner::{choose_path, CollectionStats, QueryPath};
use super::scan::{scan_strands, ScanConfig};
use super::workload_stats;

/// Ordered, limited root strands matching `pattern` (no includes expanded).
#[derive(Debug)]
pub struct FetchResult<'a> {
    pub rows: Vec<&'a Strand>,
}

/// One root strand plus related strands for each requested include path.
#[derive(Debug)]
pub struct FetchRow<'a> {
    pub root: &'a Strand,
    /// Pairs of `(include_path, related_strands)` in declaration order.
    pub included: Vec<(String, Vec<&'a Strand>)>,
}

#[derive(Debug)]
pub struct FetchWithIncludes<'a> {
    pub rows: Vec<FetchRow<'a>>,
}

/// Minimal strand header plus projected field payloads.
#[derive(Debug, Clone)]
pub struct ProjectedRow {
    pub signature: [u8; 8],
    pub version: u64,
    pub created_at: u64,
    pub updated_at: u64,
    pub fields: HashMap<String, Vec<u8>>,
}

#[derive(Debug)]
struct QueryPlanCache {
    capacity: usize,
    order: VecDeque<u64>,
    plans: HashMap<u64, QueryPath>,
}

impl QueryPlanCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            order: VecDeque::new(),
            plans: HashMap::new(),
        }
    }

    fn get(&mut self, key: u64) -> Option<QueryPath> {
        let plan = self.plans.get(&key).cloned()?;
        if let Some(pos) = self.order.iter().position(|k| *k == key) {
            self.order.remove(pos);
        }
        self.order.push_back(key);
        Some(plan)
    }

    fn insert(&mut self, key: u64, plan: QueryPath) {
        if self.plans.contains_key(&key) {
            self.plans.insert(key, plan);
            if let Some(pos) = self.order.iter().position(|k| *k == key) {
                self.order.remove(pos);
            }
            self.order.push_back(key);
            return;
        }
        if self.plans.len() >= self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.plans.remove(&oldest);
            }
        }
        self.plans.insert(key, plan);
        self.order.push_back(key);
    }

    #[cfg(test)]
    fn clear(&mut self) {
        self.order.clear();
        self.plans.clear();
    }
}

const QUERY_PLAN_CACHE_CAPACITY: usize = 1024;
static QUERY_PLAN_CACHE: OnceLock<Mutex<QueryPlanCache>> = OnceLock::new();
static QUERY_PLAN_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static QUERY_PLAN_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);

fn query_plan_cache() -> &'static Mutex<QueryPlanCache> {
    QUERY_PLAN_CACHE.get_or_init(|| Mutex::new(QueryPlanCache::new(QUERY_PLAN_CACHE_CAPACITY)))
}

fn hash_u64<T: Hash>(value: &T) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn record_count_bucket(record_count: usize) -> usize {
    if record_count == 0 {
        0
    } else {
        record_count.ilog2() as usize
    }
}

fn planner_cache_key(pattern: &GuidePattern, stats: &CollectionStats) -> u64 {
    let mut clauses: Vec<String> = pattern
        .clauses
        .iter()
        .map(|c| match c {
            Clause::ExactMatch { field_intron, .. } => format!("exact:{field_intron}"),
            Clause::RangeMatch {
                field_intron,
                operator,
                ..
            } => format!("range:{field_intron}:{operator:?}"),
            Clause::LikePattern { field_intron, .. } => format!("like:{field_intron}"),
        })
        .collect();
    clauses.sort();

    let mut indexed_fields: Vec<String> = stats.indexed_fields.iter().cloned().collect();
    indexed_fields.sort();
    let mut direct_fields: Vec<String> = stats.direct_lookup_fields.iter().cloned().collect();
    direct_fields.sort();

    let order_shape = pattern
        .order_by
        .as_ref()
        .map(|(f, d)| format!("{f}:{d:?}"))
        .unwrap_or_default();
    let includes = {
        let mut v = pattern.includes.clone();
        v.sort();
        v
    };
    // Coarse buckets limit stale-plan risk when collection distribution shifts.
    let bloom_bucket = (stats.estimated_bloom_skip_rate.clamp(0.0, 1.0) * 10.0).round() as u8;
    let rec_bucket = record_count_bucket(stats.record_count);
    hash_u64(&(
        pattern.collection.as_str(),
        clauses,
        includes,
        order_shape,
        pattern.limit,
        indexed_fields,
        direct_fields,
        bloom_bucket,
        rec_bucket,
    ))
}

fn choose_path_cached(pattern: &GuidePattern, stats: &CollectionStats) -> QueryPath {
    let key = planner_cache_key(pattern, stats);
    if let Ok(mut cache) = query_plan_cache().lock() {
        if let Some(plan) = cache.get(key) {
            QUERY_PLAN_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
            return plan;
        }
    }
    QUERY_PLAN_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
    let plan = choose_path(pattern, stats);
    if let Ok(mut cache) = query_plan_cache().lock() {
        cache.insert(key, plan.clone());
    }
    plan
}

pub fn planner_cache_stats() -> (u64, u64) {
    (
        QUERY_PLAN_CACHE_HITS.load(Ordering::Relaxed),
        QUERY_PLAN_CACHE_MISSES.load(Ordering::Relaxed),
    )
}

#[cfg(test)]
fn clear_planner_cache_for_tests() {
    QUERY_PLAN_CACHE_HITS.store(0, Ordering::Relaxed);
    QUERY_PLAN_CACHE_MISSES.store(0, Ordering::Relaxed);
    if let Ok(mut cache) = query_plan_cache().lock() {
        cache.clear();
    }
}

fn strand_by_signature<'a>(strands: &'a [Strand]) -> HashMap<[u8; 8], &'a Strand> {
    strands.iter().map(|s| (s.signature, s)).collect()
}

fn hits_to_strands<'a>(
    index: &HashMap<[u8; 8], &'a Strand>,
    sigs: &[[u8; 8]],
) -> Vec<&'a Strand> {
    sigs.iter()
        .filter_map(|sig| index.get(sig).copied())
        .collect()
}

fn intron_payload_bytes(strand: &Strand, intron: &Intron) -> Option<Vec<u8>> {
    let start = intron.codon_offset as usize;
    let len = intron.codon_length as usize;
    let end = start.checked_add(len)?;
    if end > strand.codons.len() {
        return None;
    }
    // Encode path emits 4 symbols per byte and packs 3 symbols per codon.
    // Given codon count c, original length is uniquely floor(3c/4) for non-empty payloads.
    let original_len = (len * 3) / 4;
    let payload = EncodedPayload {
        codons: strand.codons[start..end].to_vec(),
        original_len,
    };
    decode_codons_to_bytes(&payload).ok()
}

fn like_matches(value: &str, pattern: &str) -> bool {
    let v: Vec<char> = value.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let mut dp = vec![vec![false; v.len() + 1]; p.len() + 1];
    dp[0][0] = true;
    for i in 1..=p.len() {
        if p[i - 1] == '%' {
            dp[i][0] = dp[i - 1][0];
        }
    }
    for i in 1..=p.len() {
        for j in 1..=v.len() {
            dp[i][j] = match p[i - 1] {
                '%' => dp[i - 1][j] || dp[i][j - 1],
                '_' => dp[i - 1][j - 1],
                c => dp[i - 1][j - 1] && c == v[j - 1],
            };
        }
    }
    dp[p.len()][v.len()]
}

fn required_clause_fields(pattern: &GuidePattern) -> HashSet<String> {
    pattern
        .clauses
        .iter()
        .filter_map(|c| match c {
            Clause::ExactMatch { field_intron, .. } => Some(field_intron.clone()),
            Clause::RangeMatch { field_intron, .. } => Some(field_intron.clone()),
            Clause::LikePattern { field_intron, .. } => Some(field_intron.clone()),
        })
        .filter(|f| f != "_signature")
        .collect()
}

fn decode_required_field_values(
    strand: &Strand,
    required_fields: &HashSet<String>,
) -> HashMap<String, Vec<Vec<u8>>> {
    if required_fields.is_empty() {
        return HashMap::new();
    }
    let mut out: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
    for intron in &strand.introns {
        if !required_fields.contains(&intron.field_name) {
            continue;
        }
        if let Some(v) = intron_payload_bytes(strand, intron) {
            out.entry(intron.field_name.clone()).or_default().push(v);
        }
    }
    out
}

/// Project only selected fields from already-matched strands.
///
/// Field values are decoded from intron codon slices on demand; non-requested fields
/// are never materialized into the output row.
pub fn project_rows(rows: &[&Strand], required_fields: &HashSet<String>) -> Vec<ProjectedRow> {
    rows.iter()
        .map(|strand| {
            let mut fields = HashMap::new();
            if !required_fields.is_empty() {
                // Keep first intron value for each requested field to avoid duplicate work.
                for intron in &strand.introns {
                    if !required_fields.contains(&intron.field_name)
                        || fields.contains_key(&intron.field_name)
                    {
                        continue;
                    }
                    if let Some(v) = intron_payload_bytes(strand, intron) {
                        fields.insert(intron.field_name.clone(), v);
                    }
                }
            }
            ProjectedRow {
                signature: strand.signature,
                version: strand.version,
                created_at: strand.created_at,
                updated_at: strand.updated_at,
                fields,
            }
        })
        .collect()
}

fn clause_matches_strand(
    strand: &Strand,
    clause: &Clause,
    decoded_fields: Option<&HashMap<String, Vec<Vec<u8>>>>,
) -> bool {
    match clause {
        Clause::ExactMatch {
            field_intron,
            operand_wire,
            operand_codons,
        } => {
            if field_intron == "_signature" {
                return strand.signature.as_slice() == operand_wire.as_slice();
            }
            if let Some(decoded) = decoded_fields {
                return decoded
                    .get(field_intron)
                    .is_some_and(|vals| vals.iter().any(|v| v == operand_wire));
            }
            strand
                .introns
                .iter()
                .filter(|i| i.field_name == *field_intron)
                .any(|i| {
                    if field_intron == "_payload" {
                        intron_payload_bytes(strand, i).is_some_and(|v| v == *operand_wire)
                    } else {
                        let start = i.codon_offset as usize;
                        let end = start.saturating_add(i.codon_length as usize);
                        end <= strand.codons.len() && strand.codons[start..end] == operand_codons.codons
                    }
                })
        }
        Clause::RangeMatch {
            field_intron,
            operator,
            operand_wire,
            ..
        } => {
            let eval = |value: &[u8]| match operator {
                RangeOp::GreaterThan => value > operand_wire.as_slice(),
                RangeOp::GreaterOrEqual => value >= operand_wire.as_slice(),
                RangeOp::LessThan => value < operand_wire.as_slice(),
                RangeOp::LessOrEqual => value <= operand_wire.as_slice(),
                RangeOp::NotEqual => value != operand_wire.as_slice(),
            };
            if let Some(decoded) = decoded_fields {
                return decoded
                    .get(field_intron)
                    .is_some_and(|vals| vals.iter().any(|v| eval(v)));
            }
            strand
                .introns
                .iter()
                .filter(|i| i.field_name == *field_intron)
                .any(|i| {
                    let Some(value) = intron_payload_bytes(strand, i) else {
                        return false;
                    };
                    eval(&value)
                })
        }
        Clause::LikePattern {
            field_intron,
            pattern,
        } => {
            let eval = |value: &[u8]| {
                let s = String::from_utf8_lossy(value);
                like_matches(&s, pattern)
            };
            if let Some(decoded) = decoded_fields {
                return decoded
                    .get(field_intron)
                    .is_some_and(|vals| vals.iter().any(|v| eval(v)));
            }
            strand
                .introns
                .iter()
                .filter(|i| i.field_name == *field_intron)
                .any(|i| intron_payload_bytes(strand, i).is_some_and(|v| eval(&v)))
        }
    }
}

fn verify_clauses<'a>(rows: Vec<&'a Strand>, pattern: &GuidePattern) -> Vec<&'a Strand> {
    let required_fields = required_clause_fields(pattern);
    rows.into_iter()
        .filter(|strand| {
            if required_fields.is_empty() {
                return pattern
                    .clauses
                    .iter()
                    .all(|clause| clause_matches_strand(strand, clause, None));
            }
            let decoded = decode_required_field_values(strand, &required_fields);
            pattern
                .clauses
                .iter()
                .all(|clause| clause_matches_strand(strand, clause, Some(&decoded)))
        })
        .collect()
}

fn sort_key(s: &Strand, field: &str) -> u64 {
    match field {
        "version" => s.version,
        "created_at" => s.created_at,
        "updated_at" => s.updated_at,
        _ => u64::from_le_bytes(s.signature),
    }
}

fn apply_order_by(rows: &mut Vec<&Strand>, pattern: &GuidePattern) {
    let Some((field, dir)) = &pattern.order_by else {
        return;
    };
    rows.sort_by(|a, b| {
        let ord = sort_key(a, field).cmp(&sort_key(b, field));
        match dir {
            SortDirection::Asc => ord,
            SortDirection::Desc => ord.reverse(),
        }
    });
}

fn apply_limit(rows: &mut Vec<&Strand>, pattern: &GuidePattern) {
    let Some(lim) = pattern.limit else {
        return;
    };
    let n = lim as usize;
    if rows.len() > n {
        rows.truncate(n);
    }
}

fn matches_all_clauses(strand: &Strand, pattern: &GuidePattern) -> bool {
    let required_fields = required_clause_fields(pattern);
    let decoded = if required_fields.is_empty() {
        None
    } else {
        Some(decode_required_field_values(strand, &required_fields))
    };
    pattern
        .clauses
        .iter()
        .all(|clause| clause_matches_strand(strand, clause, decoded.as_ref()))
}

fn direct_signature_lookup<'a>(
    pattern: &GuidePattern,
    index: &HashMap<[u8; 8], &'a Strand>,
) -> Option<Vec<&'a Strand>> {
    let Clause::ExactMatch {
        field_intron,
        operand_wire,
        ..
    } = pattern.clauses.first()?
    else {
        return None;
    };
    if field_intron != "_signature" || operand_wire.len() != 8 {
        return None;
    }
    let mut sig = [0u8; 8];
    sig.copy_from_slice(operand_wire);
    Some(index.get(&sig).copied().into_iter().collect())
}

fn parse_signature_operand(pattern: &GuidePattern) -> Option<[u8; 8]> {
    let Clause::ExactMatch {
        field_intron,
        operand_wire,
        ..
    } = pattern.clauses.first()?
    else {
        return None;
    };
    if field_intron != "_signature" || operand_wire.len() != 8 {
        return None;
    }
    let mut sig = [0u8; 8];
    sig.copy_from_slice(operand_wire);
    Some(sig)
}

/// Dead-reckoning optimization for direct signature lookups:
/// estimate record position from signature sequence and probe nearby rows before fallback.
fn dead_reckoning_signature_lookup<'a>(
    pattern: &GuidePattern,
    strands: &'a [Strand],
) -> Option<Vec<&'a Strand>> {
    let target_sig = parse_signature_operand(pattern)?;
    if strands.is_empty() {
        return Some(Vec::new());
    }
    let target_seq = u64::from_le_bytes(target_sig);
    let first_seq = u64::from_le_bytes(strands.first()?.signature);
    let last_seq = u64::from_le_bytes(strands.last()?.signature);
    // Only use estimation when sequence range is monotonic and target is within bounds.
    if first_seq > last_seq || target_seq < first_seq || target_seq > last_seq {
        return None;
    }
    let mut estimate = (target_seq - first_seq) as usize;
    if estimate >= strands.len() {
        estimate = strands.len().saturating_sub(1);
    }
    // Probe a local window first; if this misses, caller falls back to map lookup.
    let window = 64usize;
    let start = estimate.saturating_sub(window);
    let end = (estimate + window + 1).min(strands.len());
    for strand in strands.iter().take(end).skip(start) {
        if strand.signature == target_sig {
            return Some(vec![strand]);
        }
    }
    None
}

fn build_collection_stats(strands: &[Strand], config: &ScanConfig, pattern: &GuidePattern) -> CollectionStats {
    let mut stats = CollectionStats::fake();
    stats.record_count = strands.len();
    if let Some(indexed) = &config.indexed_fields {
        for f in indexed {
            stats.add_index(f.clone());
            let distinct = config
                .global_hash_indexes
                .as_ref()
                .and_then(|m| m.get(f))
                .map(|idx| idx.map.len())
                .unwrap_or_else(|| build_exact_hash_index(strands, f).len())
                .max(1);
            stats
                .index_selectivity
                .insert(f.clone(), 1.0 / distinct as f64);
        }
    }
    if let (Some(metas), Some(_ranges)) = (&config.segment_metas, &config.segment_ranges) {
        if !metas.is_empty() {
            let skipped = metas
                .iter()
                .filter(|m| super::segment::should_skip_segment(m, pattern))
                .count();
            stats.estimated_bloom_skip_rate = skipped as f64 / metas.len() as f64;
        }
    }
    stats
}

fn indexed_exact_lookup<'a>(
    pattern: &GuidePattern,
    strands: &'a [Strand],
    field: &str,
    config: &ScanConfig,
    index: &HashMap<[u8; 8], &'a Strand>,
) -> Option<Vec<&'a Strand>> {
    let Clause::ExactMatch {
        field_intron,
        operand_wire,
        ..
    } = pattern.clauses.iter().find(|c| matches!(c, Clause::ExactMatch { .. }))?
    else {
        return None;
    };
    if field_intron != field {
        return None;
    }
    let sigs = if let Some(global) = config
        .global_hash_indexes
        .as_ref()
        .and_then(|m| m.get(field))
    {
        global.lookup(operand_wire)
    } else {
        let hash_idx = build_exact_hash_index(strands, field);
        exact_lookup_signatures(&hash_idx, operand_wire)
    };
    Some(
        sigs.iter()
            .filter_map(|sig| index.get(sig).copied())
            .collect(),
    )
}

fn parse_range_operand_u64(wire: &[u8]) -> Option<u64> {
    if let Ok(lit) = bincode::deserialize::<super::ast::QueryLiteral>(wire) {
        return match lit {
            super::ast::QueryLiteral::U64(v) => Some(v),
            super::ast::QueryLiteral::I64(v) if v >= 0 => Some(v as u64),
            _ => None,
        };
    }
    if let Ok(s) = std::str::from_utf8(wire) {
        return s.parse::<u64>().ok();
    }
    None
}

fn indexed_range_lookup<'a>(
    pattern: &GuidePattern,
    strands: &'a [Strand],
    field: &str,
    config: &ScanConfig,
    index: &HashMap<[u8; 8], &'a Strand>,
) -> Option<Vec<&'a Strand>> {
    if parse_indexed_between_bounds(pattern, field).is_some() {
        return None;
    }
    let Clause::RangeMatch {
        field_intron,
        operator,
        operand_wire,
        ..
    } = pattern.clauses.iter().find(|c| matches!(c, Clause::RangeMatch { .. }))?
    else {
        return None;
    };
    if field_intron != field {
        return None;
    }
    let needle = parse_range_operand_u64(operand_wire)?;
    let sigs = if let Some(global) = config
        .global_range_indexes
        .as_ref()
        .and_then(|m| m.get(field))
    {
        global.lookup(*operator, needle)
    } else {
        let idx = GlobalRangeIndex::build(field.to_string(), strands);
        idx.lookup(*operator, needle)
    };
    Some(
        sigs.iter()
            .filter_map(|sig| index.get(sig).copied())
            .collect(),
    )
}

/// Two range clauses on the same indexed field: lower bound (GT/GTE) + upper bound (LT/LTE).
fn parse_indexed_between_bounds(pattern: &GuidePattern, field: &str) -> Option<(u64, u64)> {
    if pattern.clauses.len() != 2 {
        return None;
    }
    let mut lower = None::<(bool, u64)>;
    let mut upper = None::<(bool, u64)>;
    for c in &pattern.clauses {
        let Clause::RangeMatch {
            field_intron,
            operator,
            operand_wire,
            ..
        } = c
        else {
            return None;
        };
        if field_intron != field {
            return None;
        }
        let n = parse_range_operand_u64(operand_wire)?;
        match operator {
            RangeOp::GreaterThan => lower = Some((true, n)),
            RangeOp::GreaterOrEqual => lower = Some((false, n)),
            RangeOp::LessThan => upper = Some((true, n)),
            RangeOp::LessOrEqual => upper = Some((false, n)),
            RangeOp::NotEqual => return None,
        }
    }
    let (lo_strict, lo) = lower?;
    let (hi_strict, hi) = upper?;
    let lo_inc = if lo_strict { lo.saturating_add(1) } else { lo };
    let hi_inc = if hi_strict { hi.saturating_sub(1) } else { hi };
    if lo_inc <= hi_inc {
        Some((lo_inc, hi_inc))
    } else {
        None
    }
}

fn indexed_between_lookup<'a>(
    pattern: &GuidePattern,
    strands: &'a [Strand],
    field: &str,
    config: &ScanConfig,
    index: &HashMap<[u8; 8], &'a Strand>,
) -> Option<Vec<&'a Strand>> {
    let (lo, hi) = parse_indexed_between_bounds(pattern, field)?;
    let sigs = if let Some(global) = config
        .global_range_indexes
        .as_ref()
        .and_then(|m| m.get(field))
    {
        global.lookup_between_inclusive(lo, hi)
    } else {
        GlobalRangeIndex::build(field.to_string(), strands).lookup_between_inclusive(lo, hi)
    };
    Some(
        sigs.iter()
            .filter_map(|sig| index.get(sig).copied())
            .collect(),
    )
}

/// Full `fetch`: CRISPR scan, then `orderBy` / `limit` from [`GuidePattern`].
pub fn fetch<'a>(
    pattern: &GuidePattern,
    strands: &'a [Strand],
    config: &ScanConfig,
) -> FetchResult<'a> {
    workload_stats::record_fetch();
    let index = strand_by_signature(strands);
    let stats = build_collection_stats(strands, config, pattern);
    let path = choose_path_cached(pattern, &stats);
    let base_rows = match path {
        QueryPath::Direct => dead_reckoning_signature_lookup(pattern, strands)
            .or_else(|| direct_signature_lookup(pattern, &index))
            .unwrap_or_else(|| hits_to_strands(&index, &scan_strands(pattern, strands, config))),
        QueryPath::Index { field } => {
            let rows = indexed_exact_lookup(pattern, strands, &field, config, &index)
                .or_else(|| indexed_between_lookup(pattern, strands, &field, config, &index))
                .or_else(|| indexed_range_lookup(pattern, strands, &field, config, &index));
            rows.unwrap_or_else(|| hits_to_strands(&index, &scan_strands(pattern, strands, config)))
        }
        QueryPath::GuidedScan => {
            let sigs = scan_strands(pattern, strands, config);
            hits_to_strands(&index, &sigs)
        }
    };
    let mut rows = verify_clauses(base_rows, pattern);
    apply_order_by(&mut rows, pattern);
    apply_limit(&mut rows, pattern);
    FetchResult { rows }
}

/// Streaming-oriented fetch path.
///
/// - If `order_by` is set, this falls back to [`fetch`] to preserve global ordering semantics.
/// - Otherwise, rows are emitted as they are verified, and the callback may stop early by
///   returning `false`.
///
/// Returns the number of emitted rows.
pub fn fetch_stream<'a, F>(
    pattern: &GuidePattern,
    strands: &'a [Strand],
    config: &ScanConfig,
    mut on_row: F,
) -> usize
where
    F: FnMut(&'a Strand) -> bool,
{
    if pattern.order_by.is_some() {
        let rows = fetch(pattern, strands, config).rows;
        let mut emitted = 0usize;
        for row in rows {
            if !on_row(row) {
                break;
            }
            emitted += 1;
        }
        return emitted;
    }

    workload_stats::record_fetch_stream();
    let index = strand_by_signature(strands);
    let stats = build_collection_stats(strands, config, pattern);
    let path = choose_path_cached(pattern, &stats);
    let base_rows = match path {
        QueryPath::Direct => direct_signature_lookup(pattern, &index)
            .unwrap_or_else(|| hits_to_strands(&index, &scan_strands(pattern, strands, config))),
        QueryPath::Index { field } => {
            let rows = indexed_exact_lookup(pattern, strands, &field, config, &index)
                .or_else(|| indexed_between_lookup(pattern, strands, &field, config, &index))
                .or_else(|| indexed_range_lookup(pattern, strands, &field, config, &index));
            rows.unwrap_or_else(|| hits_to_strands(&index, &scan_strands(pattern, strands, config)))
        }
        QueryPath::GuidedScan => {
            let sigs = scan_strands(pattern, strands, config);
            hits_to_strands(&index, &sigs)
        }
    };

    let mut emitted = 0usize;
    let limit = pattern.limit.map(|n| n as usize);
    for row in base_rows {
        if !matches_all_clauses(row, pattern) {
            continue;
        }
        if let Some(max_rows) = limit {
            if emitted >= max_rows {
                break;
            }
        }
        if !on_row(row) {
            break;
        }
        emitted += 1;
    }
    emitted
}

/// `fetchOne`: same pipeline as [`fetch`], then return the first row (if any).
pub fn fetch_one<'a>(
    pattern: &GuidePattern,
    strands: &'a [Strand],
    config: &ScanConfig,
) -> Option<&'a Strand> {
    fetch(pattern, strands, config).rows.into_iter().next()
}

/// Follow `pattern.includes` paths using [`Intron::references_strand`](crate::model::Intron::references_strand)
/// (dot-separated multi-hop, e.g. `orders.products`).
pub fn resolve_include_path<'a>(
    root: &'a Strand,
    path: &str,
    index: &HashMap<[u8; 8], &'a Strand>,
) -> Vec<&'a Strand> {
    let segments: Vec<&str> = path.split('.').filter(|s| !s.is_empty()).collect();
    if segments.is_empty() {
        return Vec::new();
    }

    let mut frontier = vec![root];
    for seg in segments {
        let mut next = Vec::new();
        for node in frontier {
            for intron in &node.introns {
                if intron.field_name == seg {
                    if let Some(sig) = intron.references_strand {
                        if let Some(t) = index.get(&sig) {
                            next.push(*t);
                        }
                    }
                }
            }
        }
        frontier = next;
        if frontier.is_empty() {
            return Vec::new();
        }
    }
    frontier
}

/// [`fetch`] plus include expansion for every root row.
pub fn fetch_with_includes<'a>(
    pattern: &GuidePattern,
    strands: &'a [Strand],
    config: &ScanConfig,
) -> FetchWithIncludes<'a> {
    let base = fetch(pattern, strands, config);
    let index = strand_by_signature(strands);
    let rows = base
        .rows
        .into_iter()
        .map(|root| {
            let included = pattern
                .includes
                .iter()
                .map(|path| {
                    let related = resolve_include_path(root, path, &index);
                    (path.clone(), related)
                })
                .collect();
            FetchRow { root, included }
        })
        .collect();
    FetchWithIncludes { rows }
}

#[cfg(test)]
mod tests {
    use super::{
        clear_planner_cache_for_tests, dead_reckoning_signature_lookup, fetch, fetch_one,
        fetch_stream, fetch_with_includes, planner_cache_stats, resolve_include_path,
    };
    use crate::encoding::encode_bytes_to_codons;
    use crate::model::{Intron, RefreshPolicy, Strand, Telomere};
    use crate::processor::{fnv1a64, strand_from_wal_payload};
    use crate::query::ast::SortDirection;
    use crate::query::guide::{Clause, GuidePattern, RangeOp};
    use crate::query::planner::{choose_path, CollectionStats, QueryPath};
    use crate::query::ScanConfig;

    fn strand_with_payload(collection_id: u32, seq: u64, payload: &[u8], created: u64) -> Strand {
        let mut s = strand_from_wal_payload(collection_id, seq, payload);
        s.created_at = created;
        s
    }

    fn pattern_exact_payload(wire: &[u8]) -> GuidePattern {
        GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::ExactMatch {
                field_intron: "_payload".into(),
                operand_wire: wire.to_vec(),
                operand_codons: encode_bytes_to_codons(wire),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        }
    }

    #[test]
    fn order_by_created_at_and_limit() {
        let strands = vec![
            strand_with_payload(1, 1, b"a", 100),
            strand_with_payload(1, 2, b"b", 300),
            strand_with_payload(1, 3, b"a", 200),
        ];
        let mut pat = pattern_exact_payload(b"a");
        pat.order_by = Some(("created_at".into(), SortDirection::Desc));
        pat.limit = Some(1);

        let out = fetch(&pat, &strands, &ScanConfig::default());
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].version, 3);

        let one = fetch_one(&pat, &strands, &ScanConfig::default()).expect("one");
        assert_eq!(one.version, 3);
    }

    #[test]
    fn include_multi_hop() {
        let order_sig = 10u64.to_le_bytes();
        let product_sig = 20u64.to_le_bytes();

        let product = Strand {
            signature: product_sig,
            collection_id: 1,
            codons: vec![],
            complement: vec![],
            introns: vec![],
            telomere: Telomere {
                count: 0,
                immortal: true,
                last_refresh: 0,
                refresh_policy: RefreshPolicy::Immortal,
            },
            epigenetic_tags: vec![],
            version: 20,
            created_at: 0,
            updated_at: 0,
        };

        let order = Strand {
            signature: order_sig,
            collection_id: 1,
            codons: vec![],
            complement: vec![],
            introns: vec![Intron {
                field_name: "products".into(),
                codon_offset: 0,
                codon_length: 0,
                value_hash: 0,
                references_strand: Some(product_sig),
            }],
            telomere: Telomere {
                count: 0,
                immortal: true,
                last_refresh: 0,
                refresh_policy: RefreshPolicy::Immortal,
            },
            epigenetic_tags: vec![],
            version: 10,
            created_at: 0,
            updated_at: 0,
        };

        let user_sig = 1u64.to_le_bytes();
        let wire = b"user1";
        let mut user = strand_from_wal_payload(1, 1, wire);
        user.signature = user_sig;
        user.introns.push(Intron {
            field_name: "orders".into(),
            codon_offset: 0,
            codon_length: 0,
            value_hash: fnv1a64(wire),
            references_strand: Some(order_sig),
        });

        let strands = vec![user, order, product];
        let index = super::strand_by_signature(&strands);

        let chain = resolve_include_path(&strands[0], "orders.products", &index);
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].signature, product_sig);

        let mut pat = pattern_exact_payload(wire);
        pat.includes = vec!["orders.products".into()];

        let with = fetch_with_includes(&pat, &strands, &ScanConfig::default());
        assert_eq!(with.rows.len(), 1);
        let inc = &with.rows[0].included[0];
        assert_eq!(inc.0, "orders.products");
        assert_eq!(inc.1.len(), 1);
        assert_eq!(inc.1[0].signature, product_sig);
    }

    #[test]
    fn like_pattern_is_verified_after_fast_match() {
        let strands = vec![
            strand_with_payload(1, 1, b"user1@gmail.com", 100),
            strand_with_payload(1, 2, b"user2@example.com", 200),
            strand_with_payload(1, 3, b"user3@gmail.com", 300),
        ];
        let pattern = GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::LikePattern {
                field_intron: "_payload".into(),
                pattern: "%@gmail.com".into(),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        };
        let out = fetch(&pattern, &strands, &ScanConfig::default());
        assert_eq!(out.rows.len(), 2);
        assert_eq!(out.rows[0].version, 1);
        assert_eq!(out.rows[1].version, 3);
    }

    #[test]
    fn range_not_equal_is_verified_after_fast_match() {
        let strands = vec![
            strand_with_payload(1, 1, b"alpha", 100),
            strand_with_payload(1, 2, b"beta", 200),
        ];
        let pattern = GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::RangeMatch {
                field_intron: "_payload".into(),
                operator: RangeOp::NotEqual,
                operand_wire: b"beta".to_vec(),
                operand_codons: encode_bytes_to_codons(b"beta"),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        };
        let out = fetch(&pattern, &strands, &ScanConfig::default());
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].version, 1);
    }

    #[test]
    fn direct_signature_path_returns_expected_row() {
        let strands = vec![
            strand_with_payload(1, 1, b"a", 100),
            strand_with_payload(1, 2, b"b", 200),
        ];
        let p = GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::ExactMatch {
                field_intron: "_signature".into(),
                operand_wire: strands[1].signature.to_vec(),
                operand_codons: encode_bytes_to_codons(&strands[1].signature),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: Some(1),
        };
        let stats = CollectionStats::fake();
        assert!(matches!(choose_path(&p, &stats), QueryPath::Direct));
        let out = fetch(&p, &strands, &ScanConfig::default());
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].signature, strands[1].signature);
    }

    #[test]
    fn indexed_exact_path_returns_expected_rows() {
        let strands = vec![
            strand_with_payload(1, 1, b"a@example.com", 100),
            strand_with_payload(1, 2, b"b@example.com", 200),
            strand_with_payload(1, 3, b"a@example.com", 300),
        ];
        let p = pattern_exact_payload(b"a@example.com");
        let cfg = ScanConfig {
            collection_id: Some(1),
            indexed_fields: Some(["_payload".to_string()].into_iter().collect()),
            ..ScanConfig::default()
        };
        let out = fetch(&p, &strands, &cfg);
        assert_eq!(out.rows.len(), 2);
        assert!(out.rows.iter().any(|s| s.version == 1));
        assert!(out.rows.iter().any(|s| s.version == 3));
    }

    #[test]
    fn indexed_range_path_returns_expected_rows() {
        let strands = vec![
            strand_with_payload(1, 1, b"10", 100),
            strand_with_payload(1, 2, b"20", 200),
            strand_with_payload(1, 3, b"30", 300),
        ];
        let p = GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::RangeMatch {
                field_intron: "_payload".into(),
                operator: RangeOp::GreaterThan,
                operand_wire: b"15".to_vec(),
                operand_codons: encode_bytes_to_codons(b"15"),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        };
        let cfg = ScanConfig {
            collection_id: Some(1),
            indexed_fields: Some(["_payload".to_string()].into_iter().collect()),
            ..ScanConfig::default()
        };
        let out = fetch(&p, &strands, &cfg);
        assert_eq!(out.rows.len(), 2);
        assert!(out.rows.iter().any(|s| s.version == 2));
        assert!(out.rows.iter().any(|s| s.version == 3));
    }

    #[test]
    fn indexed_between_path_returns_expected_rows() {
        let strands = vec![
            strand_with_payload(1, 1, b"10", 100),
            strand_with_payload(1, 2, b"20", 200),
            strand_with_payload(1, 3, b"30", 300),
        ];
        let p = GuidePattern {
            collection: "c".into(),
            clauses: vec![
                Clause::RangeMatch {
                    field_intron: "_payload".into(),
                    operator: RangeOp::GreaterOrEqual,
                    operand_wire: b"15".to_vec(),
                    operand_codons: encode_bytes_to_codons(b"15"),
                },
                Clause::RangeMatch {
                    field_intron: "_payload".into(),
                    operator: RangeOp::LessOrEqual,
                    operand_wire: b"25".to_vec(),
                    operand_codons: encode_bytes_to_codons(b"25"),
                },
            ],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: None,
        };
        let cfg = ScanConfig {
            collection_id: Some(1),
            indexed_fields: Some(["_payload".to_string()].into_iter().collect()),
            ..ScanConfig::default()
        };
        let mut stats = CollectionStats::fake();
        stats.record_count = 3;
        stats.add_index("_payload");
        assert!(matches!(choose_path(&p, &stats), QueryPath::Index { .. }));
        let out = fetch(&p, &strands, &cfg);
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0].version, 2);
    }

    #[test]
    fn shape_cache_hits_on_repeated_query_shape() {
        clear_planner_cache_for_tests();
        let before = planner_cache_stats();
        // Distinct payloads so parallel tests sharing the global plan cache cannot collide.
        let a = b"shape-cache-probe-a-9f3c2e1d@dnadb.test";
        let b = b"shape-cache-probe-b-9f3c2e1d@dnadb.test";
        let strands = vec![
            strand_with_payload(1, 1, a, 100),
            strand_with_payload(1, 2, b, 200),
            strand_with_payload(1, 3, a, 300),
        ];
        let cfg = ScanConfig {
            collection_id: Some(1),
            indexed_fields: Some(["_payload".to_string()].into_iter().collect()),
            ..ScanConfig::default()
        };

        let p1 = pattern_exact_payload(a);
        let p2 = pattern_exact_payload(b);
        let _ = fetch(&p1, &strands, &cfg);
        let after_first = planner_cache_stats();
        let _ = fetch(&p2, &strands, &cfg);
        let after_second = planner_cache_stats();
        assert!(
            after_first.1 > before.1,
            "first run should increment cache misses"
        );
        assert!(
            after_second.0 > after_first.0,
            "second run of same shape should produce a cache hit"
        );
    }

    #[test]
    fn fetch_stream_emits_rows_without_order_by() {
        let strands = vec![
            strand_with_payload(1, 1, b"a@example.com", 100),
            strand_with_payload(1, 2, b"b@example.com", 200),
            strand_with_payload(1, 3, b"a@example.com", 300),
        ];
        let p = pattern_exact_payload(b"a@example.com");
        let mut versions = Vec::new();
        let emitted = fetch_stream(&p, &strands, &ScanConfig::default(), |row| {
            versions.push(row.version);
            true
        });
        assert_eq!(emitted, 2);
        assert_eq!(versions.len(), 2);
        assert!(versions.contains(&1));
        assert!(versions.contains(&3));
    }

    #[test]
    fn fetch_stream_respects_limit_and_early_stop() {
        let strands = vec![
            strand_with_payload(1, 1, b"a@example.com", 100),
            strand_with_payload(1, 2, b"a@example.com", 200),
            strand_with_payload(1, 3, b"a@example.com", 300),
        ];
        let mut p = pattern_exact_payload(b"a@example.com");
        p.limit = Some(2);
        let mut seen = Vec::new();
        let emitted = fetch_stream(&p, &strands, &ScanConfig::default(), |row| {
            seen.push(row.version);
            seen.len() < 2
        });
        assert_eq!(emitted, 1);
        assert_eq!(seen.len(), 2, "callback can stop before counting second row");
    }

    #[test]
    fn fetch_stream_falls_back_to_fetch_for_ordered_queries() {
        let strands = vec![
            strand_with_payload(1, 1, b"a", 100),
            strand_with_payload(1, 2, b"a", 300),
            strand_with_payload(1, 3, b"a", 200),
        ];
        let mut p = pattern_exact_payload(b"a");
        p.order_by = Some(("created_at".into(), SortDirection::Desc));
        p.limit = Some(2);
        let mut out = Vec::new();
        let emitted = fetch_stream(&p, &strands, &ScanConfig::default(), |row| {
            out.push(row.version);
            true
        });
        assert_eq!(emitted, 2);
        assert_eq!(out, vec![2, 3]);
    }

    #[test]
    fn dead_reckoning_signature_lookup_hits_near_estimate() {
        let strands: Vec<_> = (1u64..=500)
            .map(|i| strand_with_payload(1, i, format!("u{i}").as_bytes(), i))
            .collect();
        let p = GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::ExactMatch {
                field_intron: "_signature".into(),
                operand_wire: 321u64.to_le_bytes().to_vec(),
                operand_codons: encode_bytes_to_codons(&321u64.to_le_bytes()),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: Some(1),
        };
        let out = dead_reckoning_signature_lookup(&p, &strands).expect("direct path applies");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].version, 321);
    }

    #[test]
    fn dead_reckoning_signature_lookup_out_of_range_returns_none() {
        let strands: Vec<_> = (1u64..=100)
            .map(|i| strand_with_payload(1, i, format!("u{i}").as_bytes(), i))
            .collect();
        let p = GuidePattern {
            collection: "c".into(),
            clauses: vec![Clause::ExactMatch {
                field_intron: "_signature".into(),
                operand_wire: 9999u64.to_le_bytes().to_vec(),
                operand_codons: encode_bytes_to_codons(&9999u64.to_le_bytes()),
            }],
            includes: vec![],
            overlay: None,
            order_by: None,
            limit: Some(1),
        };
        assert!(dead_reckoning_signature_lookup(&p, &strands).is_none());
    }
}
