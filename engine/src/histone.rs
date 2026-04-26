//! Stage 4 histone block temperature model primitives.
//!
//! Implements the core data structures and deterministic tier classification rules
//! from the specification (Hot/Warm/Cold/Frozen).

use std::time::Duration;
use std::collections::HashMap;

use crate::model::Strand;

pub type StrandSignature = [u8; 8];

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Temperature {
    Hot,
    Warm,
    Cold,
    Frozen,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HistoneBlock {
    pub block_id: u64,
    pub strands: Vec<StrandSignature>,
    pub temperature: Temperature,
    pub semantic_key: Option<String>,
    pub compressed: bool,
    pub last_access_ns: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BlockAccessStats {
    pub last_access_ns: u64,
    pub access_count_total: u64,
    pub access_count_recent: u32,
}

impl BlockAccessStats {
    pub fn new(last_access_ns: u64) -> Self {
        Self {
            last_access_ns,
            access_count_total: 0,
            access_count_recent: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TierDecision {
    pub previous: Temperature,
    pub next: Temperature,
    pub changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoLocationPlan {
    pub semantic_key: String,
    pub members: Vec<StrandSignature>,
    pub block_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceActionKind {
    Noop,
    Promote,
    Demote,
    Freeze,
    Compress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebalanceAction {
    pub block_id: u64,
    pub from: Temperature,
    pub to: Temperature,
    pub kind: RebalanceActionKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalanceTickResult {
    pub actions: Vec<RebalanceAction>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CompactionSegment {
    pub segment_id: u32,
    pub temperature: Temperature,
    pub records: Vec<CompactionRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CompactionRecord {
    pub record_id: u64,
    pub clustering_key: Option<String>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionPlan {
    pub selected_segment_ids: Vec<u32>,
    pub output_segment_id: u32,
}

pub const HOT_WINDOW: Duration = Duration::from_secs(5 * 60);
pub const WARM_WINDOW: Duration = Duration::from_secs(7 * 24 * 60 * 60);
pub const REACCESS_PROMOTION_WINDOW: Duration = Duration::from_secs(10 * 60);

/// Classify block tier based on recency and telomere lifecycle state.
///
/// Rule precedence:
/// 1) If `telomere_count == 0 && !immortal` => Frozen.
/// 2) If idle <= 5m => Hot.
/// 3) If idle <= 7d => Warm.
/// 4) Else => Cold.
pub fn classify_temperature(
    now_ns: u64,
    last_access_ns: u64,
    telomere_count: u16,
    immortal: bool,
) -> Temperature {
    if telomere_count == 0 && !immortal {
        return Temperature::Frozen;
    }
    let idle_ns = now_ns.saturating_sub(last_access_ns);
    let idle = Duration::from_nanos(idle_ns);

    if idle <= HOT_WINDOW {
        Temperature::Hot
    } else if idle <= WARM_WINDOW {
        Temperature::Warm
    } else {
        Temperature::Cold
    }
}

/// Immediate tier transition on access:
/// - Cold/Frozen => Warm
/// - Warm/Hot unchanged
pub fn on_access_promote_immediate(current: Temperature) -> Temperature {
    match current {
        Temperature::Cold | Temperature::Frozen => Temperature::Warm,
        Temperature::Warm | Temperature::Hot => current,
    }
}

/// When a block was recently accessed as Warm, a second access within 10 minutes
/// can trigger Hot promotion.
pub fn should_promote_warm_to_hot(last_access_ns: u64, now_ns: u64) -> bool {
    let delta_ns = now_ns.saturating_sub(last_access_ns);
    Duration::from_nanos(delta_ns) <= REACCESS_PROMOTION_WINDOW
}

/// Update access counters and access timestamps for a block.
pub fn record_block_access(stats: &mut BlockAccessStats, now_ns: u64) {
    if should_promote_warm_to_hot(stats.last_access_ns, now_ns) {
        stats.access_count_recent = stats.access_count_recent.saturating_add(1);
    } else {
        stats.access_count_recent = 1;
    }
    stats.access_count_total = stats.access_count_total.saturating_add(1);
    stats.last_access_ns = now_ns;
}

/// Evaluate tier transition based on current block state + lifecycle information.
///
/// Decision order:
/// 1) Reclassify by lifecycle/recency (`classify_temperature`).
/// 2) If current tier is Cold/Frozen and block is accessed, immediate promotion to Warm.
/// 3) If current tier is Warm and access burst is recent, promote to Hot.
pub fn evaluate_tier_transition(
    current: Temperature,
    stats: &BlockAccessStats,
    now_ns: u64,
    telomere_count: u16,
    immortal: bool,
    was_accessed: bool,
) -> TierDecision {
    let mut next = classify_temperature(now_ns, stats.last_access_ns, telomere_count, immortal);

    if next == Temperature::Frozen {
        return TierDecision {
            previous: current,
            next,
            changed: current != next,
        };
    }

    if was_accessed {
        if matches!(current, Temperature::Cold | Temperature::Frozen) {
            next = on_access_promote_immediate(current);
        } else if current == Temperature::Warm && stats.access_count_recent >= 2 {
            next = Temperature::Hot;
        }
    }

    TierDecision {
        previous: current,
        next,
        changed: current != next,
    }
}

/// Compute a deterministic block id from semantic key.
pub fn block_id_for_semantic_key(key: &str) -> u64 {
    let h = blake3::hash(key.as_bytes());
    let bytes = h.as_bytes();
    u64::from_le_bytes(bytes[0..8].try_into().expect("len 8"))
}

/// Infer a semantic grouping key for a strand.
///
/// Current policy:
/// - If strand has `user` or `user_id` intron reference -> `user:<referenced-signature-hex>`
/// - Else fallback to `collection:<collection_id>`
pub fn infer_semantic_key(strand: &Strand) -> String {
    for intron in &strand.introns {
        if (intron.field_name == "user" || intron.field_name == "user_id")
            && intron.references_strand.is_some()
        {
            let ref_sig = intron.references_strand.expect("checked is_some");
            return format!("user:{}", hex_signature(ref_sig));
        }
    }
    format!("collection:{}", strand.collection_id)
}

/// Build semantic co-location plans by grouping strands with same semantic key.
pub fn plan_semantic_colocation(strands: &[Strand]) -> Vec<CoLocationPlan> {
    let mut groups: HashMap<String, Vec<StrandSignature>> = HashMap::new();
    for s in strands {
        let key = infer_semantic_key(s);
        groups.entry(key).or_default().push(s.signature);
    }

    let mut plans: Vec<CoLocationPlan> = groups
        .into_iter()
        .map(|(semantic_key, mut members)| {
            members.sort_unstable();
            let block_id = block_id_for_semantic_key(&semantic_key);
            CoLocationPlan {
                semantic_key,
                members,
                block_id,
            }
        })
        .collect();
    plans.sort_by(|a, b| a.semantic_key.cmp(&b.semantic_key));
    plans
}

/// Execute one scheduler tick across histone blocks.
///
/// Inputs:
/// - `blocks`: current block state snapshot.
/// - `stats_by_block`: access counters and last access data.
/// - `telomere_state_by_block`: `(count, immortal)` tuple used for Frozen rule.
/// - `accessed_blocks`: block ids accessed since last tick.
///
/// Output:
/// - list of rebalance actions for the caller to apply.
pub fn run_rebalance_tick(
    now_ns: u64,
    blocks: &[HistoneBlock],
    stats_by_block: &HashMap<u64, BlockAccessStats>,
    telomere_state_by_block: &HashMap<u64, (u16, bool)>,
    accessed_blocks: &std::collections::HashSet<u64>,
) -> RebalanceTickResult {
    let mut actions = Vec::with_capacity(blocks.len());

    for block in blocks {
        let stats = stats_by_block
            .get(&block.block_id)
            .cloned()
            .unwrap_or_else(|| BlockAccessStats::new(block.last_access_ns));
        let (telomere_count, immortal) = telomere_state_by_block
            .get(&block.block_id)
            .copied()
            .unwrap_or((u16::MAX, true));
        let was_accessed = accessed_blocks.contains(&block.block_id);

        let decision = evaluate_tier_transition(
            block.temperature,
            &stats,
            now_ns,
            telomere_count,
            immortal,
            was_accessed,
        );

        let kind = if !decision.changed {
            RebalanceActionKind::Noop
        } else if decision.next == Temperature::Frozen {
            RebalanceActionKind::Freeze
        } else if matches!(decision.next, Temperature::Cold) {
            RebalanceActionKind::Compress
        } else if tier_rank(decision.next) > tier_rank(decision.previous) {
            RebalanceActionKind::Promote
        } else {
            RebalanceActionKind::Demote
        };

        actions.push(RebalanceAction {
            block_id: block.block_id,
            from: decision.previous,
            to: decision.next,
            kind,
        });
    }

    actions.sort_by_key(|a| a.block_id);
    RebalanceTickResult { actions }
}

fn hex_signature(sig: [u8; 8]) -> String {
    let mut out = String::with_capacity(16);
    for b in sig {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

fn tier_rank(t: Temperature) -> i32 {
    match t {
        Temperature::Frozen => 0,
        Temperature::Cold => 1,
        Temperature::Warm => 2,
        Temperature::Hot => 3,
    }
}

/// Pick compactable segments (cold/frozen) and propose a single merged output id.
pub fn plan_compaction(segments: &[CompactionSegment], next_segment_id: u32) -> Option<CompactionPlan> {
    let mut selected: Vec<u32> = segments
        .iter()
        .filter(|s| matches!(s.temperature, Temperature::Cold | Temperature::Frozen))
        .map(|s| s.segment_id)
        .collect();
    selected.sort_unstable();
    if selected.len() < 2 {
        return None;
    }
    Some(CompactionPlan {
        selected_segment_ids: selected,
        output_segment_id: next_segment_id,
    })
}

/// Compaction-time clustering:
/// - merge records from input segments
/// - stable sort by `(clustering_key, record_id)` when clustering key exists
/// - emit one merged immutable segment
pub fn compact_segments(
    segments: Vec<CompactionSegment>,
    output_segment_id: u32,
) -> CompactionSegment {
    let mut records: Vec<CompactionRecord> = segments
        .into_iter()
        .flat_map(|s| s.records.into_iter())
        .collect();
    records.sort_by(|a, b| {
        a.clustering_key
            .cmp(&b.clustering_key)
            .then(a.record_id.cmp(&b.record_id))
    });
    CompactionSegment {
        segment_id: output_segment_id,
        temperature: Temperature::Cold,
        records,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        block_id_for_semantic_key, classify_temperature, evaluate_tier_transition,
        compact_segments, infer_semantic_key, on_access_promote_immediate, plan_compaction,
        plan_semantic_colocation, record_block_access, run_rebalance_tick,
        should_promote_warm_to_hot, BlockAccessStats, CompactionRecord, CompactionSegment,
        HistoneBlock, RebalanceActionKind, Temperature, HOT_WINDOW, WARM_WINDOW,
    };
    use crate::model::{Intron, RefreshPolicy, Strand, Tag, Telomere};
    use std::collections::{HashMap, HashSet};

    #[test]
    fn classification_respects_windows() {
        let now = 10_000_000_000_000_000_u64;
        let hot = classify_temperature(
            now,
            now - HOT_WINDOW.as_nanos() as u64 + 1,
            100,
            true,
        );
        assert_eq!(hot, Temperature::Hot);

        let warm = classify_temperature(
            now,
            now - HOT_WINDOW.as_nanos() as u64 - 1,
            100,
            true,
        );
        assert_eq!(warm, Temperature::Warm);

        let cold = classify_temperature(
            now,
            now - WARM_WINDOW.as_nanos() as u64 - 1,
            100,
            true,
        );
        assert_eq!(cold, Temperature::Cold);
    }

    #[test]
    fn frozen_overrides_recency_when_telomere_depleted() {
        let now = 1_000_000_u64;
        let t = classify_temperature(now, now, 0, false);
        assert_eq!(t, Temperature::Frozen);
    }

    #[test]
    fn access_promotion_rules_apply() {
        assert_eq!(
            on_access_promote_immediate(Temperature::Cold),
            Temperature::Warm
        );
        assert_eq!(
            on_access_promote_immediate(Temperature::Frozen),
            Temperature::Warm
        );
        assert_eq!(
            on_access_promote_immediate(Temperature::Warm),
            Temperature::Warm
        );
    }

    #[test]
    fn warm_to_hot_reaccess_window() {
        let last = 1_000_000_u64;
        assert!(should_promote_warm_to_hot(last, last + 60_000_000_000)); // +60s
        assert!(!should_promote_warm_to_hot(last, last + 700_000_000_000)); // +700s
    }

    #[test]
    fn record_access_updates_counters() {
        let mut stats = BlockAccessStats::new(1_000);
        record_block_access(&mut stats, 2_000);
        assert_eq!(stats.access_count_total, 1);
        assert_eq!(stats.access_count_recent, 1);
        record_block_access(&mut stats, 2_500);
        assert_eq!(stats.access_count_total, 2);
        assert_eq!(stats.access_count_recent, 2);
    }

    #[test]
    fn transition_promotes_cold_on_access() {
        let stats = BlockAccessStats {
            last_access_ns: 1_000,
            access_count_total: 10,
            access_count_recent: 1,
        };
        let d = evaluate_tier_transition(Temperature::Cold, &stats, 2_000, 10, true, true);
        assert_eq!(d.next, Temperature::Warm);
        assert!(d.changed);
    }

    #[test]
    fn transition_promotes_warm_to_hot_on_reaccess_burst() {
        let stats = BlockAccessStats {
            last_access_ns: 1_500,
            access_count_total: 12,
            access_count_recent: 2,
        };
        let d = evaluate_tier_transition(Temperature::Warm, &stats, 2_000, 10, true, true);
        assert_eq!(d.next, Temperature::Hot);
        assert!(d.changed);
    }

    #[test]
    fn transition_respects_frozen_rule_without_access() {
        let stats = BlockAccessStats {
            last_access_ns: 1_000,
            access_count_total: 1,
            access_count_recent: 1,
        };
        let d = evaluate_tier_transition(Temperature::Warm, &stats, 2_000, 0, false, false);
        assert_eq!(d.next, Temperature::Frozen);
    }

    fn mk_strand(sig: [u8; 8], collection_id: u32, introns: Vec<Intron>) -> Strand {
        Strand {
            signature: sig,
            collection_id,
            codons: vec![],
            complement: vec![],
            introns,
            telomere: Telomere {
                count: 100,
                immortal: true,
                last_refresh: 0,
                refresh_policy: RefreshPolicy::Immortal,
            },
            epigenetic_tags: vec![Tag {
                key: "test".into(),
                value: "true".into(),
            }],
            version: 1,
            created_at: 0,
            updated_at: 0,
        }
    }

    #[test]
    fn semantic_key_uses_user_reference_when_present() {
        let user_sig = 0xA1u64.to_le_bytes();
        let s = mk_strand(
            1u64.to_le_bytes(),
            7,
            vec![Intron {
                field_name: "user_id".into(),
                codon_offset: 0,
                codon_length: 0,
                value_hash: 0,
                references_strand: Some(user_sig),
            }],
        );
        let key = infer_semantic_key(&s);
        assert!(key.starts_with("user:"));
    }

    #[test]
    fn semantic_key_falls_back_to_collection() {
        let s = mk_strand(1u64.to_le_bytes(), 55, vec![]);
        assert_eq!(infer_semantic_key(&s), "collection:55");
    }

    #[test]
    fn block_id_is_deterministic_for_key() {
        let a = block_id_for_semantic_key("user:abc");
        let b = block_id_for_semantic_key("user:abc");
        let c = block_id_for_semantic_key("user:def");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn planner_groups_related_members() {
        let user_sig = 7u64.to_le_bytes();
        let order1 = mk_strand(
            11u64.to_le_bytes(),
            1,
            vec![Intron {
                field_name: "user_id".into(),
                codon_offset: 0,
                codon_length: 0,
                value_hash: 0,
                references_strand: Some(user_sig),
            }],
        );
        let order2 = mk_strand(
            12u64.to_le_bytes(),
            1,
            vec![Intron {
                field_name: "user".into(),
                codon_offset: 0,
                codon_length: 0,
                value_hash: 0,
                references_strand: Some(user_sig),
            }],
        );
        let other = mk_strand(99u64.to_le_bytes(), 9, vec![]);

        let plans = plan_semantic_colocation(&[order1.clone(), order2.clone(), other.clone()]);
        let user_plan = plans
            .iter()
            .find(|p| p.semantic_key.starts_with("user:"))
            .expect("user group");
        assert_eq!(user_plan.members.len(), 2);
        assert!(user_plan.members.contains(&order1.signature));
        assert!(user_plan.members.contains(&order2.signature));
    }

    #[test]
    fn rebalance_tick_computes_actions() {
        let now = 10_000_000_000_000_000_u64;
        let blocks = vec![
            HistoneBlock {
                block_id: 1,
                strands: vec![],
                temperature: Temperature::Cold,
                semantic_key: None,
                compressed: true,
                last_access_ns: now - HOT_WINDOW.as_nanos() as u64 + 1,
            },
            HistoneBlock {
                block_id: 2,
                strands: vec![],
                temperature: Temperature::Warm,
                semantic_key: None,
                compressed: false,
                last_access_ns: now - WARM_WINDOW.as_nanos() as u64 - 1,
            },
            HistoneBlock {
                block_id: 3,
                strands: vec![],
                temperature: Temperature::Warm,
                semantic_key: None,
                compressed: false,
                last_access_ns: now - 1000,
            },
        ];

        let mut stats = HashMap::new();
        stats.insert(
            1,
            BlockAccessStats {
                last_access_ns: now - 1_000,
                access_count_total: 10,
                access_count_recent: 1,
            },
        );
        stats.insert(
            3,
            BlockAccessStats {
                last_access_ns: now - 1_000,
                access_count_total: 10,
                access_count_recent: 2,
            },
        );

        let mut telo = HashMap::new();
        telo.insert(1, (100, true));
        telo.insert(2, (100, true));
        telo.insert(3, (0, false));

        let mut accessed = HashSet::new();
        accessed.insert(1);
        accessed.insert(3);

        let result = run_rebalance_tick(now, &blocks, &stats, &telo, &accessed);
        assert_eq!(result.actions.len(), 3);
        let a1 = result.actions.iter().find(|a| a.block_id == 1).unwrap();
        assert_eq!(a1.kind, RebalanceActionKind::Promote);
        assert_eq!(a1.to, Temperature::Warm);

        let a2 = result.actions.iter().find(|a| a.block_id == 2).unwrap();
        assert_eq!(a2.kind, RebalanceActionKind::Compress);
        assert_eq!(a2.to, Temperature::Cold);

        let a3 = result.actions.iter().find(|a| a.block_id == 3).unwrap();
        assert_eq!(a3.kind, RebalanceActionKind::Freeze);
        assert_eq!(a3.to, Temperature::Frozen);
    }

    #[test]
    fn compaction_plan_selects_cold_and_frozen_segments() {
        let segments = vec![
            CompactionSegment {
                segment_id: 1,
                temperature: Temperature::Hot,
                records: vec![],
            },
            CompactionSegment {
                segment_id: 2,
                temperature: Temperature::Cold,
                records: vec![],
            },
            CompactionSegment {
                segment_id: 3,
                temperature: Temperature::Frozen,
                records: vec![],
            },
        ];
        let plan = plan_compaction(&segments, 10).expect("plan");
        assert_eq!(plan.selected_segment_ids, vec![2, 3]);
        assert_eq!(plan.output_segment_id, 10);
    }

    #[test]
    fn compact_segments_clusters_by_foreign_key() {
        let s1 = CompactionSegment {
            segment_id: 7,
            temperature: Temperature::Cold,
            records: vec![
                CompactionRecord {
                    record_id: 3,
                    clustering_key: Some("user:2".into()),
                    payload: b"a".to_vec(),
                },
                CompactionRecord {
                    record_id: 1,
                    clustering_key: Some("user:1".into()),
                    payload: b"b".to_vec(),
                },
            ],
        };
        let s2 = CompactionSegment {
            segment_id: 8,
            temperature: Temperature::Frozen,
            records: vec![CompactionRecord {
                record_id: 2,
                clustering_key: Some("user:1".into()),
                payload: b"c".to_vec(),
            }],
        };

        let out = compact_segments(vec![s1, s2], 99);
        assert_eq!(out.segment_id, 99);
        assert_eq!(out.records.len(), 3);
        // clustered adjacency: user:1 records first, then user:2
        assert_eq!(out.records[0].record_id, 1);
        assert_eq!(out.records[1].record_id, 2);
        assert_eq!(out.records[2].record_id, 3);
    }
}

