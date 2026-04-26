//! Stage 5 lifecycle policy primitives.
//!
//! Implements immortal-by-default safety and explicit opt-in expiry policies.

use std::collections::HashMap;
use std::time::Duration;

use crate::model::{RefreshPolicy, Strand};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LifecycleMode {
    Immortal,
    Telomere,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ExpireAction {
    None,
    Archive,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LifecyclePolicy {
    pub mode: LifecycleMode,
    pub initial_count: u16,
    pub refresh_policy: RefreshPolicy,
    pub on_expire: ExpireAction,
    pub minimum_age_ns: u64,
}

#[derive(Debug, Clone, Default)]
pub struct LifecyclePolicyStore {
    policies_by_collection: HashMap<u32, LifecyclePolicy>,
}

impl LifecyclePolicyStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_policy(&mut self, collection_id: u32, policy: LifecyclePolicy) {
        self.policies_by_collection.insert(collection_id, policy);
    }

    pub fn clear_policy(&mut self, collection_id: u32) {
        self.policies_by_collection.remove(&collection_id);
    }

    pub fn policy_for(&self, collection_id: u32) -> LifecyclePolicy {
        effective_policy_for_collection(collection_id, &self.policies_by_collection)
    }
}

impl Default for LifecyclePolicy {
    fn default() -> Self {
        Self::immortal_default()
    }
}

impl LifecyclePolicy {
    pub fn immortal_default() -> Self {
        Self {
            mode: LifecycleMode::Immortal,
            initial_count: u16::MAX,
            refresh_policy: RefreshPolicy::Immortal,
            on_expire: ExpireAction::None,
            minimum_age_ns: 0,
        }
    }

    pub fn telomere_opt_in(
        initial_count: u16,
        refresh_policy: RefreshPolicy,
        on_expire: ExpireAction,
        minimum_age: Duration,
    ) -> Self {
        Self {
            mode: LifecycleMode::Telomere,
            initial_count,
            refresh_policy,
            on_expire,
            minimum_age_ns: minimum_age.as_nanos() as u64,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleDecision {
    Keep,
    NeedsManualRefresh,
    Archive,
    SoftDelete,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeletedStrandRecord {
    pub signature: [u8; 8],
    pub collection_id: u32,
    pub deleted_at_ns: u64,
    pub purge_after_ns: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeleteConfig {
    pub grace_period_ns: u64,
}

impl DeleteConfig {
    pub fn with_grace_period(period: Duration) -> Self {
        Self {
            grace_period_ns: period.as_nanos() as u64,
        }
    }

    pub fn default_soft_delete() -> Self {
        // Spec-safe default: 30 days grace before hard purge.
        Self::with_grace_period(Duration::from_secs(30 * 24 * 60 * 60))
    }
}

impl Default for DeleteConfig {
    fn default() -> Self {
        Self::default_soft_delete()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardDeleteSweepResult {
    pub purged_signatures: Vec<[u8; 8]>,
}

/// Collection-level lifecycle policy lookup with immortal fallback.
pub fn effective_policy_for_collection(
    collection_id: u32,
    collection_policies: &HashMap<u32, LifecyclePolicy>,
) -> LifecyclePolicy {
    collection_policies
        .get(&collection_id)
        .copied()
        .unwrap_or_else(LifecyclePolicy::immortal_default)
}

/// Apply lifecycle defaults to a newly created strand.
pub fn initialize_telomere_for_new_strand(
    strand: &mut Strand,
    policy: LifecyclePolicy,
    now_ns: u64,
) {
    strand.telomere.immortal = policy.mode == LifecycleMode::Immortal;
    strand.telomere.count = if strand.telomere.immortal {
        u16::MAX
    } else {
        policy.initial_count
    };
    strand.telomere.refresh_policy = policy.refresh_policy;
    strand.telomere.last_refresh = now_ns;
}

/// Apply replication semantics to telomere counters.
///
/// Immortal strands are never decremented. Telomere-mode strands decrement until zero.
pub fn apply_replication_event(strand: &mut Strand) {
    if strand.telomere.immortal {
        return;
    }
    strand.telomere.count = strand.telomere.count.saturating_sub(1);
}

/// Refresh one strand's telomere counter according to policy.
///
/// For immortal mode, refresh just updates metadata timestamp.
pub fn refresh_strand_telomere(strand: &mut Strand, policy: LifecyclePolicy, now_ns: u64) {
    strand.telomere.last_refresh = now_ns;
    if policy.mode == LifecycleMode::Immortal || strand.telomere.immortal {
        strand.telomere.immortal = true;
        strand.telomere.count = u16::MAX;
        strand.telomere.refresh_policy = RefreshPolicy::Immortal;
        return;
    }
    strand.telomere.immortal = false;
    strand.telomere.count = policy.initial_count;
    strand.telomere.refresh_policy = policy.refresh_policy;
}

/// Refresh all strands from a collection matching a predicate.
///
/// Returns number of refreshed strands.
pub fn refresh_collection_strands<F>(
    strands: &mut [Strand],
    collection_id: u32,
    policy: LifecyclePolicy,
    now_ns: u64,
    mut predicate: F,
) -> usize
where
    F: FnMut(&Strand) -> bool,
{
    let mut refreshed = 0usize;
    for strand in strands {
        if strand.collection_id != collection_id || !predicate(strand) {
            continue;
        }
        refresh_strand_telomere(strand, policy, now_ns);
        refreshed = refreshed.saturating_add(1);
    }
    refreshed
}

/// Move a strand into soft-deleted state by creating a deleted-pool record.
pub fn soft_delete_record_for_strand(
    strand: &Strand,
    now_ns: u64,
    cfg: DeleteConfig,
    reason: impl Into<String>,
) -> DeletedStrandRecord {
    DeletedStrandRecord {
        signature: strand.signature,
        collection_id: strand.collection_id,
        deleted_at_ns: now_ns,
        purge_after_ns: now_ns.saturating_add(cfg.grace_period_ns),
        reason: reason.into(),
    }
}

/// Sweep deleted-pool records and hard-delete those past grace period.
pub fn sweep_deleted_pool(
    deleted_pool: &mut Vec<DeletedStrandRecord>,
    now_ns: u64,
) -> HardDeleteSweepResult {
    let mut purged_signatures = Vec::new();
    deleted_pool.retain(|record| {
        let expired = now_ns >= record.purge_after_ns;
        if expired {
            purged_signatures.push(record.signature);
        }
        !expired
    });
    purged_signatures.sort_unstable();
    HardDeleteSweepResult { purged_signatures }
}

/// Evaluate what lifecycle action should occur now for a strand.
pub fn evaluate_lifecycle_action(
    strand: &Strand,
    policy: LifecyclePolicy,
    now_ns: u64,
) -> LifecycleDecision {
    if policy.mode == LifecycleMode::Immortal || strand.telomere.immortal {
        return LifecycleDecision::Keep;
    }

    if strand.telomere.count > 0 {
        return LifecycleDecision::Keep;
    }

    if now_ns.saturating_sub(strand.created_at) < policy.minimum_age_ns {
        return LifecycleDecision::Keep;
    }

    match policy.refresh_policy {
        RefreshPolicy::AutoOnRead => LifecycleDecision::Keep,
        RefreshPolicy::Manual => LifecycleDecision::NeedsManualRefresh,
        RefreshPolicy::Immortal => LifecycleDecision::Keep,
    }
    .max(match policy.on_expire {
        ExpireAction::None => LifecycleDecision::Keep,
        ExpireAction::Archive => LifecycleDecision::Archive,
        ExpireAction::Delete => LifecycleDecision::SoftDelete,
    })
}

impl Ord for LifecycleDecision {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        rank(*self).cmp(&rank(*other))
    }
}

impl PartialOrd for LifecycleDecision {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn rank(d: LifecycleDecision) -> i32 {
    match d {
        LifecycleDecision::Keep => 0,
        LifecycleDecision::NeedsManualRefresh => 1,
        LifecycleDecision::Archive => 2,
        LifecycleDecision::SoftDelete => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        apply_replication_event,
        effective_policy_for_collection, evaluate_lifecycle_action,
        initialize_telomere_for_new_strand, refresh_collection_strands, refresh_strand_telomere,
        soft_delete_record_for_strand, sweep_deleted_pool, DeleteConfig, ExpireAction,
        LifecycleDecision, LifecyclePolicy, LifecycleMode, LifecyclePolicyStore,
    };
    use crate::model::{RefreshPolicy, Strand, Tag, Telomere};
    use std::collections::HashMap;
    use std::time::Duration;

    fn mk_strand(collection_id: u32) -> Strand {
        Strand {
            signature: 1u64.to_le_bytes(),
            collection_id,
            codons: vec![],
            complement: vec![],
            introns: vec![],
            telomere: Telomere {
                count: 100,
                immortal: true,
                last_refresh: 0,
                refresh_policy: RefreshPolicy::Immortal,
            },
            epigenetic_tags: vec![Tag {
                key: "t".into(),
                value: "v".into(),
            }],
            version: 1,
            created_at: 0,
            updated_at: 0,
        }
    }

    #[test]
    fn default_policy_is_immortal() {
        let p = LifecyclePolicy::immortal_default();
        assert_eq!(p.mode, super::LifecycleMode::Immortal);
        assert_eq!(p.on_expire, ExpireAction::None);
    }

    #[test]
    fn collection_policy_falls_back_to_immortal() {
        let table = HashMap::new();
        let p = effective_policy_for_collection(42, &table);
        assert_eq!(p.mode, super::LifecycleMode::Immortal);
    }

    #[test]
    fn new_strands_use_immortal_defaults() {
        let mut s = mk_strand(1);
        initialize_telomere_for_new_strand(&mut s, LifecyclePolicy::immortal_default(), 100);
        assert!(s.telomere.immortal);
        assert_eq!(s.telomere.count, u16::MAX);
    }

    #[test]
    fn telomere_policy_can_soft_delete_after_min_age() {
        let mut s = mk_strand(1);
        s.telomere.immortal = false;
        s.telomere.count = 0;
        s.created_at = 10;
        let p = LifecyclePolicy::telomere_opt_in(
            100,
            RefreshPolicy::Manual,
            ExpireAction::Delete,
            Duration::from_secs(30),
        );
        let d = evaluate_lifecycle_action(&s, p, 50_000_000_000);
        assert_eq!(d, LifecycleDecision::SoftDelete);
    }

    #[test]
    fn min_age_blocks_expiry() {
        let mut s = mk_strand(1);
        s.telomere.immortal = false;
        s.telomere.count = 0;
        s.created_at = 1_000;
        let p = LifecyclePolicy::telomere_opt_in(
            100,
            RefreshPolicy::Manual,
            ExpireAction::Delete,
            Duration::from_secs(60),
        );
        let d = evaluate_lifecycle_action(&s, p, 2_000);
        assert_eq!(d, LifecycleDecision::Keep);
    }

    #[test]
    fn on_read_refresh_policy_keeps_expired_records() {
        let mut s = mk_strand(1);
        s.telomere.immortal = false;
        s.telomere.count = 0;
        s.created_at = 0;
        let p = LifecyclePolicy::telomere_opt_in(
            100,
            RefreshPolicy::AutoOnRead,
            ExpireAction::None,
            Duration::from_secs(0),
        );
        let d = evaluate_lifecycle_action(&s, p, 1_000);
        assert_eq!(d, LifecycleDecision::Keep);
    }

    #[test]
    fn lifecycle_policy_store_supports_collection_overrides() {
        let mut store = LifecyclePolicyStore::new();
        let p = LifecyclePolicy::telomere_opt_in(
            20,
            RefreshPolicy::Manual,
            ExpireAction::Archive,
            Duration::from_secs(10),
        );
        store.set_policy(7, p);
        assert_eq!(store.policy_for(7).mode, LifecycleMode::Telomere);
        assert_eq!(store.policy_for(8).mode, LifecycleMode::Immortal);
        store.clear_policy(7);
        assert_eq!(store.policy_for(7).mode, LifecycleMode::Immortal);
    }

    #[test]
    fn replication_decrements_non_immortal_only() {
        let mut immortal = mk_strand(1);
        immortal.telomere.immortal = true;
        immortal.telomere.count = u16::MAX;
        apply_replication_event(&mut immortal);
        assert_eq!(immortal.telomere.count, u16::MAX);

        let mut mortal = mk_strand(1);
        mortal.telomere.immortal = false;
        mortal.telomere.count = 2;
        apply_replication_event(&mut mortal);
        apply_replication_event(&mut mortal);
        apply_replication_event(&mut mortal);
        assert_eq!(mortal.telomere.count, 0);
    }

    #[test]
    fn explicit_refresh_restores_count_for_telomere_mode() {
        let mut s = mk_strand(10);
        s.telomere.immortal = false;
        s.telomere.count = 0;
        let p = LifecyclePolicy::telomere_opt_in(
            77,
            RefreshPolicy::Manual,
            ExpireAction::Archive,
            Duration::from_secs(1),
        );
        refresh_strand_telomere(&mut s, p, 900);
        assert!(!s.telomere.immortal);
        assert_eq!(s.telomere.count, 77);
        assert_eq!(s.telomere.refresh_policy, RefreshPolicy::Manual);
        assert_eq!(s.telomere.last_refresh, 900);
    }

    #[test]
    fn bulk_refresh_updates_only_matching_collection_and_filter() {
        let mut a = mk_strand(5);
        a.signature = 1u64.to_le_bytes();
        a.telomere.immortal = false;
        a.telomere.count = 0;

        let mut b = mk_strand(5);
        b.signature = 2u64.to_le_bytes();
        b.telomere.immortal = false;
        b.telomere.count = 0;

        let mut other = mk_strand(6);
        other.signature = 3u64.to_le_bytes();
        other.telomere.immortal = false;
        other.telomere.count = 0;

        let mut strands = vec![a, b, other];
        let policy = LifecyclePolicy::telomere_opt_in(
            50,
            RefreshPolicy::Manual,
            ExpireAction::Archive,
            Duration::from_secs(0),
        );
        let refreshed = refresh_collection_strands(
            &mut strands,
            5,
            policy,
            5_000,
            |s| s.signature == 1u64.to_le_bytes(),
        );
        assert_eq!(refreshed, 1);
        assert_eq!(strands[0].telomere.count, 50);
        assert_eq!(strands[1].telomere.count, 0);
        assert_eq!(strands[2].telomere.count, 0);
    }

    #[test]
    fn soft_delete_record_uses_grace_window() {
        let s = mk_strand(8);
        let now = 1_000u64;
        let cfg = DeleteConfig::with_grace_period(Duration::from_secs(5));
        let rec = soft_delete_record_for_strand(&s, now, cfg, "telomere-expired");
        assert_eq!(rec.signature, s.signature);
        assert_eq!(rec.collection_id, 8);
        assert_eq!(rec.deleted_at_ns, now);
        assert_eq!(rec.purge_after_ns, now + 5_000_000_000);
    }

    #[test]
    fn deleted_pool_sweeper_purges_only_expired_records() {
        let s1 = mk_strand(1);
        let mut s2 = mk_strand(1);
        s2.signature = 2u64.to_le_bytes();
        let cfg = DeleteConfig::with_grace_period(Duration::from_secs(10));
        let r1 = soft_delete_record_for_strand(&s1, 100, cfg, "expired");
        let r2 = soft_delete_record_for_strand(&s2, 250, cfg, "expired");
        let mut pool = vec![r1.clone(), r2.clone()];

        let result = sweep_deleted_pool(&mut pool, 10_000_000_150);
        assert_eq!(result.purged_signatures, vec![s1.signature]);
        assert_eq!(pool.len(), 1);
        assert_eq!(pool[0].signature, s2.signature);
    }
}
