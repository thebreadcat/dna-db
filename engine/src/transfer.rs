//! Stage 7 lateral transfer protocol primitives.
//!
//! Defines transfer packets for schema mutations and deterministic absorption
//! rules so nodes can converge without lockstep upgrades.

use std::collections::{HashMap, HashSet};

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FieldType {
    String,
    Integer,
    Float,
    Boolean,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MutationType {
    AddField {
        field_name: String,
        field_type: FieldType,
        nullable: bool,
    },
    DropField {
        field_name: String,
    },
    RenameField {
        from: String,
        to: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TransferPacket {
    pub packet_id: String,
    pub collection: String,
    pub mutation: MutationType,
    pub first_seen_ns: u64,
    pub originating_node: String,
    pub origin_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldSchema {
    pub field_type: FieldType,
    pub nullable: bool,
    pub first_seen_ns: u64,
    pub introduced_by_packet: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CollectionSchema {
    pub fields: HashMap<String, FieldSchema>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SchemaCatalog {
    pub collections: HashMap<String, CollectionSchema>,
    pub applied_packets: HashSet<String>,
    pub node_high_water: HashMap<String, u64>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TransferError {
    #[error("collection cannot be empty")]
    EmptyCollection,
    #[error("originating node cannot be empty")]
    EmptyOriginatingNode,
    #[error("packet id cannot be empty")]
    EmptyPacketId,
    #[error("out-of-order packet sequence for node `{node}`: got {got}, expected > {last}")]
    OutOfOrderSequence { node: String, got: u64, last: u64 },
    #[error("field already exists: {0}")]
    FieldAlreadyExists(String),
    #[error("field not found: {0}")]
    FieldNotFound(String),
    #[error("rename target already exists: {0}")]
    RenameTargetExists(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbsorbOutcome {
    pub applied: bool,
    pub idempotent_replay: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SyncApplyResult {
    pub applied: usize,
    pub replayed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldCoexistenceState {
    Present,
    Absent,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SchemaCoexistenceView {
    pub fields: HashMap<String, FieldCoexistenceState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PartitionBuffer {
    pub queued_packets: Vec<TransferPacket>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplayGuard {
    seen_packet_ids: HashSet<String>,
}

/// Apply one transfer packet to local schema catalog.
///
/// Rules:
/// - packet IDs are idempotent (replay-safe)
/// - per-origin sequence must increase monotonically
/// - mutation semantics are validated against current schema
pub fn absorb_transfer_packet(
    catalog: &mut SchemaCatalog,
    packet: &TransferPacket,
) -> Result<AbsorbOutcome, TransferError> {
    validate_packet(packet)?;
    if catalog.applied_packets.contains(&packet.packet_id) {
        return Ok(AbsorbOutcome {
            applied: false,
            idempotent_replay: true,
        });
    }

    let last = catalog
        .node_high_water
        .get(&packet.originating_node)
        .copied()
        .unwrap_or(0);
    if packet.origin_sequence <= last {
        return Err(TransferError::OutOfOrderSequence {
            node: packet.originating_node.clone(),
            got: packet.origin_sequence,
            last,
        });
    }

    let collection = catalog
        .collections
        .entry(packet.collection.clone())
        .or_default();

    match &packet.mutation {
        MutationType::AddField {
            field_name,
            field_type,
            nullable,
        } => {
            if collection.fields.contains_key(field_name) {
                return Err(TransferError::FieldAlreadyExists(field_name.clone()));
            }
            collection.fields.insert(
                field_name.clone(),
                FieldSchema {
                    field_type: *field_type,
                    nullable: *nullable,
                    first_seen_ns: packet.first_seen_ns,
                    introduced_by_packet: packet.packet_id.clone(),
                },
            );
        }
        MutationType::DropField { field_name } => {
            if collection.fields.remove(field_name).is_none() {
                return Err(TransferError::FieldNotFound(field_name.clone()));
            }
        }
        MutationType::RenameField { from, to } => {
            if collection.fields.contains_key(to) {
                return Err(TransferError::RenameTargetExists(to.clone()));
            }
            let Some(existing) = collection.fields.remove(from) else {
                return Err(TransferError::FieldNotFound(from.clone()));
            };
            collection.fields.insert(to.clone(), existing);
        }
    }

    catalog.applied_packets.insert(packet.packet_id.clone());
    catalog
        .node_high_water
        .insert(packet.originating_node.clone(), packet.origin_sequence);
    Ok(AbsorbOutcome {
        applied: true,
        idempotent_replay: false,
    })
}

fn validate_packet(packet: &TransferPacket) -> Result<(), TransferError> {
    if packet.collection.trim().is_empty() {
        return Err(TransferError::EmptyCollection);
    }
    if packet.originating_node.trim().is_empty() {
        return Err(TransferError::EmptyOriginatingNode);
    }
    if packet.packet_id.trim().is_empty() {
        return Err(TransferError::EmptyPacketId);
    }
    Ok(())
}

/// Plan packets a node should absorb this sync tick.
///
/// Packets are selected when:
/// - not already applied by this node, and
/// - origin sequence is above node high-water for the packet's originating node.
///
/// Output ordering is deterministic: by `(originating_node, origin_sequence, packet_id)`.
pub fn plan_sync_packets_for_node(
    catalog: &SchemaCatalog,
    incoming_packets: &[TransferPacket],
) -> Vec<TransferPacket> {
    let mut out: Vec<TransferPacket> = incoming_packets
        .iter()
        .filter(|p| !catalog.applied_packets.contains(&p.packet_id))
        .filter(|p| {
            let last = catalog
                .node_high_water
                .get(&p.originating_node)
                .copied()
                .unwrap_or(0);
            p.origin_sequence > last
        })
        .cloned()
        .collect();
    out.sort_by(|a, b| {
        a.originating_node
            .cmp(&b.originating_node)
            .then(a.origin_sequence.cmp(&b.origin_sequence))
            .then(a.packet_id.cmp(&b.packet_id))
    });
    out
}

/// Apply a sync batch after planning.
///
/// Stops on first absorption error to preserve deterministic safety.
pub fn absorb_sync_batch(
    catalog: &mut SchemaCatalog,
    packets: &[TransferPacket],
) -> Result<SyncApplyResult, TransferError> {
    let mut result = SyncApplyResult::default();
    for packet in packets {
        let out = absorb_transfer_packet(catalog, packet)?;
        if out.applied {
            result.applied = result.applied.saturating_add(1);
        } else if out.idempotent_replay {
            result.replayed = result.replayed.saturating_add(1);
        }
    }
    Ok(result)
}

/// Build a schema compatibility view for a client/app version.
///
/// Requested fields absent in local schema are marked `Absent`, allowing
/// old and new app versions to coexist gracefully while packets propagate.
pub fn schema_view_for_requested_fields(
    catalog: &SchemaCatalog,
    collection: &str,
    requested_fields: &[String],
) -> SchemaCoexistenceView {
    let mut view = SchemaCoexistenceView::default();
    let known = catalog
        .collections
        .get(collection)
        .map(|c| &c.fields)
        .cloned()
        .unwrap_or_default();
    for field in requested_fields {
        let state = if known.contains_key(field) {
            FieldCoexistenceState::Present
        } else {
            FieldCoexistenceState::Absent
        };
        view.fields.insert(field.clone(), state);
    }
    view
}

/// Queue packets while partitioned/offline and keep deterministic order.
pub fn buffer_partition_packets(buffer: &mut PartitionBuffer, packets: &[TransferPacket]) {
    buffer.queued_packets.extend_from_slice(packets);
    buffer.queued_packets.sort_by(|a, b| {
        a.originating_node
            .cmp(&b.originating_node)
            .then(a.origin_sequence.cmp(&b.origin_sequence))
            .then(a.packet_id.cmp(&b.packet_id))
    });
}

/// Produce a catch-up plan for delayed nodes from a partition buffer.
pub fn plan_delayed_node_catchup(
    catalog: &SchemaCatalog,
    buffer: &PartitionBuffer,
) -> Vec<TransferPacket> {
    plan_sync_packets_for_node(catalog, &buffer.queued_packets)
}

/// Replay safety helper for transport retries.
///
/// Returns `true` when a packet should be processed for first time.
pub fn should_process_replay_packet(guard: &mut ReplayGuard, packet_id: &str) -> bool {
    if guard.seen_packet_ids.contains(packet_id) {
        return false;
    }
    guard.seen_packet_ids.insert(packet_id.to_string());
    true
}

#[cfg(test)]
mod tests {
    use super::{
        absorb_sync_batch, absorb_transfer_packet, buffer_partition_packets,
        plan_delayed_node_catchup, plan_sync_packets_for_node, schema_view_for_requested_fields,
        should_process_replay_packet, FieldCoexistenceState, FieldType, MutationType,
        PartitionBuffer, ReplayGuard, SchemaCatalog, TransferError, TransferPacket,
    };

    fn packet(id: &str, seq: u64, mutation: MutationType) -> TransferPacket {
        TransferPacket {
            packet_id: id.to_string(),
            collection: "users".into(),
            mutation,
            first_seen_ns: 123,
            originating_node: "node-1".into(),
            origin_sequence: seq,
        }
    }

    #[test]
    fn absorb_add_field_packet() {
        let mut catalog = SchemaCatalog::default();
        let p = packet(
            "p1",
            1,
            MutationType::AddField {
                field_name: "loyalty_tier".into(),
                field_type: FieldType::String,
                nullable: true,
            },
        );
        let out = absorb_transfer_packet(&mut catalog, &p).expect("applies");
        assert!(out.applied);
        assert!(catalog
            .collections
            .get("users")
            .expect("users schema")
            .fields
            .contains_key("loyalty_tier"));
    }

    #[test]
    fn absorb_is_idempotent_by_packet_id() {
        let mut catalog = SchemaCatalog::default();
        let p = packet(
            "p1",
            1,
            MutationType::AddField {
                field_name: "loyalty_tier".into(),
                field_type: FieldType::String,
                nullable: true,
            },
        );
        absorb_transfer_packet(&mut catalog, &p).expect("first apply");
        let replay = absorb_transfer_packet(&mut catalog, &p).expect("replay should not fail");
        assert!(!replay.applied);
        assert!(replay.idempotent_replay);
    }

    #[test]
    fn reject_out_of_order_sequence() {
        let mut catalog = SchemaCatalog::default();
        let p1 = packet(
            "p1",
            2,
            MutationType::AddField {
                field_name: "loyalty_tier".into(),
                field_type: FieldType::String,
                nullable: true,
            },
        );
        absorb_transfer_packet(&mut catalog, &p1).expect("first apply");

        let p2 = packet(
            "p2",
            1,
            MutationType::AddField {
                field_name: "status".into(),
                field_type: FieldType::String,
                nullable: true,
            },
        );
        let err = absorb_transfer_packet(&mut catalog, &p2).expect_err("must reject");
        assert!(matches!(err, TransferError::OutOfOrderSequence { .. }));
    }

    #[test]
    fn rename_and_drop_rules_enforced() {
        let mut catalog = SchemaCatalog::default();
        absorb_transfer_packet(
            &mut catalog,
            &packet(
                "p1",
                1,
                MutationType::AddField {
                    field_name: "loyalty_tier".into(),
                    field_type: FieldType::String,
                    nullable: true,
                },
            ),
        )
        .expect("add");

        absorb_transfer_packet(
            &mut catalog,
            &packet(
                "p2",
                2,
                MutationType::RenameField {
                    from: "loyalty_tier".into(),
                    to: "tier".into(),
                },
            ),
        )
        .expect("rename");
        assert!(catalog
            .collections
            .get("users")
            .expect("users")
            .fields
            .contains_key("tier"));

        absorb_transfer_packet(
            &mut catalog,
            &packet(
                "p3",
                3,
                MutationType::DropField {
                    field_name: "tier".into(),
                },
            ),
        )
        .expect("drop");
        assert!(!catalog
            .collections
            .get("users")
            .expect("users")
            .fields
            .contains_key("tier"));
    }

    #[test]
    fn sync_plan_respects_high_water_and_replay_state() {
        let mut catalog = SchemaCatalog::default();
        absorb_transfer_packet(
            &mut catalog,
            &packet(
                "p1",
                1,
                MutationType::AddField {
                    field_name: "email".into(),
                    field_type: FieldType::String,
                    nullable: false,
                },
            ),
        )
        .expect("apply p1");
        let incoming = vec![
            packet(
                "p1",
                1,
                MutationType::AddField {
                    field_name: "email".into(),
                    field_type: FieldType::String,
                    nullable: false,
                },
            ),
            packet(
                "p2",
                2,
                MutationType::AddField {
                    field_name: "loyalty_tier".into(),
                    field_type: FieldType::String,
                    nullable: true,
                },
            ),
        ];
        let plan = plan_sync_packets_for_node(&catalog, &incoming);
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].packet_id, "p2");
    }

    #[test]
    fn sync_batch_applies_packets_in_order() {
        let mut catalog = SchemaCatalog::default();
        let packets = vec![
            packet(
                "p1",
                1,
                MutationType::AddField {
                    field_name: "email".into(),
                    field_type: FieldType::String,
                    nullable: false,
                },
            ),
            packet(
                "p2",
                2,
                MutationType::RenameField {
                    from: "email".into(),
                    to: "contact_email".into(),
                },
            ),
        ];
        let result = absorb_sync_batch(&mut catalog, &packets).expect("batch");
        assert_eq!(result.applied, 2);
        assert_eq!(result.replayed, 0);
        assert!(catalog
            .collections
            .get("users")
            .expect("users")
            .fields
            .contains_key("contact_email"));
    }

    #[test]
    fn coexistence_view_marks_unknown_fields_absent() {
        let mut catalog = SchemaCatalog::default();
        absorb_transfer_packet(
            &mut catalog,
            &packet(
                "p1",
                1,
                MutationType::AddField {
                    field_name: "name".into(),
                    field_type: FieldType::String,
                    nullable: false,
                },
            ),
        )
        .expect("apply p1");
        let view = schema_view_for_requested_fields(
            &catalog,
            "users",
            &["name".into(), "loyalty_tier".into()],
        );
        assert_eq!(
            view.fields.get("name"),
            Some(&FieldCoexistenceState::Present)
        );
        assert_eq!(
            view.fields.get("loyalty_tier"),
            Some(&FieldCoexistenceState::Absent)
        );
    }

    #[test]
    fn partition_buffer_orders_packets_deterministically() {
        let mut buffer = PartitionBuffer::default();
        let p2 = packet(
            "p2",
            2,
            MutationType::AddField {
                field_name: "b".into(),
                field_type: FieldType::String,
                nullable: true,
            },
        );
        let p1 = packet(
            "p1",
            1,
            MutationType::AddField {
                field_name: "a".into(),
                field_type: FieldType::String,
                nullable: true,
            },
        );
        buffer_partition_packets(&mut buffer, &[p2, p1]);
        assert_eq!(buffer.queued_packets[0].packet_id, "p1");
        assert_eq!(buffer.queued_packets[1].packet_id, "p2");
    }

    #[test]
    fn delayed_node_catchup_filters_already_applied_packets() {
        let mut catalog = SchemaCatalog::default();
        absorb_transfer_packet(
            &mut catalog,
            &packet(
                "p1",
                1,
                MutationType::AddField {
                    field_name: "email".into(),
                    field_type: FieldType::String,
                    nullable: false,
                },
            ),
        )
        .expect("apply p1");
        let mut buffer = PartitionBuffer::default();
        buffer_partition_packets(
            &mut buffer,
            &[
                packet(
                    "p1",
                    1,
                    MutationType::AddField {
                        field_name: "email".into(),
                        field_type: FieldType::String,
                        nullable: false,
                    },
                ),
                packet(
                    "p2",
                    2,
                    MutationType::AddField {
                        field_name: "tier".into(),
                        field_type: FieldType::String,
                        nullable: true,
                    },
                ),
            ],
        );
        let catchup = plan_delayed_node_catchup(&catalog, &buffer);
        assert_eq!(catchup.len(), 1);
        assert_eq!(catchup[0].packet_id, "p2");
    }

    #[test]
    fn replay_guard_deduplicates_retried_packets() {
        let mut guard = ReplayGuard::default();
        assert!(should_process_replay_packet(&mut guard, "packet-1"));
        assert!(!should_process_replay_packet(&mut guard, "packet-1"));
        assert!(should_process_replay_packet(&mut guard, "packet-2"));
    }
}
