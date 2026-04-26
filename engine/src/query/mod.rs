//! Query AST and guide-pattern compilation (CRISPR query engine, Stage 2).

pub mod ast;
pub mod compile;
pub mod fast_match;
pub mod fetch;
pub mod guide;
pub mod index;
pub mod index_lifecycle;
pub mod planner;
pub mod scan;
pub mod segment;
pub mod vector;

pub use ast::{OrderByClause, QueryAst, QueryLiteral, SortDirection, WhereClause, WhereOp};
pub use compile::{compile_query, CompileError};
pub use fast_match::{
    clause_introns_fast_match, guide_introns_fast_match, intron_hash_matches_field,
};
pub use fetch::{
    fetch, fetch_one, fetch_with_includes, resolve_include_path, FetchResult, FetchRow,
    FetchWithIncludes,
};
pub use guide::{Clause, GuidePattern, RangeOp};
pub use index::{
    build_exact_hash_index, exact_lookup_signatures, GlobalHashIndex, GlobalHashIndexError,
    GlobalRangeIndex,
};
pub use index_lifecycle::{GlobalIndexCatalog, GlobalIndexCatalogError};
pub use planner::{choose_path, CollectionStats, QueryPath};
pub use scan::{
    scan_strands, scan_strands_parallel, scan_strands_sequential, ScanConfig,
    PARALLEL_STRAND_THRESHOLD,
};
pub use segment::{should_skip_segment, BloomFilter, FieldStats, SegmentMeta};
pub use vector::{filter_rows_vectorized, DEFAULT_VECTOR_BATCH_SIZE};
