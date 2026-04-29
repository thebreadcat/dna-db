"""Python DNA-DB SDK baseline client surfaces (Stage 8)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Generic, Literal, Optional, Protocol, TypeVar

QueryOperator = Literal["=", "!=", ">", ">=", "<", "<=", "like"]
SortDirection = Literal["asc", "desc"]
QueryValue = str | int | float | bool
Record = dict[str, Any]
TRecord = TypeVar("TRecord", bound=Record)


@dataclass(frozen=True)
class DNAdbClientConfig:
    host: str
    port: int
    database: str
    api_key: Optional[str] = None


@dataclass(frozen=True)
class WhereClause:
    field: str
    op: QueryOperator
    value: QueryValue


@dataclass(frozen=True)
class OrderByClause:
    field: str
    direction: SortDirection = "asc"


@dataclass(frozen=True)
class QueryRequest:
    collection: str
    where: list[WhereClause] = field(default_factory=list)
    include: list[str] = field(default_factory=list)
    order_by: Optional[OrderByClause] = None
    limit: Optional[int] = None
    fetch_one: bool = False


@dataclass(frozen=True)
class InsertRequest(Generic[TRecord]):
    collection: str
    record: TRecord


@dataclass(frozen=True)
class ConfigureCollectionRequest:
    collection: str
    sort_indexes: list[str]
    composite_sort_indexes: list[tuple[str, str]] = field(default_factory=list)
    exact_string_fields: Optional[list[str]] = None


@dataclass(frozen=True)
class ConfigureCollectionResult:
    collection: str
    sort_indexes: list[str]
    composite_sort_indexes: list[tuple[str, str]] = field(default_factory=list)
    exact_string_fields: list[str] = field(default_factory=list)


class Transport(Protocol):
    def insert(self, request: InsertRequest[TRecord]) -> TRecord: ...
    def query(self, request: QueryRequest) -> list[TRecord]: ...
    def configure_collection(
        self, request: ConfigureCollectionRequest
    ) -> ConfigureCollectionResult: ...


class NotImplementedTransport(Transport):
    def insert(self, request: InsertRequest[TRecord]) -> TRecord:
        raise RuntimeError("DNADB transport not configured: insert is unavailable.")

    def query(self, request: QueryRequest) -> list[TRecord]:
        raise RuntimeError("DNADB transport not configured: query is unavailable.")

    def configure_collection(
        self, request: ConfigureCollectionRequest
    ) -> ConfigureCollectionResult:
        raise RuntimeError(
            "DNADB transport not configured: configure_collection is unavailable."
        )


class DNAdb:
    def __init__(self, config: DNAdbClientConfig, transport: Optional[Transport] = None):
        self._config = config
        self._transport = transport or NotImplementedTransport()

    @property
    def config(self) -> DNAdbClientConfig:
        return self._config

    def collection(self, name: str) -> "CollectionClient[Record]":
        return CollectionClient(name, self._transport)


class CollectionClient(Generic[TRecord]):
    def __init__(self, collection_name: str, transport: Transport):
        self._collection_name = collection_name
        self._transport = transport

    def insert(self, record: TRecord) -> TRecord:
        request: InsertRequest[TRecord] = InsertRequest(
            collection=self._collection_name, record=record
        )
        return self._transport.insert(request)

    def where(self, field: str, op: QueryOperator, value: QueryValue) -> "QueryBuilder[TRecord]":
        return QueryBuilder(self._collection_name, self._transport).where(field, op, value)

    def query(self) -> "QueryBuilder[TRecord]":
        return QueryBuilder(self._collection_name, self._transport)

    def configure(
        self,
        *,
        sort_indexes: list[str],
        composite_sort_indexes: Optional[list[tuple[str, str]]] = None,
        exact_string_fields: Optional[list[str]] = None,
    ) -> ConfigureCollectionResult:
        if not sort_indexes:
            raise ValueError("sort_indexes must contain at least one field")
        return self._transport.configure_collection(
            ConfigureCollectionRequest(
                collection=self._collection_name,
                sort_indexes=list(sort_indexes),
                composite_sort_indexes=list(composite_sort_indexes or []),
                exact_string_fields=list(exact_string_fields)
                if exact_string_fields is not None
                else None,
            )
        )


class QueryBuilder(Generic[TRecord]):
    def __init__(self, collection_name: str, transport: Transport):
        self._collection_name = collection_name
        self._transport = transport
        self._where_clauses: list[WhereClause] = []
        self._include_paths: list[str] = []
        self._order_by: Optional[OrderByClause] = None
        self._limit: Optional[int] = None

    def where(self, field: str, op: QueryOperator, value: QueryValue) -> "QueryBuilder[TRecord]":
        self._where_clauses.append(WhereClause(field=field, op=op, value=value))
        return self

    def include(self, path: str) -> "QueryBuilder[TRecord]":
        self._include_paths.append(path)
        return self

    def order_by(self, field: str, direction: SortDirection = "asc") -> "QueryBuilder[TRecord]":
        self._order_by = OrderByClause(field=field, direction=direction)
        return self

    def limit(self, n: int) -> "QueryBuilder[TRecord]":
        if n <= 0:
            raise ValueError("limit must be a positive integer")
        self._limit = int(n)
        return self

    def fetch(self) -> list[TRecord]:
        return self._transport.query(self._to_query_request(fetch_one=False))

    def fetch_one(self) -> Optional[TRecord]:
        rows = self._transport.query(self._to_query_request(fetch_one=True))
        return rows[0] if rows else None

    def _to_query_request(self, fetch_one: bool) -> QueryRequest:
        return QueryRequest(
            collection=self._collection_name,
            where=list(self._where_clauses),
            include=list(self._include_paths),
            order_by=self._order_by,
            limit=self._limit,
            fetch_one=fetch_one,
        )
