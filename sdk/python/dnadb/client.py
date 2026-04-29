"""Python DNA-DB SDK client surfaces with default HTTP transport."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from urllib import error as urllib_error
import urllib.request
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

class DNAdbHttpError(RuntimeError):
    def __init__(self, status: int, payload: Any):
        super().__init__(f"DNADB HTTP error {status}")
        self.status = status
        self.payload = payload


class HttpTransport(Transport):
    def __init__(self, base_url: str, api_key: Optional[str] = None):
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key

    def insert(self, request: InsertRequest[TRecord]) -> TRecord:
        self._request(
            f"/api/collections/{request.collection}/documents",
            request.record,
        )
        return request.record

    def query(self, request: QueryRequest) -> list[TRecord]:
        body: dict[str, Any] = {
            "filter": _query_where_to_filter(request.where),
        }
        if request.order_by is not None:
            body["sort"] = {
                request.order_by.field: 1 if request.order_by.direction == "asc" else -1
            }
        body["limit"] = 1 if request.fetch_one else request.limit
        payload = self._request(
            f"/api/collections/{request.collection}/query",
            body,
        )
        rows = payload.get("rows", [])
        return rows if isinstance(rows, list) else []

    def configure_collection(
        self, request: ConfigureCollectionRequest
    ) -> ConfigureCollectionResult:
        payload = self._request(
            f"/api/collections/{request.collection}/configure",
            {
                "sort_indexes": request.sort_indexes,
                "composite_sort_indexes": [
                    {"fields": [a, b]} for a, b in request.composite_sort_indexes
                ],
                "exact_string_fields": request.exact_string_fields,
            },
        )
        composite_raw = payload.get("composite_sort_indexes", [])
        composite = []
        for item in composite_raw:
            fields = item.get("fields") if isinstance(item, dict) else None
            if isinstance(fields, list) and len(fields) == 2:
                composite.append((str(fields[0]), str(fields[1])))
        return ConfigureCollectionResult(
            collection=str(payload.get("collection", request.collection)),
            sort_indexes=[str(v) for v in payload.get("sort_indexes", [])],
            composite_sort_indexes=composite,
            exact_string_fields=[str(v) for v in payload.get("exact_string_fields", [])],
        )

    def _request(self, path: str, body: Optional[Any] = None) -> dict[str, Any]:
        headers = {"Content-Type": "application/json"}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(
            f"{self._base_url}{path}",
            data=data,
            headers=headers,
            method="POST" if body is not None else "GET",
        )
        try:
            with urllib.request.urlopen(req) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib_error.HTTPError as e:
            try:
                payload = json.loads(e.read().decode("utf-8"))
            except Exception:
                payload = {}
            raise DNAdbHttpError(e.code, payload) from e
        if payload.get("ok") is False:
            raise DNAdbHttpError(200, payload)
        return payload


class DNAdb:
    def __init__(self, config: DNAdbClientConfig, transport: Optional[Transport] = None):
        self._config = config
        self._transport = transport or HttpTransport(_config_base_url(config), config.api_key)

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


def _config_base_url(config: DNAdbClientConfig) -> str:
    return f"http://{config.host}:{config.port}"


def _query_where_to_filter(where: list[WhereClause]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for c in where:
        if c.op == "=":
            out[c.field] = c.value
        elif c.op == "like":
            out[c.field] = {"$regex": str(c.value)}
        elif c.op == "!=":
            out[c.field] = {"$ne": c.value}
        elif c.op == ">":
            out[c.field] = {"$gt": c.value}
        elif c.op == ">=":
            out[c.field] = {"$gte": c.value}
        elif c.op == "<":
            out[c.field] = {"$lt": c.value}
        elif c.op == "<=":
            out[c.field] = {"$lte": c.value}
        else:
            raise ValueError(f"unsupported operator: {c.op}")
    return out
