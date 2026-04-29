export interface DNAdbClientConfig {
  host?: string;
  port?: number;
  database?: string;
  url?: string;
  apiKey?: string;
}

export type QueryOperator = "=" | "!=" | ">" | ">=" | "<" | "<=" | "like";

export type QueryValue = string | number | boolean;

export interface WhereClause {
  field: string;
  op: QueryOperator;
  value: QueryValue;
}

export interface OrderByClause {
  field: string;
  direction: "asc" | "desc";
}

export interface QueryRequest {
  collection: string;
  where: WhereClause[];
  include: string[];
  orderBy?: OrderByClause;
  limit?: number;
  fetchOne?: boolean;
}

export interface InsertRequest<TRecord extends Record<string, unknown>> {
  collection: string;
  record: TRecord;
}

export interface ConfigureCollectionRequest {
  collection: string;
  sortIndexes: string[];
  compositeSortIndexes?: [string, string][];
  exactStringFields?: string[];
}

export interface ConfigureCollectionResult {
  collection: string;
  sortIndexes: string[];
  compositeSortIndexes: [string, string][];
  exactStringFields: string[];
}

export interface Transport {
  insert<TRecord extends Record<string, unknown>>(request: InsertRequest<TRecord>): Promise<TRecord>;
  query<TRecord extends Record<string, unknown>>(request: QueryRequest): Promise<TRecord[]>;
  configureCollection(
    request: ConfigureCollectionRequest
  ): Promise<ConfigureCollectionResult>;
}

export class DNAdbHttpError extends Error {
  constructor(public readonly status: number, public readonly payload: unknown) {
    super(`DNADB HTTP error ${status}`);
  }
}

type HttpTransportOptions = {
  url: string;
  apiKey?: string;
};

export class HttpTransport implements Transport {
  private readonly baseUrl: string;
  private readonly apiKey?: string;

  constructor(options: HttpTransportOptions) {
    this.baseUrl = options.url.replace(/\/$/, "");
    this.apiKey = options.apiKey;
  }

  async insert<TRecord extends Record<string, unknown>>(
    request: InsertRequest<TRecord>
  ): Promise<TRecord> {
    await this.request(`/api/collections/${encodeURIComponent(request.collection)}/documents`, request.record);
    return request.record;
  }

  async query<TRecord extends Record<string, unknown>>(request: QueryRequest): Promise<TRecord[]> {
    const body: Record<string, unknown> = {
      filter: queryWhereToFilter(request.where),
    };
    if (request.orderBy) {
      body.sort = { [request.orderBy.field]: request.orderBy.direction === "asc" ? 1 : -1 };
    }
    body.limit = request.fetchOne ? 1 : request.limit;
    const out = await this.request(
      `/api/collections/${encodeURIComponent(request.collection)}/query`,
      body
    );
    const rows = (out as { rows?: TRecord[] }).rows;
    return Array.isArray(rows) ? rows : [];
  }

  async configureCollection(
    request: ConfigureCollectionRequest
  ): Promise<ConfigureCollectionResult> {
    const out = await this.request(
      `/api/collections/${encodeURIComponent(request.collection)}/configure`,
      {
        sort_indexes: request.sortIndexes,
        composite_sort_indexes: request.compositeSortIndexes?.map(([a, b]) => ({ fields: [a, b] })),
        exact_string_fields: request.exactStringFields,
      }
    );
    const payload = out as Record<string, unknown>;
    const composite = (payload.composite_sort_indexes as { fields: [string, string] }[] | undefined) ?? [];
    return {
      collection: String(payload.collection ?? request.collection),
      sortIndexes: ((payload.sort_indexes as string[] | undefined) ?? []).map(String),
      compositeSortIndexes: composite.map((c) => [c.fields[0], c.fields[1]]),
      exactStringFields: ((payload.exact_string_fields as string[] | undefined) ?? []).map(String),
    };
  }

  private async request(path: string, body?: unknown): Promise<unknown> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };
    if (this.apiKey) {
      headers["Authorization"] = `Bearer ${this.apiKey}`;
    }
    const response = await fetch(`${this.baseUrl}${path}`, {
      method: body === undefined ? "GET" : "POST",
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const payload = await response
      .json()
      .catch(() => ({} as Record<string, unknown>));
    if (!response.ok || ((payload as { ok?: boolean }).ok === false)) {
      throw new DNAdbHttpError(response.status, payload);
    }
    return payload;
  }
}

export class NotImplementedTransport implements Transport {
  async insert<TRecord extends Record<string, unknown>>(
    _request: InsertRequest<TRecord>
  ): Promise<TRecord> {
    throw new Error("DNADB transport not configured: insert is unavailable.");
  }

  async query<TRecord extends Record<string, unknown>>(_request: QueryRequest): Promise<TRecord[]> {
    throw new Error("DNADB transport not configured: query is unavailable.");
  }

  async configureCollection(
    _request: ConfigureCollectionRequest
  ): Promise<ConfigureCollectionResult> {
    throw new Error("DNADB transport not configured: configureCollection is unavailable.");
  }
}

export class DNAdb {
  private readonly transport: Transport;

  constructor(private readonly config: DNAdbClientConfig, transport?: Transport) {
    this.transport = transport ?? new HttpTransport({
      url: resolveBaseUrl(config),
      apiKey: config.apiKey,
    });
  }

  getConfig(): DNAdbClientConfig {
    return this.config;
  }

  collection<TRecord extends Record<string, unknown>>(name: string): CollectionClient<TRecord> {
    return new CollectionClient<TRecord>(name, this.transport);
  }
}

export class CollectionClient<TRecord extends Record<string, unknown>> {
  constructor(
    private readonly collectionName: string,
    private readonly transport: Transport
  ) {}

  async insert(record: TRecord): Promise<TRecord> {
    return this.transport.insert<TRecord>({
      collection: this.collectionName,
      record,
    });
  }

  async configure(config: {
    sortIndexes: string[];
    compositeSortIndexes?: [string, string][];
    exactStringFields?: string[];
  }): Promise<ConfigureCollectionResult> {
    return this.transport.configureCollection({
      collection: this.collectionName,
      sortIndexes: [...config.sortIndexes],
      compositeSortIndexes: config.compositeSortIndexes
        ? [...config.compositeSortIndexes]
        : undefined,
      exactStringFields: config.exactStringFields
        ? [...config.exactStringFields]
        : undefined,
    });
  }

  where(field: string, op: QueryOperator, value: QueryValue): QueryBuilder<TRecord> {
    return new QueryBuilder<TRecord>(this.collectionName, this.transport).where(field, op, value);
  }

  query(): QueryBuilder<TRecord> {
    return new QueryBuilder<TRecord>(this.collectionName, this.transport);
  }
}

export class QueryBuilder<TRecord extends Record<string, unknown>> {
  private readonly whereClauses: WhereClause[] = [];
  private readonly includePaths: string[] = [];
  private orderByClause: OrderByClause | undefined;
  private limitValue: number | undefined;

  constructor(
    private readonly collectionName: string,
    private readonly transport: Transport
  ) {}

  where(field: string, op: QueryOperator, value: QueryValue): this {
    this.whereClauses.push({ field, op, value });
    return this;
  }

  include(path: string): this {
    this.includePaths.push(path);
    return this;
  }

  orderBy(field: string, direction: "asc" | "desc" = "asc"): this {
    this.orderByClause = { field, direction };
    return this;
  }

  limit(n: number): this {
    if (!Number.isFinite(n) || n <= 0) {
      throw new Error("limit must be a positive number");
    }
    this.limitValue = Math.floor(n);
    return this;
  }

  async fetch(): Promise<TRecord[]> {
    return this.transport.query<TRecord>(this.toQueryRequest(false));
  }

  async fetchOne(): Promise<TRecord | null> {
    const rows = await this.transport.query<TRecord>(this.toQueryRequest(true));
    return rows[0] ?? null;
  }

  private toQueryRequest(fetchOne: boolean): QueryRequest {
    return {
      collection: this.collectionName,
      where: [...this.whereClauses],
      include: [...this.includePaths],
      orderBy: this.orderByClause,
      limit: this.limitValue,
      fetchOne,
    };
  }
}

export {
  BASE_GRAPHQL_SCHEMA,
  GraphqlAdapter,
  type GraphqlCollectionQuery,
  type GraphqlInsertMutation,
  type GraphqlOperation,
  type GraphqlOrderBy,
  type GraphqlSdkClientLike,
  type GraphqlWhereClause,
} from "./graphql.js";

function resolveBaseUrl(config: DNAdbClientConfig): string {
  if (config.url && config.url.trim() !== "") {
    return config.url;
  }
  const host = config.host ?? "127.0.0.1";
  const port = config.port ?? 8787;
  return `http://${host}:${port}`;
}

function queryWhereToFilter(where: WhereClause[]): Record<string, unknown> {
  const filter: Record<string, unknown> = {};
  for (const clause of where) {
    switch (clause.op) {
      case "=":
        filter[clause.field] = clause.value;
        break;
      case "like":
        filter[clause.field] = { $regex: String(clause.value) };
        break;
      case "!=":
        filter[clause.field] = { $ne: clause.value };
        break;
      case ">":
        filter[clause.field] = { $gt: clause.value };
        break;
      case ">=":
        filter[clause.field] = { $gte: clause.value };
        break;
      case "<":
        filter[clause.field] = { $lt: clause.value };
        break;
      case "<=":
        filter[clause.field] = { $lte: clause.value };
        break;
      default:
        throw new Error(`unsupported query operator: ${String(clause.op)}`);
    }
  }
  return filter;
}
