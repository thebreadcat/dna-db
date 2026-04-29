export interface DNAdbClientConfig {
  host: string;
  port: number;
  database: string;
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
    this.transport = transport ?? new NotImplementedTransport();
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
