export type QueryOperator = "=" | "!=" | ">" | ">=" | "<" | "<=" | "like";
export type QueryValue = string | number | boolean;

export const BASE_GRAPHQL_SCHEMA = `# DNA-DB GraphQL baseline schema
scalar JSON

input WhereClauseInput {
  field: String!
  op: String!
  value: JSON!
}

input OrderByInput {
  field: String!
  direction: String
}

type Query {
  collection(
    name: String!
    where: [WhereClauseInput!]
    include: [String!]
    orderBy: OrderByInput
    limit: Int
    fetchOne: Boolean
  ): JSON
}

type Mutation {
  insert(collection: String!, record: JSON!): JSON!
}
`;

export interface GraphqlWhereClause {
  field: string;
  op: QueryOperator;
  value: QueryValue;
}

export interface GraphqlOrderBy {
  field: string;
  direction?: "asc" | "desc";
}

export interface GraphqlCollectionQuery {
  kind: "collectionQuery";
  collection: string;
  where?: GraphqlWhereClause[];
  include?: string[];
  orderBy?: GraphqlOrderBy;
  limit?: number;
  fetchOne?: boolean;
}

export interface GraphqlInsertMutation {
  kind: "insertMutation";
  collection: string;
  record: Record<string, unknown>;
}

export type GraphqlOperation = GraphqlCollectionQuery | GraphqlInsertMutation;

export interface GraphqlSdkClientLike {
  collection<TRecord extends Record<string, unknown>>(name: string): {
    insert(record: TRecord): Promise<TRecord>;
    query(): {
      where(field: string, op: QueryOperator, value: QueryValue): any;
      include(path: string): any;
      orderBy(field: string, direction?: "asc" | "desc"): any;
      limit(n: number): any;
      fetch(): Promise<TRecord[]>;
      fetchOne(): Promise<TRecord | null>;
    };
  };
}

export class GraphqlAdapter {
  constructor(private readonly db: GraphqlSdkClientLike) {}

  async execute(operation: GraphqlOperation): Promise<unknown> {
    if (operation.kind === "insertMutation") {
      return this.db.collection(operation.collection).insert(operation.record);
    }

    let query = this.db.collection(operation.collection).query();
    for (const clause of operation.where ?? []) {
      query = query.where(clause.field, clause.op, clause.value);
    }
    for (const include of operation.include ?? []) {
      query = query.include(include);
    }
    if (operation.orderBy) {
      query = query.orderBy(operation.orderBy.field, operation.orderBy.direction ?? "asc");
    }
    if (operation.limit !== undefined) {
      query = query.limit(operation.limit);
    }
    if (operation.fetchOne) {
      return query.fetchOne();
    }
    return query.fetch();
  }
}
