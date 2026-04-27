class DemoDb {
  constructor() {
    this.collections = new Map();
    this.overlayPolicies = new Map();
    this.activeRole = "admin";
  }

  collection(name) {
    if (!this.collections.has(name)) {
      this.collections.set(name, []);
    }
    return new DemoCollection(this, name);
  }

  as(role) {
    const scoped = new DemoDb();
    scoped.collections = this.collections;
    scoped.overlayPolicies = this.overlayPolicies;
    scoped.activeRole = role;
    return scoped;
  }

  setOverlay(collection, role, field, maskFn) {
    const key = `${collection}:${role}:${field}`;
    this.overlayPolicies.set(key, maskFn);
  }
}

class DemoCollection {
  constructor(db, name) {
    this.db = db;
    this.name = name;
  }

  async insert(record) {
    this.db.collections.get(this.name).push(record);
    return record;
  }

  async insertMany(records) {
    const rows = this.db.collections.get(this.name);
    rows.push(...records);
    return records.length;
  }

  query() {
    return new DemoQuery(this.db, this.name);
  }

  where(field, op, value) {
    return this.query().where(field, op, value);
  }

  async fetch() {
    return this.query().fetch();
  }
}

class DemoQuery {
  constructor(db, name) {
    this.db = db;
    this.name = name;
    this.clauses = [];
    this.limitValue = undefined;
    this.orderByField = undefined;
    this.orderByDirection = "asc";
  }

  where(field, op, value) {
    this.clauses.push({ field, op, value });
    return this;
  }

  orderBy(field, direction = "asc") {
    this.orderByField = field;
    this.orderByDirection = direction;
    return this;
  }

  limit(n) {
    this.limitValue = n;
    return this;
  }

  async fetch() {
    const rows = this.db.collections.get(this.name) || [];
    let out = rows.filter((row) => this.clauses.every((clause) => matchClause(row, clause)));
    if (this.orderByField) {
      out.sort((a, b) => compareValues(a[this.orderByField], b[this.orderByField]));
      if (this.orderByDirection === "desc") {
        out.reverse();
      }
    }
    if (this.limitValue !== undefined) {
      out = out.slice(0, this.limitValue);
    }
    return out.map((row) => applyOverlay(this.db, this.name, row));
  }
}

function compareValues(a, b) {
  if (a === b) return 0;
  return a > b ? 1 : -1;
}

function matchClause(row, clause) {
  const value = row[clause.field];
  if (clause.op === "=") return value === clause.value;
  if (clause.op === "!=") return value !== clause.value;
  if (clause.op === ">") return value > clause.value;
  if (clause.op === ">=") return value >= clause.value;
  if (clause.op === "<") return value < clause.value;
  if (clause.op === "<=") return value <= clause.value;
  if (clause.op === "like") {
    const needle = String(clause.value).replace(/%/g, "");
    return String(value).toLowerCase().includes(needle.toLowerCase());
  }
  return false;
}

function applyOverlay(db, collection, row) {
  const copy = { ...row };
  for (const [key, maskFn] of db.overlayPolicies.entries()) {
    const [policyCollection, role, field] = key.split(":");
    if (policyCollection === collection && role === db.activeRole && field in copy) {
      copy[field] = maskFn(copy[field]);
    }
  }
  return copy;
}

function timed(label, fn) {
  const start = process.hrtime.bigint();
  const result = fn();
  const end = process.hrtime.bigint();
  const ms = Number(end - start) / 1e6;
  return { label, ms, result };
}

module.exports = {
  DemoDb,
  timed,
};
