import unittest

from dnadb import DNAdb, DNAdbClientConfig
from dnadb.client import InsertRequest, QueryRequest, Transport


class InMemoryTransport(Transport):
    def __init__(self) -> None:
        self.insert_calls: list[InsertRequest] = []
        self.query_calls: list[QueryRequest] = []
        self.query_result: list[dict] = []

    def insert(self, request: InsertRequest):
        self.insert_calls.append(request)
        return request.record

    def query(self, request: QueryRequest):
        self.query_calls.append(request)
        return self.query_result


class PythonSdkClientTests(unittest.TestCase):
    def test_insert_routes_to_transport(self):
        transport = InMemoryTransport()
        db = DNAdb(
            DNAdbClientConfig(host="127.0.0.1", port=27017, database="app"),
            transport=transport,
        )
        users = db.collection("users")
        row = {"email": "a@b.com"}
        inserted = users.insert(row)
        self.assertEqual(inserted, row)
        self.assertEqual(len(transport.insert_calls), 1)
        self.assertEqual(transport.insert_calls[0].collection, "users")

    def test_query_builder_builds_expected_request(self):
        transport = InMemoryTransport()
        transport.query_result = [{"email": "a@b.com"}]
        db = DNAdb(
            DNAdbClientConfig(host="127.0.0.1", port=27017, database="app"),
            transport=transport,
        )
        row = (
            db.collection("users")
            .query()
            .where("email", "=", "a@b.com")
            .include("profile")
            .order_by("created_at", "desc")
            .limit(5)
            .fetch_one()
        )
        self.assertEqual(row, {"email": "a@b.com"})
        self.assertEqual(len(transport.query_calls), 1)
        call = transport.query_calls[0]
        self.assertEqual(call.collection, "users")
        self.assertTrue(call.fetch_one)
        self.assertEqual(call.limit, 5)
        self.assertEqual(call.include, ["profile"])
        self.assertEqual(call.where[0].field, "email")
        self.assertEqual(call.where[0].op, "=")
        self.assertEqual(call.order_by.field, "created_at")
        self.assertEqual(call.order_by.direction, "desc")

    def test_limit_validation_rejects_non_positive(self):
        transport = InMemoryTransport()
        db = DNAdb(
            DNAdbClientConfig(host="127.0.0.1", port=27017, database="app"),
            transport=transport,
        )
        builder = db.collection("users").query()
        with self.assertRaises(ValueError):
            builder.limit(0)


if __name__ == "__main__":
    unittest.main()
