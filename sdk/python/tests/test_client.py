import unittest

from dnadb import DNAdb, DNAdbClientConfig
from dnadb.client import (
    ConfigureCollectionRequest,
    ConfigureCollectionResult,
    InsertRequest,
    QueryRequest,
    Transport,
)


class InMemoryTransport(Transport):
    def __init__(self) -> None:
        self.insert_calls: list[InsertRequest] = []
        self.query_calls: list[QueryRequest] = []
        self.configure_calls: list[ConfigureCollectionRequest] = []
        self.query_result: list[dict] = []

    def insert(self, request: InsertRequest):
        self.insert_calls.append(request)
        return request.record

    def query(self, request: QueryRequest):
        self.query_calls.append(request)
        return self.query_result

    def configure_collection(self, request: ConfigureCollectionRequest):
        self.configure_calls.append(request)
        return ConfigureCollectionResult(
            collection=request.collection,
            sort_indexes=list(request.sort_indexes),
            composite_sort_indexes=list(request.composite_sort_indexes),
            exact_string_fields=list(request.exact_string_fields or []),
        )


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

    def test_collection_configure_routes_to_transport(self):
        transport = InMemoryTransport()
        db = DNAdb(
            DNAdbClientConfig(host="127.0.0.1", port=27017, database="app"),
            transport=transport,
        )
        result = db.collection("products").configure(
            sort_indexes=["price", "created_at", "rating"],
            composite_sort_indexes=[("status", "updated_at")],
            exact_string_fields=["sku", "title"],
        )
        self.assertEqual(result.collection, "products")
        self.assertEqual(result.sort_indexes, ["price", "created_at", "rating"])
        self.assertEqual(result.composite_sort_indexes, [("status", "updated_at")])
        self.assertEqual(result.exact_string_fields, ["sku", "title"])
        self.assertEqual(len(transport.configure_calls), 1)
        self.assertEqual(transport.configure_calls[0].collection, "products")


if __name__ == "__main__":
    unittest.main()
