import os
import unittest

from dnadb import DNAdb, DNAdbClientConfig


@unittest.skipUnless(os.getenv("DNADB_URL"), "DNADB_URL not set")
class PythonSdkIntegrationTests(unittest.TestCase):
    def test_insert_query_and_configure(self):
        url = os.environ["DNADB_URL"]
        host_port = url.replace("http://", "").replace("https://", "")
        host, port = host_port.split(":", 1)
        db = DNAdb(
            DNAdbClientConfig(
                host=host,
                port=int(port),
                database="default",
            )
        )
        coll = db.collection("sdk_test_py")
        coll.insert({"id": 1, "title": "Hello", "status": "published", "updated_at": 1})
        rows = coll.where("status", "=", "published").limit(10).fetch()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["title"], "Hello")

        out = coll.configure(sort_indexes=["updated_at"], exact_string_fields=["status"])
        self.assertIn("updated_at", out.sort_indexes)


if __name__ == "__main__":
    unittest.main()
