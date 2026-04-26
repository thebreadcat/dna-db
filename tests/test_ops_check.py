import importlib.util
import tempfile
import unittest
from pathlib import Path


def load_ops_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "ops_check.py"
    spec = importlib.util.spec_from_file_location("ops_check", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class OpsCheckTests(unittest.TestCase):
    def setUp(self):
        self.mod = load_ops_module()

    def test_required_files_check_reports_missing(self):
        root = Path(tempfile.mkdtemp())
        payload = self.mod.check_required_files(root)
        self.assertFalse(payload["ok"])
        self.assertEqual(len(payload["files"]), 3)

    def test_required_files_check_reports_present(self):
        root = Path(tempfile.mkdtemp())
        (root / "config").mkdir(parents=True)
        (root / "docs").mkdir(parents=True)
        (root / "docker-compose.observability.yml").write_text("version: '3.9'\n", encoding="utf-8")
        (root / "config" / "dnadb.config.toml.example").write_text("", encoding="utf-8")
        (root / "docs" / "DEPLOYMENT_MONITORING.md").write_text("", encoding="utf-8")
        payload = self.mod.check_required_files(root)
        self.assertTrue(payload["ok"])


if __name__ == "__main__":
    unittest.main()
