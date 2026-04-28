import importlib.util
import tempfile
import unittest
from pathlib import Path


def load_admin_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "admin_inspect.py"
    spec = importlib.util.spec_from_file_location("admin_inspect", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class AdminInspectTests(unittest.TestCase):
    def setUp(self):
        self.mod = load_admin_module()

    def _mk_repo(self, progress_text: str) -> Path:
        tmp = Path(tempfile.mkdtemp())
        (tmp / "progress.md").write_text(progress_text, encoding="utf-8")
        (tmp / "sdk" / "typescript" / "src").mkdir(parents=True)
        (tmp / "sdk" / "typescript" / "src" / "index.ts").write_text("// test", encoding="utf-8")
        (tmp / "sdk" / "python" / "dnadb").mkdir(parents=True)
        (tmp / "sdk" / "python" / "dnadb" / "client.py").write_text("# test", encoding="utf-8")
        return tmp

    def test_stage_status_parsing(self):
        repo = self._mk_repo(
            "- [x] Stage 1 - A\n"
            "- [ ] Stage 2 - B\n"
            "- [x] Stage 3 - C\n"
        )
        payload = self.mod.inspect_stage_status(repo)
        self.assertEqual(payload["completed_stages"], 2)
        self.assertEqual(payload["total_stages"], 3)

    def test_completion_row_parsing(self):
        repo = self._mk_repo(
            "| **Stage 1** (x) | **100%** | **0%** |\n"
            "| **Full staged roadmap** (x) | **50%** | **50%** |\n"
        )
        payload = self.mod.inspect_build_completion(repo)
        self.assertEqual(len(payload["rows"]), 2)
        self.assertEqual(payload["rows"][0]["scope"], "Stage 1")

    def test_sdk_status_detects_entries(self):
        repo = self._mk_repo("- [x] Stage 1 - A\n")
        payload = self.mod.inspect_sdk_status(repo)
        self.assertTrue(payload["typescript_sdk_present"])
        self.assertTrue(payload["python_sdk_present"])

    def test_inspect_all_without_progress_md(self):
        """Public CI clones omit gitignored progress.md — inspect must not crash."""
        root = Path(tempfile.mkdtemp())
        (root / "sdk" / "typescript" / "src").mkdir(parents=True)
        (root / "sdk" / "typescript" / "src" / "index.ts").write_text("// test", encoding="utf-8")
        (root / "sdk" / "python" / "dnadb").mkdir(parents=True)
        (root / "sdk" / "python" / "dnadb" / "client.py").write_text("# test", encoding="utf-8")
        payload = self.mod.inspect_all(root)
        self.assertEqual(payload["stage_status"]["progress_md"], "missing")
        self.assertEqual(payload["stage_status"]["total_stages"], 0)
        self.assertEqual(payload["build_completion"]["progress_md"], "missing")
        self.assertEqual(payload["build_completion"]["rows"], [])


if __name__ == "__main__":
    unittest.main()
