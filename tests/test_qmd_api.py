import importlib.util
import json
import sqlite3
import subprocess
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]
MAIN_PATH = ROOT / "main.py"


def load_main_module():
    spec = importlib.util.spec_from_file_location("knowledge_viewer_main", MAIN_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def create_qmd_index(index_path: Path) -> None:
    index_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(index_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE documents (
            id INTEGER PRIMARY KEY,
            collection TEXT NOT NULL,
            path TEXT NOT NULL,
            title TEXT NOT NULL,
            hash TEXT NOT NULL,
            created_at TEXT NOT NULL,
            modified_at TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE content (
            hash TEXT PRIMARY KEY,
            doc TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE content_vectors (
            hash TEXT NOT NULL,
            seq INTEGER NOT NULL DEFAULT 0,
            pos INTEGER NOT NULL DEFAULT 0,
            model TEXT NOT NULL,
            embedded_at TEXT NOT NULL,
            PRIMARY KEY (hash, seq)
        )
        """
    )

    docs = [
        (1, "sessions", "C:/docs/session-1.md", "Session 1", "hash-a", "2026-04-07T20:00:00", "2026-04-07T20:00:00", 1),
        (2, "workflow-docs", "C:/docs/workflow-1.md", "Workflow 1", "hash-a", "2026-04-07T20:01:00", "2026-04-07T20:01:00", 1),
        (3, "workflow-docs", "C:/docs/workflow-2.md", "Workflow 2", "hash-b", "2026-04-07T20:02:00", "2026-04-07T20:02:00", 1),
        (4, "rules", "C:/docs/rule-1.md", "Rule 1", "hash-c", "2026-04-07T20:03:00", "2026-04-07T20:03:00", 0),
    ]
    cur.executemany("INSERT INTO documents VALUES (?, ?, ?, ?, ?, ?, ?, ?)", docs)

    content_rows = [
        ("hash-a", "content a", "2026-04-07T20:00:00"),
        ("hash-b", "content b", "2026-04-07T20:01:00"),
        ("hash-c", "content c", "2026-04-07T20:02:00"),
    ]
    cur.executemany("INSERT INTO content VALUES (?, ?, ?)", content_rows)

    vectors = [
        ("hash-a", 0, 0, "embeddinggemma", "2026-04-07T20:05:00"),
        ("hash-a", 1, 1, "embeddinggemma", "2026-04-07T20:05:00"),
        ("hash-b", 0, 0, "embeddinggemma", "2026-04-07T20:05:00"),
        ("orphan-hash", 0, 0, "embeddinggemma", "2026-04-07T20:05:00"),
    ]
    cur.executemany("INSERT INTO content_vectors VALUES (?, ?, ?, ?, ?)", vectors)
    conn.commit()
    conn.close()


def create_knowledge_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE qmd_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            command TEXT NOT NULL,
            mode TEXT NOT NULL,
            status TEXT NOT NULL,
            target_host TEXT,
            requested_by TEXT,
            created_at TEXT NOT NULL,
            started_at TEXT,
            updated_at TEXT NOT NULL,
            finished_at TEXT,
            exit_code INTEGER,
            summary_json TEXT,
            stdout_excerpt TEXT,
            stderr_excerpt TEXT
        )
        """
    )
    cur.execute(
        """
        INSERT INTO qmd_runs (
            command, mode, status, target_host, requested_by,
            created_at, started_at, updated_at, finished_at, exit_code, summary_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "embed",
            "remote",
            "remote_running",
            "bhaclaw",
            "session_end",
            "2026-04-07T20:10:00",
            "2026-04-07T20:10:05",
            "2026-04-07T20:10:06",
            None,
            None,
            json.dumps({"status": "running"}),
        ),
    )
    cur.execute(
        """
        INSERT INTO qmd_runs (
            command, mode, status, target_host, requested_by,
            created_at, started_at, updated_at, finished_at, exit_code, summary_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "update",
            "local",
            "succeeded",
            None,
            "viewer",
            "2026-04-07T19:55:00",
            "2026-04-07T19:55:01",
            "2026-04-07T19:55:03",
            "2026-04-07T19:55:03",
            0,
            json.dumps({"collections": [{"name": "sessions", "new": 1, "updated": 2, "unchanged": 3, "removed": 0}]}),
        ),
    )
    conn.commit()
    conn.close()


def create_index_config(config_path: Path) -> None:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "\n".join(
            [
                "collections:",
                "  sessions:",
                "    path: C:\\Users\\bhara\\dev\\knowledge\\sessions",
                '    pattern: "**/*.md"',
                "    context:",
                '      "": Parsed sessions',
                "  workflow-docs:",
                "    path: C:\\Users\\bhara\\dev",
                '    pattern: "{*.md,*workflow-system/**/*.md}"',
                "    context:",
                '      "": Workflow docs',
                "  rules:",
                "    path: C:\\Users\\bhara\\.claude\\rules",
                '    pattern: "**/*.md"',
                "    context:",
                '      "": Rules',
            ]
        ),
        encoding="utf-8",
    )


def clear_qmd_runs(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("DELETE FROM qmd_runs")
    conn.commit()
    conn.close()


@pytest.fixture()
def client(tmp_path, monkeypatch):
    module = load_main_module()

    knowledge_db = tmp_path / "knowledge.db"
    prompts_db = tmp_path / "prompts.db"
    qmd_home = tmp_path / "home"
    qmd_cache_home = qmd_home / ".cache"
    qmd_index_path = qmd_cache_home / "qmd" / "index.sqlite"
    qmd_config_path = qmd_home / ".config" / "qmd" / "index.yml"

    create_knowledge_db(knowledge_db)
    create_qmd_index(qmd_index_path)
    create_index_config(qmd_config_path)

    monkeypatch.setattr(module, "KNOWLEDGE_DB", knowledge_db)
    monkeypatch.setattr(module, "PROMPTS_DB", prompts_db)
    monkeypatch.setattr(module, "QMD_HOME", qmd_home)
    monkeypatch.setattr(module, "QMD_CACHE_HOME", qmd_cache_home)
    monkeypatch.setattr(module, "index_documents", lambda: {"indexed": 0, "updated": 0, "unchanged": 0, "removed": 0, "errors": []})
    monkeypatch.setattr(module, "QMD_INDEX_PATH", qmd_index_path, raising=False)
    monkeypatch.setattr(module, "QMD_CONFIG_PATH", qmd_config_path, raising=False)

    with TestClient(module.app) as test_client:
        yield test_client, module


def test_qmd_status_reports_live_index_counts_and_jobs(client):
    test_client, _module = client

    response = test_client.get("/api/qmd/status")

    assert response.status_code == 200
    data = response.json()

    assert data["index"]["exists"] is True
    assert data["metrics"]["active_documents"] == 3
    assert data["metrics"]["active_hashes"] == 2
    assert data["metrics"]["embedded_hashes"] == 2
    assert data["metrics"]["pending_hashes"] == 0
    assert data["metrics"]["vector_chunks"] == 3
    assert data["jobs"]["active"]["status"] == "remote_running"

    collections = {row["name"]: row for row in data["collections"]}
    assert collections["sessions"]["documents"] == 1
    assert collections["workflow-docs"]["documents"] == 2
    assert collections["rules"]["documents"] == 0
    assert collections["workflow-docs"]["pattern"] == "{*.md,*workflow-system/**/*.md}"


def test_qmd_runs_returns_recent_rows_with_parsed_summary(client):
    test_client, _module = client

    response = test_client.get("/api/qmd/runs?limit=10")

    assert response.status_code == 200
    data = response.json()

    assert len(data["runs"]) == 2
    assert data["runs"][0]["command"] == "embed"
    assert data["runs"][0]["summary"]["status"] == "running"
    assert data["runs"][1]["command"] == "update"
    assert data["runs"][1]["summary"]["collections"][0]["updated"] == 2


def test_qmd_update_runs_locally_and_records_succeeded_row(client, monkeypatch):
    test_client, module = client
    clear_qmd_runs(module.KNOWLEDGE_DB)

    monkeypatch.setattr(module, "get_qmd_command", lambda *args: ["qmd", *args])

    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout=(
                "Updating 2 collection(s)...\n\n"
                "[1/2] sessions (**/*.md)\n"
                "Collection: C:\\Users\\bhara\\dev\\knowledge\\sessions (**/*.md)\n\n"
                "Indexed: 1 new, 2 updated, 3 unchanged, 0 removed\n\n"
                "[2/2] workflow-docs ({*.md,*workflow-system/**/*.md})\n"
                "Collection: C:\\Users\\bhara\\dev ({*.md,*workflow-system/**/*.md})\n\n"
                "Indexed: 0 new, 1 updated, 5 unchanged, 1 removed\n"
            ),
            stderr="",
        )

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    response = test_client.post("/api/qmd/update")

    assert response.status_code == 200
    payload = response.json()
    assert payload["run"]["command"] == "update"
    assert payload["run"]["status"] == "succeeded"
    assert payload["run"]["summary"]["collections"][0]["name"] == "sessions"
    assert payload["run"]["summary"]["collections"][0]["updated"] == 2

    runs = test_client.get("/api/qmd/runs?limit=5").json()["runs"]
    assert runs[0]["command"] == "update"
    assert runs[0]["status"] == "succeeded"


def test_qmd_embed_enqueues_remote_job_in_shared_ledger(client):
    test_client, module = client
    clear_qmd_runs(module.KNOWLEDGE_DB)

    response = test_client.post("/api/qmd/embed")

    assert response.status_code == 200
    payload = response.json()
    assert payload["run"]["command"] == "embed"
    assert payload["run"]["mode"] == "remote"
    assert payload["run"]["status"] == "queued"
    assert payload["run"]["requested_by"] == "viewer"

    runs = test_client.get("/api/qmd/runs?limit=5").json()["runs"]
    assert runs[0]["command"] == "embed"
    assert runs[0]["status"] == "queued"


def test_qmd_documents_reports_hash_based_embedded_status_and_filters(client):
    test_client, _module = client

    response = test_client.get(
        "/api/qmd/documents?collection=workflow-docs&embedded=yes&q=workflow-1"
    )

    assert response.status_code == 200
    data = response.json()

    assert data["total"] == 1
    assert len(data["documents"]) == 1

    doc = data["documents"][0]
    assert doc["collection"] == "workflow-docs"
    assert doc["title"] == "Workflow 1"
    assert doc["path"] == "C:/docs/workflow-1.md"
    assert doc["hash"] == "hash-a"
    assert doc["embedded"] is True
