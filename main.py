"""
Knowledge Viewer - FastAPI backend for browsing knowledge database.
# v2: two-dimensional visibility (lifecycle x type)

Serves a single-file frontend and API endpoints for:
- Sessions
- Errors + Resolutions
- Decisions
- Prompts
"""

import asyncio
import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, UploadFile, File, Body
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel

# Resolve base dev directory from env var, defaulting to parent of this project
DEV_ROOT = Path(os.getenv("KV_DEV_ROOT", Path(__file__).resolve().parent.parent))

# Load .env from dev directory
dev_env = DEV_ROOT / ".env"
if dev_env.exists():
    load_dotenv(dev_env, override=True)

# Optional: OpenAI client for score suggestions
# Check both OPENAI_API_KEY and OPENAI_COUNCIL_KEY (fallback)
try:
    from openai import OpenAI
    openai_key = os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_COUNCIL_KEY")
    OPENAI_CLIENT = OpenAI(api_key=openai_key) if openai_key else None
except ImportError:
    OPENAI_CLIENT = None

# Database paths (configurable via env vars, default to DEV_ROOT/knowledge/)
KNOWLEDGE_DB = Path(os.getenv("KV_KNOWLEDGE_DB", DEV_ROOT / "knowledge" / "knowledge.db"))
PROMPTS_DB = Path(os.getenv("KV_PROMPTS_DB", DEV_ROOT / "knowledge" / "prompts.db"))
CODEX_PROMPT_WATCHER = DEV_ROOT / "claude-workflow-system" / "codex" / "prompt_watcher.py"
QMD_INSTALL_DIR = Path(os.getenv("APPDATA", "")) / "fnm" / "node-versions" / "v22.15.1" / "installation"
QMD_NODE = QMD_INSTALL_DIR / "node.exe"
QMD_JS = QMD_INSTALL_DIR / "node_modules" / "@tobilu" / "qmd" / "dist" / "qmd.js"
QMD_HOME = Path.home()
QMD_CACHE_HOME = QMD_HOME / ".cache"
QMD_INDEX_PATH = QMD_CACHE_HOME / "qmd" / "index.sqlite"
QMD_CONFIG_PATH = QMD_HOME / ".config" / "qmd" / "index.yml"
CLAUDE_WORKFLOW_SCRIPTS = DEV_ROOT / "claude-workflow-system" / "scripts"

if CLAUDE_WORKFLOW_SCRIPTS.exists():
    sys.path.insert(0, str(CLAUDE_WORKFLOW_SCRIPTS))

try:
    import qmd_queue
except ImportError:
    qmd_queue = None

_debug = os.getenv("DEBUG", "").lower() in ("1", "true", "yes")
app = FastAPI(
    title="Knowledge Viewer", version="1.0.0",
    docs_url="/docs" if _debug else None,
    redoc_url="/redoc" if _debug else None,
    openapi_url="/openapi.json" if _debug else None,
)


def get_qmd_env(extra: Optional[dict] = None) -> dict:
    env = dict(os.environ)
    env["HOME"] = str(QMD_HOME)
    env["XDG_CACHE_HOME"] = str(QMD_CACHE_HOME)
    if extra:
        env.update(extra)
    return env


def get_qmd_command(*args: str) -> list[str]:
    if QMD_NODE.exists() and QMD_JS.exists():
        return [str(QMD_NODE), str(QMD_JS), *args]

    qmd_bin = shutil.which("qmd")
    if qmd_bin:
        return [qmd_bin, *args]

    raise FileNotFoundError("QMD not installed")


def ensure_qmd_runs_schema():
    """Ensure the shared QMD run ledger exists in knowledge.db."""
    if not KNOWLEDGE_DB.exists():
        return
    conn = sqlite3.connect(KNOWLEDGE_DB)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS qmd_runs (
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
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_qmd_runs_command_status "
        "ON qmd_runs(command, status, id)"
    )
    conn.commit()
    conn.close()


def parse_qmd_run(row: Optional[sqlite3.Row | dict]) -> Optional[dict]:
    if row is None:
        return None
    data = dict(row)
    summary_json = data.get("summary_json")
    if summary_json:
        try:
            data["summary"] = json.loads(summary_json)
        except json.JSONDecodeError:
            data["summary"] = None
    else:
        data["summary"] = None
    return data


def load_qmd_runs(limit: int = 20) -> list[dict]:
    ensure_qmd_runs_schema()
    conn = get_knowledge_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT *
        FROM qmd_runs
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [parse_qmd_run(row) for row in cursor.fetchall()]
    conn.close()
    return [row for row in rows if row is not None]


def get_qmd_job_snapshot() -> dict:
    ensure_qmd_runs_schema()
    conn = get_knowledge_db()
    cursor = conn.cursor()

    active = cursor.execute(
        """
        SELECT *
        FROM qmd_runs
        WHERE status IN ('running', 'snapshotting', 'uploaded', 'remote_running', 'downloading', 'publishing')
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    queued = cursor.execute(
        """
        SELECT *
        FROM qmd_runs
        WHERE status = 'queued'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    latest_update = cursor.execute(
        """
        SELECT *
        FROM qmd_runs
        WHERE command = 'update' AND status = 'succeeded'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    latest_embed = cursor.execute(
        """
        SELECT *
        FROM qmd_runs
        WHERE command = 'embed' AND status = 'published'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    return {
        "active": parse_qmd_run(active),
        "queued": parse_qmd_run(queued),
        "latest_update": parse_qmd_run(latest_update),
        "latest_embed": parse_qmd_run(latest_embed),
    }


def create_qmd_run(
    *,
    command: str,
    mode: str,
    status: str,
    requested_by: str,
    target_host: Optional[str] = None,
    summary: Optional[dict] = None,
) -> dict:
    ensure_qmd_runs_schema()
    now = datetime.now().isoformat()
    conn = get_knowledge_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO qmd_runs (
            command, mode, status, target_host, requested_by,
            created_at, started_at, updated_at, summary_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            command,
            mode,
            status,
            target_host,
            requested_by,
            now,
            now if status in ("running",) else None,
            now,
            json.dumps(summary) if summary is not None else None,
        ),
    )
    row_id = cursor.lastrowid
    conn.commit()
    row = cursor.execute("SELECT * FROM qmd_runs WHERE id = ?", (row_id,)).fetchone()
    conn.close()
    return parse_qmd_run(row) or {}


def update_qmd_run(
    run_id: int,
    *,
    status: str,
    exit_code: Optional[int] = None,
    summary: Optional[dict] = None,
    stdout_excerpt: Optional[str] = None,
    stderr_excerpt: Optional[str] = None,
) -> dict:
    ensure_qmd_runs_schema()
    now = datetime.now().isoformat()
    conn = get_knowledge_db()
    cursor = conn.cursor()
    current = cursor.execute("SELECT * FROM qmd_runs WHERE id = ?", (run_id,)).fetchone()
    if current is None:
        conn.close()
        raise HTTPException(status_code=404, detail=f"QMD run not found: {run_id}")

    finished_at = now if status in ("succeeded", "published", "failed", "superseded") else current["finished_at"]
    cursor.execute(
        """
        UPDATE qmd_runs
        SET status = ?,
            updated_at = ?,
            finished_at = ?,
            exit_code = COALESCE(?, exit_code),
            summary_json = COALESCE(?, summary_json),
            stdout_excerpt = COALESCE(?, stdout_excerpt),
            stderr_excerpt = COALESCE(?, stderr_excerpt)
        WHERE id = ?
        """,
        (
            status,
            now,
            finished_at,
            exit_code,
            json.dumps(summary) if summary is not None else None,
            stdout_excerpt,
            stderr_excerpt,
            run_id,
        ),
    )
    conn.commit()
    row = cursor.execute("SELECT * FROM qmd_runs WHERE id = ?", (run_id,)).fetchone()
    conn.close()
    return parse_qmd_run(row) or {}


def has_active_qmd_mutation() -> bool:
    snapshot = get_qmd_job_snapshot()
    return snapshot["active"] is not None


def parse_qmd_update_output(stdout: str) -> dict:
    collections = []
    current = None
    cleaned_hashes = 0

    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        header_match = re.match(r"^\[\d+/\d+\]\s+(.+?)\s+\((.+)\)$", line)
        if header_match:
            current = {
                "name": header_match.group(1),
                "pattern": header_match.group(2),
            }
            collections.append(current)
            continue

        collection_match = re.match(r"^Collection:\s+(.+?)\s+\((.+)\)$", line)
        if collection_match and current is not None:
            current["path"] = collection_match.group(1)
            continue

        indexed_match = re.match(
            r"^Indexed:\s+(\d+)\s+new,\s+(\d+)\s+updated,\s+(\d+)\s+unchanged,\s+(\d+)\s+removed$",
            line,
        )
        if indexed_match and current is not None:
            current["new"] = int(indexed_match.group(1))
            current["updated"] = int(indexed_match.group(2))
            current["unchanged"] = int(indexed_match.group(3))
            current["removed"] = int(indexed_match.group(4))
            continue

        cleanup_match = re.match(r"^Cleaned up\s+(\d+)\s+orphaned content hash", line)
        if cleanup_match:
            cleaned_hashes += int(cleanup_match.group(1))

    return {
        "collections": collections,
        "cleaned_orphaned_hashes": cleaned_hashes,
    }


def load_qmd_collections_from_config() -> list[dict]:
    if not QMD_CONFIG_PATH.exists():
        return []

    collections = []
    current = None

    for raw_line in QMD_CONFIG_PATH.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue

        stripped = raw_line.strip()
        if raw_line.startswith("  ") and not raw_line.startswith("    ") and stripped.endswith(":"):
            name = stripped[:-1]
            current = {"name": name}
            collections.append(current)
            continue

        if current is None:
            continue

        if raw_line.startswith("    path:"):
            current["path"] = stripped.split(":", 1)[1].strip()
        elif raw_line.startswith("    pattern:"):
            value = stripped.split(":", 1)[1].strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]
            current["pattern"] = value
        elif raw_line.startswith('      "":'):
            value = stripped.split(":", 1)[1].strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]
            current["context"] = value

    return collections


def get_qmd_status_payload() -> dict:
    metrics = {
        "active_documents": 0,
        "active_hashes": 0,
        "embedded_hashes": 0,
        "pending_hashes": 0,
        "vector_chunks": 0,
    }
    collections = []

    config_collections = load_qmd_collections_from_config()
    config_map = {item["name"]: item for item in config_collections}

    index_info = {
        "path": str(QMD_INDEX_PATH),
        "exists": QMD_INDEX_PATH.exists(),
        "size_bytes": QMD_INDEX_PATH.stat().st_size if QMD_INDEX_PATH.exists() else 0,
        "modified_at": datetime.fromtimestamp(QMD_INDEX_PATH.stat().st_mtime).isoformat() if QMD_INDEX_PATH.exists() else None,
        "config_path": str(QMD_CONFIG_PATH),
        "config_exists": QMD_CONFIG_PATH.exists(),
    }

    if QMD_INDEX_PATH.exists():
        conn = sqlite3.connect(QMD_INDEX_PATH)
        cursor = conn.cursor()

        metrics["active_documents"] = cursor.execute(
            "SELECT COUNT(*) FROM documents WHERE active = 1"
        ).fetchone()[0]
        metrics["active_hashes"] = cursor.execute(
            "SELECT COUNT(DISTINCT hash) FROM documents WHERE active = 1"
        ).fetchone()[0]
        metrics["embedded_hashes"] = cursor.execute(
            """
            SELECT COUNT(DISTINCT d.hash)
            FROM documents d
            WHERE d.active = 1
              AND EXISTS (
                SELECT 1
                FROM content_vectors v
                WHERE v.hash = d.hash
              )
            """
        ).fetchone()[0]
        metrics["pending_hashes"] = cursor.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT hash
                FROM documents
                WHERE active = 1
            ) active_hashes
            WHERE NOT EXISTS (
                SELECT 1
                FROM content_vectors v
                WHERE v.hash = active_hashes.hash
            )
            """
        ).fetchone()[0]
        metrics["vector_chunks"] = cursor.execute(
            """
            SELECT COUNT(*)
            FROM content_vectors v
            WHERE EXISTS (
                SELECT 1
                FROM documents d
                WHERE d.active = 1
                  AND d.hash = v.hash
            )
            """
        ).fetchone()[0]

        collection_rows = cursor.execute(
            """
            SELECT collection, COUNT(*) AS documents, MAX(modified_at) AS latest_modified_at
            FROM documents
            WHERE active = 1
            GROUP BY collection
            """
        ).fetchall()
        conn.close()

        collection_counts = {
            row[0]: {
                "documents": row[1],
                "latest_modified_at": row[2],
            }
            for row in collection_rows
        }
    else:
        collection_counts = {}

    for name, config in config_map.items():
        row = collection_counts.get(name, {})
        collections.append(
            {
                "name": name,
                "path": config.get("path"),
                "pattern": config.get("pattern"),
                "context": config.get("context"),
                "documents": row.get("documents", 0),
                "latest_modified_at": row.get("latest_modified_at"),
            }
        )

    for name, row in collection_counts.items():
        if name not in config_map:
            collections.append(
                {
                    "name": name,
                    "path": None,
                    "pattern": None,
                    "context": None,
                    "documents": row.get("documents", 0),
                    "latest_modified_at": row.get("latest_modified_at"),
                }
            )

    collections.sort(key=lambda item: item["name"])

    return {
        "index": index_info,
        "metrics": metrics,
        "collections": collections,
        "jobs": get_qmd_job_snapshot(),
    }


def get_qmd_documents_payload(
    *,
    q: Optional[str] = None,
    collection: Optional[str] = None,
    embedded: str = "all",
    limit: int = 50,
    offset: int = 0,
) -> dict:
    if embedded not in {"all", "yes", "no"}:
        raise HTTPException(status_code=400, detail="embedded must be one of: all, yes, no")

    if not QMD_INDEX_PATH.exists():
        return {"documents": [], "total": 0, "limit": limit, "offset": offset}

    conditions = ["d.active = 1"]
    params: list = []

    if collection:
        conditions.append("d.collection = ?")
        params.append(collection)

    if q:
        like = f"%{q.lower()}%"
        conditions.append("(LOWER(d.path) LIKE ? OR LOWER(d.title) LIKE ?)")
        params.extend([like, like])

    embedded_exists_sql = (
        "EXISTS (SELECT 1 FROM content_vectors v WHERE v.hash = d.hash)"
    )
    if embedded == "yes":
        conditions.append(embedded_exists_sql)
    elif embedded == "no":
        conditions.append(f"NOT {embedded_exists_sql}")

    where_sql = " AND ".join(conditions)

    conn = sqlite3.connect(QMD_INDEX_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()
        total = cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM documents d
            WHERE {where_sql}
            """,
            params,
        ).fetchone()[0]

        rows = cursor.execute(
            f"""
            SELECT
                d.collection,
                d.path,
                d.title,
                d.hash,
                d.modified_at,
                CASE WHEN {embedded_exists_sql} THEN 1 ELSE 0 END AS embedded,
                (
                    SELECT COUNT(*)
                    FROM content_vectors v
                    WHERE v.hash = d.hash
                ) AS vector_chunks
            FROM documents d
            WHERE {where_sql}
            ORDER BY datetime(d.modified_at) DESC, d.path ASC
            LIMIT ? OFFSET ?
            """,
            [*params, limit, offset],
        ).fetchall()
    finally:
        conn.close()

    documents = []
    for row in rows:
        item = dict(row)
        item["embedded"] = bool(item["embedded"])
        documents.append(item)

    return {
        "documents": documents,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


def ensure_prompts_schema():
    """Ensure prompts table has all required columns."""
    if not PROMPTS_DB.exists():
        return
    conn = sqlite3.connect(PROMPTS_DB)
    cursor = conn.cursor()

    # Check existing columns
    cursor.execute("PRAGMA table_info(prompts)")
    columns = [row[1] for row in cursor.fetchall()]

    # Add missing columns
    migrations = [
        ("deleted_at", "ALTER TABLE prompts ADD COLUMN deleted_at TEXT"),
        ("tags", "ALTER TABLE prompts ADD COLUMN tags TEXT DEFAULT '[]'"),
        ("source", "ALTER TABLE prompts ADD COLUMN source TEXT DEFAULT 'claude'"),
    ]

    for col_name, sql in migrations:
        if col_name not in columns:
            cursor.execute(sql)

    conn.commit()
    conn.close()


# Run schema migration on startup
ensure_prompts_schema()


def ensure_documents_schema():
    """Ensure documents tables exist in knowledge.db for document browser."""
    if not KNOWLEDGE_DB.exists():
        return
    conn = sqlite3.connect(KNOWLEDGE_DB)
    cursor = conn.cursor()

    # Check if documents table exists
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='documents'
    """)
    if not cursor.fetchone():
        # Create documents table
        cursor.execute("""
            CREATE TABLE documents (
                id INTEGER PRIMARY KEY,
                path TEXT UNIQUE NOT NULL,
                category TEXT NOT NULL,
                project TEXT,
                title TEXT,
                content TEXT,
                file_type TEXT,
                checksum TEXT,
                indexed_at TEXT
            )
        """)
        cursor.execute("CREATE INDEX idx_documents_category ON documents(category)")
        cursor.execute("CREATE INDEX idx_documents_project ON documents(project)")

        # Create FTS5 virtual table
        cursor.execute("""
            CREATE VIRTUAL TABLE documents_fts USING fts5(
                title, content,
                content='documents',
                content_rowid='id'
            )
        """)

        # Triggers to keep FTS in sync
        cursor.execute("""
            CREATE TRIGGER documents_ai AFTER INSERT ON documents BEGIN
                INSERT INTO documents_fts(rowid, title, content)
                VALUES (new.id, new.title, new.content);
            END
        """)
        cursor.execute("""
            CREATE TRIGGER documents_ad AFTER DELETE ON documents BEGIN
                INSERT INTO documents_fts(documents_fts, rowid, title, content)
                VALUES('delete', old.id, old.title, old.content);
            END
        """)
        cursor.execute("""
            CREATE TRIGGER documents_au AFTER UPDATE ON documents BEGIN
                INSERT INTO documents_fts(documents_fts, rowid, title, content)
                VALUES('delete', old.id, old.title, old.content);
                INSERT INTO documents_fts(rowid, title, content)
                VALUES (new.id, new.title, new.content);
            END
        """)

    # Check if document_links table exists
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='document_links'
    """)
    if not cursor.fetchone():
        cursor.execute("""
            CREATE TABLE document_links (
                id INTEGER PRIMARY KEY,
                source_id INTEGER NOT NULL,
                target_id INTEGER,
                target_pattern TEXT,
                link_type TEXT,
                FOREIGN KEY (source_id) REFERENCES documents(id) ON DELETE CASCADE,
                FOREIGN KEY (target_id) REFERENCES documents(id) ON DELETE SET NULL
            )
        """)
        cursor.execute("CREATE INDEX idx_document_links_source ON document_links(source_id)")
        cursor.execute("CREATE INDEX idx_document_links_target ON document_links(target_id)")

    # Migration: Add modified_at column if not exists
    cursor.execute("PRAGMA table_info(documents)")
    columns = [row[1] for row in cursor.fetchall()]
    if "modified_at" not in columns:
        cursor.execute("ALTER TABLE documents ADD COLUMN modified_at TEXT")

    # Migration: Add is_custom column if not exists (for manually-added docs)
    if "is_custom" not in columns:
        cursor.execute("ALTER TABLE documents ADD COLUMN is_custom INTEGER DEFAULT 0")

    # Migration: Add watched_dir_id column if not exists
    if "watched_dir_id" not in columns:
        cursor.execute("ALTER TABLE documents ADD COLUMN watched_dir_id INTEGER")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_documents_watched_dir ON documents(watched_dir_id)")

    # Create watched_directories table
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='watched_directories'
    """)
    if not cursor.fetchone():
        cursor.execute("""
            CREATE TABLE watched_directories (
                id INTEGER PRIMARY KEY,
                directory TEXT NOT NULL,
                glob_pattern TEXT NOT NULL DEFAULT '**/*.md',
                category TEXT NOT NULL DEFAULT 'watched',
                project TEXT,
                label TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(directory, glob_pattern)
            )
        """)

    # Migration: Consolidate categories (18 → 9)
    # Idempotent — safe to run repeatedly since old values won't exist after first run
    category_migrations = [
        ("global", ("constitution", "design-system")),
        ("extension", ("agent", "rule")),
        ("workflow", ("hook", "mcp-server", "workflow-docs")),
        ("spec", ("spec-design", "spec-status", "spec-prompts", "spec-other")),
        ("doc", ("project-docs", "custom-docs", "custom-markdown")),
        ("memory", ("claude-memory",)),
        ("custom", ("custom-spec", "custom-python")),
    ]
    for new_cat, old_cats in category_migrations:
        placeholders = ",".join("?" for _ in old_cats)
        cursor.execute(
            f"UPDATE documents SET category = ? WHERE category IN ({placeholders})",
            (new_cat, *old_cats),
        )

    conn.commit()
    conn.close()


ensure_documents_schema()


# =============================================================================
# DOCUMENT INDEXING
# =============================================================================

# Document source configuration (DEV_ROOT already set above)
CLAUDE_CONFIG = Path.home() / ".claude"
WORKFLOW_SYSTEM = DEV_ROOT / "claude-workflow-system"

DOCUMENT_SOURCES = [
    # Global docs (constitution + design system)
    {"pattern": DEV_ROOT / "CLAUDE.md", "category": "global", "project": None},
    {"pattern": DEV_ROOT / "system-map.md", "category": "global", "project": None},
    {"pattern": DEV_ROOT / "design" / "*.md", "category": "global", "project": None},
    # Claude Code config
    {"pattern": CLAUDE_CONFIG / "skills" / "*" / "SKILL.md", "category": "skill", "project": None},
    {"pattern": CLAUDE_CONFIG / "agents" / "*.md", "category": "extension", "project": None},
    {"pattern": CLAUDE_CONFIG / "rules" / "*.md", "category": "extension", "project": None},
    {"pattern": CLAUDE_CONFIG / "settings.json", "category": "config", "project": None},
    # Workflow system (hooks + MCP server + docs)
    {"pattern": WORKFLOW_SYSTEM / "hooks" / "*.py", "category": "workflow", "project": "claude-workflow-system"},
    {"pattern": WORKFLOW_SYSTEM / "mcp-server" / "*.py", "category": "workflow", "project": "claude-workflow-system"},
    {"pattern": WORKFLOW_SYSTEM / "docs" / "roadmap.md", "category": "workflow", "project": "claude-workflow-system"},
    {"pattern": WORKFLOW_SYSTEM / "docs" / "best-practices.md", "category": "workflow", "project": "claude-workflow-system"},
    {"pattern": WORKFLOW_SYSTEM / "docs" / "workflow-diagrams.md", "category": "workflow", "project": "claude-workflow-system"},
    {"pattern": WORKFLOW_SYSTEM / "docs" / "*-codemap.md", "category": "workflow", "project": "claude-workflow-system"},
    # Workflow system manifest
    {"pattern": WORKFLOW_SYSTEM / "manifest.json", "category": "config", "project": "claude-workflow-system"},
]

# Claude Code auto memory directory
CLAUDE_PROJECTS_DIR = CLAUDE_CONFIG / "projects"


def derive_memory_project_name(mangled_dir_name: str) -> str:
    """Reverse-map a mangled directory name to a human-readable project name.

    Mangling convention: absolute path with \\ and / → - and : → -
    e.g. C--Users-user-dev-my-project → my-project
         C--Users-user-dev → global
    """
    # Build the mangled prefix dynamically from DEV_ROOT
    base_exact = str(DEV_ROOT).replace("\\", "-").replace("/", "-").replace(":", "-")
    base_prefix = base_exact + "-"

    if mangled_dir_name == base_exact:
        return "global"
    elif mangled_dir_name.startswith(base_prefix):
        return mangled_dir_name[len(base_prefix):]
    else:
        # Fallback: return the mangled name as-is
        return mangled_dir_name


def derive_memory_title(file_path: Path, project_name: str) -> str:
    """Derive a title for a memory file based on filename and project.

    MEMORY.md → "Memory: {project}"
    other.md  → "Memory: {project} / {stem}"
    """
    if file_path.name.upper() == "MEMORY.MD":
        return f"Memory: {project_name}"
    else:
        return f"Memory: {project_name} / {file_path.stem}"


def get_document_sources() -> list[dict]:
    """Get all document sources including project specs."""
    sources = DOCUMENT_SOURCES.copy()

    # Add project specs dynamically
    for project_dir in DEV_ROOT.iterdir():
        if not project_dir.is_dir():
            continue
        specs_dir = project_dir / "specs"
        if specs_dir.exists():
            project_name = project_dir.name
            sources.append({
                "pattern": specs_dir / "*-design.md",
                "category": "spec",
                "project": project_name,
            })
            sources.append({
                "pattern": specs_dir / "*-status.md",
                "category": "spec",
                "project": project_name,
            })
            sources.append({
                "pattern": specs_dir / "*-prompts.md",
                "category": "spec",
                "project": project_name,
            })
            # Broad specs: pick up ALL .md in specs/ (specific patterns above win via dedup)
            sources.append({
                "pattern": specs_dir / "*.md",
                "category": "spec",
                "project": project_name,
            })

        # Project docs directory
        docs_dir = project_dir / "docs"
        if docs_dir.exists():
            project_name = project_dir.name
            sources.append({
                "pattern": docs_dir / "**" / "*.md",
                "category": "doc",
                "project": project_name,
            })

    # Add Claude Code memory files dynamically
    if CLAUDE_PROJECTS_DIR.exists():
        for project_dir in CLAUDE_PROJECTS_DIR.iterdir():
            if not project_dir.is_dir():
                continue
            memory_dir = project_dir / "memory"
            if not memory_dir.exists() or not memory_dir.is_dir():
                continue
            project_name = derive_memory_project_name(project_dir.name)
            sources.append({
                "pattern": memory_dir / "*.md",
                "category": "memory",
                "project": project_name,
            })

    # Add watched directories from database
    if KNOWLEDGE_DB.exists():
        try:
            conn = sqlite3.connect(KNOWLEDGE_DB)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT id, directory, glob_pattern, category, project FROM watched_directories")
            for row in cursor.fetchall():
                watch_dir = Path(row["directory"])
                if watch_dir.exists():
                    sources.append({
                        "pattern": watch_dir / row["glob_pattern"],
                        "category": row["category"],
                        "project": row["project"],
                        "_watched_dir_id": row["id"],
                    })
            conn.close()
        except Exception:
            pass  # Table might not exist yet on first run

    return sources


def compute_checksum(content: str) -> str:
    """Compute MD5 checksum of content."""
    return hashlib.md5(content.encode("utf-8")).hexdigest()


def extract_title(content: str, file_path: Path) -> str:
    """Extract title from markdown heading or use filename."""
    # Look for first # heading
    match = re.search(r"^#\s+(.+)$", content, re.MULTILINE)
    if match:
        return match.group(1).strip()
    # Fallback to filename without extension
    return file_path.stem


def detect_file_type(file_path: Path) -> str:
    """Detect file type from extension."""
    suffix = file_path.suffix.lower()
    if suffix == ".md":
        return "md"
    elif suffix == ".py":
        return "py"
    elif suffix == ".yaml" or suffix == ".yml":
        return "yaml"
    elif suffix == ".json":
        return "json"
    return "text"


def extract_links(content: str, file_type: str) -> list[dict]:
    """Extract links from document content."""
    links = []

    # Skill references: /skillname
    for match in re.finditer(r"(?<![`\w])/([a-z][a-z0-9-]+)(?![`\w])", content):
        links.append({
            "pattern": f"/{match.group(1)}",
            "type": "skill-ref",
        })

    # Markdown links: [text](path)
    for match in re.finditer(r"\[([^\]]+)\]\(([^)]+)\)", content):
        path = match.group(2)
        # Skip URLs
        if not path.startswith(("http://", "https://", "#")):
            links.append({
                "pattern": path,
                "type": "markdown-link",
            })

    # File references in backticks: `filename.ext`
    for match in re.finditer(r"`([^`]+\.(md|py|json|yaml|yml))`", content):
        links.append({
            "pattern": match.group(1),
            "type": "file-ref",
        })

    # CLAUDE.md references
    if "CLAUDE.md" in content:
        links.append({
            "pattern": "CLAUDE.md",
            "type": "file-ref",
        })

    return links


def index_documents() -> dict:
    """Scan and index all document sources."""
    if not KNOWLEDGE_DB.exists():
        return {"error": "Knowledge database not found"}

    conn = sqlite3.connect(KNOWLEDGE_DB)
    cursor = conn.cursor()

    stats = {"indexed": 0, "updated": 0, "unchanged": 0, "removed": 0, "errors": []}
    indexed_paths = set()

    sources = get_document_sources()

    for source in sources:
        pattern = source["pattern"]
        category = source["category"]
        project = source["project"]
        watched_dir_id = source.get("_watched_dir_id")

        # Handle both single files and glob patterns
        if pattern.exists() and pattern.is_file():
            files = [pattern]
        else:
            # Use glob - find the first path component with wildcard
            pattern_str = str(pattern)
            if "*" in pattern_str:
                # Find the deepest directory without wildcards
                parts = pattern.parts
                root_parts = []
                glob_parts = []
                found_wildcard = False
                for part in parts:
                    if "*" in part or found_wildcard:
                        found_wildcard = True
                        glob_parts.append(part)
                    else:
                        root_parts.append(part)
                root_dir = Path(*root_parts) if root_parts else Path(".")
                glob_pattern = str(Path(*glob_parts)) if glob_parts else "*"
                if root_dir.exists():
                    files = list(root_dir.glob(glob_pattern))
                else:
                    files = []
            else:
                files = []

        for file_path in files:
            try:
                content = file_path.read_text(encoding="utf-8")

                # Skip empty files (no content after stripping whitespace)
                if not content.strip():
                    continue

                checksum = compute_checksum(content)
                path_str = str(file_path)
                indexed_paths.add(path_str)

                # Check if already indexed with same checksum
                cursor.execute(
                    "SELECT id, checksum FROM documents WHERE path = ?",
                    (path_str,)
                )
                existing = cursor.fetchone()

                # Get file modification time
                modified_at = datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()

                # Derive title — use special logic for memory files
                if category == "memory":
                    title = derive_memory_title(file_path, project)
                else:
                    title = extract_title(content, file_path)

                if existing:
                    if existing[1] == checksum:
                        # Still update modified_at if it's NULL (migration)
                        # Also claim ownership for watched_dir_id if this source provides one
                        if watched_dir_id:
                            cursor.execute(
                                "UPDATE documents SET modified_at = COALESCE(modified_at, ?), watched_dir_id = ? WHERE id = ?",
                                (modified_at, watched_dir_id, existing[0])
                            )
                        else:
                            cursor.execute(
                                "UPDATE documents SET modified_at = ? WHERE id = ? AND modified_at IS NULL",
                                (modified_at, existing[0])
                            )
                        stats["unchanged"] += 1
                        continue
                    # Update existing
                    file_type = detect_file_type(file_path)
                    cursor.execute("""
                        UPDATE documents
                        SET title = ?, content = ?, file_type = ?, checksum = ?,
                            indexed_at = ?, category = ?, project = ?, modified_at = ?,
                            watched_dir_id = ?
                        WHERE id = ?
                    """, (title, content, file_type, checksum,
                          datetime.now().isoformat(), category, project, modified_at,
                          watched_dir_id, existing[0]))
                    stats["updated"] += 1
                else:
                    # Insert new
                    file_type = detect_file_type(file_path)
                    cursor.execute("""
                        INSERT INTO documents
                        (path, category, project, title, content, file_type, checksum, indexed_at, modified_at, watched_dir_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (path_str, category, project, title, content, file_type,
                          checksum, datetime.now().isoformat(), modified_at, watched_dir_id))
                    stats["indexed"] += 1

            except Exception as e:
                stats["errors"].append({"path": str(file_path), "error": str(e)})

    # Remove documents that no longer exist
    # For custom docs (is_custom=1): only remove if file no longer exists
    # For auto-indexed docs: remove if not in indexed_paths
    cursor.execute("SELECT id, path, is_custom FROM documents")
    for row in cursor.fetchall():
        doc_id, doc_path, is_custom = row
        if is_custom:
            # Custom document - only remove if file no longer exists
            if not Path(doc_path).exists():
                cursor.execute("DELETE FROM documents WHERE id = ?", (doc_id,))
                stats["removed"] += 1
        else:
            # Auto-indexed document - remove if not in current indexed paths
            if doc_path not in indexed_paths:
                cursor.execute("DELETE FROM documents WHERE id = ?", (doc_id,))
                stats["removed"] += 1

    conn.commit()

    # Build link graph
    build_link_graph(conn)

    conn.close()

    return stats


def build_link_graph(conn: sqlite3.Connection):
    """Build document_links table from extracted links."""
    cursor = conn.cursor()

    # Clear existing links
    cursor.execute("DELETE FROM document_links")

    # Get all documents
    cursor.execute("SELECT id, path, content, file_type, category FROM documents")
    documents = cursor.fetchall()

    # Build path lookup for resolving links
    path_to_id = {}
    skill_to_id = {}
    filename_to_id = {}

    for doc in documents:
        doc_id, path, content, file_type, category = doc
        path_obj = Path(path)
        path_to_id[path] = doc_id
        filename_to_id[path_obj.name] = doc_id

        # Map skill names
        if category == "skill":
            skill_name = path_obj.parent.name
            skill_to_id[skill_name] = doc_id

    # Extract and resolve links
    for doc in documents:
        doc_id, path, content, file_type, category = doc
        links = extract_links(content, file_type)

        for link in links:
            pattern = link["pattern"]
            link_type = link["type"]
            target_id = None

            # Try to resolve the link
            if link_type == "skill-ref":
                skill_name = pattern.lstrip("/")
                target_id = skill_to_id.get(skill_name)
            elif link_type == "file-ref":
                # Check by filename
                target_id = filename_to_id.get(pattern)
            elif link_type == "markdown-link":
                # Try to resolve relative path
                try:
                    resolved = (Path(path).parent / pattern).resolve()
                    target_id = path_to_id.get(str(resolved))
                except Exception:
                    pass

            cursor.execute("""
                INSERT INTO document_links (source_id, target_id, target_pattern, link_type)
                VALUES (?, ?, ?, ?)
            """, (doc_id, target_id, pattern, link_type))

    conn.commit()


def infer_document_metadata(file_path: Path) -> dict:
    """Infer project and category from file path."""
    path_str = str(file_path)
    parts = file_path.parts

    # Try to extract project from DEV_ROOT/[project]/...
    project = None
    dev_root_str = str(DEV_ROOT)
    if path_str.startswith(dev_root_str):
        relative = file_path.relative_to(DEV_ROOT)
        if len(relative.parts) > 1:
            project = relative.parts[0]

    # Infer category from path patterns
    category = "doc"  # default for .md files
    filename = file_path.name.lower()

    # Check for specs patterns
    if "specs" in parts or "spec" in parts:
        category = "custom"
    elif "docs" in parts:
        category = "doc"
    elif file_path.suffix.lower() == ".py":
        category = "custom"
    elif file_path.suffix.lower() == ".md":
        category = "doc"

    return {"project": project, "category": category}


def update_links_for_document(conn: sqlite3.Connection, doc_id: int):
    """Update links for a single document (both directions)."""
    cursor = conn.cursor()

    # Get the new document's info
    cursor.execute("SELECT id, path, content, file_type, category FROM documents WHERE id = ?", (doc_id,))
    doc = cursor.fetchone()
    if not doc:
        return

    doc_id, path, content, file_type, category = doc
    path_obj = Path(path)
    filename = path_obj.name

    # Build lookup tables for resolving links
    cursor.execute("SELECT id, path, category FROM documents")
    all_docs = cursor.fetchall()

    path_to_id = {}
    filename_to_id = {}
    skill_to_id = {}

    for d in all_docs:
        d_id, d_path, d_category = d
        d_path_obj = Path(d_path)
        path_to_id[d_path] = d_id
        filename_to_id[d_path_obj.name] = d_id

        if d_category == "skill":
            skill_name = d_path_obj.parent.name
            skill_to_id[skill_name] = d_id

    # --- FORWARD DIRECTION ---
    # Extract links FROM new doc and resolve them
    links = extract_links(content, file_type)

    for link in links:
        pattern = link["pattern"]
        link_type = link["type"]
        target_id = None

        if link_type == "skill-ref":
            skill_name = pattern.lstrip("/")
            target_id = skill_to_id.get(skill_name)
        elif link_type == "file-ref":
            target_id = filename_to_id.get(pattern)
        elif link_type == "markdown-link":
            try:
                resolved = (path_obj.parent / pattern).resolve()
                target_id = path_to_id.get(str(resolved))
            except Exception:
                pass

        cursor.execute("""
            INSERT INTO document_links (source_id, target_id, target_pattern, link_type)
            VALUES (?, ?, ?, ?)
        """, (doc_id, target_id, pattern, link_type))

    # --- BACKWARD DIRECTION ---
    # Find unresolved links that might now resolve to this document
    # Check by filename
    cursor.execute("""
        UPDATE document_links
        SET target_id = ?
        WHERE target_id IS NULL AND target_pattern = ?
    """, (doc_id, filename))

    # Check for skill reference if this is a skill
    if category == "skill":
        skill_name = path_obj.parent.name
        cursor.execute("""
            UPDATE document_links
            SET target_id = ?
            WHERE target_id IS NULL AND target_pattern = ?
        """, (doc_id, "/" + skill_name))

    # Check for markdown links that resolve to this path
    cursor.execute("""
        SELECT id, source_id, target_pattern
        FROM document_links
        WHERE target_id IS NULL AND link_type = 'markdown-link'
    """)
    unresolved_links = cursor.fetchall()

    for link_id, source_id, target_pattern in unresolved_links:
        # Get source document's path
        cursor.execute("SELECT path FROM documents WHERE id = ?", (source_id,))
        source_row = cursor.fetchone()
        if source_row:
            source_path = Path(source_row[0])
            try:
                resolved = (source_path.parent / target_pattern).resolve()
                if str(resolved) == path:
                    cursor.execute("""
                        UPDATE document_links SET target_id = ? WHERE id = ?
                    """, (doc_id, link_id))
            except Exception:
                pass

    conn.commit()


def add_single_document(file_path: Path) -> dict:
    """Add a single document and update link graph."""
    # Validate path exists and is file
    if not file_path.exists():
        return {"error": f"File not found: {file_path}"}
    if not file_path.is_file():
        return {"error": f"Path is not a file: {file_path}"}

    conn = sqlite3.connect(KNOWLEDGE_DB)
    cursor = conn.cursor()

    path_str = str(file_path)

    # Check not already indexed
    cursor.execute("SELECT id FROM documents WHERE path = ?", (path_str,))
    if cursor.fetchone():
        conn.close()
        return {"error": f"Document already indexed: {file_path}"}

    try:
        content = file_path.read_text(encoding="utf-8")
    except Exception as e:
        conn.close()
        return {"error": f"Failed to read file: {e}"}

    checksum = compute_checksum(content)
    title = extract_title(content, file_path)
    file_type = detect_file_type(file_path)
    metadata = infer_document_metadata(file_path)
    modified_at = datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()

    cursor.execute("""
        INSERT INTO documents
        (path, category, project, title, content, file_type, checksum, indexed_at, modified_at, is_custom)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
    """, (path_str, metadata["category"], metadata["project"], title, content,
          file_type, checksum, datetime.now().isoformat(), modified_at))

    doc_id = cursor.lastrowid
    conn.commit()

    # Update link graph for this document
    update_links_for_document(conn, doc_id)

    conn.close()

    return {
        "success": True,
        "id": doc_id,
        "title": title,
        "category": metadata["category"],
        "project": metadata["project"],
        "path": path_str
    }


def get_knowledge_db():
    """Get connection to knowledge.db with row factory."""
    if not KNOWLEDGE_DB.exists():
        raise HTTPException(status_code=500, detail=f"Knowledge database not found: {KNOWLEDGE_DB}")
    conn = sqlite3.connect(KNOWLEDGE_DB)
    conn.row_factory = sqlite3.Row
    return conn


def get_prompts_db():
    """Get connection to prompts.db with row factory."""
    if not PROMPTS_DB.exists():
        raise HTTPException(status_code=500, detail=f"Prompts database not found: {PROMPTS_DB}")
    conn = sqlite3.connect(PROMPTS_DB)
    conn.row_factory = sqlite3.Row
    return conn


def row_to_dict(row):
    """Convert sqlite3.Row to dict."""
    if row is None:
        return None
    return dict(row)


def rows_to_list(rows):
    """Convert list of sqlite3.Row to list of dicts."""
    return [dict(row) for row in rows]


# =============================================================================
# FRONTEND
# =============================================================================

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    """Serve the single-file frontend."""
    index_path = Path(__file__).parent / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=500, detail="Frontend not found")
    return FileResponse(index_path, media_type="text/html", headers={"Cache-Control": "no-store"})


# =============================================================================
# STATS
# =============================================================================

@app.get("/api/stats")
async def get_stats():
    """Get summary statistics for dashboard."""
    stats = {
        "sessions": 0,
        "errors": 0,
        "resolutions": 0,
        "decisions": 0,
        "prompts": 0,
        "learnings": 0,
        "tags": 0,
        "docs": 0,
    }

    # Knowledge DB stats
    try:
        conn = get_knowledge_db()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM sessions")
        stats["sessions"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM global_errors")
        stats["errors"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM global_resolutions")
        stats["resolutions"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM global_decisions")
        stats["decisions"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM global_learnings")
        stats["learnings"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM tag_vocabulary")
        stats["tags"] = cursor.fetchone()[0]

        # Documents count (may not exist yet)
        try:
            cursor.execute("SELECT COUNT(*) FROM documents")
            stats["docs"] = cursor.fetchone()[0]
        except Exception:
            pass

        # Projects count
        try:
            cursor.execute("SELECT COUNT(*) FROM projects")
            stats["projects"] = cursor.fetchone()[0]
        except Exception:
            stats["projects"] = 0

        # Conversations count (sessions with transcripts)
        try:
            cursor.execute("SELECT COUNT(*) FROM sessions WHERE claude_session_id IS NOT NULL")
            stats["conversations"] = cursor.fetchone()[0]
        except Exception:
            stats["conversations"] = 0

        conn.close()
    except Exception as e:
        # Return partial stats if knowledge db fails
        pass

    # Prompts DB stats (only non-deleted and passing filter)
    try:
        conn = get_prompts_db()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM prompts WHERE deleted_at IS NULL AND passes_filter = 1")
        stats["prompts"] = cursor.fetchone()[0]
        conn.close()
    except Exception:
        pass

    return stats


@app.get("/api/activity")
async def get_recent_activity(limit: int = Query(default=10, le=50)):
    """Get recent activity across all tables."""
    activity = []

    try:
        conn = get_knowledge_db()
        cursor = conn.cursor()

        # Recent sessions
        cursor.execute("""
            SELECT 'session' as type, id, project, start_time as timestamp,
                   'Session started' as description
            FROM sessions
            ORDER BY start_time DESC LIMIT ?
        """, (limit,))
        activity.extend(rows_to_list(cursor.fetchall()))

        # Recent errors
        cursor.execute("""
            SELECT 'error' as type, id, project, date as timestamp,
                   description
            FROM global_errors
            ORDER BY date DESC LIMIT ?
        """, (limit,))
        activity.extend(rows_to_list(cursor.fetchall()))

        # Recent decisions
        cursor.execute("""
            SELECT 'decision' as type, id, project, date as timestamp,
                   title as description
            FROM global_decisions
            ORDER BY date DESC LIMIT ?
        """, (limit,))
        activity.extend(rows_to_list(cursor.fetchall()))

        # Recent learnings
        cursor.execute("""
            SELECT 'learning' as type, id, project, date as timestamp,
                   title as description
            FROM global_learnings
            ORDER BY date DESC LIMIT ?
        """, (limit,))
        activity.extend(rows_to_list(cursor.fetchall()))

        conn.close()
    except Exception:
        pass

    # Sort by timestamp descending and limit
    activity.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    return activity[:limit]


# =============================================================================
# PROJECTS
# =============================================================================

@app.get("/api/projects")
async def list_projects(category: Optional[str] = None):
    """List all projects from the registry with aggregated stats."""
    conn = get_knowledge_db()
    cursor = conn.cursor()

    try:
        if category:
            cursor.execute(
                "SELECT * FROM projects WHERE category = ? ORDER BY name",
                (category,),
            )
        else:
            cursor.execute("SELECT * FROM projects ORDER BY name")
        projects = rows_to_list(cursor.fetchall())
    except Exception:
        conn.close()
        return []

    # Enrich each project with stats
    for proj in projects:
        name = proj["name"]
        for key, sql in [
            ("session_count", "SELECT COUNT(*) FROM sessions WHERE project = ?"),
            ("error_count", "SELECT COUNT(*) FROM global_errors WHERE project = ?"),
            ("decision_count", "SELECT COUNT(*) FROM global_decisions WHERE project = ?"),
            ("learning_count", "SELECT COUNT(*) FROM global_learnings WHERE project = ?"),
        ]:
            try:
                cursor.execute(sql, (name,))
                proj[key] = cursor.fetchone()[0]
            except Exception:
                proj[key] = 0

    conn.close()
    return projects


# =============================================================================
# SESSIONS
# =============================================================================

@app.get("/api/sessions")
async def list_sessions(
    project: Optional[str] = None,
    limit: int = Query(default=500, le=5000),
    offset: int = 0
):
    """List all sessions with optional project filter."""
    conn = get_knowledge_db()
    cursor = conn.cursor()

    if project:
        cursor.execute("""
            SELECT * FROM sessions
            WHERE project = ?
            ORDER BY start_time DESC
            LIMIT ? OFFSET ?
        """, (project, limit, offset))
    else:
        cursor.execute("""
            SELECT * FROM sessions
            ORDER BY start_time DESC
            LIMIT ? OFFSET ?
        """, (limit, offset))

    sessions = rows_to_list(cursor.fetchall())
    conn.close()
    return sessions


@app.get("/api/sessions/{session_id}")
async def get_session(session_id: int):
    """Get session detail by ID."""
    conn = get_knowledge_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM sessions WHERE id = ?", (session_id,))
    session = row_to_dict(cursor.fetchone())
    conn.close()

    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    # Check transcript availability
    claude_sid = session.get("claude_session_id", "")
    proj = session.get("project", "global") or "global"
    if claude_sid:
        md_path = SESSIONS_DIR / proj / f"{claude_sid}.md"
        session["has_transcript"] = md_path.exists()
    else:
        session["has_transcript"] = False

    return session


# =============================================================================
# CONVERSATIONS (parsed session transcripts)
# =============================================================================

SESSIONS_DIR = DEV_ROOT / "knowledge" / "sessions"


@app.get("/api/conversations")
async def list_conversations(
    project: Optional[str] = None,
    search: Optional[str] = None,
    has_transcript_only: bool = True,
    limit: int = Query(default=100, le=1000),
    offset: int = 0,
):
    """List sessions that have parsed transcripts.

    Joins sessions table metadata with parsed markdown files on disk.
    """
    conn = get_knowledge_db()
    cursor = conn.cursor()

    # Build query dynamically
    conditions = []
    params: list = []

    if project:
        conditions.append("project = ?")
        params.append(project)
    if search:
        conditions.append("current_task LIKE ?")
        params.append(f"%{search}%")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    cursor.execute(f"""
        SELECT id, claude_session_id, project, start_time, end_time,
               current_task, files_modified
        FROM sessions
        {where}
        ORDER BY start_time DESC
        LIMIT ? OFFSET ?
    """, (*params, limit, offset))

    rows = rows_to_list(cursor.fetchall())
    conn.close()

    # Enrich with transcript availability and size
    results = []
    for row in rows:
        claude_sid = row.get("claude_session_id", "")
        proj = row.get("project", "global") or "global"
        has_transcript = False
        transcript_size = 0
        if claude_sid:
            md_path = SESSIONS_DIR / proj / f"{claude_sid}.md"
            has_transcript = md_path.exists()
            if has_transcript:
                transcript_size = md_path.stat().st_size
        row["has_transcript"] = has_transcript
        row["transcript_size"] = transcript_size

        if has_transcript_only and not has_transcript:
            continue
        results.append(row)

    return results


@app.get("/api/conversations/{session_id}")
async def get_conversation(session_id: str):
    """Get a parsed session transcript by claude_session_id.

    Returns session metadata + full markdown transcript content.
    """
    # Find the markdown file across all project subdirs
    transcript_path = None
    project_name = None
    for proj_dir in SESSIONS_DIR.iterdir():
        if not proj_dir.is_dir():
            continue
        candidate = proj_dir / f"{session_id}.md"
        if candidate.exists():
            transcript_path = candidate
            project_name = proj_dir.name
            break

    if not transcript_path:
        raise HTTPException(status_code=404, detail="Transcript not found")

    content = transcript_path.read_text(encoding="utf-8")

    # Try to get DB metadata too
    metadata = {}
    conn = get_knowledge_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM sessions WHERE claude_session_id = ?",
        (session_id,),
    )
    row = cursor.fetchone()
    if row:
        metadata = dict(row)
    conn.close()

    return {
        "session_id": session_id,
        "project": project_name,
        "content": content,
        "metadata": metadata,
    }


# =============================================================================
# QMD SEARCH PROXY
# =============================================================================

@app.get("/api/search/qmd")
async def qmd_search_proxy(
    q: str = Query(..., min_length=1),
    collection: Optional[str] = None,
    limit: int = Query(default=10, le=50),
):
    """Proxy search requests to QMD CLI.

    Shells out to `qmd search --json` and returns parsed JSON results.
    """
    try:
        cmd = get_qmd_command("search", q, "-n", str(limit), "--json")
    except FileNotFoundError:
        raise HTTPException(status_code=503, detail="QMD not installed")

    if collection:
        cmd.extend(["--collection", collection])

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            env=get_qmd_env(),
        )
    except FileNotFoundError:
        raise HTTPException(status_code=503, detail="QMD not installed")
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="QMD search timed out")

    if result.returncode != 0:
        return {"results": [], "error": result.stderr.strip()}

    try:
        results = json.loads(result.stdout)
    except json.JSONDecodeError:
        results = []

    return {"results": results, "query": q, "collection": collection}


@app.get("/api/qmd/status")
async def get_qmd_status():
    """Return live QMD index health and shared job state."""
    return get_qmd_status_payload()


@app.get("/api/qmd/documents")
async def get_qmd_documents(
    q: Optional[str] = None,
    collection: Optional[str] = None,
    embedded: str = Query(default="all"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    """Return per-document QMD embed status from the live canonical index."""
    return get_qmd_documents_payload(
        q=q,
        collection=collection,
        embedded=embedded,
        limit=limit,
        offset=offset,
    )


@app.get("/api/qmd/runs")
async def get_qmd_runs(limit: int = Query(default=20, ge=1, le=100)):
    """Return recent QMD runs from the shared ledger."""
    return {"runs": load_qmd_runs(limit=limit)}


@app.post("/api/qmd/update")
async def run_qmd_update():
    """Run a serialized local QMD update and record it in the shared ledger."""
    if has_active_qmd_mutation():
        raise HTTPException(status_code=409, detail="Another QMD mutation is already active")

    try:
        cmd = get_qmd_command("update")
    except FileNotFoundError:
        raise HTTPException(status_code=503, detail="QMD not installed")

    run = create_qmd_run(
        command="update",
        mode="local",
        status="running",
        requested_by="viewer",
    )

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,
            env=get_qmd_env(),
        )
    except subprocess.TimeoutExpired:
        failed = update_qmd_run(
            run["id"],
            status="failed",
            stdout_excerpt=None,
            stderr_excerpt="QMD update timed out",
        )
        raise HTTPException(status_code=504, detail={"run": failed, "error": "QMD update timed out"})

    summary = parse_qmd_update_output(result.stdout or "")
    summary["command"] = cmd
    summary["returncode"] = result.returncode

    if result.returncode != 0:
        failed = update_qmd_run(
            run["id"],
            status="failed",
            exit_code=result.returncode,
            summary=summary,
            stdout_excerpt=(result.stdout or "")[-5000:],
            stderr_excerpt=(result.stderr or "")[-2000:],
        )
        raise HTTPException(status_code=500, detail={"run": failed, "error": (result.stderr or "").strip() or "QMD update failed"})

    succeeded = update_qmd_run(
        run["id"],
        status="succeeded",
        exit_code=0,
        summary=summary,
        stdout_excerpt=(result.stdout or "")[-5000:],
        stderr_excerpt=(result.stderr or "")[-2000:],
    )
    return {"run": succeeded, "status": get_qmd_status_payload()}


@app.post("/api/qmd/embed")
async def queue_qmd_embed():
    """Queue one remote QMD embed job against the shared bhaclaw handoff."""
    active = get_qmd_job_snapshot()["active"]
    if active and active.get("command") == "update":
        raise HTTPException(status_code=409, detail="Cannot queue embed while a local QMD update is active")
    if qmd_queue is None:
        raise HTTPException(status_code=503, detail="Shared QMD queue not available")

    run = qmd_queue.enqueue_embed_job(db_path=KNOWLEDGE_DB, requested_by="viewer")
    return {"run": parse_qmd_run(run), "status": get_qmd_status_payload()}


# =============================================================================
# ERRORS
# =============================================================================

@app.get("/api/errors")
async def list_errors(
    project: Optional[str] = None,
    search: Optional[str] = None,
    resolved: Optional[bool] = None,
    limit: int = Query(default=500, le=5000),
    offset: int = 0
):
    """List errors with optional filters and FTS search."""
    conn = get_knowledge_db()
    cursor = conn.cursor()

    # Build query dynamically
    conditions = []
    params = []

    if project == "universal":
        conditions.append("e.project IS NULL")
    elif project:
        conditions.append("e.project = ?")
        params.append(project)

    if resolved is not None:
        if resolved:
            conditions.append("r.id IS NOT NULL")
        else:
            conditions.append("r.id IS NULL")

    # Use FTS if search provided
    if search:
        # Search in FTS table
        cursor.execute("""
            SELECT e.id, e.project, e.description, e.symptom, e.module,
                   e.stack_trace, e.context, e.date as logged_at, e.resolved,
                   e.session_id, r.id as resolution_id, r.cause, r.fix, r.lesson
            FROM global_errors e
            LEFT JOIN global_resolutions r ON e.id = r.error_id
            WHERE e.id IN (
                SELECT rowid FROM errors_fts WHERE errors_fts MATCH ?
            )
            {}
            ORDER BY e.date DESC
            LIMIT ? OFFSET ?
        """.format("AND " + " AND ".join(conditions) if conditions else ""),
        [search] + params + [limit, offset])
    else:
        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
        cursor.execute(f"""
            SELECT e.id, e.project, e.description, e.symptom, e.module,
                   e.stack_trace, e.context, e.date as logged_at, e.resolved,
                   e.session_id, r.id as resolution_id, r.cause, r.fix, r.lesson
            FROM global_errors e
            LEFT JOIN global_resolutions r ON e.id = r.error_id
            {where_clause}
            ORDER BY e.date DESC
            LIMIT ? OFFSET ?
        """, params + [limit, offset])

    errors = rows_to_list(cursor.fetchall())
    conn.close()
    return errors


@app.get("/api/errors/{error_id}")
async def get_error(error_id: int):
    """Get error detail with resolution if exists."""
    conn = get_knowledge_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT e.id, e.project, e.description, e.symptom, e.module,
               e.stack_trace, e.context, e.date as logged_at, e.resolved,
               e.session_id, r.id as resolution_id, r.cause, r.fix, r.lesson,
               r.date as resolved_at, r.commit_ref
        FROM global_errors e
        LEFT JOIN global_resolutions r ON e.id = r.error_id
        WHERE e.id = ?
    """, (error_id,))

    error = row_to_dict(cursor.fetchone())
    conn.close()

    if not error:
        raise HTTPException(status_code=404, detail="Error not found")

    return error


# =============================================================================
# DECISIONS
# =============================================================================

@app.get("/api/decisions")
async def list_decisions(
    project: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = Query(default=500, le=5000),
    offset: int = 0
):
    """List decisions with optional filters and FTS search."""
    conn = get_knowledge_db()
    cursor = conn.cursor()

    # Build project condition
    project_condition = ""
    project_params = []
    if project == "universal":
        project_condition = "d.project IS NULL"
    elif project:
        project_condition = "d.project = ?"
        project_params = [project]

    if search:
        fts_where = "WHERE d.id IN (SELECT rowid FROM decisions_fts WHERE decisions_fts MATCH ?)"
        if project_condition:
            fts_where += f" AND {project_condition}"
        cursor.execute(f"""
            SELECT d.*, s.claude_session_id, s.start_time as session_start
            FROM global_decisions d
            LEFT JOIN sessions s ON d.session_id = s.id
            {fts_where}
            ORDER BY d.date DESC
            LIMIT ? OFFSET ?
        """, [search] + project_params + [limit, offset])
    else:
        where_clause = f"WHERE {project_condition}" if project_condition else ""
        cursor.execute(f"""
            SELECT d.*, s.claude_session_id, s.start_time as session_start
            FROM global_decisions d
            LEFT JOIN sessions s ON d.session_id = s.id
            {where_clause}
            ORDER BY d.date DESC
            LIMIT ? OFFSET ?
        """, project_params + [limit, offset])

    decisions = rows_to_list(cursor.fetchall())
    conn.close()
    return decisions


@app.get("/api/decisions/{decision_id}")
async def get_decision(decision_id: int):
    """Get decision detail by ID with session info."""
    conn = get_knowledge_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT d.*, s.claude_session_id, s.start_time as session_start
        FROM global_decisions d
        LEFT JOIN sessions s ON d.session_id = s.id
        WHERE d.id = ?
    """, (decision_id,))
    decision = row_to_dict(cursor.fetchone())
    conn.close()

    if not decision:
        raise HTTPException(status_code=404, detail="Decision not found")

    return decision


# =============================================================================
# LEARNINGS
# =============================================================================

@app.get("/api/learnings")
async def list_learnings(
    project: Optional[str] = None,
    search: Optional[str] = None,
    tags: Optional[str] = None,
    include_universal: bool = True,
    limit: int = Query(default=500, le=5000),
    offset: int = 0
):
    """List learnings with optional filters and FTS search."""
    conn = get_knowledge_db()
    cursor = conn.cursor()

    # Build project condition
    project_condition = ""
    project_params = []
    if project == "universal":
        project_condition = "l.project IS NULL"
    elif project:
        if include_universal:
            project_condition = "(l.project = ? OR l.project IS NULL)"
        else:
            project_condition = "l.project = ?"
        project_params = [project]

    if search:
        base_query = """
            SELECT l.*, s.claude_session_id, s.start_time as session_start
            FROM global_learnings l
            LEFT JOIN sessions s ON l.session_id = s.id
            WHERE l.id IN (
                SELECT rowid FROM learnings_fts WHERE learnings_fts MATCH ?
            )
        """
        params = [search]

        if project_condition:
            base_query += f" AND {project_condition}"
            params.extend(project_params)

        base_query += " ORDER BY l.date DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        cursor.execute(base_query, params)
    else:
        where_clause = f"WHERE {project_condition}" if project_condition else ""

        cursor.execute(f"""
            SELECT l.*, s.claude_session_id, s.start_time as session_start
            FROM global_learnings l
            LEFT JOIN sessions s ON l.session_id = s.id
            {where_clause}
            ORDER BY l.date DESC
            LIMIT ? OFFSET ?
        """, project_params + [limit, offset])

    learnings = rows_to_list(cursor.fetchall())
    conn.close()

    # Filter by tags if specified (post-query since tags is JSON)
    if tags:
        tag_list = [t.strip().lower() for t in tags.split(",")]
        filtered = []
        for learning in learnings:
            try:
                learning_tags = json.loads(learning.get("tags", "[]"))
                if any(t.lower() in [lt.lower() for lt in learning_tags] for t in tag_list):
                    filtered.append(learning)
            except (json.JSONDecodeError, TypeError):
                pass
        learnings = filtered

    return learnings


@app.get("/api/learnings/{learning_id}")
async def get_learning(learning_id: int):
    """Get learning detail by ID with session info."""
    conn = get_knowledge_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT l.*, s.claude_session_id, s.start_time as session_start, s.project as session_project
        FROM global_learnings l
        LEFT JOIN sessions s ON l.session_id = s.id
        WHERE l.id = ?
    """, (learning_id,))
    learning = row_to_dict(cursor.fetchone())
    conn.close()

    if not learning:
        raise HTTPException(status_code=404, detail="Learning not found")

    # Parse JSON fields
    try:
        learning["tags"] = json.loads(learning.get("tags", "[]"))
    except (json.JSONDecodeError, TypeError):
        learning["tags"] = []

    try:
        learning["examples"] = json.loads(learning.get("examples", "[]"))
    except (json.JSONDecodeError, TypeError):
        learning["examples"] = []

    return learning


# =============================================================================
# PROMPTS
# =============================================================================

@app.get("/api/prompts")
async def list_prompts(
    project: Optional[str] = None,
    scored: Optional[bool] = None,
    min_score: Optional[int] = None,
    max_score: Optional[int] = None,
    tag: Optional[str] = None,
    source: Optional[str] = None,
    # Two-dimensional visibility: lifecycle x type
    lifecycle_active: bool = Query(default=True, description="Include non-deleted prompts"),
    lifecycle_deleted: bool = Query(default=False, description="Include deleted prompts"),
    type_substantive: bool = Query(default=True, description="Include substantive prompts (passes_filter=1)"),
    type_trivial: bool = Query(default=False, description="Include trivial/short prompts"),
    type_pastes: bool = Query(default=False, description="Include large pastes"),
    limit: int = Query(default=500, le=5000),
    offset: int = 0
):
    """List prompts with optional filters."""
    conn = get_prompts_db()
    cursor = conn.cursor()

    conditions = []
    params = []

    # Two-dimensional visibility filter: lifecycle (rows) x type (columns)
    # Result = any matching lifecycle AND any matching type
    lifecycle_parts = []
    if lifecycle_active:
        lifecycle_parts.append("deleted_at IS NULL")
    if lifecycle_deleted:
        lifecycle_parts.append("deleted_at IS NOT NULL")

    type_parts = []
    if type_substantive:
        type_parts.append("passes_filter = 1")
    if type_trivial:
        type_parts.append(
            "(passes_filter = 0 AND (filter_reason IS NULL OR filter_reason NOT LIKE 'large_paste%'))"
        )
    if type_pastes:
        type_parts.append("filter_reason LIKE 'large_paste%'")

    if lifecycle_parts and type_parts:
        conditions.append(f"({' OR '.join(lifecycle_parts)})")
        conditions.append(f"({' OR '.join(type_parts)})")
    else:
        # If either dimension has nothing selected, show nothing
        conditions.append("1 = 0")

    if project:
        conditions.append("project = ?")
        params.append(project)

    if scored is not None:
        if scored:
            conditions.append("importance_score IS NOT NULL")
        else:
            conditions.append("importance_score IS NULL")

    if min_score is not None:
        conditions.append("importance_score >= ?")
        params.append(min_score)

    if max_score is not None:
        conditions.append("importance_score <= ?")
        params.append(max_score)

    if source:
        conditions.append("source = ?")
        params.append(source)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

    cursor.execute(f"""
        SELECT id, timestamp, project, source, char_count, importance_score,
               SUBSTR(prompt, 1, 200) as preview, deleted_at, tags
        FROM prompts
        {where_clause}
        ORDER BY (timestamp IS NULL OR timestamp = ''), datetime(timestamp) DESC, id DESC
        LIMIT ? OFFSET ?
    """, params + [limit, offset])

    prompts = rows_to_list(cursor.fetchall())
    conn.close()

    # Parse tags JSON and filter if tag specified
    for p in prompts:
        try:
            p["tags"] = json.loads(p.get("tags") or "[]")
        except (json.JSONDecodeError, TypeError):
            p["tags"] = []

    # Filter by tag (post-query since it's JSON)
    if tag:
        tag_lower = tag.lower()
        prompts = [p for p in prompts if tag_lower in [t.lower() for t in p["tags"]]]

    return prompts


class PromptScores(BaseModel):
    """Model for bulk prompt scoring."""
    scores: list[dict]  # [{id: int, score: int}, ...]


@app.post("/api/prompts/score")
async def score_prompts(data: PromptScores):
    """Bulk update importance scores for prompts."""
    conn = get_prompts_db()
    cursor = conn.cursor()

    updated = 0
    for item in data.scores:
        scored_by = item.get("scored_by", "manual")
        cursor.execute("""
            UPDATE prompts SET importance_score = ?, scored_by = ? WHERE id = ?
        """, (item["score"], scored_by, item["id"]))
        updated += cursor.rowcount

    conn.commit()
    conn.close()

    return {"updated": updated}


class BulkDeleteRequest(BaseModel):
    """Model for bulk delete request."""
    prompt_ids: list[int]


@app.post("/api/prompts/bulk-delete")
async def bulk_delete_prompts(data: BulkDeleteRequest):
    """Soft delete multiple prompts at once."""
    conn = get_prompts_db()
    cursor = conn.cursor()

    deleted_count = 0
    now = datetime.now().isoformat()

    for prompt_id in data.prompt_ids:
        cursor.execute("""
            UPDATE prompts SET deleted_at = ? WHERE id = ? AND deleted_at IS NULL
        """, (now, prompt_id))
        deleted_count += cursor.rowcount

    conn.commit()
    conn.close()

    return {"deleted": deleted_count}


class SuggestScoresRequest(BaseModel):
    """Request model for score suggestions."""
    prompt_ids: Optional[list[int]] = None
    limit: int = 100


@app.post("/api/prompts/suggest-scores")
async def suggest_scores(data: SuggestScoresRequest):
    """Use OpenAI to suggest importance scores for unscored prompts."""
    if OPENAI_CLIENT is None:
        raise HTTPException(
            status_code=503,
            detail="OpenAI client not available. Check OPENAI_API_KEY in .env"
        )

    conn = get_prompts_db()
    cursor = conn.cursor()

    # Get prompts to score
    if data.prompt_ids:
        placeholders = ",".join("?" * len(data.prompt_ids))
        cursor.execute(f"""
            SELECT id, timestamp, project, char_count,
                   SUBSTR(prompt, 1, 500) as prompt_preview
            FROM prompts
            WHERE id IN ({placeholders}) AND deleted_at IS NULL
              AND passes_filter = 1
            ORDER BY (timestamp IS NULL OR timestamp = ''), datetime(timestamp) DESC, id DESC
        """, data.prompt_ids)
    else:
        # Get oldest unscored prompts (only those passing filter)
        cursor.execute("""
            SELECT id, timestamp, project, char_count,
                   SUBSTR(prompt, 1, 500) as prompt_preview
            FROM prompts
            WHERE importance_score IS NULL AND deleted_at IS NULL
              AND passes_filter = 1
            ORDER BY timestamp ASC
            LIMIT ?
        """, (data.limit,))

    prompts = rows_to_list(cursor.fetchall())
    conn.close()

    if not prompts:
        return {"suggestions": [], "message": "No prompts to score"}

    # Batch into groups of 10 for API calls with rate limiting
    suggestions = []
    batch_size = 10

    for i in range(0, len(prompts), batch_size):
        batch = prompts[i:i + batch_size]
        batch_suggestions = await score_batch_with_llm(batch)
        suggestions.extend(batch_suggestions)
        # Rate limit: 0.5s delay between batches
        if i + batch_size < len(prompts):
            await asyncio.sleep(0.5)

    return {"suggestions": suggestions}


def resolve_project_from_text(text: str) -> str:
    """Extract project dir under DEV_ROOT from text if present and valid."""
    if not text:
        return ""
    marker = str(DEV_ROOT).replace("/", "\\") + "\\"
    idx = text.find(marker)
    if idx == -1:
        return ""
    rest = text[idx + len(marker):]
    parts = rest.split("\\", 1)
    if not parts:
        return ""
    project = parts[0].strip()
    if not project:
        return ""
    candidate = DEV_ROOT / project
    if candidate.exists() and candidate.is_dir():
        return project
    return ""


def is_valid_project_name(name: str) -> bool:
    if not name:
        return False
    candidate = DEV_ROOT / name
    return candidate.exists() and candidate.is_dir()


class SyncCodexRequest(BaseModel):
    """Request to sync Codex prompts into prompts.db."""
    project_dir: Optional[str] = None


@app.post("/api/prompts/sync-codex")
async def sync_codex_prompts(data: SyncCodexRequest = SyncCodexRequest()):
    """Run Codex prompt watcher once to backfill new prompts."""
    if not CODEX_PROMPT_WATCHER.exists():
        raise HTTPException(status_code=500, detail=f"Codex prompt watcher not found: {CODEX_PROMPT_WATCHER}")

    cmd = [sys.executable, str(CODEX_PROMPT_WATCHER), "--once"]
    if data.project_dir:
        cmd += ["--project-dir", data.project_dir]

    result = subprocess.run(cmd, capture_output=True, text=True)
    return {
        "returncode": result.returncode,
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip()
    }


class CleanupCodexProjectsRequest(BaseModel):
    """Cleanup Codex project names."""
    default_project: Optional[str] = None
    only_missing: bool = True


@app.post("/api/prompts/cleanup-codex-projects")
async def cleanup_codex_projects(data: CleanupCodexProjectsRequest = CleanupCodexProjectsRequest()):
    """Normalize Codex project names: infer from prompt text or apply default."""
    conn = get_prompts_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, prompt, project
        FROM prompts
        WHERE source = 'codex'
    """)
    rows = cursor.fetchall()

    updated = 0
    inferred = 0
    overwritten = 0

    for row in rows:
        prompt_id = row["id"]
        prompt_text = row["prompt"] or ""
        project = (row["project"] or "").strip()

        if data.only_missing and project and is_valid_project_name(project):
            continue

        new_project = resolve_project_from_text(prompt_text)
        if new_project:
            inferred += 1
        elif data.default_project:
            new_project = data.default_project
            overwritten += 1
        else:
            new_project = ""

        if new_project != project:
            cursor.execute(
                "UPDATE prompts SET project = ? WHERE id = ?",
                (new_project, prompt_id)
            )
            updated += cursor.rowcount

    conn.commit()
    conn.close()

    return {
        "updated": updated,
        "inferred": inferred,
        "overwritten": overwritten,
        "total_codex": len(rows)
    }


async def score_batch_with_llm(prompts: list[dict]) -> list[dict]:
    """Score a batch of prompts using GPT-4o-mini."""
    # Format prompts for the LLM
    prompts_text = "\n\n".join([
        f"[ID: {p['id']}]\nProject: {p['project'] or 'Unknown'}\n"
        f"Timestamp: {p['timestamp']}\n"
        f"Content: {p['prompt_preview']}"
        for p in prompts
    ])

    scoring_prompt = f"""You are scoring user prompts for importance/value in a coding assistant context.

Score each prompt on a scale of 1-10:
- 1-3: Low value (routine confirmations, simple pastes, short responses, "yes/no" answers)
- 4-6: Normal work (typical coding prompts, questions, standard requests)
- 7-9: High value (debugging sessions, architectural decisions, learning moments)
- 10: Critical (major decisions, breakthroughs, complex problem-solving)

Here are the prompts to score:

{prompts_text}

Return a JSON array with objects containing "id" (integer) and "score" (integer 1-10).
Only return the JSON array, no explanation. Example:
[{{"id": 1, "score": 5}}, {{"id": 2, "score": 7}}]"""

    try:
        response = OPENAI_CLIENT.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=1024,
            messages=[{"role": "user", "content": scoring_prompt}]
        )

        response_text = response.choices[0].message.content.strip()
        # Parse JSON response
        scores = json.loads(response_text)

        # Validate and format
        result = []
        for item in scores:
            if isinstance(item, dict) and "id" in item and "score" in item:
                score = max(1, min(10, int(item["score"])))
                prompt_info = next((p for p in prompts if p["id"] == item["id"]), None)
                if prompt_info:
                    result.append({
                        "id": item["id"],
                        "score": score,
                        "preview": prompt_info["prompt_preview"][:100]
                    })
        return result

    except Exception as e:
        # Return prompts without scores if LLM fails
        return [{"id": p["id"], "score": None, "preview": p["prompt_preview"][:100], "error": str(e)} for p in prompts]


@app.get("/api/prompts/deleted")
async def list_deleted_prompts(
    days: int = Query(default=30, le=365),
    limit: int = Query(default=500, le=5000)
):
    """List prompts that were deleted within the last N days."""
    conn = get_prompts_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, timestamp, project, char_count, importance_score,
               SUBSTR(prompt, 1, 200) as preview, deleted_at
        FROM prompts
        WHERE deleted_at IS NOT NULL
          AND datetime(deleted_at) >= datetime('now', ?)
        ORDER BY deleted_at DESC
        LIMIT ?
    """, (f"-{days} days", limit))

    prompts = rows_to_list(cursor.fetchall())
    conn.close()
    return prompts


@app.get("/api/prompt-projects")
async def list_prompt_projects():
    """List distinct project values present in prompts.db."""
    conn = get_prompts_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT project
        FROM prompts
        WHERE project IS NOT NULL
          AND TRIM(project) != ''
        ORDER BY LOWER(project), project
    """)
    projects = [row[0] for row in cursor.fetchall()]
    conn.close()
    return projects


@app.get("/api/prompts/prune-candidates")
async def get_prune_candidates(
    max_score: int = Query(default=3, ge=1, le=10),
    min_age_days: int = Query(default=30, ge=1),
    limit: int = Query(default=500, le=5000)
):
    """Get prompts that are candidates for pruning (low score + old)."""
    conn = get_prompts_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, timestamp, project, char_count, importance_score,
               SUBSTR(prompt, 1, 200) as preview
        FROM prompts
        WHERE deleted_at IS NULL
          AND passes_filter = 1
          AND importance_score IS NOT NULL
          AND importance_score <= ?
          AND datetime(timestamp) <= datetime('now', ?)
        ORDER BY importance_score ASC, timestamp ASC
        LIMIT ?
    """, (max_score, f"-{min_age_days} days", limit))

    prompts = rows_to_list(cursor.fetchall())
    conn.close()
    return prompts


# Parameterized routes MUST come after specific routes
@app.get("/api/prompts/{prompt_id}")
async def get_prompt(prompt_id: int):
    """Get full prompt detail by ID."""
    conn = get_prompts_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM prompts WHERE id = ?", (prompt_id,))
    prompt = row_to_dict(cursor.fetchone())
    conn.close()

    if not prompt:
        raise HTTPException(status_code=404, detail="Prompt not found")

    # Parse tags JSON
    try:
        prompt["tags"] = json.loads(prompt.get("tags") or "[]")
    except (json.JSONDecodeError, TypeError):
        prompt["tags"] = []

    return prompt


@app.delete("/api/prompts/{prompt_id}")
async def delete_prompt(prompt_id: int):
    """Soft delete a prompt by setting deleted_at timestamp."""
    conn = get_prompts_db()
    cursor = conn.cursor()

    # Check if prompt exists
    cursor.execute("SELECT id FROM prompts WHERE id = ?", (prompt_id,))
    if not cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Prompt not found")

    # Soft delete by setting deleted_at
    cursor.execute("""
        UPDATE prompts SET deleted_at = ? WHERE id = ?
    """, (datetime.now().isoformat(), prompt_id))

    conn.commit()
    conn.close()

    return {"deleted": True, "id": prompt_id}


# =============================================================================
# TAG VOCABULARY
# =============================================================================

@app.get("/api/tags")
async def list_tags(category: Optional[str] = None):
    """List all tags from the vocabulary."""
    conn = get_knowledge_db()
    cursor = conn.cursor()

    if category:
        cursor.execute("""
            SELECT id, name, category, description, color, source
            FROM tag_vocabulary
            WHERE category = ?
            ORDER BY category, name
        """, (category,))
    else:
        cursor.execute("""
            SELECT id, name, category, description, color, source
            FROM tag_vocabulary
            ORDER BY category, name
        """)

    tags = rows_to_list(cursor.fetchall())
    conn.close()

    # Group by category
    by_category = {}
    for tag in tags:
        cat = tag["category"] or "uncategorized"
        if cat not in by_category:
            by_category[cat] = []
        by_category[cat].append(tag)

    return {"tags": tags, "by_category": by_category}


class TagCreate(BaseModel):
    """Model for creating a new tag."""
    name: str
    category: Optional[str] = None
    description: Optional[str] = None
    color: Optional[str] = None


@app.post("/api/tags")
async def create_tag(tag: TagCreate):
    """Add a new tag to the vocabulary."""
    # Normalize name
    normalized = tag.name.lower().strip().replace(" ", "-")

    conn = get_knowledge_db()
    cursor = conn.cursor()

    # Check if exists
    cursor.execute("SELECT id FROM tag_vocabulary WHERE name = ?", (normalized,))
    if cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=409, detail=f"Tag '{normalized}' already exists")

    # Create new tag
    now = datetime.now().isoformat()
    cursor.execute("""
        INSERT INTO tag_vocabulary (name, category, description, color, created_at, source)
        VALUES (?, ?, ?, ?, ?, 'user-created')
    """, (normalized, tag.category, tag.description, tag.color, now))

    tag_id = cursor.lastrowid
    conn.commit()
    conn.close()

    return {
        "id": tag_id,
        "name": normalized,
        "category": tag.category,
        "description": tag.description,
        "color": tag.color,
        "source": "user-created"
    }


@app.delete("/api/tags/{tag_id}")
async def delete_tag(tag_id: int):
    """Delete a tag from the vocabulary."""
    conn = get_knowledge_db()
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM tag_vocabulary WHERE id = ?", (tag_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Tag not found")

    cursor.execute("DELETE FROM tag_vocabulary WHERE id = ?", (tag_id,))
    conn.commit()
    conn.close()

    return {"deleted": True, "id": tag_id}


class PromptTagsUpdate(BaseModel):
    """Model for updating prompt tags."""
    tags: list[str]


@app.put("/api/prompts/{prompt_id}/tags")
async def update_prompt_tags(prompt_id: int, data: PromptTagsUpdate):
    """Update tags for a specific prompt."""
    # Normalize tags
    tag_list = [t.strip().lower().replace(" ", "-") for t in data.tags if t.strip()]

    # Validate against vocabulary
    conn = get_knowledge_db()
    cursor = conn.cursor()
    if tag_list:
        placeholders = ",".join("?" * len(tag_list))
        cursor.execute(f"SELECT name FROM tag_vocabulary WHERE name IN ({placeholders})", tag_list)
        valid_tags = [row[0] for row in cursor.fetchall()]
    else:
        valid_tags = []
    conn.close()

    unknown_tags = [t for t in tag_list if t not in valid_tags]

    # Update prompt
    prompts_conn = get_prompts_db()
    cursor = prompts_conn.cursor()

    cursor.execute("SELECT id FROM prompts WHERE id = ?", (prompt_id,))
    if not cursor.fetchone():
        prompts_conn.close()
        raise HTTPException(status_code=404, detail="Prompt not found")

    cursor.execute(
        "UPDATE prompts SET tags = ? WHERE id = ?",
        (json.dumps(valid_tags), prompt_id)
    )
    prompts_conn.commit()
    prompts_conn.close()

    return {
        "prompt_id": prompt_id,
        "tags": valid_tags,
        "unknown_tags": unknown_tags if unknown_tags else None
    }


class SuggestTagsRequest(BaseModel):
    """Request model for tag suggestions."""
    prompt_ids: list[int]


@app.post("/api/prompts/suggest-tags")
async def suggest_tags_for_prompts(data: SuggestTagsRequest):
    """Use AI to suggest tags for prompts."""
    if OPENAI_CLIENT is None:
        raise HTTPException(
            status_code=503,
            detail="OpenAI client not available. Check OPENAI_API_KEY in .env"
        )

    # Get prompts
    prompts_conn = get_prompts_db()
    cursor = prompts_conn.cursor()

    placeholders = ",".join("?" * len(data.prompt_ids))
    cursor.execute(f"""
        SELECT id, SUBSTR(prompt, 1, 500) as prompt_preview, project
        FROM prompts
        WHERE id IN ({placeholders}) AND deleted_at IS NULL
          AND passes_filter = 1
    """, data.prompt_ids)
    prompts = rows_to_list(cursor.fetchall())
    prompts_conn.close()

    if not prompts:
        return {"suggestions": [], "message": "No prompts found"}

    # Get vocabulary
    conn = get_knowledge_db()
    cursor = conn.cursor()
    cursor.execute("SELECT name, category, description FROM tag_vocabulary")
    vocab = rows_to_list(cursor.fetchall())
    conn.close()

    vocab_text = "\n".join([f"- {t['name']} ({t['category']}): {t['description'] or 'no description'}" for t in vocab])

    # Format prompts for LLM
    prompts_text = "\n\n".join([
        f"[ID: {p['id']}]\nProject: {p['project'] or 'Unknown'}\nContent: {p['prompt_preview']}"
        for p in prompts
    ])

    suggest_prompt = f"""You are classifying user prompts with tags from a fixed vocabulary.

Available tags:
{vocab_text}

For each prompt, suggest 1-3 tags that best describe it. Only use tags from the vocabulary above.

Prompts to classify:

{prompts_text}

Return a JSON array with objects containing "id" (integer) and "tags" (array of tag names).
Only return the JSON array, no explanation. Example:
[{{"id": 1, "tags": ["feature-request", "frontend"]}}, {{"id": 2, "tags": ["bug-report"]}}]"""

    try:
        response = OPENAI_CLIENT.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=1024,
            messages=[{"role": "user", "content": suggest_prompt}]
        )

        response_text = response.choices[0].message.content.strip()
        suggestions = json.loads(response_text)

        return {"suggestions": suggestions}

    except Exception as e:
        logger.exception("AI suggestion failed")
        raise HTTPException(status_code=500, detail="AI suggestion failed")


class ProcessAllRequest(BaseModel):
    """Request model for bulk processing prompts."""
    min_score_to_keep: int = 7  # Soft-delete prompts below this score
    dry_run: bool = False  # If True, don't apply changes, just report what would happen


@app.post("/api/prompts/process-all")
async def process_all_prompts(data: ProcessAllRequest = ProcessAllRequest()):
    """
    Bulk process all prompts: score unscored, tag untagged, hide low scores.

    This is a long-running operation that:
    1. Scores all unscored prompts using GPT-4o-mini
    2. Tags all untagged prompts using GPT-4o-mini
    3. Soft-deletes prompts with score < min_score_to_keep

    Returns summary of actions taken.
    """
    if OPENAI_CLIENT is None:
        raise HTTPException(
            status_code=503,
            detail="OpenAI client not available. Check OPENAI_API_KEY in .env"
        )

    results = {
        "scored": 0,
        "tagged": 0,
        "soft_deleted": 0,
        "errors": [],
        "dry_run": data.dry_run
    }

    conn = get_prompts_db()
    cursor = conn.cursor()

    # =========================================================================
    # STEP 1: Score all unscored prompts
    # =========================================================================
    cursor.execute("""
        SELECT id, timestamp, project, char_count,
               SUBSTR(prompt, 1, 500) as prompt_preview
        FROM prompts
        WHERE importance_score IS NULL AND deleted_at IS NULL
          AND passes_filter = 1
        ORDER BY timestamp ASC
    """)
    unscored = rows_to_list(cursor.fetchall())
    results["unscored_count"] = len(unscored)

    # For dry_run, just report counts without API calls
    if data.dry_run:
        results["scored"] = len(unscored)  # Would score all unscored
    elif unscored:
        batch_size = 10
        for i in range(0, len(unscored), batch_size):
            batch = unscored[i:i + batch_size]
            try:
                batch_suggestions = await score_batch_with_llm(batch)

                # Apply scores directly to DB
                for suggestion in batch_suggestions:
                    if suggestion.get("score") is not None:
                        cursor.execute(
                            "UPDATE prompts SET importance_score = ?, scored_by = 'gpt' WHERE id = ?",
                            (suggestion["score"], suggestion["id"])
                        )
                        results["scored"] += 1
                conn.commit()

            except Exception as e:
                results["errors"].append(f"Scoring batch {i//batch_size + 1}: {str(e)}")

            # Rate limit
            if i + batch_size < len(unscored):
                await asyncio.sleep(0.5)

    # =========================================================================
    # STEP 2: Soft-delete prompts below threshold
    # =========================================================================
    cursor.execute("""
        SELECT COUNT(*) FROM prompts
        WHERE importance_score IS NOT NULL
          AND importance_score < ?
          AND deleted_at IS NULL
          AND passes_filter = 1
    """, (data.min_score_to_keep,))
    to_delete_count = cursor.fetchone()[0]
    results["to_soft_delete"] = to_delete_count

    if not data.dry_run and to_delete_count > 0:
        now = datetime.now().isoformat()
        cursor.execute("""
            UPDATE prompts SET deleted_at = ?
            WHERE importance_score IS NOT NULL
              AND importance_score < ?
              AND deleted_at IS NULL
              AND passes_filter = 1
        """, (now, data.min_score_to_keep))
        results["soft_deleted"] = cursor.rowcount
        conn.commit()

    # =========================================================================
    # STEP 3: Tag all untagged prompts (only those still active after pruning)
    # =========================================================================
    cursor.execute("""
        SELECT id, SUBSTR(prompt, 1, 500) as prompt_preview, project
        FROM prompts
        WHERE (tags IS NULL OR tags = '[]' OR tags = '')
          AND deleted_at IS NULL
          AND passes_filter = 1
        ORDER BY timestamp ASC
    """)
    untagged = rows_to_list(cursor.fetchall())
    results["untagged_count"] = len(untagged)

    # For dry_run, just report counts without API calls
    if data.dry_run:
        results["tagged"] = len(untagged)  # Would tag all untagged
    elif untagged:
        # Get vocabulary for tagging
        knowledge_conn = get_knowledge_db()
        knowledge_cursor = knowledge_conn.cursor()
        knowledge_cursor.execute("SELECT name, category, description FROM tag_vocabulary")
        vocab = rows_to_list(knowledge_cursor.fetchall())
        knowledge_conn.close()

        vocab_text = "\n".join([
            f"- {t['name']} ({t['category']}): {t['description'] or 'no description'}"
            for t in vocab
        ])

        batch_size = 10
        for i in range(0, len(untagged), batch_size):
            batch = untagged[i:i + batch_size]
            try:
                # Format prompts for LLM
                prompts_text = "\n\n".join([
                    f"[ID: {p['id']}]\nProject: {p['project'] or 'Unknown'}\nContent: {p['prompt_preview']}"
                    for p in batch
                ])

                tag_prompt = f"""You are classifying user prompts with tags from a fixed vocabulary.

Available tags:
{vocab_text}

For each prompt, suggest 1-3 tags that best describe it. Only use tags from the vocabulary above.

Prompts to classify:

{prompts_text}

Return a JSON array with objects containing "id" (integer) and "tags" (array of tag names).
Only return the JSON array, no explanation. Example:
[{{"id": 1, "tags": ["feature-request", "frontend"]}}, {{"id": 2, "tags": ["bug-report"]}}]"""

                response = OPENAI_CLIENT.chat.completions.create(
                    model="gpt-4o-mini",
                    max_tokens=1024,
                    messages=[{"role": "user", "content": tag_prompt}]
                )

                response_text = response.choices[0].message.content.strip()
                suggestions = json.loads(response_text)

                for suggestion in suggestions:
                    if suggestion.get("tags"):
                        tags_json = json.dumps(suggestion["tags"])
                        cursor.execute(
                            "UPDATE prompts SET tags = ? WHERE id = ?",
                            (tags_json, suggestion["id"])
                        )
                        results["tagged"] += 1
                conn.commit()

            except Exception as e:
                results["errors"].append(f"Tagging batch {i//batch_size + 1}: {str(e)}")

            # Rate limit
            if i + batch_size < len(untagged):
                await asyncio.sleep(0.5)

    conn.close()

    # Summary
    results["summary"] = (
        f"Scored {results['scored']}/{results['unscored_count']}, "
        f"pruned {results.get('soft_deleted', 0)}/{results['to_soft_delete']} below {data.min_score_to_keep}, "
        f"tagged {results['tagged']}/{results['untagged_count']}"
    )

    return results


# =============================================================================
# PROJECTS LIST (legacy endpoint removed — now served by /api/projects above)
# =============================================================================


# =============================================================================
# DOCUMENTS BROWSER
# =============================================================================

@app.get("/api/docs")
async def list_documents(category: Optional[str] = None, project: Optional[str] = None):
    """List all indexed documents with tree structure."""
    conn = get_knowledge_db()
    cursor = conn.cursor()

    conditions = []
    params = []

    if category:
        conditions.append("category = ?")
        params.append(category)

    if project:
        conditions.append("project = ?")
        params.append(project)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

    cursor.execute(f"""
        SELECT id, path, category, project, title, file_type, indexed_at, modified_at, watched_dir_id
        FROM documents
        {where_clause}
        ORDER BY category, modified_at DESC
    """, params)

    documents = rows_to_list(cursor.fetchall())
    conn.close()

    # Build tree structure
    tree = {}
    for doc in documents:
        category = doc["category"]
        project = doc["project"] or "_global"

        if category not in tree:
            tree[category] = {"_global": [], "projects": {}}

        if project == "_global":
            tree[category]["_global"].append(doc)
        else:
            if project not in tree[category]["projects"]:
                tree[category]["projects"][project] = []
            tree[category]["projects"][project].append(doc)

    return {"documents": documents, "tree": tree}


@app.get("/api/docs/search")
async def search_documents(
    q: str = Query(..., min_length=1),
    category: Optional[str] = None,
    limit: int = Query(default=50, le=200)
):
    """Full-text search across all indexed documents."""
    conn = get_knowledge_db()
    cursor = conn.cursor()

    # Build FTS query
    if category:
        cursor.execute("""
            SELECT d.id, d.path, d.title, d.category, d.project, d.file_type,
                   snippet(documents_fts, 1, '<mark>', '</mark>', '...', 32) as snippet
            FROM documents d
            JOIN documents_fts fts ON d.id = fts.rowid
            WHERE documents_fts MATCH ? AND d.category = ?
            ORDER BY rank
            LIMIT ?
        """, (q, category, limit))
    else:
        cursor.execute("""
            SELECT d.id, d.path, d.title, d.category, d.project, d.file_type,
                   snippet(documents_fts, 1, '<mark>', '</mark>', '...', 32) as snippet
            FROM documents d
            JOIN documents_fts fts ON d.id = fts.rowid
            WHERE documents_fts MATCH ?
            ORDER BY rank
            LIMIT ?
        """, (q, limit))

    results = rows_to_list(cursor.fetchall())
    conn.close()

    return {"results": results, "query": q}


@app.get("/api/docs/categories")
async def list_document_categories():
    """Get list of document categories with counts."""
    conn = get_knowledge_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT category, COUNT(*) as count
        FROM documents
        GROUP BY category
        ORDER BY category
    """)

    categories = rows_to_list(cursor.fetchall())
    conn.close()

    return categories


@app.post("/api/docs/reindex")
async def reindex_documents():
    """Re-scan and index all document sources."""
    stats = index_documents()
    return stats


class AddDocumentRequest(BaseModel):
    """Request model for adding a single document."""
    path: str


@app.post("/api/docs/add")
async def add_document(data: AddDocumentRequest):
    """Add a single document by file path."""
    # Validate absolute path
    file_path = Path(data.path)
    if not file_path.is_absolute():
        raise HTTPException(status_code=400, detail="Path must be absolute")

    result = add_single_document(file_path)

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    return result


# --- Watched directories endpoints (BEFORE parameterized /{doc_id} routes) ---

class WatchDirectoryRequest(BaseModel):
    """Request model for watching a directory."""
    directory: str
    glob_pattern: str = "**/*.md"
    category: Optional[str] = None
    project: Optional[str] = None
    label: Optional[str] = None


@app.get("/api/docs/browse")
async def browse_directory(
    directory: str = Query(...),
    pattern: str = Query(default="**/*.md"),
):
    """Preview files in a directory matching a glob pattern (max 200)."""
    dir_path = Path(directory)
    if not dir_path.is_absolute():
        raise HTTPException(status_code=400, detail="Directory must be an absolute path")
    if not dir_path.exists():
        raise HTTPException(status_code=404, detail=f"Directory not found: {directory}")
    if not dir_path.is_dir():
        raise HTTPException(status_code=400, detail=f"Path is not a directory: {directory}")

    files = []
    try:
        for f in dir_path.glob(pattern):
            if f.is_file():
                files.append({
                    "path": str(f),
                    "name": f.name,
                    "size": f.stat().st_size,
                    "modified": datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
                })
                if len(files) >= 200:
                    break
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Glob error: {e}")

    return {"directory": directory, "pattern": pattern, "files": files, "count": len(files)}


@app.post("/api/docs/watch")
async def add_watch(data: WatchDirectoryRequest):
    """Add a watched directory and trigger reindex."""
    dir_path = Path(data.directory)
    if not dir_path.is_absolute():
        raise HTTPException(status_code=400, detail="Directory must be an absolute path")
    if not dir_path.exists():
        raise HTTPException(status_code=404, detail=f"Directory not found: {data.directory}")
    if not dir_path.is_dir():
        raise HTTPException(status_code=400, detail=f"Path is not a directory: {data.directory}")

    # Infer category if not provided
    category = data.category
    if not category:
        dir_name = dir_path.name.lower()
        if dir_name == "docs":
            category = "doc"
        elif dir_name == "specs":
            category = "spec"
        else:
            category = "watched"

    # Infer project from path if under DEV_ROOT
    project = data.project
    if not project:
        dev_root_str = str(DEV_ROOT)
        if str(dir_path).startswith(dev_root_str):
            try:
                relative = dir_path.relative_to(DEV_ROOT)
                if len(relative.parts) >= 1:
                    project = relative.parts[0]
            except ValueError:
                pass

    # Label defaults to directory basename
    label = data.label or dir_path.name

    conn = get_knowledge_db()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO watched_directories (directory, glob_pattern, category, project, label, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (str(dir_path), data.glob_pattern, category, project, label,
              datetime.now().isoformat()))
        watch_id = cursor.lastrowid
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        raise HTTPException(
            status_code=409,
            detail=f"Already watching {data.directory} with pattern {data.glob_pattern}"
        )

    conn.close()

    # Trigger reindex to pick up the new watch
    stats = index_documents()

    return {
        "id": watch_id,
        "directory": str(dir_path),
        "glob_pattern": data.glob_pattern,
        "category": category,
        "project": project,
        "label": label,
        "reindex_stats": stats,
    }


@app.get("/api/docs/watches")
async def list_watches():
    """List all watched directories with file counts and existence check."""
    conn = get_knowledge_db()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM watched_directories ORDER BY created_at DESC")
    watches = rows_to_list(cursor.fetchall())

    # Enrich with file counts and existence check
    for watch in watches:
        dir_path = Path(watch["directory"])
        watch["exists"] = dir_path.exists()

        # Count indexed docs from this watch
        cursor.execute(
            "SELECT COUNT(*) FROM documents WHERE watched_dir_id = ?",
            (watch["id"],)
        )
        watch["file_count"] = cursor.fetchone()[0]

    conn.close()
    return watches


@app.delete("/api/docs/watch/{watch_id}")
async def remove_watch(watch_id: int):
    """Remove a watched directory and delete its indexed documents."""
    conn = get_knowledge_db()
    cursor = conn.cursor()

    # Check the watch exists
    cursor.execute("SELECT id, directory FROM watched_directories WHERE id = ?", (watch_id,))
    watch = cursor.fetchone()
    if not watch:
        conn.close()
        raise HTTPException(status_code=404, detail="Watch not found")

    # Delete indexed docs that came from this watch
    cursor.execute("DELETE FROM documents WHERE watched_dir_id = ?", (watch_id,))
    removed_docs = cursor.rowcount

    # Delete the watch itself
    cursor.execute("DELETE FROM watched_directories WHERE id = ?", (watch_id,))
    conn.commit()

    # Rebuild link graph since docs were removed
    build_link_graph(conn)

    conn.close()

    return {"deleted": True, "watch_id": watch_id, "removed_docs": removed_docs}


@app.get("/api/docs/{doc_id}")
async def get_document(doc_id: int):
    """Get full document content by ID."""
    conn = get_knowledge_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, path, category, project, title, content, file_type, checksum, indexed_at, modified_at
        FROM documents
        WHERE id = ?
    """, (doc_id,))

    doc = row_to_dict(cursor.fetchone())
    conn.close()

    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")

    return doc


@app.get("/api/docs/{doc_id}/backlinks")
async def get_document_backlinks(doc_id: int):
    """Get documents that link to this document."""
    conn = get_knowledge_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT d.id, d.path, d.title, d.category, d.project, dl.target_pattern, dl.link_type
        FROM document_links dl
        JOIN documents d ON dl.source_id = d.id
        WHERE dl.target_id = ?
        ORDER BY d.category, d.title
    """, (doc_id,))

    backlinks = rows_to_list(cursor.fetchall())
    conn.close()

    return {"backlinks": backlinks, "count": len(backlinks)}


@app.get("/api/docs/{doc_id}/links")
async def get_document_outlinks(doc_id: int):
    """Get documents that this document links to."""
    conn = get_knowledge_db()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT dl.target_pattern, dl.link_type, dl.target_id,
               d.path, d.title, d.category
        FROM document_links dl
        LEFT JOIN documents d ON dl.target_id = d.id
        WHERE dl.source_id = ?
        ORDER BY dl.link_type, dl.target_pattern
    """, (doc_id,))

    links = rows_to_list(cursor.fetchall())
    conn.close()

    return {"links": links, "count": len(links)}


@app.post("/api/docs/{doc_id}/open")
async def open_document_in_editor(doc_id: int):
    """Open document in VS Code."""
    conn = get_knowledge_db()
    cursor = conn.cursor()

    cursor.execute("SELECT path FROM documents WHERE id = ?", (doc_id,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Document not found")

    file_path = row[0]

    try:
        # Open in VS Code
        subprocess.Popen(["code", file_path])
        return {"opened": True, "path": file_path}
    except Exception as e:
        logger.exception("Failed to open file: %s", file_path)
        raise HTTPException(status_code=500, detail="Failed to open file")


# =============================================================================
# QA — Verification Control Panel
# =============================================================================

# Project discovery: scan DEV_ROOT for projects that have a graph.json in specs/
def _discover_qa_projects():
    """Find projects with graph.json files."""
    projects = []
    for child in DEV_ROOT.iterdir():
        if not child.is_dir():
            continue
        # Look for graph.json files in specs/
        specs_dir = child / "specs"
        if not specs_dir.exists():
            continue
        for f in specs_dir.iterdir():
            if f.name.endswith("-graph.json") or f.name == "graph.json":
                projects.append({
                    "name": child.name,
                    "path": str(child),
                    "graph_file": str(f),
                    "has_coverage": (child / "tests" / "e2e" / "coverage.json").exists(),
                    "has_screenshots": (child / "demos" / "images").is_dir(),
                    "has_findings": (child / "specs" / "qa-findings.json").exists(),
                    "has_manual_qa": (child / "tests" / "e2e" / "manual-qa.json").exists(),
                })
                break  # one graph per project is enough
    return projects


def _resolve_project(project: str):
    """Resolve project name to paths, raise 404 if not found."""
    project_dir = DEV_ROOT / project
    if not project_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"Project not found: {project}")

    # Find graph file
    specs_dir = project_dir / "specs"
    graph_file = None
    if specs_dir.exists():
        for f in specs_dir.iterdir():
            if f.name.endswith("-graph.json") or f.name == "graph.json":
                graph_file = f
                break

    return {
        "dir": project_dir,
        "specs": specs_dir,
        "graph_file": graph_file,
        "coverage_file": project_dir / "tests" / "e2e" / "coverage.json",
        "findings_file": specs_dir / "qa-findings.json" if specs_dir.exists() else None,
        "verify_plan_file": project_dir / "tests" / "e2e" / "verify-plan.json",
        "progress_file": project_dir / "tests" / "e2e" / "verify-progress.json",
        "manual_qa_file": project_dir / "tests" / "e2e" / "manual-qa.json",
        "findings_images_dir": project_dir / "demos" / "images" / "findings",
        "scenario_images_dir": project_dir / "demos" / "images" / "scenarios",
        "demos_dir": project_dir / "demos" / "images",
        "prev_dir": project_dir / "demos" / "images" / "_previous",
    }


def _load_json(path: Path):
    """Load a JSON file, return None if not found."""
    if path and path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def _compute_manual_status(scenarios: dict) -> str | None:
    """Compute aggregate manual status for an event from its scenario statuses.

    Returns: "pass" | "finding" | "fail" | "partial" | None (if no reviews)
    """
    statuses = [
        s.get("manual_status") for s in scenarios.values()
        if s.get("manual_status") not in (None, "untested", "n/a", "skip")
    ]
    if not statuses:
        return None
    if any(s == "fail" for s in statuses):
        return "fail"
    if any(s == "finding" for s in statuses):
        return "finding"
    if all(s == "pass" for s in statuses):
        return "pass"
    return "partial"


def _count_testable_scenarios(plan_scenarios: list) -> int:
    """Count scenarios that can be manually tested (excludes api_only/expected_fail)."""
    return sum(
        1 for s in plan_scenarios
        if s.get("tag") not in ("api_only", "expected_fail")
    )


@app.get("/api/qa/projects")
async def qa_list_projects():
    """List projects that have graph.json files."""
    return {"projects": _discover_qa_projects()}


@app.get("/api/qa/coverage")
async def qa_coverage(project: str = Query(..., description="Project directory name")):
    """Get event coverage: merge graph.json events with coverage.json results."""
    paths = _resolve_project(project)

    graph = _load_json(paths["graph_file"])
    if not graph:
        raise HTTPException(status_code=404, detail="No graph.json found")

    raw_coverage = _load_json(paths["coverage_file"]) or {}

    # Normalize coverage format: support both event-keyed and spec-keyed schemas
    if "events" in raw_coverage and isinstance(raw_coverage["events"], dict):
        # Event-keyed format (brief spec): { "events": { "e_po_create": { ... } } }
        coverage_by_event = raw_coverage["events"]
    elif "specs" in raw_coverage:
        # Spec-keyed format (verify output): { "specs": { "file.spec.ts": { "events": [...], ... } } }
        coverage_by_event = {}
        for spec_name, spec_data in raw_coverage["specs"].items():
            spec_events = spec_data.get("events", [])
            for eid in spec_events:
                coverage_by_event[eid] = {
                    "status": spec_data.get("status", "unknown"),
                    "scenarios": {"total": spec_data.get("tests", 0),
                                  "passed": spec_data.get("pass", 0),
                                  "failed": spec_data.get("fail", 0),
                                  "skipped": spec_data.get("skip", 0)},
                    "last_run": spec_data.get("last_run"),
                    "spec_file": f"tests/e2e/{spec_name}",
                    "notes": spec_data.get("notes", ""),
                }
    else:
        coverage_by_event = {}

    # Extract events from graph nodes
    events = [n for n in graph.get("nodes", []) if n.get("type") == "event"]

    # Group by category
    categories = {}
    stats = {"total": 0, "passed": 0, "failed": 0, "untested": 0}

    for event in events:
        cat = event.get("category", "uncategorized")
        if cat not in categories:
            categories[cat] = {"events": [], "covered": 0, "total": 0}

        event_id = event["id"]
        cov = coverage_by_event.get(event_id)

        entry = {
            "id": event_id,
            "name": event["name"],
            "category": cat,
            "description": event.get("description", ""),
        }

        if cov:
            entry["status"] = cov.get("status", "unknown")
            entry["scenarios"] = cov.get("scenarios", {})
            entry["last_run"] = cov.get("last_run")
            entry["duration_ms"] = cov.get("duration_ms")
            entry["spec_file"] = cov.get("spec_file")
            entry["video"] = cov.get("video")
            entry["findings_count"] = len(cov.get("findings", []))
            categories[cat]["covered"] += 1
            if entry["status"] == "pass":
                stats["passed"] += 1
            else:
                stats["failed"] += 1
        else:
            entry["status"] = "untested"
            stats["untested"] += 1

        stats["total"] += 1
        categories[cat]["total"] += 1
        categories[cat]["events"].append(entry)

    # Coverage percentage
    tested = stats["passed"] + stats["failed"]
    stats["coverage_pct"] = round(tested / stats["total"] * 100) if stats["total"] > 0 else 0

    # --- Manual QA merge ---
    manual = _load_json(paths["manual_qa_file"]) or {}
    manual_events = manual.get("events", {})
    verify_plan = _load_json(paths["verify_plan_file"])

    manual_reviewed_total = 0
    manual_events_total = 0

    for cat_data in categories.values():
        for entry in cat_data["events"]:
            eid = entry["id"]
            me = manual_events.get(eid, {})
            me_scenarios = me.get("scenarios", {})

            # Count testable scenarios from verify-plan if available
            plan_scenarios = []
            if verify_plan and "plans" in verify_plan:
                plan_scenarios = (
                    verify_plan["plans"]
                    .get(eid, {})
                    .get("matrix", {})
                    .get("scenarios", [])
                )
            testable_count = _count_testable_scenarios(plan_scenarios) if plan_scenarios else 0

            # Count reviewed (anything not untested/None/n/a)
            reviewed = sum(
                1 for s in me_scenarios.values()
                if s.get("manual_status") not in (None, "untested", "n/a")
            )
            findings_count = sum(
                len(s.get("findings", [])) for s in me_scenarios.values()
            )

            entry["manual"] = {
                "reviewed": reviewed,
                "total": testable_count,
                "findings_count": findings_count,
                "status": _compute_manual_status(me_scenarios),
                "tested_at": me.get("tested_at"),
            }

            manual_reviewed_total += reviewed
            manual_events_total += testable_count

    stats["manual_reviewed"] = manual_reviewed_total
    stats["manual_total"] = manual_events_total
    stats["manual_pct"] = (
        round(manual_reviewed_total / manual_events_total * 100)
        if manual_events_total > 0 else 0
    )

    # --- Lifecycle chains from graph ---
    chains = graph.get("lifecycle_chains", [])

    # Run metadata from coverage.json
    run_meta = {
        "generated": raw_coverage.get("generated"),
        "run_duration_ms": raw_coverage.get("run_duration_ms"),
        "spec_count": len(raw_coverage.get("specs", {})) or len(set(
            v.get("spec_file", "") for v in coverage_by_event.values() if v.get("spec_file")
        )),
    }

    return {"stats": stats, "categories": categories, "run": run_meta, "chains": chains}


@app.get("/api/qa/coverage/{event_id}")
async def qa_coverage_detail(event_id: str, project: str = Query(...)):
    """Get detailed coverage for a single event (scenarios, findings)."""
    paths = _resolve_project(project)

    graph = _load_json(paths["graph_file"])
    coverage = _load_json(paths["coverage_file"]) or {"events": {}}

    # Find event in graph — events are top-level events[] array, NOT in nodes[]
    event_node = None
    if graph:
        for ev in graph.get("events", []):
            if ev.get("id") == event_id:
                event_node = ev
                break
        # Fallback: check nodes[] for legacy graphs
        if not event_node:
            for n in graph.get("nodes", []):
                if n.get("id") == event_id:
                    event_node = n
                    break

    if not event_node:
        # Return a stub instead of 404 — event may exist in coverage but not graph
        event_node = {"id": event_id, "name": event_id}

    cov = coverage.get("events", {}).get(event_id, {})

    # Find edges involving this event
    edges_out = []
    edges_in = []
    for edge in graph.get("edges", []):
        if edge.get("source") == event_id:
            edges_out.append(edge)
        if edge.get("target") == event_id:
            edges_in.append(edge)

    # Load verify-plan data for this event (if exists)
    plan = None
    verify_plan = _load_json(paths["verify_plan_file"])
    if verify_plan and "plans" in verify_plan:
        plan = verify_plan["plans"].get(event_id)

    # --- Manual QA overlay per scenario ---
    manual = _load_json(paths["manual_qa_file"]) or {}
    event_manual = manual.get("events", {}).get(event_id, {}).get("scenarios", {})

    if plan and plan.get("matrix", {}).get("scenarios"):
        for scenario in plan["matrix"]["scenarios"]:
            sid = scenario["id"]
            ms = event_manual.get(sid, {})
            # Auto-classify api_only/expected_fail as n/a if no manual entry
            if scenario.get("tag") in ("api_only", "expected_fail") and not ms:
                ms = {"manual_status": "n/a"}
            scenario["manual_status"] = ms.get("manual_status", "untested")
            scenario["manual_notes"] = ms.get("notes")
            scenario["manual_findings"] = ms.get("findings", [])
            scenario["manual_screenshots"] = ms.get("screenshots", [])
            scenario["manual_tested_at"] = ms.get("tested_at")

    # Manual summary for this event
    manual_summary = None
    if event_manual:
        reviewed = sum(
            1 for s in event_manual.values()
            if s.get("manual_status") not in (None, "untested", "n/a")
        )
        total_scenarios = len(
            plan.get("matrix", {}).get("scenarios", [])
        ) if plan else 0
        testable = _count_testable_scenarios(
            plan.get("matrix", {}).get("scenarios", [])
        ) if plan else 0
        findings_count = sum(
            len(s.get("findings", [])) for s in event_manual.values()
        )
        manual_summary = {
            "reviewed": reviewed,
            "total": testable,
            "findings_count": findings_count,
            "status": _compute_manual_status(event_manual),
            "tested_at": manual.get("events", {}).get(event_id, {}).get("tested_at"),
        }

    return {
        "event": event_node,
        "coverage": cov,
        "results": cov.get("results", []),
        "findings": cov.get("findings", []),
        "read_assertions": cov.get("read_assertions", {}),
        "edges_in": edges_in,
        "edges_out": edges_out,
        "plan": plan,
        "manual_summary": manual_summary,
    }


def _screenshot_url(project: str, version: str, filename: str, base_dir: Path) -> str:
    """Build screenshot URL with mtime cache-buster."""
    url = f"/api/qa/screenshots/{project}/{version}/{filename}"
    file_path = base_dir / filename
    if file_path.exists():
        mtime = int(file_path.stat().st_mtime)
        url += f"?v={mtime}"
    return url


@app.get("/api/qa/gallery")
async def qa_gallery(
    project: str = Query(...),
    node_type: Optional[str] = Query(None, alias="type"),
):
    """Get screenshot gallery grouped by viewer hierarchy."""
    paths = _resolve_project(project)

    graph = _load_json(paths["graph_file"])
    if not graph:
        raise HTTPException(status_code=404, detail="No graph.json found")

    nodes = graph.get("nodes", [])
    demos_dir = paths["demos_dir"]
    prev_dir = paths["prev_dir"]

    # Filter to nodes that have screenshots
    visual_types = {"viewer", "sub-viewer", "modal"}
    if node_type:
        visual_types = {node_type}

    # Build viewer groups (parent → children)
    viewers = {}  # id → node info
    children = {}  # parent_id → [child nodes]

    # Collect all node IDs to distinguish variants from other nodes' base screenshots
    all_node_ids = {n["id"] for n in nodes}

    for n in nodes:
        ntype = n.get("type")
        if ntype not in visual_types and ntype not in ("viewer",):
            continue

        nid = n["id"]
        screenshot_name = f"{nid}.png"
        has_current = (demos_dir / screenshot_name).exists() if demos_dir.exists() else False
        has_previous = (prev_dir / screenshot_name).exists() if prev_dir and prev_dir.exists() else False

        entry = {
            "id": nid,
            "name": n["name"],
            "type": ntype,
            "parent": n.get("parent"),
            "group": n.get("group", ""),
            "description": n.get("description", ""),
            "source_file": n.get("source_file", ""),
            "has_screenshot": has_current,
            "has_previous": has_previous,
            "screenshot_url": _screenshot_url(project, "current", screenshot_name, demos_dir) if has_current else None,
            "previous_url": _screenshot_url(project, "previous", screenshot_name, prev_dir) if has_previous else None,
        }

        # Detect variants (files matching {id}_{suffix}.png but NOT other node IDs)
        variants = []
        if demos_dir.exists():
            prefix = f"{nid}_"
            for img_file in demos_dir.iterdir():
                if img_file.name.startswith(prefix) and img_file.suffix == ".png":
                    # Skip if this file is actually another node's base screenshot
                    file_stem = img_file.stem  # e.g. "v_inv_skus" or "v_inv_skus_expanded"
                    if file_stem in all_node_ids:
                        continue
                    suffix = file_stem[len(prefix):]
                    # Skip if suffix contains underscore — means it belongs to a child node
                    # (e.g. v_inv_skus_expanded belongs to v_inv_skus, not v_inv)
                    if "_" in suffix:
                        continue
                    prev_exists = (prev_dir / img_file.name).exists() if prev_dir and prev_dir.exists() else False
                    variants.append({
                        "suffix": suffix,
                        "screenshot_url": _screenshot_url(project, "current", img_file.name, demos_dir),
                        "previous_url": _screenshot_url(project, "previous", img_file.name, prev_dir) if prev_exists else None,
                    })
        entry["variants"] = variants

        if ntype == "viewer":
            viewers[nid] = entry
            if nid not in children:
                children[nid] = []
        elif ntype in ("sub-viewer", "modal"):
            parent = n.get("parent")
            if parent:
                if parent not in children:
                    children[parent] = []
                children[parent].append(entry)
                # Ensure parent viewer exists in viewers dict
                if parent not in viewers:
                    parent_node = next((x for x in nodes if x["id"] == parent), None)
                    if parent_node:
                        p_name = f"{parent}.png"
                        # Detect variants for parent viewer
                        p_variants = []
                        if demos_dir.exists():
                            p_prefix = f"{parent}_"
                            for img_file in demos_dir.iterdir():
                                if img_file.name.startswith(p_prefix) and img_file.suffix == ".png":
                                    p_suffix = img_file.stem[len(p_prefix):]
                                    p_prev_exists = (prev_dir / img_file.name).exists() if prev_dir and prev_dir.exists() else False
                                    p_variants.append({
                                        "suffix": p_suffix,
                                        "screenshot_url": _screenshot_url(project, "current", img_file.name, demos_dir),
                                        "previous_url": _screenshot_url(project, "previous", img_file.name, prev_dir) if p_prev_exists else None,
                                    })
                        p_has_current = (demos_dir / p_name).exists() if demos_dir.exists() else False
                        p_has_previous = (prev_dir / p_name).exists() if prev_dir and prev_dir.exists() else False
                        viewers[parent] = {
                            "id": parent,
                            "name": parent_node["name"],
                            "type": "viewer",
                            "parent": None,
                            "group": parent_node.get("group", ""),
                            "description": parent_node.get("description", ""),
                            "source_file": parent_node.get("source_file", ""),
                            "has_screenshot": p_has_current,
                            "has_previous": p_has_previous,
                            "screenshot_url": _screenshot_url(project, "current", p_name, demos_dir) if p_has_current else None,
                            "previous_url": _screenshot_url(project, "previous", p_name, prev_dir) if p_has_previous else None,
                            "variants": p_variants,
                        }

    # Build grouped output
    groups = []
    for vid, viewer in viewers.items():
        group = {
            "viewer": viewer,
            "children": children.get(vid, []),
        }
        groups.append(group)

    # Sort by group field
    groups.sort(key=lambda g: g["viewer"].get("group", ""))

    # Stats — count base screenshots + variants
    total_nodes = sum(1 for n in nodes if n.get("type") in visual_types)
    captured = sum(1 for g in groups if g["viewer"]["has_screenshot"])
    captured += sum(1 for g in groups for c in g["children"] if c["has_screenshot"])
    variant_count = sum(len(g["viewer"].get("variants", [])) for g in groups)
    variant_count += sum(len(c.get("variants", [])) for g in groups for c in g["children"])

    return {"groups": groups, "stats": {"total": total_nodes, "captured": captured, "variants": variant_count}}


@app.get("/api/qa/gallery/{node_id}")
async def qa_gallery_node(node_id: str, project: str = Query(...)):
    """Get detailed node info with edges for expanded screenshot view."""
    paths = _resolve_project(project)

    graph = _load_json(paths["graph_file"])
    if not graph:
        raise HTTPException(status_code=404, detail="No graph.json found")

    node = None
    for n in graph.get("nodes", []):
        if n.get("id") == node_id:
            node = n
            break

    if not node:
        raise HTTPException(status_code=404, detail=f"Node not found: {node_id}")

    # Find edges
    edges_in = [e for e in graph.get("edges", []) if e.get("target") == node_id]
    edges_out = [e for e in graph.get("edges", []) if e.get("source") == node_id]

    # Resolve edge node names
    node_names = {n["id"]: n["name"] for n in graph.get("nodes", [])}
    for e in edges_in:
        e["source_name"] = node_names.get(e.get("source"), e.get("source"))
    for e in edges_out:
        e["target_name"] = node_names.get(e.get("target"), e.get("target"))

    capture = node.get("capture", {})
    declared_variants = capture.get("variants", [])

    return {
        "node": node,
        "edges_in": edges_in,
        "edges_out": edges_out,
        "edge_count": {"in": len(edges_in), "out": len(edges_out)},
        "declared_variants": declared_variants,
    }


@app.get("/api/qa/screenshots/{project}/{version}/{filename}")
async def qa_serve_screenshot(project: str, version: str, filename: str):
    """Serve screenshot files from demos/images/ or demos/images/_previous/."""
    project_dir = DEV_ROOT / project
    if not project_dir.is_dir():
        raise HTTPException(status_code=404, detail="Project not found")

    if version == "current":
        file_path = project_dir / "demos" / "images" / filename
    elif version == "previous":
        file_path = project_dir / "demos" / "images" / "_previous" / filename
    else:
        raise HTTPException(status_code=400, detail="Version must be 'current' or 'previous'")

    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Screenshot not found")

    # Security: ensure path is within project
    try:
        file_path.resolve().relative_to(project_dir.resolve())
    except ValueError:
        raise HTTPException(status_code=403, detail="Access denied")

    return FileResponse(
        file_path,
        media_type="image/png",
        headers={"Cache-Control": "no-store"},
    )


@app.get("/api/qa/monitor")
async def qa_monitor(project: str = Query(...)):
    """Get live verify progress (from verify-plan.json) and run history."""
    paths = _resolve_project(project)

    # Check for active run via verify-plan.json active_event
    verify_plan = _load_json(paths["verify_plan_file"])
    running = False
    active_event = None
    active_plan = None
    phases_done = []

    if verify_plan:
        active_event = verify_plan.get("active_event")
        if active_event:
            active_plan = verify_plan.get("plans", {}).get(active_event, {})
            current_phase = active_plan.get("current_phase", "")
            running = current_phase not in ("complete", "")

            # Determine which phases are done based on which sections exist
            phase_order = ["scope", "matrix", "helpers", "spec", "results"]
            for phase in phase_order:
                if phase in active_plan:
                    phases_done.append(phase)

            # Check if stale (>1 hour)
            started = active_plan.get("started", "")
            if started and running:
                try:
                    started_dt = datetime.fromisoformat(started)
                    age_seconds = (datetime.now() - started_dt).total_seconds()
                    if age_seconds > 3600:
                        running = False
                        active_plan["stale"] = True
                except (ValueError, TypeError):
                    pass

    # All completed plans for history
    all_plans = verify_plan.get("plans", {}) if verify_plan else {}
    completed_plans = [
        {"event": eid, **p}
        for eid, p in all_plans.items()
        if p.get("current_phase") == "complete"
    ]

    # Run history from coverage.json
    cov_raw = _load_json(paths["coverage_file"])
    history = []
    if cov_raw:
        if "specs" in cov_raw:
            specs = cov_raw["specs"]
            passed = sum(1 for s in specs.values() if s.get("status") == "pass")
            failed = sum(1 for s in specs.values() if s.get("status") in ("fail", "error", "fixing"))
            total_events = sum(len(s.get("events", [])) for s in specs.values())
            history.append({
                "date": cov_raw.get("generated"),
                "events_tested": total_events,
                "passed": passed,
                "failed": failed,
                "duration_ms": cov_raw.get("run_duration_ms"),
            })
        elif "events" in cov_raw and isinstance(cov_raw["events"], dict):
            events = cov_raw["events"]
            passed = sum(1 for v in events.values() if v.get("status") == "pass")
            failed = sum(1 for v in events.values() if v.get("status") in ("fail", "error"))
            history.append({
                "date": cov_raw.get("generated"),
                "events_tested": len(events),
                "passed": passed,
                "failed": failed,
                "duration_ms": cov_raw.get("run_duration_ms"),
            })

    return {
        "running": running,
        "active_event": active_event,
        "active_plan": active_plan,
        "phases_done": phases_done,
        "completed_plans": completed_plans,
        "history": history,
    }


@app.get("/api/qa/matrix")
async def qa_matrix(project: str = Query(...)):
    """Build test matrix: events × viewers with read/write markers."""
    paths = _resolve_project(project)

    graph = _load_json(paths["graph_file"])
    coverage = _load_json(paths["coverage_file"]) or {"events": {}}

    if not graph:
        raise HTTPException(status_code=404, detail="No graph.json found")

    nodes = graph.get("nodes", [])
    edges = graph.get("edges", [])

    events = [n for n in nodes if n.get("type") == "event"]
    # Viewers that appear as read targets in coverage
    viewer_types = {"viewer", "sub-viewer"}
    visual_nodes = [n for n in nodes if n.get("type") in viewer_types]

    # Build matrix from coverage read_assertions and graph edges
    matrix = {}
    viewer_ids = set()

    for event in events:
        eid = event["id"]
        cov = coverage["events"].get(eid, {})
        read_assertions = cov.get("read_assertions", {})

        row = {}

        # Read assertions from coverage
        for vid, assertions in read_assertions.items():
            row[vid] = {"type": "R", "assertions": assertions}
            viewer_ids.add(vid)

        # Write targets from graph edges
        for edge in edges:
            if edge.get("source") == eid:
                target = edge.get("target", "")
                target_node = next((n for n in nodes if n["id"] == target), None)
                if target_node and target_node.get("type") in viewer_types:
                    if target not in row:
                        row[target] = {"type": "W", "assertions": []}
                    else:
                        row[target]["type"] = "W"  # write supersedes read
                    viewer_ids.add(target)

        matrix[eid] = row

    # Build ordered viewer list (only those that appear in matrix)
    viewer_list = [
        {"id": n["id"], "name": n["name"], "short": n["id"].replace("v_", "").replace("_", " ")}
        for n in visual_nodes if n["id"] in viewer_ids
    ]

    event_list = [
        {"id": e["id"], "name": e["name"], "short": e["name"][:12], "category": e.get("category", "")}
        for e in events
    ]

    return {
        "events": event_list,
        "viewers": viewer_list,
        "matrix": matrix,
    }


@app.get("/api/qa/findings")
async def qa_findings(project: str = Query(...)):
    """Get QA findings from verify-plan.json + manual-qa.json merged."""
    paths = _resolve_project(project)

    items = []

    # Automated findings from verify-plan.json
    verify_plan = _load_json(paths["verify_plan_file"])
    if verify_plan and "plans" in verify_plan:
        for event_id, plan in verify_plan["plans"].items():
            plan_findings = plan.get("results", {}).get("findings", [])
            for f in plan_findings:
                f.setdefault("event", event_id)
                f["source"] = "auto"
                items.append(f)

    # Fallback: if no plan findings, try qa-findings.json
    if not items:
        findings_data = _load_json(paths["findings_file"]) if paths["findings_file"] else None
        if findings_data:
            for f in findings_data.get("findings", []):
                f["source"] = "auto"
                items.append(f)

    # Manual findings from manual-qa.json
    manual_data = _load_json(paths["manual_qa_file"])
    if manual_data and "events" in manual_data:
        for event_id, event_data in manual_data["events"].items():
            for scenario_id, scenario_data in event_data.get("scenarios", {}).items():
                for f in scenario_data.get("findings", []):
                    items.append({
                        "id": f.get("id", f"MF_{event_id}_{scenario_id}"),
                        "title": f.get("title", ""),
                        "severity": f.get("severity", "medium"),
                        "expected": f.get("expected"),
                        "actual": f.get("actual"),
                        "pipeline_ref": f.get("pipeline_ref"),
                        "screenshots": f.get("screenshots", []),
                        "event": event_id,
                        "scenario": scenario_id,
                        "source": "manual",
                        "status": f.get("status", "open"),
                    })

    # Stats
    stats = {
        "total": len(items), "high": 0, "medium": 0, "low": 0,
        "open": 0, "fixed": 0, "verified": 0,
        "auto": sum(1 for i in items if i.get("source") == "auto"),
        "manual": sum(1 for i in items if i.get("source") == "manual"),
    }
    for item in items:
        sev = item.get("severity", "medium").lower()
        if sev in stats:
            stats[sev] += 1
        status = item.get("status", "open").lower()
        if status in stats:
            stats[status] += 1

    return {"stats": stats, "findings": items}


@app.post("/api/qa/findings/{finding_id}/status")
async def qa_update_finding_status(finding_id: str, project: str = Query(...), status: str = Query(...)):
    """Update a finding's status (open → fixed → verified)."""
    VALID_STATUSES = {"open", "fixed", "verified"}
    if status not in VALID_STATUSES:
        raise HTTPException(status_code=400, detail=f"Status must be one of: {VALID_STATUSES}")

    paths = _resolve_project(project)
    findings_file = paths["findings_file"]
    if not findings_file:
        raise HTTPException(status_code=404, detail="No findings file path")

    findings_data = _load_json(findings_file) or {"findings": []}

    # Find and update
    found = False
    for item in findings_data.get("findings", []):
        if item.get("id") == finding_id:
            item["status"] = status
            item["updated_at"] = datetime.now().isoformat()
            found = True
            break

    if not found:
        raise HTTPException(status_code=404, detail=f"Finding not found: {finding_id}")

    # Write back
    with open(findings_file, "w", encoding="utf-8") as f:
        json.dump(findings_data, f, indent=2)

    return {"updated": True, "id": finding_id, "status": status}


# ---- Manual QA endpoints ----

MAX_SCREENSHOT_SIZE = 5 * 1024 * 1024  # 5MB
ALLOWED_IMAGE_TYPES = {"image/png", "image/jpeg", "image/webp"}


@app.get("/api/qa/manual")
async def qa_get_manual(project: str = Query(..., description="Project directory name")):
    """Read manual-qa.json for a project."""
    paths = _resolve_project(project)
    data = _load_json(paths["manual_qa_file"]) or {"events": {}}
    return {"events": data.get("events", {}), "updated_at": data.get("updated_at")}


@app.post("/api/qa/manual")
async def qa_save_manual(project: str = Query(...), body: dict = Body(...)):
    """Save manual QA data for a single event.

    Body: {"event_id": "e_po_delivery", "scenarios": {"L4": {...}, "P5": {...}}}
    Merges into existing manual-qa.json — one event at a time.
    """
    paths = _resolve_project(project)
    manual_file = paths["manual_qa_file"]

    event_id = body.get("event_id")
    scenarios = body.get("scenarios")
    if not event_id or not isinstance(scenarios, dict):
        raise HTTPException(status_code=400, detail="Body must include event_id (str) and scenarios (dict)")

    # Load existing or create new
    data = _load_json(manual_file) or {"project": project, "events": {}}

    # Auto-classify n/a scenarios using verify-plan tags
    verify_plan = _load_json(paths["verify_plan_file"])
    if verify_plan:
        plan_scenarios = (
            verify_plan.get("plans", {})
            .get(event_id, {})
            .get("matrix", {})
            .get("scenarios", [])
        )
        auto_na_ids = {
            s["id"] for s in plan_scenarios
            if s.get("tag") in ("api_only", "expected_fail")
        }
        for sid in auto_na_ids:
            if sid not in scenarios:
                scenarios[sid] = {"manual_status": "n/a"}

    # Preserve scenario-level screenshots (managed by upload/delete endpoints)
    existing_event = data.get("events", {}).get(event_id, {})
    existing_scenarios = existing_event.get("scenarios", {})
    for sid, sc_data in scenarios.items():
        existing_sc = existing_scenarios.get(sid, {})
        if "screenshots" in existing_sc and "screenshots" not in sc_data:
            sc_data["screenshots"] = existing_sc["screenshots"]

    # Merge this event into the file
    now = datetime.now().isoformat()
    data["events"][event_id] = {
        "tested_at": now,
        "scenarios": scenarios,
    }
    data["updated_at"] = now

    # Write back
    manual_file.parent.mkdir(parents=True, exist_ok=True)
    with open(manual_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    return {"saved": True, "event_id": event_id, "updated_at": now}


@app.post("/api/qa/manual/screenshot")
async def qa_upload_finding_screenshot(
    project: str = Query(...),
    finding_id: str = Query(...),
    file: UploadFile = File(...),
):
    """Upload a screenshot as evidence for a manual finding."""
    paths = _resolve_project(project)
    findings_dir = paths["findings_images_dir"]

    # Validate file type
    if file.content_type not in ALLOWED_IMAGE_TYPES:
        raise HTTPException(status_code=400, detail=f"File type must be one of: {ALLOWED_IMAGE_TYPES}")

    # Read and validate size
    contents = await file.read()
    if len(contents) > MAX_SCREENSHOT_SIZE:
        raise HTTPException(status_code=413, detail=f"File too large. Max {MAX_SCREENSHOT_SIZE // (1024*1024)}MB")

    # Determine extension from content type
    ext_map = {"image/png": ".png", "image/jpeg": ".jpg", "image/webp": ".webp"}
    ext = ext_map.get(file.content_type, ".png")
    filename = f"{finding_id}{ext}"

    # Write file
    findings_dir.mkdir(parents=True, exist_ok=True)
    file_path = findings_dir / filename

    # Security: ensure path is within project
    try:
        file_path.resolve().relative_to(paths["dir"].resolve())
    except ValueError:
        raise HTTPException(status_code=403, detail="Path traversal detected")

    with open(file_path, "wb") as f:
        f.write(contents)

    serve_url = f"/api/qa/manual/screenshot/{project}/{filename}"
    return {"filename": filename, "url": serve_url}


@app.get("/api/qa/manual/screenshot/{project}/{filename}")
async def qa_serve_finding_screenshot(project: str, filename: str):
    """Serve a finding evidence screenshot."""
    paths = _resolve_project(project)
    file_path = paths["findings_images_dir"] / filename

    # Security: ensure path is within project
    try:
        file_path.resolve().relative_to(paths["dir"].resolve())
    except ValueError:
        raise HTTPException(status_code=403, detail="Path traversal detected")

    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Screenshot not found: {filename}")

    media_type = "image/png"
    if filename.endswith(".jpg") or filename.endswith(".jpeg"):
        media_type = "image/jpeg"
    elif filename.endswith(".webp"):
        media_type = "image/webp"

    return FileResponse(str(file_path), media_type=media_type)


@app.post("/api/qa/gallery/upload")
async def qa_gallery_upload(
    project: str = Query(...),
    node_id: str = Query(...),
    file: UploadFile = File(...),
):
    """Upload a manual gallery correction screenshot.

    Moves current automated screenshot to _previous/, saves uploaded file as current.
    """
    paths = _resolve_project(project)
    demos_dir = paths["demos_dir"]
    prev_dir = paths["prev_dir"]

    # Validate file type
    if file.content_type not in ALLOWED_IMAGE_TYPES:
        raise HTTPException(status_code=400, detail=f"File type must be one of: {ALLOWED_IMAGE_TYPES}")

    # Read and validate size
    contents = await file.read()
    if len(contents) > MAX_SCREENSHOT_SIZE:
        raise HTTPException(status_code=413, detail=f"File too large. Max {MAX_SCREENSHOT_SIZE // (1024*1024)}MB")

    filename = f"{node_id}.png"
    current_path = demos_dir / filename

    # Security: ensure path is within project
    try:
        current_path.resolve().relative_to(paths["dir"].resolve())
    except ValueError:
        raise HTTPException(status_code=403, detail="Path traversal detected")

    # Move current to _previous/ if it exists
    previous_saved = False
    if current_path.exists():
        prev_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(current_path), str(prev_dir / filename))
        previous_saved = True

    # Write new file
    demos_dir.mkdir(parents=True, exist_ok=True)
    with open(current_path, "wb") as f:
        f.write(contents)

    return {"filename": filename, "previous_saved": previous_saved}


# Run initial indexing on startup (async)
@app.get("/api/qa/recent-screenshots")
async def qa_recent_screenshots(limit: int = Query(12)):
    """List recent screenshots from Windows Screenshots folder, newest first."""
    screenshots_dir = Path(os.path.expanduser("~")) / "OneDrive" / "Pictures" / "Screenshots"
    if not screenshots_dir.exists():
        # Fallback to non-OneDrive path
        screenshots_dir = Path(os.path.expanduser("~")) / "Pictures" / "Screenshots"
    if not screenshots_dir.exists():
        return {"files": []}

    files = []
    for f in screenshots_dir.iterdir():
        if f.suffix.lower() in (".png", ".jpg", ".jpeg", ".webp"):
            files.append({"name": f.name, "path": str(f), "mtime": f.stat().st_mtime})

    files.sort(key=lambda x: x["mtime"], reverse=True)
    return {"files": files[:limit], "dir": str(screenshots_dir)}


@app.get("/api/qa/recent-screenshots/{filename}")
async def qa_serve_recent_screenshot(filename: str):
    """Serve a screenshot file from the Screenshots folder (for thumbnail preview)."""
    screenshots_dir = Path(os.path.expanduser("~")) / "OneDrive" / "Pictures" / "Screenshots"
    if not screenshots_dir.exists():
        screenshots_dir = Path(os.path.expanduser("~")) / "Pictures" / "Screenshots"

    file_path = screenshots_dir / filename
    # Security: ensure within screenshots dir
    try:
        file_path.resolve().relative_to(screenshots_dir.resolve())
    except ValueError:
        raise HTTPException(status_code=403, detail="Path traversal detected")

    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Not found")

    media_type = "image/png"
    if filename.lower().endswith((".jpg", ".jpeg")):
        media_type = "image/jpeg"

    return FileResponse(str(file_path), media_type=media_type)


@app.post("/api/qa/snip")
async def qa_launch_snipping_tool():
    """Launch Windows screen capture overlay directly (no +New needed)."""
    try:
        # ms-screenclip: opens the capture overlay immediately on Windows 11
        os.startfile("ms-screenclip:")
        return {"ok": True}
    except Exception:
        try:
            subprocess.Popen(["snippingtool", "/clip"], shell=False)
            return {"ok": True}
        except Exception:
            raise HTTPException(status_code=500, detail="Could not launch snipping tool")


@app.post("/api/qa/manual/screenshot/paste")
async def qa_paste_screenshot(
    project: str = Query(...),
    finding_id: str = Query(...),
    file: UploadFile = File(...),
):
    """Save a pasted clipboard screenshot as finding evidence."""
    paths = _resolve_project(project)
    findings_dir = paths["findings_images_dir"]
    findings_dir.mkdir(parents=True, exist_ok=True)

    # Read and validate
    content = await file.read()
    if len(content) > MAX_SCREENSHOT_SIZE:
        raise HTTPException(status_code=413, detail="File too large (5MB max)")

    # Generate filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{finding_id}_{timestamp}.png"
    dest = findings_dir / filename

    with open(dest, "wb") as f:
        f.write(content)

    return {"filename": filename, "path": str(dest)}


# --- Scenario-level screenshots (context captures, not finding evidence) ---

@app.post("/api/qa/scenario/screenshot")
async def qa_upload_scenario_screenshot(
    project: str = Query(...),
    event_id: str = Query(...),
    scenario_id: str = Query(...),
    file: UploadFile = File(...),
):
    """Upload a context screenshot for a specific scenario."""
    paths = _resolve_project(project)
    scenario_dir = paths["scenario_images_dir"] / event_id
    scenario_dir.mkdir(parents=True, exist_ok=True)

    if file.content_type not in ALLOWED_IMAGE_TYPES:
        raise HTTPException(status_code=400, detail=f"File type must be one of: {ALLOWED_IMAGE_TYPES}")

    contents = await file.read()
    if len(contents) > MAX_SCREENSHOT_SIZE:
        raise HTTPException(status_code=413, detail=f"File too large. Max {MAX_SCREENSHOT_SIZE // (1024*1024)}MB")

    ext_map = {"image/png": ".png", "image/jpeg": ".jpg", "image/webp": ".webp"}
    ext = ext_map.get(file.content_type, ".png")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{scenario_id}_{timestamp}{ext}"
    file_path = scenario_dir / filename

    # Security: ensure path is within project
    try:
        file_path.resolve().relative_to(paths["dir"].resolve())
    except ValueError:
        raise HTTPException(status_code=403, detail="Path traversal detected")

    with open(file_path, "wb") as f:
        f.write(contents)

    # Auto-register in manual-qa.json
    manual_file = paths["manual_qa_file"]
    data = _load_json(manual_file) or {"project": project, "events": {}}
    if event_id not in data["events"]:
        data["events"][event_id] = {"tested_at": datetime.now().isoformat(), "scenarios": {}}
    event_data = data["events"][event_id]
    if scenario_id not in event_data["scenarios"]:
        event_data["scenarios"][scenario_id] = {"manual_status": "untested"}
    sc = event_data["scenarios"][scenario_id]
    if "screenshots" not in sc:
        sc["screenshots"] = []
    sc["screenshots"].append(filename)
    data["updated_at"] = datetime.now().isoformat()
    with open(manual_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    serve_url = f"/api/qa/scenario/screenshot/{project}/{event_id}/{filename}"
    return {"filename": filename, "url": serve_url, "event_id": event_id, "scenario_id": scenario_id}


@app.get("/api/qa/scenario/screenshot/{project}/{event_id}/{filename}")
async def qa_serve_scenario_screenshot(project: str, event_id: str, filename: str):
    """Serve a scenario context screenshot."""
    paths = _resolve_project(project)
    file_path = paths["scenario_images_dir"] / event_id / filename

    try:
        file_path.resolve().relative_to(paths["dir"].resolve())
    except ValueError:
        raise HTTPException(status_code=403, detail="Path traversal detected")

    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Screenshot not found: {filename}")

    media_type = "image/png"
    if filename.endswith(".jpg") or filename.endswith(".jpeg"):
        media_type = "image/jpeg"
    elif filename.endswith(".webp"):
        media_type = "image/webp"

    return FileResponse(str(file_path), media_type=media_type, headers={"Cache-Control": "no-store"})


@app.delete("/api/qa/scenario/screenshot")
async def qa_delete_scenario_screenshot(
    project: str = Query(...),
    event_id: str = Query(...),
    scenario_id: str = Query(...),
    filename: str = Query(...),
):
    """Delete a scenario context screenshot."""
    paths = _resolve_project(project)
    file_path = paths["scenario_images_dir"] / event_id / filename

    try:
        file_path.resolve().relative_to(paths["dir"].resolve())
    except ValueError:
        raise HTTPException(status_code=403, detail="Path traversal detected")

    if file_path.exists():
        file_path.unlink()

    # Remove from manual-qa.json
    manual_file = paths["manual_qa_file"]
    data = _load_json(manual_file)
    if data:
        sc = data.get("events", {}).get(event_id, {}).get("scenarios", {}).get(scenario_id, {})
        if "screenshots" in sc and filename in sc["screenshots"]:
            sc["screenshots"].remove(filename)
            if not sc["screenshots"]:
                del sc["screenshots"]
            data["updated_at"] = datetime.now().isoformat()
            with open(manual_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

    return {"deleted": True, "filename": filename}


@app.on_event("startup")
async def startup_index():
    """Index documents on server startup."""
    index_documents()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8400)
