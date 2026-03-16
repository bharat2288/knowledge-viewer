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
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
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

_debug = os.getenv("DEBUG", "").lower() in ("1", "true", "yes")
app = FastAPI(
    title="Knowledge Viewer", version="1.0.0",
    docs_url="/docs" if _debug else None,
    redoc_url="/redoc" if _debug else None,
    openapi_url="/openapi.json" if _debug else None,
)


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
    # Find qmd binary — check PATH first, then known fnm location
    import shutil
    qmd_bin = shutil.which("qmd")
    if not qmd_bin:
        # Windows fnm installs global packages here
        candidate = Path(os.getenv("APPDATA", "")) / "fnm" / "node-versions" / "v22.15.1" / "installation" / "qmd.cmd"
        if candidate.exists():
            qmd_bin = str(candidate)

    if not qmd_bin:
        raise HTTPException(status_code=503, detail="QMD not installed")

    cmd = [qmd_bin, "search", q, "-n", str(limit), "--json"]
    if collection:
        cmd.extend(["--collection", collection])

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
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
        cursor.execute("""
            UPDATE prompts SET importance_score = ? WHERE id = ?
        """, (item["score"], item["id"]))
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
    marker = r"C:\Users\bhara\dev\\"
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
                            "UPDATE prompts SET importance_score = ? WHERE id = ?",
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


# Run initial indexing on startup (async)
@app.on_event("startup")
async def startup_index():
    """Index documents on server startup."""
    index_documents()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8400)
