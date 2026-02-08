# Knowledge Viewer

A local web UI for browsing and managing a personal knowledge database. Provides a unified interface for sessions, errors, decisions, learnings, prompts, tags, and workflow documentation — no folder-hopping required.

## Features

- **Dashboard** with summary stats and clickable recent activity feed
- **Sessions** browser with context display and project filtering
- **Errors & Resolutions** with full-text search
- **Decisions** log with session linkage
- **Learnings** with tags, examples, and search
- **Prompts** with inline scoring, AI-powered score suggestions, batch review workflow, and tag management
- **Tags** vocabulary management across categories
- **Document Browser** (Obsidian-like) with tree navigation, reader/source toggle, full-text search, backlinks, and "Open in VS Code"

## Architecture

- **Backend:** FastAPI (Python) — single `main.py`
- **Frontend:** Single-file HTML/CSS/JS — no build step, no framework
- **Database:** SQLite (reads from existing `knowledge.db` and `prompts.db`)
- **Design:** Dark theme following a custom design system

## Prerequisites

- Python 3.11+
- SQLite knowledge database (the app reads from an existing `knowledge.db`)

## Installation

```bash
git clone https://github.com/bharat2288/knowledge-viewer.git
cd knowledge-viewer
python -m venv venv
venv\Scripts\activate       # Windows
pip install -r requirements.txt
```

Optional: install `openai` for AI-powered prompt score suggestions:

```bash
pip install openai
```

Set `OPENAI_API_KEY` in a `.env` file if using score suggestions.

## Running

```bash
python -m uvicorn main:app --reload --port 8402
```

Then open [http://localhost:8402](http://localhost:8402).

## Limitations

- **Read-only** document viewer — use "Open in VS Code" for edits
- Designed for personal/local use, not multi-user deployment
- Expects specific database schemas from the [claude-workflow-system](https://github.com/bharat2288/claude-workflow-system) MCP server

## License

MIT
