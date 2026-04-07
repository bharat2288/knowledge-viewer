---
type: project-home
project: knowledge-viewer
date: 2026-03-07
cssclasses:
  - project-home
---
# Knowledge Viewer
*[[dev-hub|Hub]] · [[README|GitHub]]*
<span class="hub-status">QMD ops console is live — health, runs, manual update/embed, and per-file embed visibility now work from the viewer. Next: active-job heartbeat/timeline.</span>

Local web UI for browsing knowledge DB, sessions, prompts, and workflow docs. Transitioning to Obsidian-native views.

## Specs

```base
filters:
  and:
    - file.folder.contains("specs/knowledge-viewer")
    - type != "spec-prompts"
properties:
  "0":
    name: file.link
    label: Spec
  "1":
    name: type
    label: Type
  "2":
    name: date
    label: Date
  "3":
    name: created_by
    label: Created By
  "4":
    name: file.mtime
    label: Modified
views:
  - type: table
    name: All Specs
    order:
      - type
      - file.name
      - file.mtime
      - file.backlinks
    sort:
      - property: file.mtime
        direction: DESC
      - property: type
        direction: ASC
```

> [!abstract]- Project Plans (`$= dv.pages('"knowledge/plans"').where(p => p.project == "knowledge-viewer").length`)
> ```dataview
> TABLE title, default(date, file.ctime) as Date
> FROM "knowledge/plans"
> WHERE project = "knowledge-viewer"
> SORT default(date, file.ctime) DESC
> ```

> [!note]- Sessions (`$= dv.pages('"knowledge/sessions/knowledge-viewer"').length`)
> ```dataview
> TABLE topic
> FROM "knowledge/sessions/knowledge-viewer"
> SORT file.mtime DESC
> LIMIT 5
> ```
>
> > [!note]- All Sessions
> > ```dataview
> > TABLE topic
> > FROM "knowledge/sessions/knowledge-viewer"
> > SORT file.mtime DESC
> > ```
