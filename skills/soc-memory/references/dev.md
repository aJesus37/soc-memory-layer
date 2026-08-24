# Dev Setup for the SOC Memory Skill

This skill is developed in-repo at `skills/soc-memory/` so changes are versioned. Install it for live development via symlink.

## Install for dev (symlink — live editing)

```bash
task install-skills   # symlinks repo skills → ~/.agents/skills/ (or ~/.config/opencode/skills/ fallback)
task install-skills -- --copy   # or copy instead of symlink
```

Edits in `skills/soc-memory/` are then immediately visible to opencode — no reinstall.

## Uninstall

```bash
task uninstall-skills
```

## Package for distribution

```bash
task package-skills              # → dist/*.skill
task package-skills -- ./my-dist # custom output dir
```

## Env for the MCP server

The skill's tools talk to the memory MCP server. In dev it points at the local stack:

```bash
# .env (gitignored, Taskfile loads it)
MEM_MCP_SCOPE=team-a
MEM_EMBED_URL=http://localhost:3000
MEM_DGRAPH_ADDR=localhost:9080
# for remote MCP:
# MEM_MCP_HTTP_ADDR=127.0.0.1:18443
# MEM_MCP_TOKENS_FILE=tokens.json
```

See `opencode.json` at the repo root for the dev MCP wiring (`go run ./cmd/memmcp`).
