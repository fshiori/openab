# ADR: Control Directives

- **Status:** Proposed
- **Date:** 2026-06-02
- **Author:** chaodu-agent
- **Related:** [Output Directives](../output-directives.md)

---

## 1. Context

### 1.1 Problem

A single OAB bot instance may serve multiple projects, each with its own steering files, skills, and workspace context. Today, there is no mechanism for a user to specify session-level parameters (working directory, model) when initiating a conversation. The bot always starts with its default configuration, requiring manual reconfiguration or separate bot instances per project.

### 1.2 Existing Pattern

OAB already has **output directives** — `[[key:value]]` syntax that agents prepend to their responses to control delivery behavior (e.g., `[[reply_to:...]]`). This pattern is well-understood, parsed reliably, and invisible to end users after processing.

### 1.3 Opportunity

Extend the `[[key:value]]` convention to **input** (user → bot) messages, creating **control directives** that configure the session at creation time. This unifies the directive syntax across both directions and gives users declarative control over session initialization without requiring new slash commands or config files.

---

## 2. Decision

Introduce **Control Directives** — `[[key:value]]` patterns in user messages that configure session parameters. They share the double-bracket syntax with output directives but flow in the opposite direction (user → broker → agent runtime).

### 2.1 Syntax

```
@Bot [[ws:~/workdir/foo]] [[title:PR review]] investigate this build failure
```

### 2.2 Core Rules

| Rule | Behavior |
|------|----------|
| **Scope** | Processed only on the session's first message (the one that mentions/triggers the bot) |
| **Parsing** | Extract all `[[key:value]]` matches from the message body |
| **Stripping** | Directives are removed from the message; remaining text becomes the prompt |
| **Duplicate keys** | Last value wins |
| **Unknown keys** | Silently ignored (forward compatible) |
| **Placement** | Inline or on separate lines — parser handles both |
| **Empty value** | `[[key:]]` is valid; treated as explicit empty/reset |

### 2.3 Architecture Position

```
User message
     │
     ▼
┌─────────────────────┐
│  Directive Parser    │  ← extracts [[key:value]], strips from message
│  (middleware)        │
└─────────────────────┘
     │
     ├── structured SessionMetadata
     │
     ▼
┌─────────────────────┐
│  Agent Runtime       │  ← receives clean prompt + metadata
└─────────────────────┘
```

The directive parser runs **before** the message enters the agent pipeline. It outputs:
- `prompt: String` — the message with directives stripped
- `metadata: SessionMetadata` — parsed key-value pairs for runtime configuration

---

## 3. Supported Directives (Phase 1)

| Directive | Purpose | Example |
|-----------|---------|---------|
| `[[ws:/path]]` | Set session working directory; loads steering/skills from that path | `[[ws:~/projects/myapp]]` |
| `[[title:...]]` | Set initial thread title | `[[title:Bug triage #42]]` |
| `[[model:...]]` | Specify model for this session | `[[model:claude-sonnet-4-20250514]]` |

### 3.1 `[[ws:/path]]` — Workspace

- Resolves `~` to the bot's home directory
- Loads `AGENTS.md`, `.kiro/steering/`, and skill definitions from the target path
- If the path does not exist, session starts with default context (no error surfaced to user; logged at warn level)
- Path must be absolute or start with `~`; relative paths are rejected

### 3.2 `[[title:...]]` — Thread Title

- Sets the initial thread/channel title
- Agent may override this later per its own SOP (e.g., status-based title updates)
- Max length: 100 characters (truncated silently)

### 3.3 `[[model:...]]` — Model Selection

- Value must match a configured model identifier
- If the model is unavailable or unknown, the session falls back to the default model and logs a warning
- Does not persist beyond the session

---

## 4. Design Decisions

### 4.1 Why Session-First Only

Processing directives only on the first message keeps the mental model simple:
- No mid-conversation state mutations
- No need for "directive acknowledged" confirmation messages
- Session parameters are immutable once established
- Easier to reason about for both users and agents

### 4.2 Why Not Slash Commands

| Aspect | Slash Commands | Control Directives |
|--------|---------------|-------------------|
| Discovery | Platform UI autocomplete | Docs / muscle memory |
| Composability | One command at a time | Multiple directives in one message |
| Platform dependency | Requires registration per platform | Platform-agnostic (just text) |
| Works with mention | Awkward (`/command @bot`) | Natural (`@bot [[...]] prompt`) |

Control directives are platform-agnostic text — they work on Discord, Slack, Telegram, or any adapter without platform-specific command registration.

### 4.3 Relationship to Output Directives

| Aspect | Output Directives | Control Directives |
|--------|-------------------|-------------------|
| Direction | Agent → Platform | User → Broker |
| Processing layer | Response post-processor | Message pre-processor |
| Timing | Every response | Session first message only |
| Syntax | `[[key:value]]` | `[[key:value]]` |
| Unknown keys | Ignored | Ignored |
| Duplicate keys | Last wins | Last wins |

Shared syntax reduces cognitive load. The direction is unambiguous from context (who authored the message).

### 4.4 Security Considerations

- `[[ws:...]]` paths should be validated against an allowlist or restricted to the bot's home subtree to prevent directory traversal
- `[[model:...]]` only selects from pre-configured models; cannot inject arbitrary API endpoints
- Directive values are sanitized (no newlines, no control characters beyond the value delimiter)

---

## 5. Future Extensions

These are **not** in scope for Phase 1 but the design accommodates them:

| Directive | Purpose |
|-----------|---------|
| `[[repo:owner/repo]]` | Bind GitHub repository context |
| `[[timeout:300]]` | Session timeout in seconds |
| `[[skill:review]]` | Activate a specific skill set |
| `[[label:bug]]` | Tag the session/thread with labels (multi-value: array semantics) |

For multi-value keys (e.g., `[[label:a]] [[label:b]]`), a future revision may introduce array semantics where repeated keys accumulate rather than overwrite. Phase 1 uses last-wins for all keys.

---

## 6. Implementation Plan

### Phase 1: Parser + `ws` + `title`

1. Implement directive parser as a middleware in the message ingestion pipeline
2. Define `SessionMetadata` struct
3. Wire `[[ws:...]]` to workspace/context loading
4. Wire `[[title:...]]` to thread title initialization
5. Unit tests for parser edge cases (nested brackets, escaped content, empty values)

### Phase 2: `model`

1. Wire `[[model:...]]` to model selection in agent runtime
2. Validation and fallback logic

### Phase 3: `/new` Slash Command

Platform-specific UX sugar that translates to control directives internally.

```
/new ws:~/projects/myapp model:claude-sonnet-4-20250514
investigate the build failure
```

1. Register `/new` slash command on supported platforms (Discord, Slack)
2. Command handler parses arguments into `[[key:value]]` directives
3. Feeds through the same directive parser pipeline as inline directives
4. Creates a new thread with the parsed session metadata

**Why `/new`:**
- Short, intuitive — matches "new session/thread" mental model
- **Typed arguments with platform UI** — autocomplete for workspaces, dropdown for models, validation before submit. Users don't need to memorize exact model names or path syntax
- Does not conflict with other bots' commands
- Naturally implies "session start" — aligns with first-message-only rule

**Relationship to inline directives:**
- `/new` is a convenience layer; control directives remain the canonical spec
- Users who prefer text-only (or are on platforms without slash commands) use `@Bot [[...]]` directly
- Both paths produce identical `SessionMetadata`

---

## 7. Alternatives Considered

| Alternative | Rejected Because |
|-------------|-----------------|
| YAML front-matter in messages | Visually heavy; unfamiliar to chat users |
| Separate `/config` command before conversation | Extra round-trip; breaks single-message session start |
| Per-channel bot configuration | Doesn't scale to ad-hoc project switching |
| Environment variables per bot instance | Requires multiple bot deployments |

---

## 8. References

- [Output Directives](../output-directives.md) — existing `[[key:value]]` pattern for agent → platform
- [Steering Design Guide](../steering-design-guide.md) — how workspace steering files are structured
