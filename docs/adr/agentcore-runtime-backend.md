# ADR: AgentCore Runtime Backend

- **Status:** Proposed
- **Date:** 2026-06-10
- **Author:** @chaodu-agent

---

## 1. Context & Motivation

Today, OpenAB dispatches messages exclusively via ACP (Agent Client Protocol) — JSON-RPC over stdio to a co-located subprocess:

```
Discord/Slack msg ──► OpenAB ──stdio──► coding CLI (kiro, claude, codex…)
```

This means:
- **One agent per container.** The coding CLI binary must be bundled inside the same pod as OpenAB.
- **No parallelism across agents.** Running Claude Code *and* Kiro simultaneously requires deploying two full OpenAB stacks.
- **Pod-bound lifecycle.** If the pod restarts, the agent process (and any in-flight work) dies with it.
- **Resource coupling.** The agent shares CPU/memory/disk with OpenAB — a 90-minute refactor starves the broker.

AWS recently launched **Amazon Bedrock AgentCore Runtime**, which hosts coding agents (Kiro, Claude Code, Codex, etc.) in isolated Firecracker microVMs with persistent filesystems, session management, and streaming invoke APIs. This creates an opportunity: OpenAB can route messages to remote AgentCore sessions via SDK instead of local subprocesses, decoupling the agent lifecycle from the broker.

### What this unlocks

1. **Dynamic multi-agent routing** — one OpenAB instance routes to N different AgentCore runtimes based on @mention or config.
2. **True isolation** — each agent runs in its own microVM; no shared localhost, no credential leakage.
3. **Background execution** — agents survive pod restarts, laptop lid closures, and network drops.
4. **Cost efficiency** — microVMs spin down when idle (pay per use), no always-on pod per agent.

---

## 2. Design

### Architecture Overview

```
┌────────────────────────────────────────────────────────────────────┐
│  OpenAB (Rust, always-on pod)                                      │
│                                                                    │
│  Discord/Slack ──► Dispatch ──┬── backend=acp ──► local subprocess │
│                               │                                    │
│                               └── backend=agentcore ──► AWS SDK    │
│                                         │                          │
└─────────────────────────────────────────┼──────────────────────────┘
                                          │
                              ┌────────────▼────────────────┐
                              │  AgentCore Runtime (AWS)     │
                              │                             │
                              │  ┌─────────────────────┐    │
                              │  │ microVM (Kiro)      │    │
                              │  │ /mnt/workspace      │    │
                              │  │ session: oab-thread1│    │
                              │  └─────────────────────┘    │
                              │                             │
                              │  ┌─────────────────────┐    │
                              │  │ microVM (Claude)    │    │
                              │  │ /mnt/workspace      │    │
                              │  │ session: oab-thread2│    │
                              │  └─────────────────────┘    │
                              └─────────────────────────────┘
```

### Message Flow

```
Inbound:
  1. User sends message in Discord thread
  2. OpenAB dispatch selects backend based on config (or @mention routing)
  3. If backend=agentcore:
     a. Build runtimeSessionId from thread key (≥33 chars)
     b. Serialize prompt + sender context into JSON payload
     c. Call InvokeAgentRuntime (streaming)
  4. Stream response chunks back to Discord (same edit loop as ACP)

Session Lifecycle:
  - Discord thread ID → runtimeSessionId (deterministic mapping)
  - First invoke on new session → cold start (microVM boot, ~5-15s)
  - Subsequent invokes → reuse existing microVM (idle timer reset)
  - Idle 15min (configurable) → microVM terminates, filesystem persists
  - Next invoke → new microVM mounts same filesystem, agent resumes
```

### Two Invoke APIs

AgentCore provides two complementary APIs. OpenAB uses primarily `InvokeAgentRuntime`:

| API | Purpose | Response | OpenAB Use |
|-----|---------|----------|------------|
| `InvokeAgentRuntime` | Send prompt, get agent reasoning output | Streaming `text/event-stream` | Primary: Discord msg → agent → streaming reply |
| `InvokeAgentRuntimeCommand` | Run shell command in same microVM | Streaming stdout/stderr events | Optional: deterministic ops (git push, npm test) |

### Streaming Response Handling

`InvokeAgentRuntime` returns `text/event-stream` with `data:` lines:

```
data: Starting analysis of the code...
data: I can see the issue is in line 42...
data: Here's my proposed fix:
data: ```rust
data: fn main() { ... }
data: ```
```

OpenAB consumes this identically to ACP streaming — progressive Discord message edits via the existing `AdapterRouter::edit_message` path.

---

## 3. Configuration

### Single agent (simple)

```toml
[agent]
backend = "agentcore"
runtime_arn = "arn:aws:bedrock-agentcore:us-west-2:123456789012:runtime/kiro-agent"
region = "us-west-2"
```

### Multi-agent routing

```toml
[agent]
backend = "agentcore"
runtime_arn = "arn:aws:bedrock-agentcore:us-west-2:123456789012:runtime/kiro-agent"
region = "us-west-2"

# Future: route by @mention to different runtimes
# [agent.routes]
# kiro = "arn:aws:...:runtime/kiro-agent"
# claude = "arn:aws:...:runtime/claude-agent"
# codex = "arn:aws:...:runtime/codex-agent"
```

### Hybrid (ACP + AgentCore coexist)

In a future multi-agent config, some agents could be local ACP while others are remote AgentCore. This ADR focuses on the single-agent case first.

---

## 4. Session ID Mapping

AgentCore requires `runtimeSessionId` ≥ 33 characters. OpenAB thread keys are platform-specific (Discord: 18-20 digit snowflake, Slack: channel.thread_ts).

**Strategy:** Prefix with `oab-` and pad/hash to guarantee length:

```
Discord thread 1514294613853208667
  → runtimeSessionId = "oab-discord-1514294613853208667"  (34 chars ✓)

Slack thread C0123456789.1234567890.123456
  → runtimeSessionId = "oab-slack-C0123456789-1234567890-123456"  (43 chars ✓)
```

The mapping is deterministic (no persistent state needed). Same thread always maps to same session. This enables:
- Resume after OpenAB restart (no `thread_map.json` needed for AgentCore backend)
- Multiple OpenAB replicas sharing the same AgentCore sessions

---

## 5. Implementation Plan

### Phase 1: Minimal viable backend

1. Add `backend` field to `AgentConfig` (`"acp"` default, `"agentcore"` new)
2. Define `AgentBackend` trait:
   ```rust
   #[async_trait]
   trait AgentBackend: Send + Sync {
       /// Send a prompt and stream response blocks back.
       async fn stream_prompt(
           &self,
           session_key: &str,
           prompt: &str,
           extra_blocks: &[ContentBlock],
           tx: mpsc::Sender<ContentBlock>,
       ) -> Result<()>;

       /// Cancel an in-flight turn (best-effort).
       async fn cancel(&self, session_key: &str) -> Result<()>;
   }
   ```
3. Implement `AcpBackend` (wraps existing `SessionPool` logic)
4. Implement `AgentCoreBackend`:
   - Uses `aws-sdk-bedrockagentcore` crate
   - `stream_prompt` → `invoke_agent_runtime` with streaming response
   - Parses `text/event-stream` lines into `ContentBlock::Text`
   - Maps session_key → runtimeSessionId with prefix scheme
5. Wire into dispatcher — replace direct `SessionPool` usage with `dyn AgentBackend`

### Phase 2: Cold start UX

- Detect first-invoke latency (no streaming data for >3s)
- Show ⏳ or "Starting agent environment..." reaction
- Once streaming begins, switch to normal 🤔 thinking reaction

### Phase 3: Multi-runtime routing (future)

- Config-driven routing table (per @mention, per channel, per pattern)
- Parallel invoke to multiple runtimes (race mode)

---

## 6. Differences from ACP Backend

| Concern | ACP (current) | AgentCore (proposed) |
|---------|--------------|---------------------|
| Agent location | Same container | Remote microVM |
| Startup | Already running (subprocess) | Cold start on first invoke (~5-15s) |
| Session state | In-memory (process) | Persistent filesystem (/mnt/workspace) |
| Credential isolation | Shared pod env | Fully isolated (IAM + Gateway) |
| Tool permission prompt | Supported (mid-turn stdin) | Not supported — must trust all tools |
| Streaming format | ACP JSON-RPC notifications | HTTP/2 event-stream (`data:` lines) |
| Max session duration | Unlimited (until pod dies) | 8 hours (configurable) |
| Idle behavior | Stays alive (in pool) | Auto-terminates after idle timeout |
| Resume after kill | Lost (unless session/save) | Automatic (filesystem persists 14 days) |
| Parallelism | One process per thread, shared CPU | One microVM per session, independent |
| Cost model | Always-on pod cost | Pay per CPU-second + peak memory |

---

## 7. Open Questions

1. **Payload format standardization** — Different AgentCore runtimes may expect different payload schemas (`{"prompt": "..."}` vs raw text vs MCP). Do we need a `payload_template` config field?

2. **Response format parsing** — If the agent runtime returns structured JSON (not plain text streaming), how do we extract the "message" portion? May need a configurable response extractor.

3. **OAuth-based invocation** — AgentCore docs state that OAuth-integrated runtimes cannot use the SDK; they require raw HTTPS. If a runtime uses AgentCore Identity/Gateway with OAuth, OpenAB would need to use `reqwest` directly instead of the AWS SDK. How common is this pattern?

4. **Human-in-the-loop** — ACP supports mid-turn tool permission prompts (agent asks user "can I run this command?"). AgentCore agents run autonomously. Is this acceptable, or do we need a callback mechanism?

5. **Multi-agent routing UX** — How should users specify which agent handles which message? Options: @mention, channel binding, slash command, or auto-detect.

6. **Error recovery** — If `InvokeAgentRuntime` fails (throttling, session terminated), should OpenAB auto-retry with a new session, or surface the error to the user?

---

## 8. Alternatives Considered

### A. Keep ACP-only, run OpenAB on AgentCore

Deploy the entire OpenAB + agent container on AgentCore Runtime instead. This works but:
- Still couples agent to container
- Doesn't leverage AgentCore's multi-session isolation
- Doesn't enable multi-agent routing from one OpenAB instance
- Loses the "thin bridge" philosophy — OpenAB becomes the thing being hosted, not the router

### B. WebSocket relay to AgentCore

Instead of SDK invoke, have a persistent WebSocket between OpenAB and a custom proxy that talks to AgentCore. Rejected because:
- Adds another service to deploy and maintain
- `InvokeAgentRuntime` already supports streaming; no need for an intermediary
- Increases complexity without clear benefit

### C. MCP-based integration via AgentCore Gateway

Use AgentCore Gateway's MCP endpoint as the tool layer, keeping agents local. This is complementary (we could support it for tools) but doesn't solve the core problem of agent lifecycle coupling.

---

## 9. References

- [AWS Blog: Hosting Coding Agents on AgentCore](https://aws.amazon.com/blogs/machine-learning/its-safe-to-close-your-laptop-now-hosting-coding-agents-on-amazon-bedrock-agentcore/)
- [InvokeAgentRuntime API Reference](https://docs.aws.amazon.com/bedrock-agentcore/latest/APIReference/API_InvokeAgentRuntime.html)
- [InvokeAgentRuntimeCommand API Reference](https://docs.aws.amazon.com/bedrock-agentcore/latest/APIReference/API_InvokeAgentRuntimeCommand.html)
- [AgentCore Runtime Lifecycle Settings](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-lifecycle-settings.html)
- [AgentCore Session Storage (Preview)](https://aws.amazon.com/about-aws/whats-new/2026/03/bedrock-agentcore-runtime-session-storage/)
- [Handle Long-Running Agents](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-long-run.html)
- [OpenAB DESIGN.md](../../DESIGN.md) — "Thin Bridge" philosophy
