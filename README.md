# paseo-lite

A Rust rewrite of the [Paseo](https://github.com/getpaseo/paseo) daemon for low-spec machines
(small VMs, Raspberry Pi, free-tier cloud instances). It runs **Claude Code** and **Codex** agents
across all your projects from a single process, and reuses Paseo's **end-to-end encrypted relay**,
so you can pair it with the Paseo apps exactly like the Node daemon.

## What's different from upstream

| | Paseo (Node) | paseo-lite |
|---|---|---|
| Daemon runtime | Node.js, multi-package | one static Rust binary, single-threaded tokio runtime |
| Codex | one `codex app-server` **per agent**, plus throwaway ones for model listing | **one shared** `codex app-server` for every agent in every project; threads are routed by `threadId`, each with its own `cwd`; the model list is cached on disk |
| Claude Code | one long-lived `claude` child per agent | **one shared** `claude` process (more when the machine allows, see below); a process is bound to one session and `cwd`, so switching agents replaces it (`--resume`) |
| One CLI at a time | both run freely | only one CLI has a process at any time. All turns from every project and both providers share one FIFO queue. The active CLI stays warm (pre-started at launch for the last used provider). When the next queued turn needs the other CLI, it waits for running turns to finish, the active CLI is shut down, and the other one cold-starts and stays warm until the next switch. Codex model listing never starts Codex while Claude Code is active |
| Queued messages | each waits for its own turn | messages sent to a chat while it is busy are combined into **one** turn when the current one finishes (same conversation, one round trip, the context is sent once). Each still shows as its own bubble; slash commands always run on their own |
| Agents at once | each agent runs freely | `auto` by default: one turn at a time to start, and the shared queue adds another worker once turns are waiting and the machine has stayed calm for `PASEO_AUTO_STABLE_SECS` (low memory and CPU pressure, little CPU throttling, room for one more agent's memory and processes). It drops one again when pressure builds up; running turns are never stopped, and spare warm processes are released. In Docker every figure is the container's: the cgroup memory limit (`--memory`, `memory.high`), CPU quota (`--cpus`, `--cpuset-cpus`), pids limit (`--pids-limit`) and the container's own pressure, re-read every minute so `docker update` applies live. The ceiling is what the memory limit fits at `PASEO_AGENT_MB` per agent, twice the CPU count, 64 pids per agent, and `PASEO_AUTO_MAX_AGENTS`; on a 1 GB VM that stays at one. A number in `PASEO_CLAUDE_MAX_LIVE` / `PASEO_CODEX_MAX_ACTIVE` fixes the cap instead |
| Staying in control | – | the daemon keeps the room to step in: agents get an OOM score above the daemon's own (Docker's `--oom-score-adj` is shared by the whole container), so the kernel kills an agent first; when the container is nearly out of memory or pids, the limit drops at once and every idle `claude` process is stopped (fixed caps included); warm-ups wait while the machine is strained |
| Images and files | inline images and uploaded files | same protocol: images sent with a message and files uploaded through `file.upload` (binary frames, up to 100 MB) are saved under `~/.paseo-lite/uploads` (kept 14 days). The agent is told each file's path; images are also given to the model as images (Claude image blocks, Codex local images) |
| Usage | plan limit windows from provider plugins | tokens and cost **per provider and model**, booked from the CLIs' own totals (Claude Code `modelUsage` incl. its USD cost; Codex `tokenUsage`, priced from LiteLLM's public price list, refreshed daily). Shown in the app's Usage screen: cost today / 7 / 30 days / all time, tokens, the in/cached/out split, the top models, the 30-day daily average, a 30-day projection, the prompt cache hit rate and each of the last 30 days, plus one card per model with its cost and tokens over 7 / 30 days and all time, the token split and the effective cost per 1M tokens; an agent's view adds its own spend. Costs are API list prices (what the tokens would cost through the API), not subscription billing. Days are UTC; kept in `~/.paseo-lite/usage.json` |
| Usage limits | the turn fails; you send it again after the reset | the turn **waits** for the Claude / Codex usage limit to reset (time read from Claude's rate-limit event or Codex's rate-limit windows), then continues on its own and runs anything queued meanwhile; the chat says when. Sending a message tries again at once; stop cancels (`PASEO_RESUME_AFTER_LIMIT`) |
| Timelines | in memory | append-only JSONL on disk; streamed chunks merged into one row; unloaded from memory after `PASEO_TIMELINE_UNLOAD_SECS` idle; tool output clipped to 16 KiB |
| Data use | — | streamed text is sent to the app in ~150 ms batches instead of one message per delta; Claude Code runs with `CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1` (no telemetry or update checks); contexts are compacted at `PASEO_AUTO_COMPACT_TOKENS`. `diagnostics` lists the bytes sent to apps per message type |
| Projects | — | a project is just a directory; adding one costs no process and no memory until an agent runs a turn |

Idle cost is the daemon plus one warm process of the active CLI (`claude` or `codex app-server`). Set `PASEO_CODEX_KEEP_WARM=0` / `PASEO_CLAUDE_KEEP_WARM=0` to start them on demand and release them after `PASEO_CODEX_IDLE_SECS` / `PASEO_CLAUDE_IDLE_SECS` instead.

## Relay and pairing

The relay code is a byte-compatible port of `@getpaseo/relay`:

- relay protocol v2: one control socket plus one data socket per connected client,
  `wss://<relay>/ws?serverId=…&role=server&v=2[&connectionId=…]`;
- `e2ee_hello` / `e2ee_ready` handshake, Curve25519 + XSalsa20-Poly1305 (NaCl `box`),
  bundle `nonce(24) || ciphertext`, base64 text frames;
- the same `server-id` and `daemon-keypair.json` files and the same pairing link format
  (`https://app.paseo.sh/#offer=…`).

The default relay is `relay.paseo.sh:443`. You can point it at a self-hosted
[paseo-relay](https://github.com/getpaseo/paseo-relay) with `PASEO_RELAY_ENDPOINT`.

### Web app on GitHub Pages

`.github/workflows/pages.yml` builds the official Paseo web app (upstream `packages/app`, pinned to a
release, `v0.12.0` by default) and publishes it at `https://sudtanj.github.io/paseo-lite/`, so you don't
depend on app.paseo.sh and the app version stays one that works with paseo-lite. Run it from the
Actions tab, optionally with another upstream tag. To make the pairing QR/link open this copy:

```sh
PASEO_APP_BASE_URL=https://sudtanj.github.io/paseo-lite paseo-lite
```

The page is HTTPS, so it reaches the daemon through the relay (end-to-end encrypted, as with
app.paseo.sh). Browsers block plain `ws://` from an HTTPS page, so a direct connection only works to
`localhost` or to a daemon behind HTTPS whose name is in `PASEO_HOSTNAMES`.

The build is upstream's own `expo export` with source maps removed; the job summary shows the
JavaScript size a browser downloads. WebAssembly would not make it lighter: the app is React Native
Web, its cost is the JS UI and the DOM, which WebAssembly cannot reach without a full rewrite plus JS
glue, and the only compute-heavy part (the NaCl box encryption) is already small.

## Protocol compatibility

paseo-lite speaks Paseo's core WebSocket protocol subset with the upstream message names and shapes:

- `hello` → `status/server_info` handshake, plus `ping`/`pong`;
- agents: `fetch_agents`, `fetch_agent`, `fetch_agent_history`, `create_agent` (with `initialPrompt`),
  `send_agent_message`, `cancel_agent`, `archive_agent`, `delete_agent`, `clear_agent_attention`;
- timeline and permissions: `fetch_agent_timeline` (projected rows with seq ranges),
  `agent_permission_request` / `_response` / `_resolved`;
- live events: `agent_update` and `agent_stream`;
- providers, workspaces and directories: `set_agent_mode`, `set_agent_model`, provider models/modes/snapshot,
  `fetch_workspaces` and `directory_suggestions`;
- agent controls: `update_agent` (rename, labels), `set_agent_thinking` (`claude --effort` / Codex `effort`),
  `refresh_agent`, `resume_agent`, `list_commands` (built-ins plus `.claude/commands` and `.claude/skills`),
  `wait_for_finish`, `agent.timeline.list_prompts`, `list_provider_features`, `set_agent_feature` (declined);
- daemon and projects: `daemon.get_status`, `get_daemon_config` / `set_daemon_config` (custom models via `providers.<id>.additionalModels`, `appendSystemPrompt`; saved in `daemon-config.json`), `diagnostics`,
  `project.rename`, `project.remove` / `archive_workspace` (archive the agents and clean up the folder, see below), `workspace.title.set`,
  `workspace.clear_attention`.
- git actions for the Changes panel (no agent turn needed): commit (with an auto-written message when empty), push, pull, switch/rename/validate branch, branch suggestions, discard changes, refresh (fetch), commit history with per-file stats and diffs, stash save/pop/list, merge into base / merge from base, and pull request status/create/merge through the `gh` CLI.

Anything else (file editing, terminals, plugins, voice, schedules, Hub, worktrees) answers with
`rpc_error` / `unsupported`. The official app is expected to handle the core agent flow. Screens built
on unsupported features will show errors.

**Removing a project** (or archiving its last workspace) archives its agents and deletes the local folder
when that can't lose work: it must be a git repository with no uncommitted changes, no stashes, no
other worktrees, every commit on a remote, and no ignored files beyond regenerable ones
(`node_modules`, `target`, `dist`, `.venv`, …), so a `.env` keeps it. A folder paseo-lite created
that is still empty is deleted without checks. Otherwise the folder is kept and the app is told why.
Whatever the policy, only folders strictly inside your home directory are deleted, never one that
holds paseo-lite's data or another project. See `PASEO_DELETE_PROJECT_FILES`.

**Notifications** work like upstream: the app registers its Expo push token, and when an agent
finishes or needs permission, a client that's looking at that agent gets nothing, the most recently
active client gets an in-app notice, and if nobody is active (phone locked, app closed) the phone
gets a push notification via Expo.

paseo-lite also serves a small built-in pairing page at `http://<listen>/` (one HTML file, no build step).
It shows only a password form; after the daemon accepts the password over `/ws` it shows the relay
pairing QR code and link, and locks again after two minutes or when the tab is hidden. It never lists
projects or agents (use the Paseo app for that), and it stays disabled when `PASEO_PASSWORD` is unset.

## Build

```sh
cargo build --release        # binary: target/release/paseo-lite
```

The release profile optimises for size (`opt-level="z"`, LTO, stripped). You can cross-compile
on a bigger machine or in CI (see `.github/workflows/build.yml`) and copy the binary over. Building
on a 1 GB VM is slow.

## Run

```sh
paseo-lite                    # 127.0.0.1:6768 + relay; prints a pairing QR/link
paseo-lite --no-relay         # direct connections only (LAN/Tailscale/SSH tunnel)
paseo-lite pair               # print the pairing link again
PASEO_LISTEN=0.0.0.0:6768 PASEO_PASSWORD=secret paseo-lite
```

`claude` and/or `codex` must be on `PATH` and already logged in. To use other binaries, set
`PASEO_CLAUDE_BIN` / `PASEO_CODEX_BIN`.

| Variable | Default | Meaning |
|---|---|---|
| `PASEO_LITE_HOME` | `~/.paseo-lite` | state: agents, timelines, server id, keypair |
| `PASEO_LISTEN` | `127.0.0.1:6768` | HTTP/WS listen address |
| `PASEO_PASSWORD` | – | required for direct (non-relay) clients |
| `PASEO_RELAY_ENABLED` (alias `PASEO_RELAY`) | `true` | enable the outbound relay |
| `PASEO_HOSTNAMES` | – | comma-separated extra Host names for direct connections (`paseo.example.com,.lan`); IPs and localhost always allowed, others get `403 Host not allowed` |
| `PASEO_LOG_CONSOLE_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `PASEO_RELAY_ENDPOINT` | `relay.paseo.sh:443` | relay `host:port` |
| `PASEO_RELAY_USE_TLS` | `true` for `:443` | `wss` vs `ws` |
| `PASEO_RELAY_PUBLIC_ENDPOINT` / `_USE_TLS` | same as above | what goes into the pairing offer |
| `PASEO_APP_BASE_URL` | `https://app.paseo.sh` | pairing link base |
| `PASEO_CLAUDE_MAX_LIVE` | `auto` | max concurrent `claude` processes, or `auto` to follow the machine; extra prompts queue |
| `PASEO_CLAUDE_KEEP_WARM` | `true` | keep the `claude` process warm and pre-start it |
| `PASEO_CLAUDE_IDLE_SECS` | `90` | keep-warm time after a Claude turn (when keep-warm is off) |
| `PASEO_CODEX_IDLE_SECS` | `300` | idle time before the shared app-server exits (when keep-warm is off) |
| `PASEO_CODEX_MAX_ACTIVE` | `auto` | max concurrent Codex turns, or `auto`; extra prompts queue |
| `PASEO_AGENT_MB` | `300` | `auto`: memory budgeted per agent, for the ceiling and the room check |
| `PASEO_AUTO_MAX_AGENTS` | `8` | `auto`: never more agents at once than this |
| `PASEO_AUTO_STABLE_SECS` | `60` | `auto`: how long the machine must stay calm, with turns waiting, before adding a worker |
| `PASEO_CODEX_KEEP_WARM` | `true` | start the app-server with the daemon and keep it up |
| `PASEO_AGENT_NICE` | `10` | CPU niceness of `claude` / `codex` and everything they run (0 = off) |
| `PASEO_AGENT_OOM_SCORE_ADJ` | `500` | added to the daemon's own OOM score for agent processes (up to 1000), so the kernel kills them before the daemon (0 = off) |
| `PASEO_AGENT_LOW_IO` | `true` | lowest best-effort I/O priority for agent processes |
| `PASEO_AUTO_COMPACT_PCT` | `70` | after a turn, compact the context (`/compact`, Codex `thread/compact/start`) once it fills this % of the window (0 = off) |
| `PASEO_AUTO_COMPACT_TOKENS` | `150000` | also compact once the context reaches this many tokens, whatever the window (0 = no cap). Every model call uploads the whole context, so this caps the upload per call (~600 KB at 150K tokens) as well as cache-read cost on 1M-window models |
| `PASEO_RETRY_MAX` | `3` | automatic retries of a turn that fails with a transient error, returns nothing, or stops in the middle of a tool call; a retry after partial output asks the model to continue, otherwise the prompt is resent (0 = off) |
| `PASEO_RETRY_BASE_SECS` | `5` | delay before the first retry; doubles each attempt, max 60s |
| `PASEO_STALL_SECS` | `600` | a running turn with no output for this long (and no tool call in progress, no pending permission) is treated as stuck and retried (0 = off) |
| `PASEO_RESUME_AFTER_LIMIT` | `true` | when the Claude or Codex usage limit stops a turn, keep the turn open, wait until the limit resets (plus a minute) and continue automatically; messages sent meanwhile try again at once and then run after it; stop cancels the wait. Gives up if the reset is more than 8 days away |
| `PASEO_LIMIT_POLL_SECS` | `1800` | how often to try again when the provider doesn't say when the limit resets |
| `PASEO_CLAUDE_CONTEXT_WINDOW` | `200000` | Claude context window used for that check (`[1m]` models use 1M) |
| `PASEO_DELETE_PROJECT_FILES` | `safe` | what removing a project does to its folder: `safe` deletes it only when nothing would be lost (see above); `all` deletes any project folder inside your home; `never` keeps everything |
| `PASEO_TIMELINE_UNLOAD_SECS` | `600` | drop idle timelines from memory |
| `PASEO_DEFAULT_SYSTEM_PROMPT` | built-in low-resource note | instructions added to every agent (Claude `--append-system-prompt`, Codex `developerInstructions`); the default tells agents not to run, serve or test the app locally on a small VM. Set to an empty string to disable |

A systemd unit is in `contrib/paseo-lite.service`.

## Layout

```
src/main.rs              entry, runtime, signals
src/manager.rs           agent state actor: turns, queueing, timeline merge, persistence
src/rpc.rs               Paseo session RPC handlers
src/snapshot.rs          wire projections (agent snapshot, workspace, timeline entry)
src/session.rs           per-connection hello/auth + fan-out (transport agnostic)
src/server.rs            HTTP + /ws + embedded UI
src/relay.rs, e2ee.rs    relay v2 client + NaCl box channel
src/providers/claude*.rs Claude Code pool + stream-json parser
src/providers/codex*.rs  shared Codex app-server client + event mapping
src/store.rs             agents.json + JSONL timelines
src/usage.rs             token/cost ledger per provider and model, price list
web/index.html           built-in pairing page (QR behind the password)
```

## Support the project

paseo-lite is built and maintained by one person in their spare time. If it saves you time or money, you can support continued development:

- **GitHub Sponsors:** https://github.com/sponsors/sudtanj (monthly or one-time)
- **Star the repo**, report bugs and share it: it helps more than you'd think.
- **Commercial license, hosted/managed setups, custom features or consulting:** contact [@sudtanj](https://github.com/sudtanj) on GitHub.

## License

**Source-available: free for personal use, paid for commercial use.**

- Modifications and original code are under the [PolyForm Noncommercial License 1.0.0](LICENSE-POLYFORM-NONCOMMERCIAL.md), Copyright (c) 2025-present Sudono Tanjung. You can run, read and modify it for personal and other noncommercial use at no cost.
- **Commercial use** (in or for a business, as a hosted service, or in a commercial product) needs a paid commercial license: contact [@sudtanj](https://github.com/sudtanj).
- The portions derived from [Paseo](https://github.com/getpaseo/paseo) (Copyright (c) Mohamed Boudra) stay under [Apache-2.0](LICENSE-APACHE), and versions published before the license change remain available under it. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

## Testing

```sh
cargo test --bins          # unit tests (config, cleanup guards, push, snapshots, retries)
cargo test --test e2e      # integration tests: real binary over HTTP/WebSocket with a fake claude CLI
```

CI runs both on every push and pull request, and the rolling `latest` release is only published when they pass.
