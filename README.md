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
| MCP servers | per agent, per provider | one [zilong-router](https://github.com/sudtanj/zilong-router) MCP gateway (`PASEO_ZILONG_MCP_URL`) for every Claude Code and Codex agent, see [MCP: zilong-router](#mcp-zilong-router); Paseo's own agent-control MCP server is not provided |
| Projects | — | a project is just a directory; adding one costs no process and no memory until an agent runs a turn |

Idle cost is the daemon plus one warm process of the active CLI (`claude` or `codex app-server`). Set `PASEO_CODEX_KEEP_WARM=0` / `PASEO_CLAUDE_KEEP_WARM=0` to start them on demand and release them after `PASEO_CODEX_IDLE_SECS` / `PASEO_CLAUDE_IDLE_SECS` instead.

<!-- bench:summary -->
On a simulated GCP e2-micro (free tier) the daemon is listening 0.24 s after start and idles at 8.3 MiB RSS; a follow-up turn takes 0.81 s end to end, and 6 chats at once, 3 turns each finish in 17.12 s with the daemon peaking at 8.4 MiB RSS (0 errors; see [Benchmark](#benchmark)).
<!-- /bench:summary -->

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

The build is upstream's own `expo export` with source maps removed. For weak devices, the Mermaid
diagram runtime (~3.6 MB, inlined in the main bundle upstream) is shipped as its own file and loaded
only when a message shows a diagram, and a headless Chrome check makes sure the app still starts. The
job summary shows the JavaScript size a browser downloads. WebAssembly would not make it lighter: the app is React Native
Web, its cost is the JS UI and the DOM, which WebAssembly cannot reach without a full rewrite plus JS
glue, and the only compute-heavy part (the NaCl box encryption) is already small.

**Privacy of the pairing link.** The link holds the server id and the daemon's public key, and over
the relay it is all a client needs, so treat it like a password. On this site it stays in your browser:

- Paste it into the app's pair dialog rather than opening it: an opened link stays in the address bar
  and browser history (and in history sync). A `#fragment` is never sent to GitHub either way.
- The app parses it locally and saves the host in this browser's local storage. The web build has no
  analytics, error reporting or push registration.
- The page carries a Content Security Policy: it can only connect to `wss://relay.paseo.sh`, a daemon
  on `localhost`, and what you add in the `PAGES_EXTRA_CONNECT_SRC` repository variable (a self-hosted
  relay or an HTTPS daemon, e.g. `wss://relay.example.com wss://paseo.example.com`). No third-party
  scripts, and no referrer is sent. The relay only sees the server id and encrypted traffic.
- Local storage is shared by every page on the same origin. All `<user>.github.io/*` project sites
  share one, so a script on another of your Pages sites could read it. For full isolation, serve this
  site from its own origin: a custom (sub)domain in Settings > Pages, or a separate GitHub
  organization's `<org>.github.io`.

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
- file explorer and file pane: `file_explorer_request` (list a folder newest first, read a file: text up to 1 MiB
  inline, images up to 2 MiB as base64, other binaries as metadata only), `file_download_token_request` with
  `GET /api/files/download?token=…` on the direct listener (single-use, 60 s; over the relay the app reads the
  file instead), `fs.file.write.request` (saves a text file of up to 1 MiB atomically, with a conflict check against
  the version the app loaded), and `fs.entry.create` / `fs.entry.rename` (`git mv` for tracked files) /
  `fs.entry.delete`. Every path must stay inside the request's `cwd`, including through symlinks.
  Live file updates (`fs.file.subscribe`) and `fs.entry.duplicate` are not supported; the file pane then
  reloads the file when you open it instead of updating live.

Anything else (live file updates, duplicating files, terminals, plugins, voice, schedules, Hub, worktrees) answers with
`rpc_error` / `unsupported`. The official app is expected to handle the core agent flow. Screens built
on unsupported features will show errors.

**Removing a project** (or archiving its last workspace) archives its agents and deletes the local folder
when that can't lose work: it must be a git repository with no uncommitted changes, no stashes, no
other worktrees, every commit on a remote, and no ignored files beyond regenerable ones
(`node_modules`, `target`, `dist`, `.venv`, …, and the runtime state Claude Code keeps under `.claude/`), so a `.env` keeps it. Cloning a repository again onto a kept checkout of it reuses that checkout instead of failing. A folder paseo-lite created
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
| `PASEO_CLAUDE_IDLE_SECS` | `90` | keep-warm time after a Claude turn (when keep-warm is off). A process with background work still running (sub-agents, `run_in_background` shell commands, workflows) is kept until it reports back (or goes 30 minutes without activity; 2 hours for shells and workflows, which show none until they finish); it is not evicted, but it takes no slot, so a prompt from another chat starts alongside it (while the machine is strained the prompt waits for it instead) |
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
| `PASEO_CLAUDE_SUBAGENT_PROMPT` | built-in sub-agent note | instructions added to Claude Code agents only, after the ones above; the default tells them to hand bigger or independent parts of a task to parallel background sub-agents (Agent tool) and end the turn at once, without waiting or polling, so the chat stays free for your next message while they work; each result comes back as a notification the CLI acts on in a turn of its own, which the daemon records like any other (the same for background shell commands and workflows; their cards get their final status even when no turn follows). Sub-agents live inside the agent's own `claude` process, so they add parallelism on a machine that fits only one process. Set to an empty string to disable |
| `PASEO_CLAUDE_MAX_SUBAGENTS` | `3` | sub-agents (foreground or background) one Claude Code agent may run at once. The daemon registers a `PreToolUse` hook for `Agent`/`Task` and refuses further launches, with a reason the model acts on, until one reports back; while the machine is strained (memory or CPU pressure, low free memory) it starts none, and running ones carry on. A hook deny applies in every permission mode, Bypass included. Background shell commands and workflows don't count. `0` = no sub-agents |
| `PASEO_ZILONG_MCP_URL` | – | zilong-router MCP gateway (`http://<router>:20128/mcp`) for every Claude Code and Codex agent. See [MCP: zilong-router](#mcp-zilong-router) |
| `PASEO_ZILONG_API_KEY` | – | its API key, sent as a Bearer token |

A systemd unit is in `contrib/paseo-lite.service`.

## MCP: zilong-router

Set `PASEO_ZILONG_MCP_URL` to a [zilong-router](https://github.com/sudtanj/zilong-router) MCP gateway
(`http://<router>:20128/mcp`, or `/mcp/all` to list every tool directly) and every Claude Code and Codex agent gets
it as one Streamable HTTP server, `zilong-router`, next to the servers in your `~/.claude.json`, project `.mcp.json`
and `~/.codex/config.toml`. Register the actual MCP servers in the router's dashboard.

- Claude Code: `--mcp-config ~/.paseo-lite/claude-mcp.json`, written at startup with mode 0600 (removed when the URL
  is unset).
- Codex: `codex app-server -c mcp_servers.zilong-router.url=… -c mcp_servers.zilong-router.bearer_token_env_var="PASEO_ZILONG_API_KEY"`.
- `PASEO_ZILONG_API_KEY` is sent as `Authorization: Bearer …`; leave it unset for a router on loopback that doesn't
  require keys. The key never appears on a command line, but the agents inherit it in their environment.

Both are read once at startup; restart the daemon to change them. MCP tool calls ask for permission like any other
tool (Claude's permission prompt; Codex approvals and MCP elicitations, where requests with form fields or a URL,
which the app can't show, are declined). In a GitHub Actions workflow:

```yaml
env:
  PASEO_ZILONG_MCP_URL: ${{ secrets.PASEO_ZILONG_MCP_URL }}
  PASEO_ZILONG_API_KEY: ${{ secrets.PASEO_ZILONG_API_KEY }}
```

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
src/mcp.rs               zilong-router MCP gateway as Claude --mcp-config / Codex -c overrides
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
cargo test --release --test bench -- --ignored --nocapture   # benchmark on simulated small machines (needs Docker), see below
```

CI runs the unit and integration tests on every push and pull request, and the rolling `latest` release is only published when they pass.

## Benchmark

The Benchmark workflow (`.github/workflows/bench.yml`) runs `tests/bench.rs` on an amd64 and an arm64
runner whenever code changes on `main` (or from the Actions tab), and rewrites the numbers below and in
the summary near the top. Each simulated machine is a Docker container with that machine's memory
(swap off) and CPU quota. paseo-lite reads its limits from the container's cgroup, so the auto capacity
it picks is the one it would pick on that machine:

| System | Runner | Memory | CPUs |
|---|---|---:|---:|
| GCP e2-micro (free tier), sustained baseline | amd64 | 1 GB | 0.25 |
| 512 MB VPS | amd64 | 512 MB | 1 |
| Small VPS | amd64 | 4 GB | 2 |
| 1 GB single-board computer (Orange Pi Zero 3, Raspberry Pi 3) | arm64 | 1 GB | 4 |
| OCI Ampere A1 (free tier, scaled to the runner) | arm64 | 12 GB | 4 |

A fake `claude` CLI stands in for Claude Code. It holds 200 MiB like a real process does (`BENCH_CLAUDE_MB`),
and answers each prompt with a turn of about 1 s: streamed text, a tool call and a final message. On each
system the benchmark runs:

- one chat for 10 turns: the first turn starts the CLI process, the rest reuse it;
- 6 chats started at once, 3 turns each. These turns queue for the CLI slots the daemon allows on that
  machine, and turn latency includes that wait.

It records turn latency, the peak RSS of the daemon, the peak memory of the whole container (daemon plus
`claude` processes), the most `claude` processes running at once, the capacity the daemon chose, and OOM
kills. Auto capacity waits 10 s with work queued before adding a worker here (`PASEO_AUTO_STABLE_SECS`;
the default is 60 s), so the runs show it growing. The benchmark fails on any failed turn or OOM kill, and
the README is only updated when every system passes.

```sh
docker pull ubuntu:24.04
BENCH_SYSTEMS=gcp-e2-micro,vps-512m cargo test --release --test bench -- --ignored --nocapture
```

<!-- bench:report -->
#### x86_64 runner

Measured 2026-10-31 on x86_64 at commit `7cb964d`, binary 3.1 MiB. The fake `claude` holds 200 MiB and streams each turn in about 1 s (5 text deltas, a tool call, a final message).

### GCP e2-micro (free tier): 1 GB, 0.25 vCPU sustained

`--memory 1g --cpus 0.25` (`gcp-e2-micro`). Listening 0.24 s after `docker run`; idle 3 s later: daemon 8.3 MiB RSS, container 4.2 MiB.

| Scenario | Turns | Errors | Wall | p50 turn | p99 turn | Peak daemon RSS | Peak container memory | Peak `claude` processes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 chat, first turn (starts the CLI) | 1 | 0 | 1.31 s | 1.31 s | 1.31 s | 8.4 MiB | 231.0 MiB | 1 |
| 1 chat, 9 more turns | 9 | 0 | 7.32 s | 0.81 s | 0.81 s | 8.4 MiB | 231.0 MiB | 1 |
| 6 chats at once, 3 turns each | 18 | 0 | 17.12 s | 5.10 s | 6.33 s | 8.4 MiB | 457.3 MiB | 2 |

Auto capacity: limit 1 (ceiling 2) from 1024 MB and 1 CPU(s) seen in the cgroup. OOM kills: 0.

### 512 MB VPS: 512 MB, 1 vCPU

`--memory 512m --cpus 1` (`vps-512m`). Listening 0.13 s after `docker run`; idle 3 s later: daemon 8.3 MiB RSS, container 4.1 MiB.

| Scenario | Turns | Errors | Wall | p50 turn | p99 turn | Peak daemon RSS | Peak container memory | Peak `claude` processes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 chat, first turn (starts the CLI) | 1 | 0 | 0.93 s | 0.93 s | 0.93 s | 8.5 MiB | 231.0 MiB | 1 |
| 1 chat, 9 more turns | 9 | 0 | 7.32 s | 0.81 s | 0.81 s | 8.5 MiB | 231.0 MiB | 1 |
| 6 chats at once, 3 turns each | 18 | 0 | 16.91 s | 5.63 s | 5.65 s | 8.5 MiB | 231.1 MiB | 1 |

Auto capacity: limit 1 (ceiling 1) from 512 MB and 1 CPU(s) seen in the cgroup. OOM kills: 0.

### Small VPS: 4 GB, 2 vCPU

`--memory 4g --cpus 2` (`vps-2c-4g`). Listening 0.12 s after `docker run`; idle 3 s later: daemon 8.3 MiB RSS, container 4.5 MiB.

| Scenario | Turns | Errors | Wall | p50 turn | p99 turn | Peak daemon RSS | Peak container memory | Peak `claude` processes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 chat, first turn (starts the CLI) | 1 | 0 | 0.93 s | 0.93 s | 0.93 s | 8.4 MiB | 231.0 MiB | 1 |
| 1 chat, 9 more turns | 9 | 0 | 7.31 s | 0.81 s | 0.81 s | 8.4 MiB | 231.0 MiB | 1 |
| 6 chats at once, 3 turns each | 18 | 0 | 9.02 s | 2.79 s | 3.45 s | 8.4 MiB | 457.4 MiB | 2 |

Auto capacity: limit 2 (ceiling 4) from 4096 MB and 2 CPU(s) seen in the cgroup. OOM kills: 0.

#### aarch64 runner

Measured 2026-10-31 on aarch64 at commit `7cb964d`, binary 2.6 MiB. The fake `claude` holds 200 MiB and streams each turn in about 1 s (5 text deltas, a tool call, a final message).

### 1 GB single-board computer (Orange Pi Zero 3, Raspberry Pi 3): 1 GB, 4 cores

`--memory 1g --cpus 4` (`sbc-1g`). Listening 0.27 s after `docker run`; idle 3 s later: daemon 7.3 MiB RSS, container 4.3 MiB.

| Scenario | Turns | Errors | Wall | p50 turn | p99 turn | Peak daemon RSS | Peak container memory | Peak `claude` processes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 chat, first turn (starts the CLI) | 1 | 0 | 0.91 s | 0.91 s | 0.91 s | 7.3 MiB | 230.9 MiB | 1 |
| 1 chat, 9 more turns | 9 | 0 | 7.32 s | 0.81 s | 0.81 s | 7.3 MiB | 230.9 MiB | 1 |
| 6 chats at once, 3 turns each | 18 | 0 | 8.69 s | 2.68 s | 3.35 s | 7.3 MiB | 457.2 MiB | 2 |

Auto capacity: limit 2 (ceiling 3) from 1024 MB and 4 CPU(s) seen in the cgroup. OOM kills: 0.

### OCI Ampere A1 (free tier, scaled to the runner): 12 GB, 4 OCPU

`--memory 12g --cpus 4` (`oci-a1`). Listening 0.11 s after `docker run`; idle 3 s later: daemon 7.3 MiB RSS, container 4.3 MiB.

| Scenario | Turns | Errors | Wall | p50 turn | p99 turn | Peak daemon RSS | Peak container memory | Peak `claude` processes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 chat, first turn (starts the CLI) | 1 | 0 | 0.90 s | 0.90 s | 0.90 s | 7.3 MiB | 231.0 MiB | 1 |
| 1 chat, 9 more turns | 9 | 0 | 7.32 s | 0.81 s | 0.81 s | 7.3 MiB | 231.0 MiB | 1 |
| 6 chats at once, 3 turns each | 18 | 0 | 8.68 s | 2.67 s | 3.34 s | 7.3 MiB | 457.1 MiB | 2 |

Auto capacity: limit 2 (ceiling 8) from 12288 MB and 4 CPU(s) seen in the cgroup. OOM kills: 0.
<!-- /bench:report -->
