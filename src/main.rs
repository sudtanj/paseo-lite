mod capacity;
mod checkout;
mod cleanup;
mod config;
mod daemon_config;
mod e2ee;
mod egress;
mod files;
mod git_actions;
mod manager;
mod mcp;
mod projects;
mod push;
mod providers;
mod relay;
mod rpc;
mod rpc_ext;
mod server;
mod session;
mod snapshot;
mod store;
mod uploads;
mod usage;
mod util;
mod workspaces;

use config::Config;
use std::sync::Arc;

const HELP: &str = "paseo-lite — low-resource daemon for Claude Code and Codex agents

USAGE:
    paseo-lite [--listen HOST:PORT] [--no-relay]   run the daemon
    paseo-lite pair                                print the pairing link + QR
    paseo-lite help

ENVIRONMENT (selection):
    PASEO_LITE_HOME          state dir (default ~/.paseo-lite)
    PASEO_LISTEN             default 127.0.0.1:6768
    PASEO_PASSWORD           require this password for direct connections
    PASEO_RELAY_ENABLED=false  disable the relay
    PASEO_HOSTNAMES          extra allowed Host names (comma separated, .suffix ok)
    PASEO_LOG_CONSOLE_LEVEL  debug|info|warn|error
    PASEO_RELAY_ENDPOINT     default relay.paseo.sh:443
    PASEO_CLAUDE_MAX_LIVE    max concurrent claude processes, or auto (default: grows with the machine)
    PASEO_CLAUDE_IDLE_SECS   keep-warm time for an idle claude process (default 90)
    PASEO_CODEX_IDLE_SECS    idle time before the shared codex app-server exits (default 300)
    PASEO_DEFAULT_SYSTEM_PROMPT  instructions for every agent (default: do not run/test the app locally; set empty to disable)
    PASEO_CLAUDE_SUBAGENT_PROMPT  extra instructions for Claude Code agents (default: hand work to background sub-agents and stay free; set empty to disable)
    PASEO_CLAUDE_MAX_SUBAGENTS    sub-agents one Claude Code agent may run at once; none start while the machine is strained (default 3)
";

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "paseo-lite".into())
}

fn print_pairing(cfg: &Config, identity: &e2ee::DaemonIdentity) {
    let url = identity.pairing_url(&cfg.app_base_url, &cfg.relay_public_endpoint, cfg.relay_public_use_tls);
    if let Ok(code) = qrcode::QrCode::new(url.as_bytes()) {
        let art = code.render::<qrcode::render::unicode::Dense1x2>().quiet_zone(true).build();
        println!("{art}");
    }
    println!("Scan with the Paseo app, or open this link on the device you want to pair:\n{url}\n");
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "help" || a == "--help" || a == "-h") {
        print!("{HELP}");
        return;
    }
    let cfg = Config::load(&args);
    let identity = match e2ee::DaemonIdentity::load_or_create(&cfg.home) {
        Ok(id) => Arc::new(id),
        Err(e) => {
            eprintln!("cannot initialise {}: {e}", cfg.home.display());
            std::process::exit(1);
        }
    };
    if args.first().map(|a| a == "pair").unwrap_or(false) {
        print_pairing(&cfg, &identity);
        return;
    }

    let _ = rustls::crypto::ring::default_provider().install_default();
    // Single-threaded runtime: agents are I/O bound and this keeps the daemon
    // at one OS thread plus the child processes it supervises.
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("tokio runtime");
    rt.block_on(run(cfg, identity));
}

async fn run(cfg: Config, identity: Arc<e2ee::DaemonIdentity>) {
    let pairing = cfg.relay_enabled.then(|| identity.pairing_url(&cfg.app_base_url, &cfg.relay_public_endpoint, cfg.relay_public_use_tls));
    let (bcast, _) = tokio::sync::broadcast::channel(2048);
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();
    providers::protect_daemon();
    let (manager, events_rx) = manager::Manager::new(cfg.clone(), identity.server_id.clone(), pairing, bcast.clone(), cmd_tx.clone());
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let manager_task = tokio::spawn(manager.run(cmd_rx, events_rx, stop_rx));

    let ctx = Arc::new(session::Ctx {
        cmds: cmd_tx,
        bcast,
        server_id: identity.server_id.clone(),
        hostname: hostname(),
        password: cfg.password.clone(),
        home: cfg.home.clone(),
    });

    crate::log!("paseo-lite {} serverId={} home={}", env!("CARGO_PKG_VERSION"), identity.server_id, cfg.home.display());
    mcp::write_claude_config(&cfg);
    uploads::prune(&cfg.home);
    usage::load_prices(&cfg.home);
    tokio::spawn(usage::refresh_prices(cfg.home.clone()));
    if cfg.relay_enabled {
        print_pairing(&cfg, &identity);
        tokio::spawn(relay::run(ctx.clone(), cfg.clone(), identity.clone()));
    } else {
        crate::log!("relay disabled; connect directly over LAN/Tailscale/SSH tunnel");
    }

    let listen = cfg.listen.clone();
    let hostnames = cfg.hostnames.clone();
    let server_ctx = ctx.clone();
    let server = tokio::spawn(async move {
        if let Err(e) = server::serve(server_ctx, listen, hostnames).await {
            crate::warn!("http server failed: {e}");
        }
    });

    shutdown_signal().await;
    crate::log!("shutting down");
    server.abort();
    let _ = stop_tx.send(());
    let _ = manager_task.await;
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("signal handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
