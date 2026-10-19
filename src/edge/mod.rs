//! Edge machines: more machines running agents for one main daemon, without
//! any of main's secrets. See hub.rs (main side) and client.rs (edge side).

pub mod client;
pub mod hub;
pub mod registry;
pub mod upstream;
pub mod wire;

pub use hub::{route, Target};

use crate::config::Config;

pub const HELP: &str = "paseo-lite edge — run agents on more machines, without sharing secrets

ON MAIN (the daemon the app pairs with):
    paseo-lite edge invite NAME [--ttl MINUTES]   one-time join code for a new edge (default 15 minutes)
    paseo-lite edge ls                            joined edges and pending invites
    paseo-lite edge rm NAME                       remove an edge (it is refused from then on)

ON THE EDGE MACHINE:
    paseo-lite edge run [CODE]                    join with CODE (or PASEO_EDGE_JOIN), then serve main
                                                  (later runs reuse the saved code)

An edge connects out through the relay, so it needs no open ports. Its
workspace folders (PASEO_EDGE_WORKSPACE, default /workspace or ~/workspace)
appear on main under PASEO_EDGES_DIR/<name>/ (default /workspace/edges);
a Claude agent on such a folder runs on the edge. The edge never receives
main's API keys, password or pairing key: model calls go through main.
";

/// `paseo-lite edge ...`. Returns the process exit code.
pub fn cli(cfg: &Config, args: &[String]) -> i32 {
    let sub = args.first().map(String::as_str).unwrap_or("help");
    match sub {
        "invite" => {
            let Some(name) = args.get(1).filter(|n| !n.starts_with("--")) else {
                eprintln!("usage: paseo-lite edge invite NAME [--ttl MINUTES]");
                return 2;
            };
            let ttl_min: u64 = args.iter().position(|a| a == "--ttl").and_then(|i| args.get(i + 1)).and_then(|v| v.parse().ok()).unwrap_or(15).clamp(1, 7 * 24 * 60);
            if !cfg.relay_enabled {
                eprintln!("edges join through the relay, which is disabled here (PASEO_RELAY_ENABLED=false)");
                return 1;
            }
            let identity = match hub::identity(&cfg.home) {
                Ok(id) => id,
                Err(e) => {
                    eprintln!("cannot create the edge identity: {e}");
                    return 1;
                }
            };
            let mut reg = registry::Registry::load(&cfg.home);
            let token = match reg.invite(name, ttl_min * 60_000) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("{e}");
                    return 1;
                }
            };
            if let Err(e) = reg.save(&cfg.home) {
                eprintln!("cannot save edges.json: {e}");
                return 1;
            }
            let code = wire::JoinCode { v: 1, server_id: identity.server_id.clone(), key: identity.public_b64.clone(), relay: cfg.relay_public_endpoint.clone(), tls: cfg.relay_public_use_tls, token };
            println!("{}", code.encode());
            eprintln!("\nOne-time code for edge {name}, valid {ttl_min} minutes. On the edge machine run:\n  paseo-lite edge run <code>\nor start the image with PASEO_EDGE_JOIN=<code>. It holds no API key or password.");
            0
        }
        "ls" => {
            let reg = registry::Registry::load(&cfg.home);
            if reg.edges.is_empty() && reg.invites.is_empty() {
                println!("no edges yet (paseo-lite edge invite NAME)");
            }
            for e in &reg.edges {
                println!("{}\tjoined {}\t{}", e.name, e.added_at, cfg.edges_dir.join(&e.name).display());
            }
            let now = registry::now_ms();
            for i in reg.invites.iter().filter(|i| i.expires_at_ms > now) {
                println!("{}\tinvited, code valid {} more minutes", i.name, (i.expires_at_ms - now) / 60_000);
            }
            0
        }
        "rm" => {
            let Some(name) = args.get(1) else {
                eprintln!("usage: paseo-lite edge rm NAME");
                return 2;
            };
            let mut reg = registry::Registry::load(&cfg.home);
            if !reg.remove(name) {
                eprintln!("no edge named {name}");
                return 1;
            }
            if let Err(e) = reg.save(&cfg.home) {
                eprintln!("cannot save edges.json: {e}");
                return 1;
            }
            println!("removed {name}: its connection ends within 10 seconds and it is refused from then on");
            0
        }
        "run" | "join" => {
            let code = args.get(1).cloned();
            let _ = rustls::crypto::ring::default_provider().install_default();
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("tokio runtime");
            match rt.block_on(client::run(cfg.clone(), code)) {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("{e}");
                    1
                }
            }
        }
        _ => {
            print!("{HELP}");
            if sub == "help" || sub == "--help" || sub == "-h" { 0 } else { 2 }
        }
    }
}
