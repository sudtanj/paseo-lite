//! How many agent turns may run at once.
//!
//! With `auto` (the default) the limit adapts to the machine: it starts at
//! the ceiling, so a fresh daemon uses the whole machine at once, shrinks by
//! one when pressure builds up, and grows back by one while turns are queued
//! and the machine has stayed calm for `stable` (low memory/CPU pressure, no
//! CPU throttling, room for another agent). It never exceeds the
//! ceiling: what the memory limit fits at `agent_mb` per agent, the CPU count
//! times two, what the pids limit fits, and `max`. Running turns are never
//! stopped; a lower limit only holds back the next ones, which wait in the
//! queue as before.
//!
//! Inside Docker every limit comes from the container's cgroup (v2, else v1):
//! `--memory` (and `memory.high`), `--cpus`, `--pids-limit`, and the
//! container's own memory/CPU pressure. They are re-read every minute, so a
//! `docker update` takes effect without a restart. Outside a container the
//! host's RAM and CPUs apply.
//!
//! When the container is about to run out (memory nearly exhausted, heavy
//! memory stalls, pids nearly used up) the reading is *critical*: the limit
//! drops at once, without waiting between steps, and the manager releases
//! warm idle processes so the daemon keeps enough room to stay in control.

use serde_json::{json, Value};
use std::time::{Duration, Instant};

/// Memory kept for the daemon and the commands agents run, outside the
/// per-agent budget.
const RESERVE_MB: u64 = 96;
/// A step down waits at least this long after the last change, so one
/// pressure spike can't drop the limit to one in a few ticks.
const SHRINK_GAP: Duration = Duration::from_secs(10);
/// How often the cgroup limits are read again (`docker update`).
const LIMITS_REFRESH: Duration = Duration::from_secs(60);
/// Processes and threads budgeted per agent (the CLI plus git, rg, shells)
/// against the pids limit, and the pids kept free for the daemon.
const PIDS_PER_AGENT: u64 = 64;
const PIDS_RESERVE: u64 = 32;

/// Pressure (PSI `some avg10`, percent) at or above which the limit shrinks.
const MEM_PRESSURE_HIGH: f64 = 10.0;
const CPU_PRESSURE_HIGH: f64 = 80.0;
/// Memory stalls at or above this are critical.
const MEM_PRESSURE_CRITICAL: f64 = 40.0;
/// Pressure below which the machine counts as calm enough to grow.
/// Well under HIGH: swap-ins from zram alone show as a few percent of
/// stalls on a small VM, which shouldn't keep a second agent from starting.
const MEM_PRESSURE_CALM: f64 = 5.0;
const CPU_PRESSURE_CALM: f64 = 40.0;
/// Share of wall time the container was held back by its CPU quota.
const THROTTLE_HIGH: f64 = 0.5;
const THROTTLE_CALM: f64 = 0.2;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Mode {
    Fixed(usize),
    Auto,
}

impl Mode {
    /// `auto`, empty or unset = Auto; a number = Fixed (at least 1).
    pub fn parse(v: Option<&str>) -> Mode {
        match v.map(str::trim).filter(|v| !v.is_empty() && !v.eq_ignore_ascii_case("auto")) {
            None => Mode::Auto,
            Some(v) => v.parse::<usize>().map(|n| Mode::Fixed(n.max(1))).unwrap_or(Mode::Auto),
        }
    }

    pub fn label(self) -> String {
        match self {
            Mode::Fixed(n) => n.to_string(),
            Mode::Auto => "auto".into(),
        }
    }
}

/// What an update did.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Change {
    None,
    /// The limit went up: queued turns may start.
    Grew,
    Shrank,
    /// The container is about to run out: free whatever isn't working.
    Critical,
}

/// The limits the daemon runs under: the container's when in one.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Limits {
    pub mem_mb: Option<u64>,
    pub cpus: usize,
    pub pids_max: Option<u64>,
}

/// One reading of the machine. `None` = not available on this system.
#[derive(Clone, Copy, Debug, Default)]
pub struct Sample {
    /// Memory that can still be used before the limit, in MB.
    pub avail_mb: Option<u64>,
    /// PSI `some avg10` for memory and CPU, percent.
    pub mem_pressure: Option<f64>,
    pub cpu_pressure: Option<f64>,
    /// Processes and threads in the container.
    pub pids: Option<u64>,
    /// Share of the last interval the CPU quota throttled the container.
    pub throttle: Option<f64>,
}

pub struct Capacity {
    /// False when both caps are fixed numbers: the machine is still watched
    /// (strain, critical) but the limit never moves.
    auto: bool,
    agent_mb: u64,
    max: usize,
    stable: Duration,
    limits: Limits,
    limits_at: Instant,
    ceiling: usize,
    limit: usize,
    calm_since: Option<Instant>,
    last_change: Instant,
    last: Sample,
    strained: bool,
    critical: bool,
    /// Previous throttled-time reading (µs) for the throttle share.
    throttled: Option<(Instant, u64)>,
    /// Throttle share smoothed over ~10 s, like the kernel's avg10.
    throttle_avg: Option<f64>,
    /// The last time the limit or ceiling moved, and why (for the debug MCP server).
    last_decision: Option<(String, String)>,
}

impl Capacity {
    pub fn new(auto: bool, agent_mb: u64, max: usize, stable: Duration) -> Capacity {
        let limits = read_limits();
        let ceiling = ceiling(&limits, agent_mb, max);
        if auto {
            crate::log!("capacity: auto, up to {ceiling} agent(s) at once ({}, {agent_mb} MB per agent)", describe_limits(&limits));
        } else {
            crate::log!("capacity: fixed caps, watching {}", describe_limits(&limits));
        }
        let now = Instant::now();
        Capacity { auto, agent_mb, max, stable, limits, limits_at: now, ceiling, limit: if auto { ceiling } else { 1 }, calm_since: None, last_change: now, last: Sample::default(), strained: false, critical: false, throttled: throttled_usec().map(|t| (now, t)), throttle_avg: None, last_decision: None }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    /// The last reading showed pressure: don't start optional work (warm-ups).
    pub fn strained(&self) -> bool {
        self.strained
    }

    /// Read the machine and adjust the limit.
    pub fn update(&mut self, running: usize, queued: usize) -> Change {
        let now = Instant::now();
        let mut change = Change::None;
        if now.duration_since(self.limits_at) >= LIMITS_REFRESH {
            self.limits_at = now;
            change = self.set_limits(read_limits(), now);
        }
        let mut sample = read_sample(self.limits.mem_mb);
        sample.throttle = self.throttle_share(now);
        match self.apply(sample, running, queued, now) {
            Change::None => change,
            other => other,
        }
    }

    /// New container limits: recompute the ceiling, and pull the limit
    /// down to it at once when it shrank.
    fn set_limits(&mut self, limits: Limits, now: Instant) -> Change {
        if limits == self.limits {
            return Change::None;
        }
        self.limits = limits;
        self.ceiling = ceiling(&limits, self.agent_mb, self.max);
        crate::log!("capacity: limits changed ({}), up to {} agent(s) at once", describe_limits(&limits), self.ceiling);
        self.note(format!("limits changed ({}), ceiling {}", describe_limits(&limits), self.ceiling));
        if self.auto && self.limit > self.ceiling {
            self.limit = self.ceiling;
            self.last_change = now;
            self.note(format!("limits changed ({}), limit cut to the new ceiling {}", describe_limits(&limits), self.ceiling));
            return Change::Shrank;
        }
        Change::None
    }

    fn throttle_share(&mut self, now: Instant) -> Option<f64> {
        let cur = throttled_usec()?;
        let prev = self.throttled.replace((now, cur));
        let (at, before) = prev?;
        let wall = now.duration_since(at);
        if wall.is_zero() {
            return self.throttle_avg;
        }
        let share = (cur.saturating_sub(before) as f64 / wall.as_micros() as f64).min(1.0);
        self.throttle_avg = Some(smooth(self.throttle_avg, share, wall));
        self.throttle_avg
    }

    fn apply(&mut self, s: Sample, running: usize, queued: usize, now: Instant) -> Change {
        self.last = s;
        let over = |v: Option<f64>, max: f64| v.map(|v| v >= max).unwrap_or(false);
        let under = |v: Option<f64>, max: f64| v.map(|v| v < max).unwrap_or(true);
        let pids_left = match (s.pids, self.limits.pids_max) {
            (Some(cur), Some(max)) => Some(max.saturating_sub(cur)),
            _ => None,
        };
        let pids_below = |n: u64| pids_left.map(|l| l < n).unwrap_or(false);
        let mem_below = |mb: u64| s.avail_mb.map(|a| a < mb).unwrap_or(false);
        self.critical = over(s.mem_pressure, MEM_PRESSURE_CRITICAL) || mem_below(self.agent_mb / 8) || pids_below(PIDS_RESERVE);
        self.strained = self.critical || over(s.mem_pressure, MEM_PRESSURE_HIGH) || over(s.cpu_pressure, CPU_PRESSURE_HIGH) || over(s.throttle, THROTTLE_HIGH) || mem_below(self.agent_mb / 4) || pids_below(PIDS_PER_AGENT / 2);
        if !self.auto {
            return if self.critical { Change::Critical } else { Change::None };
        }
        if self.strained {
            self.calm_since = None;
            let mut shrank = false;
            if self.limit > 1 && (self.critical || now.duration_since(self.last_change) >= SHRINK_GAP) {
                self.limit -= 1;
                self.last_change = now;
                shrank = true;
                let why = if self.critical { "machine nearly out of room" } else { "machine under pressure" };
                crate::warn!("capacity: {why}, {} agent(s) at once now ({})", self.limit, describe(&s));
                self.note(format!("shrank to {}: {why} ({})", self.limit, describe(&s)));
            }
            return if self.critical { Change::Critical } else if shrank { Change::Shrank } else { Change::None };
        }
        // Without a memory reading the room check can't pass: stay put.
        let calm = under(s.mem_pressure, MEM_PRESSURE_CALM) && under(s.cpu_pressure, CPU_PRESSURE_CALM) && under(s.throttle, THROTTLE_CALM) && !pids_below(PIDS_PER_AGENT) && s.avail_mb.map(|a| a >= self.agent_mb).unwrap_or(false);
        if !calm {
            self.calm_since = None;
            return Change::None;
        }
        let since = *self.calm_since.get_or_insert(now);
        // Grow only when the current limit is the bottleneck.
        if queued == 0 || running < self.limit || self.limit >= self.ceiling || now.duration_since(since) < self.stable {
            return Change::None;
        }
        self.limit += 1;
        self.last_change = now;
        // The new agent's load has to prove calm again before the next step.
        self.calm_since = None;
        crate::log!("capacity: machine stable, {} agent(s) at once now ({})", self.limit, describe(&s));
        self.note(format!("grew to {}: machine stable with turns queued ({})", self.limit, describe(&s)));
        Change::Grew
    }

    fn note(&mut self, decision: String) {
        self.last_decision = Some((crate::util::now_iso(), decision));
    }

    /// `status()` plus what bounds the ceiling and the last limit change,
    /// for the debug MCP server.
    pub fn debug_status(&self) -> Value {
        let mut v = self.status();
        let l = &self.limits;
        v["ceilingBy"] = json!({
            "memory": l.mem_mb.map(|m| m.saturating_sub(RESERVE_MB) / self.agent_mb.max(1)),
            "cpu": l.cpus.max(1) * 2,
            "pids": l.pids_max.map(|p| p.saturating_sub(PIDS_RESERVE) / PIDS_PER_AGENT),
            "max": self.max,
            "reserveMb": RESERVE_MB,
        });
        v["stableSecs"] = json!(self.stable.as_secs());
        v["calmForSecs"] = json!(self.calm_since.map(|t| t.elapsed().as_secs()));
        v["lastChangeSecsAgo"] = json!(self.last_change.elapsed().as_secs());
        v["lastDecision"] = match &self.last_decision {
            Some((at, what)) => json!({ "at": at, "decision": what }),
            None => Value::Null,
        };
        v
    }

    pub fn status(&self) -> Value {
        json!({
            "auto": self.auto,
            "limit": self.limit,
            "ceiling": self.ceiling,
            "agentMb": self.agent_mb,
            "memLimitMb": self.limits.mem_mb,
            "cpus": self.limits.cpus,
            "pidsMax": self.limits.pids_max,
            "availMb": self.last.avail_mb,
            "memPressure": self.last.mem_pressure,
            "cpuPressure": self.last.cpu_pressure,
            "pids": self.last.pids,
            "cpuThrottle": self.last.throttle,
            "strained": self.strained,
            "critical": self.critical,
        })
    }
}

fn describe(s: &Sample) -> String {
    let pct = |v: Option<f64>| v.map(|v| format!("{v:.1}%")).unwrap_or_else(|| "n/a".into());
    format!(
        "free {}, memory pressure {}, CPU pressure {}, CPU throttled {}, pids {}",
        s.avail_mb.map(|a| format!("{a} MB")).unwrap_or_else(|| "n/a".into()),
        pct(s.mem_pressure),
        pct(s.cpu_pressure),
        pct(s.throttle.map(|t| t * 100.0)),
        s.pids.map(|p| p.to_string()).unwrap_or_else(|| "n/a".into())
    )
}

/// Exponential average with a ~10 s time constant, so one busy tick doesn't
/// read as sustained throttling.
fn smooth(prev: Option<f64>, value: f64, elapsed: Duration) -> f64 {
    let alpha = 1.0 - (-elapsed.as_secs_f64() / 10.0).exp();
    match prev {
        Some(p) => p + alpha * (value - p),
        None => value,
    }
}

fn describe_limits(l: &Limits) -> String {
    format!(
        "memory limit {}, {} CPU, pids limit {}",
        l.mem_mb.map(|m| format!("{m} MB")).unwrap_or_else(|| "unknown".into()),
        l.cpus,
        l.pids_max.map(|p| p.to_string()).unwrap_or_else(|| "none".into())
    )
}

fn ceiling(l: &Limits, agent_mb: u64, max: usize) -> usize {
    let by_mem = l.mem_mb.map(|m| (m.saturating_sub(RESERVE_MB) / agent_mb.max(1)) as usize).unwrap_or(1);
    let by_pids = l.pids_max.map(|p| (p.saturating_sub(PIDS_RESERVE) / PIDS_PER_AGENT) as usize).unwrap_or(usize::MAX);
    by_mem.min(l.cpus.max(1) * 2).min(by_pids).min(max).max(1)
}

fn read(path: &str) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

/// A plain number file; `max` (unlimited) and junk read as None.
fn read_num(path: &str) -> Option<u64> {
    read(path).and_then(|v| v.trim().parse().ok())
}

/// A `key value` line from meminfo / memory.stat / cpu.stat style files.
fn field(text: &str, key: &str) -> Option<u64> {
    text.lines().find_map(|l| {
        let mut it = l.split_whitespace();
        (it.next()?.trim_end_matches(':') == key).then(|| it.next()?.parse().ok()).flatten()
    })
}

/// `some avg10=` of a PSI file.
fn psi(text: &str) -> Option<f64> {
    let line = text.lines().find(|l| l.starts_with("some "))?;
    line.split_whitespace().find_map(|f| f.strip_prefix("avg10=")).and_then(|v| v.parse().ok())
}

fn host_total_mb() -> Option<u64> {
    read("/proc/meminfo").and_then(|t| field(&t, "MemTotal")).map(|kb| kb / 1024)
}

fn host_avail_mb() -> Option<u64> {
    read("/proc/meminfo").and_then(|t| field(&t, "MemAvailable")).map(|kb| kb / 1024)
}

fn read_limits() -> Limits {
    Limits { mem_mb: mem_limit_mb(), cpus: cpu_count(), pids_max: read_num("/sys/fs/cgroup/pids.max").or_else(|| read_num("/sys/fs/cgroup/pids/pids.max")) }
}

/// The cgroup memory limit (v2 `memory.max` / `memory.high`, else v1), else
/// the host's RAM. An unlimited v1 cgroup reports a huge number, which the
/// host's RAM caps.
fn mem_limit_mb() -> Option<u64> {
    let host = host_total_mb();
    let v2 = [read_num("/sys/fs/cgroup/memory.max"), read_num("/sys/fs/cgroup/memory.high")].into_iter().flatten().min();
    let cg = v2.or_else(|| read_num("/sys/fs/cgroup/memory/memory.limit_in_bytes")).map(|b| b / (1024 * 1024));
    match (cg, host) {
        (Some(c), Some(h)) => Some(c.min(h)),
        (c, h) => c.or(h),
    }
}

/// CPUs the daemon may use: the affinity mask (`--cpuset-cpus`), capped by
/// the CFS quota (`--cpus`, cgroup v2 then v1), rounded up.
fn cpu_count() -> usize {
    // Not available_parallelism(): it already applies the quota, rounded down
    // (`--cpus 1.75` gives 1), which would cap the rounded-up quota below.
    let host = affinity_cpus().unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1));
    let quota = |q: f64, p: f64| (q > 0.0 && p > 0.0).then(|| (q / p).ceil() as usize);
    // v2: "<quota> <period>" or "max <period>".
    let v2 = read("/sys/fs/cgroup/cpu.max").and_then(|v| {
        let mut it = v.split_whitespace();
        let q: f64 = it.next()?.parse().ok()?;
        let p: f64 = it.next()?.parse().ok()?;
        quota(q, p)
    });
    // v1: quota is -1 when unlimited.
    let v1 = || {
        let q: f64 = read("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")?.trim().parse().ok()?;
        let p: f64 = read("/sys/fs/cgroup/cpu/cpu.cfs_period_us")?.trim().parse().ok()?;
        quota(q, p)
    };
    v2.or_else(v1).map(|q| q.clamp(1, host)).unwrap_or(host)
}

/// CPUs in the affinity mask (`--cpuset-cpus`), ignoring any quota.
#[cfg(target_os = "linux")]
fn affinity_cpus() -> Option<usize> {
    // SAFETY: cpu_set_t is plain data; sched_getaffinity fills it for this thread.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
            return None;
        }
        Some(libc::CPU_COUNT(&set) as usize).filter(|&n| n > 0)
    }
}

#[cfg(not(target_os = "linux"))]
fn affinity_cpus() -> Option<usize> {
    None
}

/// Total time the CPU quota held the cgroup back, in µs.
fn throttled_usec() -> Option<u64> {
    read("/sys/fs/cgroup/cpu.stat")
        .and_then(|t| field(&t, "throttled_usec"))
        .or_else(|| read("/sys/fs/cgroup/cpu/cpu.stat").and_then(|t| field(&t, "throttled_time")).map(|ns| ns / 1000))
}

/// Memory left before the cgroup limit (reclaimable page cache counts as
/// free), capped by what the host has available.
fn avail_mb(limit_mb: Option<u64>) -> Option<u64> {
    let in_cgroup = (|| {
        let limit = limit_mb?;
        if let Some(cur) = read_num("/sys/fs/cgroup/memory.current") {
            let inactive = read("/sys/fs/cgroup/memory.stat").and_then(|t| field(&t, "inactive_file")).unwrap_or(0);
            return Some(limit.saturating_sub(cur.saturating_sub(inactive) / (1024 * 1024)));
        }
        let cur = read_num("/sys/fs/cgroup/memory/memory.usage_in_bytes")?;
        let inactive = read("/sys/fs/cgroup/memory/memory.stat").and_then(|t| field(&t, "total_inactive_file")).unwrap_or(0);
        Some(limit.saturating_sub(cur.saturating_sub(inactive) / (1024 * 1024)))
    })();
    match (in_cgroup, host_avail_mb()) {
        (Some(c), Some(h)) => Some(c.min(h)),
        (c, h) => c.or(h),
    }
}

fn read_sample(limit_mb: Option<u64>) -> Sample {
    // The container's own pressure when the cgroup exposes it, else the host's.
    let pressure = |name: &str| read(&format!("/sys/fs/cgroup/{name}.pressure")).or_else(|| read(&format!("/proc/pressure/{name}"))).and_then(|t| psi(&t));
    Sample {
        avail_mb: avail_mb(limit_mb),
        mem_pressure: pressure("memory"),
        cpu_pressure: pressure("cpu"),
        pids: read_num("/sys/fs/cgroup/pids.current").or_else(|| read_num("/sys/fs/cgroup/pids/pids.current")),
        throttle: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(mem_mb: Option<u64>, cpus: usize, pids_max: Option<u64>) -> Limits {
        Limits { mem_mb, cpus, pids_max }
    }

    fn cap(ceiling: usize) -> Capacity {
        let now = Instant::now();
        Capacity { auto: true, agent_mb: 300, max: 8, stable: Duration::from_secs(30), limits: limits(Some(4096), 4, None), limits_at: now, ceiling, limit: 1, calm_since: None, last_change: now, last: Sample::default(), strained: false, critical: false, throttled: None, throttle_avg: None, last_decision: None }
    }

    fn calm() -> Sample {
        Sample { avail_mb: Some(2000), mem_pressure: Some(0.0), cpu_pressure: Some(5.0), pids: None, throttle: Some(0.0) }
    }

    #[test]
    fn parses_mode() {
        assert_eq!(Mode::parse(None), Mode::Auto);
        assert_eq!(Mode::parse(Some("auto")), Mode::Auto);
        assert_eq!(Mode::parse(Some(" AUTO ")), Mode::Auto);
        assert_eq!(Mode::parse(Some("3")), Mode::Fixed(3));
        assert_eq!(Mode::parse(Some("0")), Mode::Fixed(1));
    }

    #[test]
    fn ceiling_follows_memory_cpu_pids_and_max() {
        assert_eq!(ceiling(&limits(Some(640), 2, None), 300, 8), 1);
        assert_eq!(ceiling(&limits(Some(4096), 4, None), 300, 8), 8);
        assert_eq!(ceiling(&limits(Some(4096), 2, None), 300, 8), 4);
        assert_eq!(ceiling(&limits(Some(65536), 32, None), 300, 8), 8);
        assert_eq!(ceiling(&limits(Some(200), 4, None), 300, 8), 1);
        assert_eq!(ceiling(&limits(None, 4, None), 300, 8), 1);
        // --pids-limit 256: (256 - 32) / 64 = 3 agents.
        assert_eq!(ceiling(&limits(Some(65536), 32, Some(256)), 300, 8), 3);
        assert_eq!(ceiling(&limits(Some(65536), 32, Some(40)), 300, 8), 1);
    }

    #[test]
    fn grows_only_after_a_stable_window_with_work_queued() {
        let mut c = cap(4);
        let t0 = Instant::now();
        assert_eq!(c.apply(calm(), 1, 2, t0), Change::None);
        assert_eq!(c.apply(calm(), 1, 2, t0 + Duration::from_secs(29)), Change::None);
        assert_eq!(c.apply(calm(), 1, 2, t0 + Duration::from_secs(30)), Change::Grew);
        assert_eq!(c.limit(), 2);
        // Needs a new stable window before the next step.
        assert_eq!(c.apply(calm(), 2, 1, t0 + Duration::from_secs(31)), Change::None);
        assert_eq!(c.apply(calm(), 2, 1, t0 + Duration::from_secs(61)), Change::Grew);
        assert_eq!(c.limit(), 3);
    }

    #[test]
    fn does_not_grow_without_a_queue_or_past_the_ceiling() {
        let mut c = cap(1);
        let t0 = Instant::now();
        c.apply(calm(), 1, 3, t0);
        assert_eq!(c.apply(calm(), 1, 3, t0 + Duration::from_secs(60)), Change::None);
        let mut c = cap(4);
        c.apply(calm(), 1, 0, t0);
        assert_eq!(c.apply(calm(), 1, 0, t0 + Duration::from_secs(60)), Change::None);
        assert_eq!(c.limit(), 1);
    }

    #[test]
    fn pressure_resets_the_window_and_shrinks() {
        let mut c = cap(4);
        let t0 = Instant::now();
        c.limit = 3;
        c.last_change = t0;
        let hot = Sample { mem_pressure: Some(25.0), ..calm() };
        assert_eq!(c.apply(hot, 3, 1, t0 + Duration::from_secs(5)), Change::None);
        assert_eq!(c.limit(), 3, "waits SHRINK_GAP after the last change");
        assert!(c.strained());
        assert_eq!(c.apply(hot, 3, 1, t0 + Duration::from_secs(10)), Change::Shrank);
        assert_eq!(c.limit(), 2);
        c.apply(hot, 3, 1, t0 + Duration::from_secs(12));
        assert_eq!(c.limit(), 2);
        c.apply(hot, 3, 1, t0 + Duration::from_secs(20));
        c.apply(hot, 3, 1, t0 + Duration::from_secs(30));
        assert_eq!(c.limit(), 1, "never below one");
        let low = Sample { avail_mb: Some(70), ..calm() };
        c.limit = 2;
        c.apply(low, 2, 1, t0 + Duration::from_secs(60));
        assert_eq!(c.limit(), 1, "little free memory counts as pressure");
        assert!(!c.apply(calm(), 1, 0, t0 + Duration::from_secs(61)).eq(&Change::Critical));
        assert!(!c.strained(), "a calm reading clears the strain");
    }

    #[test]
    fn cpu_throttling_counts_as_pressure() {
        let mut c = cap(4);
        let t0 = Instant::now();
        c.limit = 2;
        let throttled = Sample { throttle: Some(0.7), ..calm() };
        assert_eq!(c.apply(throttled, 2, 1, t0 + Duration::from_secs(20)), Change::Shrank);
        // Some throttling blocks growth without shrinking.
        let some = Sample { throttle: Some(0.3), ..calm() };
        c.apply(some, 1, 2, t0 + Duration::from_secs(21));
        assert_eq!(c.apply(some, 1, 2, t0 + Duration::from_secs(90)), Change::None);
        assert_eq!(c.limit(), 1);
    }

    #[test]
    fn running_out_is_critical_and_drops_at_once() {
        let mut c = cap(4);
        let t0 = Instant::now();
        c.limit = 3;
        c.last_change = t0;
        let out = Sample { avail_mb: Some(20), ..calm() };
        assert_eq!(c.apply(out, 3, 0, t0 + Duration::from_secs(1)), Change::Critical);
        assert_eq!(c.limit(), 2, "no SHRINK_GAP when critical");
        assert_eq!(c.apply(out, 3, 0, t0 + Duration::from_secs(2)), Change::Critical);
        assert_eq!(c.limit(), 1);
        assert_eq!(c.apply(out, 3, 0, t0 + Duration::from_secs(3)), Change::Critical, "still reported at one");
        let stalls = Sample { mem_pressure: Some(55.0), ..calm() };
        assert_eq!(c.apply(stalls, 1, 0, t0 + Duration::from_secs(4)), Change::Critical);
    }

    #[test]
    fn pids_limit_blocks_growth_and_is_critical_when_nearly_used() {
        let mut c = cap(4);
        c.limits.pids_max = Some(256);
        let t0 = Instant::now();
        let busy = Sample { pids: Some(210), ..calm() };
        c.apply(busy, 1, 2, t0);
        assert_eq!(c.apply(busy, 1, 2, t0 + Duration::from_secs(60)), Change::None, "no room for another agent's processes");
        let full = Sample { pids: Some(240), ..calm() };
        assert_eq!(c.apply(full, 1, 0, t0 + Duration::from_secs(61)), Change::Critical);
    }

    #[test]
    fn lower_container_limits_pull_the_limit_down() {
        let mut c = cap(8);
        let t0 = Instant::now();
        c.limit = 5;
        assert_eq!(c.set_limits(limits(Some(4096), 4, None), t0), Change::None, "same limits");
        assert_eq!(c.set_limits(limits(Some(1024), 4, None), t0), Change::Shrank);
        assert_eq!((c.ceiling, c.limit()), (3, 3));
        assert_eq!(c.set_limits(limits(Some(8192), 4, None), t0), Change::None, "raising only lifts the ceiling");
        assert_eq!((c.ceiling, c.limit()), (8, 3));
    }

    #[test]
    fn throttling_is_smoothed() {
        let tick = Duration::from_secs(2);
        // One fully throttled tick after a quiet stretch stays below THROTTLE_HIGH.
        let after_spike = smooth(Some(0.0), 1.0, tick);
        assert!(after_spike > 0.1 && after_spike < THROTTLE_HIGH, "{after_spike}");
        // Sustained throttling gets there within a few ticks.
        let sustained = (0..5).fold(Some(0.0), |avg, _| Some(smooth(avg, 1.0, tick))).unwrap();
        assert!(sustained >= THROTTLE_HIGH, "{sustained}");
        assert_eq!(smooth(None, 0.4, tick), 0.4);
    }

    #[test]
    fn fixed_caps_are_watched_but_never_moved() {
        let mut c = cap(4);
        c.auto = false;
        let t0 = Instant::now();
        c.apply(calm(), 1, 2, t0);
        assert_eq!(c.apply(calm(), 1, 2, t0 + Duration::from_secs(60)), Change::None);
        let out = Sample { avail_mb: Some(20), ..calm() };
        assert_eq!(c.apply(out, 1, 0, t0 + Duration::from_secs(61)), Change::Critical);
        assert!(c.strained());
        assert_eq!(c.limit(), 1);
    }

    #[test]
    fn needs_room_for_another_agent_to_grow() {
        let mut c = cap(4);
        let t0 = Instant::now();
        let tight = Sample { avail_mb: Some(200), ..calm() };
        c.apply(tight, 1, 2, t0);
        assert_eq!(c.apply(tight, 1, 2, t0 + Duration::from_secs(60)), Change::None);
        let unknown = Sample { avail_mb: None, ..calm() };
        c.apply(unknown, 1, 2, t0);
        assert_eq!(c.apply(unknown, 1, 2, t0 + Duration::from_secs(60)), Change::None);
    }

    #[test]
    fn parses_proc_files() {
        assert_eq!(psi("some avg10=2.34 avg60=1.75 avg300=0.46 total=1\nfull avg10=2.22 avg60=1 avg300=0 total=1\n"), Some(2.34));
        assert_eq!(field("MemTotal:        990208 kB\nMemAvailable:   631808 kB\n", "MemAvailable"), Some(631808));
        assert_eq!(field("anon 1\ninactive_file 4096\n", "inactive_file"), Some(4096));
        assert_eq!(field("nr_throttled 38\nthrottled_usec 1934124\n", "throttled_usec"), Some(1_934_124));
    }
}
