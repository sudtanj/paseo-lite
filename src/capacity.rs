//! How many agent turns may run at once.
//!
//! With `auto` (the default) the limit adapts to the machine: it starts at
//! one, grows by one while turns are queued and the machine has stayed calm
//! for `stable` (low memory/CPU pressure, room for another agent), and shrinks
//! by one when memory or CPU pressure builds up. It never exceeds the ceiling:
//! what the memory limit (the container's cgroup, else the host) fits at
//! `agent_mb` per agent, the CPU count times two, and `max`. Running turns are
//! never stopped; a lower limit only holds back the next ones, which wait in
//! the queue as before.

use serde_json::{json, Value};
use std::time::{Duration, Instant};

/// Memory kept for the daemon and the commands agents run, outside the
/// per-agent budget.
const RESERVE_MB: u64 = 96;
/// A step down waits at least this long after the last change, so one
/// pressure spike can't drop the limit to one in a few ticks.
const SHRINK_GAP: Duration = Duration::from_secs(10);

/// Pressure (PSI `some avg10`, percent) at or above which the limit shrinks.
const MEM_PRESSURE_HIGH: f64 = 10.0;
const CPU_PRESSURE_HIGH: f64 = 80.0;
/// Pressure below which the machine counts as calm enough to grow.
const MEM_PRESSURE_CALM: f64 = 2.0;
const CPU_PRESSURE_CALM: f64 = 40.0;

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

/// One reading of the machine. `None` = not available on this system.
#[derive(Clone, Copy, Debug, Default)]
pub struct Sample {
    /// Memory that can still be used before the limit, in MB.
    pub avail_mb: Option<u64>,
    /// PSI `some avg10` for memory and CPU, percent.
    pub mem_pressure: Option<f64>,
    pub cpu_pressure: Option<f64>,
}

pub struct Capacity {
    agent_mb: u64,
    stable: Duration,
    ceiling: usize,
    limit: usize,
    calm_since: Option<Instant>,
    last_change: Instant,
    last: Sample,
    mem_limit_mb: Option<u64>,
    cpus: usize,
}

impl Capacity {
    pub fn new(agent_mb: u64, max: usize, stable: Duration) -> Capacity {
        let mem_limit_mb = mem_limit_mb();
        let cpus = cpu_count();
        let ceiling = ceiling(mem_limit_mb, cpus, agent_mb, max);
        crate::log!(
            "capacity: auto, up to {ceiling} agent(s) at once (memory limit {}, {cpus} CPU, {agent_mb} MB per agent)",
            mem_limit_mb.map(|m| format!("{m} MB")).unwrap_or_else(|| "unknown".into())
        );
        Capacity { agent_mb, stable, ceiling, limit: 1, calm_since: None, last_change: Instant::now(), last: Sample::default(), mem_limit_mb, cpus }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Read the machine and adjust the limit. True when it went up, so the
    /// caller can start queued turns.
    pub fn update(&mut self, running: usize, queued: usize) -> bool {
        let sample = read_sample(self.mem_limit_mb);
        self.apply(sample, running, queued, Instant::now())
    }

    fn apply(&mut self, s: Sample, running: usize, queued: usize, now: Instant) -> bool {
        self.last = s;
        let over = |v: Option<f64>, max: f64| v.map(|v| v >= max).unwrap_or(false);
        let under = |v: Option<f64>, max: f64| v.map(|v| v < max).unwrap_or(true);
        let strained = over(s.mem_pressure, MEM_PRESSURE_HIGH) || over(s.cpu_pressure, CPU_PRESSURE_HIGH) || s.avail_mb.map(|a| a < self.agent_mb / 4).unwrap_or(false);
        if strained {
            self.calm_since = None;
            if self.limit > 1 && now.duration_since(self.last_change) >= SHRINK_GAP {
                self.limit -= 1;
                self.last_change = now;
                crate::warn!("capacity: machine under pressure, {} agent(s) at once now ({})", self.limit, describe(&s));
            }
            return false;
        }
        // Without a memory reading the room check can't pass: stay put.
        let calm = under(s.mem_pressure, MEM_PRESSURE_CALM) && under(s.cpu_pressure, CPU_PRESSURE_CALM) && s.avail_mb.map(|a| a >= self.agent_mb).unwrap_or(false);
        if !calm {
            self.calm_since = None;
            return false;
        }
        let since = *self.calm_since.get_or_insert(now);
        // Grow only when the current limit is the bottleneck.
        if queued == 0 || running < self.limit || self.limit >= self.ceiling || now.duration_since(since) < self.stable {
            return false;
        }
        self.limit += 1;
        self.last_change = now;
        // The new agent's load has to prove calm again before the next step.
        self.calm_since = None;
        crate::log!("capacity: machine stable, {} agent(s) at once now ({})", self.limit, describe(&s));
        true
    }

    pub fn status(&self) -> Value {
        json!({
            "limit": self.limit,
            "ceiling": self.ceiling,
            "agentMb": self.agent_mb,
            "memLimitMb": self.mem_limit_mb,
            "cpus": self.cpus,
            "availMb": self.last.avail_mb,
            "memPressure": self.last.mem_pressure,
            "cpuPressure": self.last.cpu_pressure,
        })
    }
}

fn describe(s: &Sample) -> String {
    let pct = |v: Option<f64>| v.map(|v| format!("{v:.1}%")).unwrap_or_else(|| "n/a".into());
    format!(
        "free {}, memory pressure {}, CPU pressure {}",
        s.avail_mb.map(|a| format!("{a} MB")).unwrap_or_else(|| "n/a".into()),
        pct(s.mem_pressure),
        pct(s.cpu_pressure)
    )
}

fn ceiling(mem_limit_mb: Option<u64>, cpus: usize, agent_mb: u64, max: usize) -> usize {
    let by_mem = mem_limit_mb.map(|m| (m.saturating_sub(RESERVE_MB) / agent_mb.max(1)) as usize).unwrap_or(1);
    by_mem.min(cpus.max(1) * 2).min(max).max(1)
}

fn read(path: &str) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

/// A `key value` line from meminfo / memory.stat style files.
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

/// The cgroup memory limit (v2, then v1), else the host's RAM.
fn mem_limit_mb() -> Option<u64> {
    let host = host_total_mb();
    let cg = read("/sys/fs/cgroup/memory.max")
        .or_else(|| read("/sys/fs/cgroup/memory/memory.limit_in_bytes"))
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(|b| b / (1024 * 1024));
    match (cg, host) {
        (Some(c), Some(h)) => Some(c.min(h)),
        (c, h) => c.or(h),
    }
}

fn cpu_count() -> usize {
    let host = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    // cgroup v2 quota: "<quota> <period>" or "max <period>".
    let quota = read("/sys/fs/cgroup/cpu.max").and_then(|v| {
        let mut it = v.split_whitespace();
        let q: f64 = it.next()?.parse().ok()?;
        let p: f64 = it.next()?.parse().ok()?;
        (p > 0.0).then(|| (q / p).ceil() as usize)
    });
    quota.map(|q| q.clamp(1, host)).unwrap_or(host)
}

/// Memory left before the cgroup limit (reclaimable page cache counts as
/// free), capped by what the host has available.
fn avail_mb(limit_mb: Option<u64>) -> Option<u64> {
    let in_cgroup = (|| {
        let limit = limit_mb?;
        if let Some(cur) = read("/sys/fs/cgroup/memory.current").and_then(|v| v.trim().parse::<u64>().ok()) {
            let inactive = read("/sys/fs/cgroup/memory.stat").and_then(|t| field(&t, "inactive_file")).unwrap_or(0);
            return Some(limit.saturating_sub(cur.saturating_sub(inactive) / (1024 * 1024)));
        }
        let cur = read("/sys/fs/cgroup/memory/memory.usage_in_bytes").and_then(|v| v.trim().parse::<u64>().ok())?;
        let inactive = read("/sys/fs/cgroup/memory/memory.stat").and_then(|t| field(&t, "total_inactive_file")).unwrap_or(0);
        Some(limit.saturating_sub(cur.saturating_sub(inactive) / (1024 * 1024)))
    })();
    match (in_cgroup, host_avail_mb()) {
        (Some(c), Some(h)) => Some(c.min(h)),
        (c, h) => c.or(h),
    }
}

fn read_sample(limit_mb: Option<u64>) -> Sample {
    let pressure = |name: &str| read(&format!("/sys/fs/cgroup/{name}.pressure")).or_else(|| read(&format!("/proc/pressure/{name}"))).and_then(|t| psi(&t));
    Sample { avail_mb: avail_mb(limit_mb), mem_pressure: pressure("memory"), cpu_pressure: pressure("cpu") }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cap(ceiling: usize) -> Capacity {
        Capacity { agent_mb: 300, stable: Duration::from_secs(30), ceiling, limit: 1, calm_since: None, last_change: Instant::now(), last: Sample::default(), mem_limit_mb: None, cpus: 4 }
    }

    fn calm() -> Sample {
        Sample { avail_mb: Some(2000), mem_pressure: Some(0.0), cpu_pressure: Some(5.0) }
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
    fn ceiling_follows_memory_cpu_and_max() {
        assert_eq!(ceiling(Some(640), 2, 300, 8), 1);
        assert_eq!(ceiling(Some(4096), 4, 300, 8), 8);
        assert_eq!(ceiling(Some(4096), 2, 300, 8), 4);
        assert_eq!(ceiling(Some(65536), 32, 300, 8), 8);
        assert_eq!(ceiling(Some(200), 4, 300, 8), 1);
        assert_eq!(ceiling(None, 4, 300, 8), 1);
    }

    #[test]
    fn grows_only_after_a_stable_window_with_work_queued() {
        let mut c = cap(4);
        let t0 = Instant::now();
        assert!(!c.apply(calm(), 1, 2, t0));
        assert!(!c.apply(calm(), 1, 2, t0 + Duration::from_secs(29)));
        assert!(c.apply(calm(), 1, 2, t0 + Duration::from_secs(30)));
        assert_eq!(c.limit(), 2);
        // Needs a new stable window before the next step.
        assert!(!c.apply(calm(), 2, 1, t0 + Duration::from_secs(31)));
        assert!(c.apply(calm(), 2, 1, t0 + Duration::from_secs(61)));
        assert_eq!(c.limit(), 3);
    }

    #[test]
    fn does_not_grow_without_a_queue_or_past_the_ceiling() {
        let mut c = cap(1);
        let t0 = Instant::now();
        c.apply(calm(), 1, 3, t0);
        assert!(!c.apply(calm(), 1, 3, t0 + Duration::from_secs(60)));
        let mut c = cap(4);
        c.apply(calm(), 1, 0, t0);
        assert!(!c.apply(calm(), 1, 0, t0 + Duration::from_secs(60)));
        assert_eq!(c.limit(), 1);
    }

    #[test]
    fn pressure_resets_the_window_and_shrinks() {
        let mut c = cap(4);
        let t0 = Instant::now();
        c.limit = 3;
        c.last_change = t0;
        let hot = Sample { mem_pressure: Some(25.0), ..calm() };
        assert!(!c.apply(hot, 3, 1, t0 + Duration::from_secs(5)));
        assert_eq!(c.limit(), 3, "waits SHRINK_GAP after the last change");
        c.apply(hot, 3, 1, t0 + Duration::from_secs(10));
        assert_eq!(c.limit(), 2);
        c.apply(hot, 3, 1, t0 + Duration::from_secs(12));
        assert_eq!(c.limit(), 2);
        c.apply(hot, 3, 1, t0 + Duration::from_secs(20));
        c.apply(hot, 3, 1, t0 + Duration::from_secs(30));
        assert_eq!(c.limit(), 1, "never below one");
        let low = Sample { avail_mb: Some(50), ..calm() };
        c.limit = 2;
        c.apply(low, 2, 1, t0 + Duration::from_secs(60));
        assert_eq!(c.limit(), 1, "little free memory counts as pressure");
    }

    #[test]
    fn needs_room_for_another_agent_to_grow() {
        let mut c = cap(4);
        let t0 = Instant::now();
        let tight = Sample { avail_mb: Some(200), ..calm() };
        c.apply(tight, 1, 2, t0);
        assert!(!c.apply(tight, 1, 2, t0 + Duration::from_secs(60)));
        let unknown = Sample { avail_mb: None, ..calm() };
        c.apply(unknown, 1, 2, t0);
        assert!(!c.apply(unknown, 1, 2, t0 + Duration::from_secs(60)));
    }

    #[test]
    fn parses_proc_files() {
        assert_eq!(psi("some avg10=2.34 avg60=1.75 avg300=0.46 total=1\nfull avg10=2.22 avg60=1 avg300=0 total=1\n"), Some(2.34));
        assert_eq!(field("MemTotal:        990208 kB\nMemAvailable:   631808 kB\n", "MemAvailable"), Some(631808));
        assert_eq!(field("anon 1\ninactive_file 4096\n", "inactive_file"), Some(4096));
    }
}
