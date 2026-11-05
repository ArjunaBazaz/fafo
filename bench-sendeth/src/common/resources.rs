//! Apply affinity before constructing any executors/database worker threads.
use super::cli::BenchmarkCli;
use exepipe::resources::ExecutionResources;
use serde_json::{json, Value};

pub fn configure(args: &BenchmarkCli) -> Vec<usize> {
    ExecutionResources::new(args.scheduler_threads).expect("invalid shared scheduler budget");
    assert!(args.occ_window > 0, "OCC window must be positive");
    #[cfg(target_os = "linux")]
    unsafe {
        let mut allowed: libc::cpu_set_t = std::mem::zeroed();
        assert_eq!(
            libc::sched_getaffinity(0, std::mem::size_of_val(&allowed), &mut allowed),
            0,
            "cannot read CPU affinity: {}",
            std::io::Error::last_os_error()
        );
        let mut cpus: Vec<_> = (0..libc::CPU_SETSIZE as usize)
            .filter(|cpu| libc::CPU_ISSET(*cpu, &allowed))
            .collect();
        if let Some(count) = args.cpu_cores {
            assert!(
                count > 0 && count <= cpus.len(),
                "cpu-cores must be between 1 and {}",
                cpus.len()
            );
            cpus.truncate(count);
            libc::CPU_ZERO(&mut allowed);
            for cpu in &cpus {
                libc::CPU_SET(*cpu, &mut allowed);
            }
            assert_eq!(
                libc::sched_setaffinity(0, std::mem::size_of_val(&allowed), &allowed),
                0,
                "cannot set CPU affinity: {}",
                std::io::Error::last_os_error()
            );
        }
        cpus
    }
    #[cfg(not(target_os = "linux"))]
    {
        assert!(
            args.cpu_cores.is_none(),
            "cpu-cores requires Linux affinity support"
        );
        Vec::new()
    }
}

pub fn report(args: &BenchmarkCli, cpus: &[usize]) -> Value {
    use exepipe::resources::*;
    let budget = ExecutionResources::new(args.scheduler_threads).unwrap();
    let database = qmdb::config::Config::from_dir("");
    json!({
        "scheduler_threads_including_coordinator": budget.scheduler_threads(),
        "fafo_execution_workers": budget.fafo_execution_workers(),
        "fafo_warmup_workers": FAFO_WARMUP_THREADS,
        "fafo_simulation_workers": FAFO_SIMULATION_THREADS,
        "fafo_scanners": exepipe::dispatcher::def::SCAN_COUNT,
        "fafo_simulator_shards": exepipe::simulator::simulator::SIM_SHARD_COUNT,
        "fafo_framer_threads": FRAMER_THREADS,
        "occ_execution_workers": budget.occ_execution_workers(),
        "coordinator_threads": COORDINATOR_THREADS,
        "allowed_logical_cpu_ids": cpus,
        "persistence_transactions_per_task": exepipe::dispatcher::def::TX_IN_TASK,
        "occ_window": args.occ_window,
        "shared_qmdb": {
            "prefetcher_threads": database.prefetcher_thread_count,
            "uring_count": database.uring_count,
            "uring_size": database.uring_size,
            "write_buffer_bytes": database.wrbuf_size,
            "file_segment_bytes": database.file_segment_size,
            "bypass_prefetcher": true,
        },
        "shared_codedb": {
            "uring_workers": codedb::def::URING_COUNT,
            "write_buffer_bytes": 8 * 1024 * 1024,
            "file_segment_bytes": 256 * 1024 * 1024,
        },
        "scope": "scheduler budget excludes identical QMDB/CodeDB and benchmark threads; FAFO auxiliary count is peak per-block",
    })
}

pub fn record_process_usage() {
    #[cfg(target_os = "linux")]
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut usage) == 0 {
            metrics::gauge!("bench-sendeth.process.peak_rss_bytes")
                .set(usage.ru_maxrss as f64 * 1024.0);
            let seconds =
                |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1_000_000.0;
            metrics::gauge!("bench-sendeth.process.cpu_seconds")
                .set(seconds(usage.ru_utime) + seconds(usage.ru_stime));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn cli_defaults_and_executor_selection_share_budget() {
        let fafo = BenchmarkCli::try_parse_from(["bench", "--workload", "eth", "init-db"]).unwrap();
        let occ = BenchmarkCli::try_parse_from([
            "bench",
            "--workload",
            "eth",
            "replay-workload",
            "--executor",
            "occ",
        ])
        .unwrap();
        assert_eq!(fafo.executor, super::super::cli::ExecutorKind::Fafo);
        assert_eq!(occ.executor, super::super::cli::ExecutorKind::Occ);
        assert_eq!(fafo.scheduler_threads, occ.scheduler_threads);
        assert_eq!(fafo.scheduler_threads, 102);
        assert!(BenchmarkCli::try_parse_from([
            "bench",
            "--workload",
            "eth",
            "--executor",
            "unknown",
            "init-db"
        ])
        .is_err());
    }
}
