use std::collections::HashMap;
use std::time::Duration;

use sysinfo::{Disks, Networks, System};
use tokio::time;

use crate::MetricsRegistry;

/// Start the background system metrics collector.
/// Runs a tokio task that samples system metrics every `interval` and records them
/// into the global MetricsRegistry.
pub fn start_system_collector(interval: Duration) {
    tokio::spawn(async move {
        let mut sys = System::new_all();
        let mut disks = Disks::new_with_refreshed_list();
        let mut networks = Networks::new_with_refreshed_list();
        let pid = sysinfo::get_current_pid().ok();

        loop {
            sys.refresh_all();
            disks.refresh(true);
            networks.refresh(true);

            let registry = MetricsRegistry::global();
            {
                let mut r = registry.lock().unwrap();

                // CPU metrics
                let sys_cpu = sys.global_cpu_usage() as f64;
                r.record_gauge("sys_cpu_usage", sys_cpu, HashMap::new());

                // Process metrics
                if let Some(pid) = pid {
                    if let Some(proc) = sys.process(pid) {
                        let process_cpu = proc.cpu_usage() as f64;
                        r.record_gauge("process_cpu_usage", process_cpu, HashMap::new());

                        // Memory (sysinfo 0.33 returns bytes)
                        r.record_gauge("process_mem_rss_bytes", proc.memory() as f64, HashMap::new());
                        r.record_gauge("process_mem_virtual_bytes", proc.virtual_memory() as f64, HashMap::new());
                    }
                }

                // System memory
                r.record_gauge("sys_mem_total_bytes", sys.total_memory() as f64, HashMap::new());
                r.record_gauge("sys_mem_used_bytes", sys.used_memory() as f64, HashMap::new());
                r.record_gauge("sys_mem_available_bytes", sys.available_memory() as f64, HashMap::new());

                // Disk
                let mut disk_total: f64 = 0.0;
                let mut disk_used: f64 = 0.0;
                let mut disk_available: f64 = 0.0;
                for disk in &disks {
                    disk_total += disk.total_space() as f64;
                    disk_used += (disk.total_space() - disk.available_space()) as f64;
                    disk_available += disk.available_space() as f64;
                }
                r.record_gauge("disk_total_bytes", disk_total, HashMap::new());
                r.record_gauge("disk_used_bytes", disk_used, HashMap::new());
                r.record_gauge("disk_available_bytes", disk_available, HashMap::new());

                // Network (cumulative bytes)
                let mut net_sent: f64 = 0.0;
                let mut net_recv: f64 = 0.0;
                for (_name, data) in networks.iter() {
                    net_sent += data.transmitted() as f64;
                    net_recv += data.received() as f64;
                }
                r.record_gauge("net_bytes_sent", net_sent, HashMap::new());
                r.record_gauge("net_bytes_recv", net_recv, HashMap::new());

                // Uptime
                let uptime = (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64
                    - r.overview().started_at) as f64
                    / 1000.0;
                r.record_gauge("process_uptime_secs", uptime, HashMap::new());
            }

            time::sleep(interval).await;
        }
    });
}
