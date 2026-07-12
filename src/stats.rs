use std::time::Duration;

pub struct TxResult {
    pub add_a_latency:   Duration,
    pub add_ptr_latency: Duration,
    pub del_ptr_latency: Duration,
    pub del_a_latency:   Duration,
}

impl TxResult {
    pub fn total(&self) -> Duration {
        self.add_a_latency
            + self.add_ptr_latency
            + self.del_ptr_latency
            + self.del_a_latency
    }
}

pub fn print_report(result: &TxResult) {
    println!("=== ddnsperf transaction result ===");
    println!("  Add A:      {:>8.3}ms", ms(result.add_a_latency));
    println!("  Add PTR:    {:>8.3}ms", ms(result.add_ptr_latency));
    println!("  Delete PTR: {:>8.3}ms", ms(result.del_ptr_latency));
    println!("  Delete A:   {:>8.3}ms", ms(result.del_a_latency));
    println!("  -----------");
    println!("  Total:      {:>8.3}ms", ms(result.total()));
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn total_sums_all_legs() {
        let r = TxResult {
            add_a_latency:   Duration::from_millis(1),
            add_ptr_latency: Duration::from_millis(2),
            del_ptr_latency: Duration::from_millis(3),
            del_a_latency:   Duration::from_millis(4),
        };
        assert_eq!(r.total(), Duration::from_millis(10));
    }
}
