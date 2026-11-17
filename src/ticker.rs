//! Simulated prices: a geometric random walk per symbol. Each step multiplies
//! the price by `exp(sigma * z)` with `z` standard normal, so prices stay
//! positive and moves scale with the price. Results are snapped to the tick size.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;

use crate::hub::Hub;

#[derive(Debug, Clone, PartialEq)]
pub struct SymbolSpec {
    pub symbol: &'static str,
    pub start_price: f64,
    pub tick_size: f64,
    /// std dev of the log return per step
    pub volatility: f64,
}

pub fn default_symbols() -> Vec<SymbolSpec> {
    vec![
        SymbolSpec {
            symbol: "BTC-USD",
            start_price: 67_000.0,
            tick_size: 0.01,
            volatility: 0.0006,
        },
        SymbolSpec {
            symbol: "ETH-USD",
            start_price: 3_400.0,
            tick_size: 0.01,
            volatility: 0.0008,
        },
        SymbolSpec {
            symbol: "SOL-USD",
            start_price: 160.0,
            tick_size: 0.001,
            volatility: 0.0012,
        },
    ]
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Tick {
    pub symbol: &'static str,
    pub price: f64,
    /// feed-wide, starts at 1
    pub seq: u64,
    /// unix millis
    pub ts: u64,
}

#[derive(Debug)]
pub struct TickGenerator {
    specs: Vec<SymbolSpec>,
    prices: Vec<f64>,
    rng: StdRng,
    seq: u64,
}

impl TickGenerator {
    pub fn new(specs: Vec<SymbolSpec>) -> Self {
        Self::with_rng(specs, StdRng::from_os_rng())
    }

    pub fn seeded(specs: Vec<SymbolSpec>, seed: u64) -> Self {
        Self::with_rng(specs, StdRng::seed_from_u64(seed))
    }

    fn with_rng(specs: Vec<SymbolSpec>, rng: StdRng) -> Self {
        assert!(!specs.is_empty(), "need at least one symbol");
        for s in &specs {
            assert!(s.start_price > 0.0, "{}: start price must be > 0", s.symbol);
            assert!(s.tick_size > 0.0, "{}: tick size must be > 0", s.symbol);
            assert!(s.volatility >= 0.0, "{}: volatility must be >= 0", s.symbol);
        }
        let prices = specs.iter().map(|s| s.start_price).collect();
        Self {
            specs,
            prices,
            rng,
            seq: 0,
        }
    }

    pub fn symbols(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.specs.iter().map(|s| s.symbol)
    }

    /// Moves every symbol one step. Ticks come back in symbol order.
    pub fn step(&mut self, ts: u64) -> Vec<Tick> {
        let mut out = Vec::with_capacity(self.specs.len());
        for (i, spec) in self.specs.iter().enumerate() {
            let z = standard_normal(&mut self.rng);
            let next = self.prices[i] * (spec.volatility * z).exp();
            // don't let rounding take a cheap symbol to zero
            let snapped = round_to(next, spec.tick_size).max(spec.tick_size);
            self.prices[i] = snapped;
            self.seq += 1;
            out.push(Tick {
                symbol: spec.symbol,
                price: snapped,
                seq: self.seq,
                ts,
            });
        }
        out
    }
}

// Box-Muller; saves pulling in rand_distr for one function
fn standard_normal(rng: &mut impl Rng) -> f64 {
    // 1 - u is in (0, 1], so ln() never sees zero
    let u1: f64 = 1.0 - rng.random::<f64>();
    let u2: f64 = rng.random::<f64>();
    (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
}

fn round_to(value: f64, step: f64) -> f64 {
    // second rounding strips float noise like 0.30000000000000004 from the JSON
    let steps = (value / step).round();
    let decimals = (-step.log10()).ceil().max(0.0) as i32;
    let factor = 10f64.powi(decimals);
    (steps * step * factor).round() / factor
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub async fn run(hub: Hub, mut generator: TickGenerator, interval: Duration) {
    let mut timer = tokio::time::interval(interval);
    // after a stall, carry on rather than firing a burst of catch-up ticks
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        timer.tick().await;
        for tick in generator.step(now_ms()) {
            hub.publish(&tick);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gen(seed: u64) -> TickGenerator {
        TickGenerator::seeded(default_symbols(), seed)
    }

    #[test]
    fn one_tick_per_symbol_per_step_in_order() {
        let mut g = gen(1);
        let ticks = g.step(42);
        let symbols: Vec<_> = ticks.iter().map(|t| t.symbol).collect();
        assert_eq!(symbols, vec!["BTC-USD", "ETH-USD", "SOL-USD"]);
        assert!(ticks.iter().all(|t| t.ts == 42));
    }

    #[test]
    fn sequence_numbers_are_contiguous_across_steps() {
        let mut g = gen(2);
        let seqs: Vec<u64> = (0..10).flat_map(|_| g.step(0)).map(|t| t.seq).collect();
        let expected: Vec<u64> = (1..=30).collect();
        assert_eq!(seqs, expected);
    }

    #[test]
    fn same_seed_gives_same_walk() {
        let (mut a, mut b) = (gen(7), gen(7));
        for _ in 0..500 {
            assert_eq!(a.step(0), b.step(0));
        }
    }

    #[test]
    fn different_seeds_diverge() {
        let (mut a, mut b) = (gen(7), gen(8));
        let walk_a: Vec<_> = (0..20).flat_map(|_| a.step(0)).collect();
        let walk_b: Vec<_> = (0..20).flat_map(|_| b.step(0)).collect();
        assert_ne!(walk_a, walk_b);
    }

    #[test]
    fn prices_stay_positive_and_on_the_tick_grid() {
        let specs = default_symbols();
        let mut g = gen(3);
        for _ in 0..10_000 {
            for (tick, spec) in g.step(0).iter().zip(&specs) {
                assert!(tick.price > 0.0, "{tick:?}");
                let steps = tick.price / spec.tick_size;
                assert!(
                    (steps - steps.round()).abs() < 1e-6,
                    "{} not a multiple of {}",
                    tick.price,
                    spec.tick_size
                );
            }
        }
    }

    #[test]
    fn steps_are_small_relative_moves() {
        // With sigma <= 0.0012, a single step beyond 1% would be a >8 sigma event.
        let mut g = gen(4);
        let mut last: Vec<f64> = default_symbols().iter().map(|s| s.start_price).collect();
        for _ in 0..10_000 {
            for (i, tick) in g.step(0).iter().enumerate() {
                let rel = (tick.price / last[i] - 1.0).abs();
                assert!(rel < 0.01, "{} jumped {:.4}%", tick.symbol, rel * 100.0);
                last[i] = tick.price;
            }
        }
    }

    #[test]
    fn walk_actually_moves() {
        let mut g = gen(5);
        let first = g.step(0);
        let later = (0..1_000).map(|_| g.step(0)).last().unwrap();
        for (a, b) in first.iter().zip(&later) {
            assert_ne!(a.price, b.price, "{} never moved", a.symbol);
        }
    }

    #[test]
    fn zero_volatility_is_flat() {
        let specs = vec![SymbolSpec {
            symbol: "FLAT",
            start_price: 100.0,
            tick_size: 0.01,
            volatility: 0.0,
        }];
        let mut g = TickGenerator::seeded(specs, 9);
        assert!((0..100).all(|_| g.step(0)[0].price == 100.0));
    }

    #[test]
    fn standard_normal_has_roughly_unit_moments() {
        let mut rng = StdRng::seed_from_u64(11);
        let n = 200_000;
        let xs: Vec<f64> = (0..n).map(|_| standard_normal(&mut rng)).collect();
        let mean = xs.iter().sum::<f64>() / n as f64;
        let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n as f64;
        assert!(mean.abs() < 0.01, "mean {mean}");
        assert!((var - 1.0).abs() < 0.02, "variance {var}");
    }

    #[test]
    fn rounding_snaps_to_grid_without_float_noise() {
        assert_eq!(round_to(0.30000000000000004, 0.01), 0.3);
        assert_eq!(round_to(160.12345, 0.001), 160.123);
        assert_eq!(round_to(67_000.006, 0.01), 67_000.01);
    }

    #[test]
    fn tick_serialises_to_expected_json() {
        let t = Tick {
            symbol: "ETH-USD",
            price: 3400.5,
            seq: 9,
            ts: 1_700_000_000_000,
        };
        let json = serde_json::to_value(&t).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"symbol": "ETH-USD", "price": 3400.5, "seq": 9, "ts": 1_700_000_000_000u64})
        );
    }
}
