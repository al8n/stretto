use atomic::Atomic;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

const MAXI64: i64 = i64::MAX;

/// `Histogram` stores the information needed to represent the sizes of the keys and values
/// as a histogram.
///
/// Histogram promises thread-safe.
#[derive(Debug)]
pub struct Histogram {
    bounds: Arc<Vec<Atomic<f64>>>,
    count: AtomicI64,
    count_per_bucket: Arc<Vec<AtomicI64>>,
    min: AtomicI64,
    max: AtomicI64,
    sum: AtomicI64,
}

impl Histogram {
    /// Returns a new instance of HistogramData with properly initialized fields.
    pub fn new(bounds: Vec<f64>) -> Self {
        let bounds = bounds.into_iter().map(Atomic::new).collect::<Vec<_>>();

        let mut cpb = init_cpb(bounds.len() + 1);
        cpb.shrink_to_fit();
        Histogram {
            bounds: Arc::new(bounds),
            count: AtomicI64::new(0),
            count_per_bucket: Arc::new(cpb),
            min: AtomicI64::new(MAXI64),
            max: AtomicI64::new(0),
            sum: AtomicI64::new(0),
        }
    }

    /// Returns HistogramData base on the min exponent and max exponent. The bounds are powers of two of the form
    /// [2^min_exponent, ..., 2^max_exponent].
    // pub fn from_exponents(min_exp: u32, max_exp: u32) -> Self {
    //     Self::new((min_exp..=max_exp).map(|idx| (1 << idx) as f64).collect())
    // }

    /// `update` changes the Min and Max fields if value is less than or greater than the current values.
    pub fn update(&self, val: i64) {
        let _ = self
            .max
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |max| {
                if val > max {
                    Some(val)
                } else {
                    None
                }
            });

        let _ = self
            .min
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |min| {
                if val < min {
                    Some(val)
                } else {
                    None
                }
            });

        self.sum.fetch_add(val, Ordering::SeqCst);
        self.count.fetch_add(1, Ordering::SeqCst);

        for idx in 0..=self.bounds.len() {
            // Allocate value in the last buckets if we reached the end of the Bounds array.
            if idx == self.bounds.len() {
                self.count_per_bucket[idx].fetch_add(1, Ordering::SeqCst);
                break;
            }

            if val < (self.bounds[idx].load(Ordering::SeqCst) as i64) {
                self.count_per_bucket[idx].fetch_add(1, Ordering::SeqCst);
                break;
            }
        }
    }

    /// `mean` returns the mean value for the histogram.
    #[allow(dead_code)]
    pub fn mean(&self) -> f64 {
        if self.count.load(Ordering::SeqCst) == 0 {
            0f64
        } else {
            (self.sum.load(Ordering::SeqCst) as f64) / (self.count.load(Ordering::SeqCst) as f64)
        }
    }

    /// `percentile` returns the percentile value for the histogram.
    /// value of p should be between [0.0-1.0]

    pub fn percentile(&self, p: f64) -> f64 {
        let count = self.count.load(Ordering::SeqCst);
        if count == 0 {
            // if no data return the minimum range
            self.bounds[0].load(Ordering::SeqCst)
        } else {
            let mut pval = ((count as f64) * p) as i64;

            for (idx, val) in self.count_per_bucket.iter().enumerate() {
                pval -= val.load(Ordering::SeqCst);
                if pval <= 0 {
                    if idx == self.bounds.len() {
                        break;
                    }
                    return self.bounds[idx].load(Ordering::SeqCst);
                }
            }
            // default return should be the max range
            self.bounds[self.bounds.len() - 1].load(Ordering::SeqCst)
        }
    }

    /// `clear` reset the histogram. Helpful in situations where we need to reset the metrics
    pub fn clear(&self) {
        self.count.store(0, Ordering::SeqCst);
        self.count_per_bucket
            .iter()
            .for_each(|val| val.store(0, Ordering::SeqCst));
        self.sum.store(0, Ordering::SeqCst);
        self.max.store(0, Ordering::SeqCst);
        self.min.store(0, Ordering::SeqCst);
    }
}

impl Display for Histogram {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut buf = Vec::<u8>::new();
        buf.extend("\n -- Histogram:\n".as_bytes());
        buf.extend(format!("Min value: {}\n", self.min.load(Ordering::SeqCst)).as_bytes());
        buf.extend(format!("Max value: {}\n", self.max.load(Ordering::SeqCst)).as_bytes());
        buf.extend(format!("Count: {}\n", self.count.load(Ordering::SeqCst)).as_bytes());
        buf.extend(format!("50p: {}\n", self.percentile(0.5)).as_bytes());
        buf.extend(format!("75p: {}\n", self.percentile(0.75)).as_bytes());
        buf.extend(format!("90p: {}\n", self.percentile(0.9)).as_bytes());

        let num_bounds = self.bounds.len();
        let count = self.count.load(Ordering::SeqCst);
        let mut cum = 0f64;
        for (idx, ct) in self.count_per_bucket.iter().enumerate() {
            let ct = ct.load(Ordering::SeqCst);
            if ct == 0 {
                continue;
            }

            // The last bucket represents the bucket that contains the range from
            // the last bound up to infinity so it's processed differently than the
            // other buckets.
            if idx == self.count_per_bucket.len() - 1 {
                let lb = self.bounds[num_bounds - 1].load(Ordering::SeqCst) as u64;
                let page = (ct * 100) as f64 / (count as f64);
                cum += page;
                buf.extend(
                    format!("[{}, {}) {} {:.2}% {:.2}%\n", lb, "infinity", ct, page, cum)
                        .as_bytes(),
                );
                continue;
            }

            let ub = self.bounds[idx].load(Ordering::SeqCst) as u64;
            let mut lb = 0u64;
            if idx > 0 {
                lb = self.bounds[idx - 1].load(Ordering::SeqCst) as u64;
            }

            let page = (ct * 100) as f64 / (count as f64);

            cum += page;
            buf.extend(format!("[{}, {}) {} {:.2}% {:.2}%\n", lb, ub, ct, page, cum).as_bytes())
        }

        buf.extend(" --\n".as_bytes());
        write!(f, "{}", String::from_utf8(buf).unwrap())
    }
}

impl Clone for Histogram {
    fn clone(&self) -> Self {
        Self {
            bounds: self.bounds.clone(),
            count: AtomicI64::new(self.count.load(Ordering::SeqCst)),
            count_per_bucket: self.count_per_bucket.clone(),
            min: AtomicI64::new(self.min.load(Ordering::SeqCst)),
            max: AtomicI64::new(self.max.load(Ordering::SeqCst)),
            sum: AtomicI64::new(self.sum.load(Ordering::SeqCst)),
        }
    }
}

fn init_cpb(num: usize) -> Vec<AtomicI64> {
    vec![0; num]
        .into_iter()
        .map(AtomicI64::new)
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod test {
    use crate::histogram::Histogram;

    struct PercentileTestCase {
        upper_bound: i64,
        lower_bound: i64,
        step: i64,
        loops: u64,
        percent: f64,
        expect: f64,
    }

    fn init_histogram(lb: f64, ub: f64, step: f64) -> Histogram {
        let size = ((ub - lb) / step).ceil() as usize;
        let mut bounds = vec![0f64; size + 1];

        let mut prev = 0f64;
        bounds.iter_mut().enumerate().for_each(|(idx, val)| {
            if idx == 0 {
                *val = lb;
                prev = lb;
            } else if idx == size {
                *val = ub;
            } else {
                *val = prev + step;
                prev = *val;
            }
        });
        Histogram::new(bounds)
    }

    fn assert_histogram_percentiles(ps: Vec<PercentileTestCase>) {
        let h = init_histogram(32.0, 514.0, 4.0);

        ps.iter().for_each(|case| {
            (case.lower_bound..=case.upper_bound)
                .filter(|x| *x % case.step == 0)
                .for_each(|v| {
                    (0..case.loops).for_each(|_| {
                        h.update(v);
                    })
                });

            assert_eq!(
                h.percentile(case.percent),
                case.expect,
                "bad: p: {}",
                case.percent
            );
            h.clear();
        });
    }

    #[test]
    fn test_mean() {
        let h = init_histogram(0.0, 16.0, 4.0);
        (0..=16).filter(|x| *x % 4 == 0).for_each(|v| {
            h.update(v);
        });
        assert_eq!(h.mean(), 40f64 / 5f64);
    }

    #[test]
    fn test_percentile() {
        let cases = vec![
            PercentileTestCase {
                upper_bound: 1024,
                lower_bound: 0,
                step: 4,
                loops: 1000,
                percent: 0.0,
                expect: 32.0,
            },
            PercentileTestCase {
                upper_bound: 512,
                lower_bound: 0,
                step: 4,
                loops: 1000,
                percent: 0.99,
                expect: 512.0,
            },
            PercentileTestCase {
                upper_bound: 1024,
                lower_bound: 0,
                step: 4,
                loops: 1000,
                percent: 1.0,
                expect: 514.0,
            },
        ];
        assert_histogram_percentiles(cases);
    }

    #[test]
    fn test_fmt() {
        let h = init_histogram(0.0, 16.0, 4.0);
        (0..=16).filter(|x| *x % 4 == 0).for_each(|v| {
            h.update(v);
        });
        let f = "\n -- Histogram:
Min value: 0
Max value: 16
Count: 5
50p: 8
75p: 12
90p: 16
[0, 4) 1 20.00% 20.00%
[4, 8) 1 20.00% 40.00%
[8, 12) 1 20.00% 60.00%
[12, 16) 1 20.00% 80.00%
[16, infinity) 1 20.00% 100.00%
 --\n";
        assert_eq!(format!("{}", h), f)
    }
}
