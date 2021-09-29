use std::fmt::{Display, Formatter, Debug};

/// CacheError contains the error of this crate
pub enum CacheError {
    /// Count Min sketch with wrong width
    InvalidCountMinWidth(u64),
    /// Invalid Samples value for TinyLFU
    InvalidSamples(usize),
    /// Invalid false positive ratio for TinyLFU
    InvalidFalsePositiveRatio(f64),
}

impl CacheError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        match self {
            CacheError::InvalidSamples(v) => write!(f, "invalid number of samples: {}", *v),
            CacheError::InvalidCountMinWidth(v) => {
                write!(f, "invalid count main sketch width: {}", *v)
            }
            CacheError::InvalidFalsePositiveRatio(v) => write!(
                f,
                "invalid false positive ratio: {}, which should be in range (0.0, 1.0)",
                *v
            ),
        }
    }
}

impl Display for CacheError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        self.fmt(f)
    }
}

impl Debug for CacheError {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        self.fmt(f)
    }
}

impl std::error::Error for CacheError {}