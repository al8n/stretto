use std::fmt::{Debug, Display, Formatter};

/// CacheError contains the error of this crate
pub enum CacheError {
    /// Count Min sketch with wrong width.
    InvalidCountMinWidth(u64),

    /// Invalid Samples value for TinyLFU.
    InvalidSamples(usize),

    /// Invalid false positive ratio for TinyLFU.
    InvalidFalsePositiveRatio(f64),

    /// Invalid number of counters for the Cache.
    InvalidNumCounters,

    /// Invalid max cost for the Cache.
    InvalidMaxCost,

    /// Invalid insert buffer size for the Cache.
    InvalidBufferSize,

    /// Error when send msg between threads.
    SendError(String),

    /// Error when receive msg between threads.
    RecvError(String),

    /// Error when updating entries
    UpdateError(String),

    /// Error when inserting entries
    InsertError(String),

    /// Error when removing entries
    RemoveError(String),

    /// Error when cleaning up
    CleanupError(String),

    /// Channel send error
    ChannelError(String),
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
            CacheError::SendError(msg) => write!(f, "fail to send msg to channel: {}", msg),
            CacheError::RecvError(msg) => write!(f, "fail to receive msg from channel: {}", msg),
            CacheError::InvalidNumCounters => write!(f, "num_counters can't be zero"),
            CacheError::InvalidMaxCost => write!(f, "max_cost can't be zero"),
            CacheError::InvalidBufferSize => write!(f, "buffer_size can't be zero"),
            CacheError::UpdateError(msg) => write!(f, "update error: {} ", msg),
            CacheError::InsertError(msg) => write!(f, "insert error: {} ", msg),
            CacheError::RemoveError(msg) => write!(f, "remove error: {} ", msg),
            CacheError::CleanupError(msg) => write!(f, "cleanup error: {} ", msg),
            CacheError::ChannelError(msg) => write!(f, "channel error: {} ", msg),
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
