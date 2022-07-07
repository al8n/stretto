/// CacheError contains the error of this crate
#[derive(thiserror::Error, Debug)]
pub enum CacheError {
    /// Count Min sketch with wrong width.
    #[error("invalid number of samples: {0}")]
    InvalidCountMinWidth(u64),

    /// Invalid Samples value for TinyLFU.
    #[error("invalid count main sketch width: {0}")] 
    InvalidSamples(usize),

    /// Invalid false positive ratio for TinyLFU.
    #[error("invalid false positive ratio: {0}, which should be in range (0.0, 1.0)")]
    InvalidFalsePositiveRatio(f64),

    /// Invalid number of counters for the Cache.
    #[error("num_counters can't be zero")]
    InvalidNumCounters,

    /// Invalid max cost for the Cache.
    #[error("max_cost can't be zero")]
    InvalidMaxCost,

    /// Invalid insert buffer size for the Cache.
    #[error("buffer_size can't be zero")]
    InvalidBufferSize,

    /// Error when send msg between threads.
    #[error("fail to send msg to channel: {0}")]
    SendError(String),

    /// Error when receive msg between threads.
    #[error("fail to receive msg from channel: {0}")]
    RecvError(String),

    /// Error when updating entries
    #[error("update error: {0}")]
    UpdateError(String),

    /// Error when inserting entries
    #[error("insert error: {0}")]
    InsertError(String),

    /// Error when removing entries
    #[error("remove error: {0}")]
    RemoveError(String),

    /// Error when cleaning up
    #[error("cleanup error: {0}")]
    CleanupError(String),

    /// Channel send error
    #[error("channel error: {0}")]
    ChannelError(String),
}
