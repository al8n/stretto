/*
 * Copyright 2021 Al Liu (https://github.com/al8n)
 *
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/// `RingConsumer` is the user-defined object responsible for receiving and
/// processing items in batches when buffers are drained.
pub trait RingConsumer {
    fn push(&mut self, data: Vec<u64>) -> bool;
}

/// `RingStripe` is a singular ring buffer that is not concurrent safe.
#[derive(Debug)]
pub struct RingStripe<T: RingConsumer> {
    c: T,
    data: Vec<u64>,
    capacity: usize,
}

impl<T: RingConsumer> RingStripe<T> {
    pub fn new(c: T, capacity: usize) -> Self {
        Self {
            c,
            data: Vec::with_capacity(capacity),
            capacity,
        }
    }

    /// `push` appends an item in the ring buffer and drains (copies items and
    /// sends to Consumer) if full.
    pub fn push(&mut self, item: u64) {
        self.data.push(item);
        let data = self.data.clone();
        // Decide if the ring buffer should be drained.
        if data.len() >= self.capacity {
            // Send elements to consumer and create a new ring stripe.
            if self.c.push(data) {
                self.data = Vec::with_capacity(self.capacity);
            } else {
                self.data.clear();
            }
        }
    }
}

/// `RingBuffer` stores multiple buffers (stripes) and distributes Pushed items
/// between them to lower contention.
///
/// This implements the "batching" process described in the [BP-Wrapper paper](https://ieeexplore.ieee.org/document/4812418)
/// (section III part A).
pub struct RingBuffer {
    // TODO: implements the "batching" process described in the BP-Wrapper paper
}

impl RingBuffer {
    pub fn new() -> Self {
        Self {}
    }

    /// `push` adds an element to one of the internal stripes and possibly drains if
    /// the stripe becomes full.
    pub fn push(&mut self, item: u64) {}
}
