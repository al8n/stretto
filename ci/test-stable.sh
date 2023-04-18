#!/bin/bash

set -ex

cmd="${1:-test}"

# Run with all features
cargo "${cmd}" --all-features

cargo doc --no-deps --all-features

if [[ "${RUST_VERSION}" == "nightly"* ]]; then
    # Check benchmarks
    cargo check --benches

    # Check minimal versions
    cargo clean
    cargo update -Zminimal-versions
    cargo check --all-features 
fi