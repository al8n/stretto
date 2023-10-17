#!/bin/bash

set -ex

export ASAN_OPTIONS="detect_odr_violation=0 detect_leaks=0"

# Run address sanitizer with cargo-hack
RUSTFLAGS="-Z sanitizer=address" \
cargo hack test --lib --each-feature --include-features sync,async --exclude-no-default-features

# Run leak sanitizer with cargo-hack
RUSTFLAGS="-Z sanitizer=leak" \
cargo hack test --lib --each-feature --include-features sync,async --exclude-no-default-features

# Run thread sanitizer with cargo-hack
RUSTFLAGS="-Z sanitizer=thread" \
cargo hack -Zbuild-std test --lib --each-feature --include-features sync,async --exclude-no-default-features
