use std::env::{self, var};

fn main() {
  // Don't rerun this on changes other than build.rs, as we only depend on
  // the rustc version.
  println!("cargo:rerun-if-changed=build.rs");

  // Check for `--features=tarpaulin`.
  let tarpaulin = var("CARGO_FEATURE_TARPAULIN").is_ok();

  if tarpaulin {
    use_feature("tarpaulin");
  } else {
    // Always rerun if these env vars change.
    println!("cargo:rerun-if-env-changed=CARGO_TARPAULIN");
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARPAULIN");

    // Detect tarpaulin by environment variable
    if env::var("CARGO_TARPAULIN").is_ok() || env::var("CARGO_CFG_TARPAULIN").is_ok() {
      use_feature("tarpaulin");
    }
  }

  // Rerun this script if any of our features or configuration flags change,
  // or if the toolchain we used for feature detection changes.
  println!("cargo:rerun-if-env-changed=CARGO_FEATURE_TARPAULIN");
}

fn use_feature(feature: &str) {
  println!("cargo:rustc-cfg={}", feature);
}
