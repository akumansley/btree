#!/usr/bin/env bash
set -euo pipefail
cargo test --all
MIRIFLAGS=-Zmiri-tree-borrows cargo +nightly miri test --all
