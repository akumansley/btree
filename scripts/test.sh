#!/usr/bin/env bash
set -euo pipefail
cargo test --all
# Run a focused subset of tests under Miri for faster CI.
# All tests still run without Miri above. The allow-list here targets
# unsafe/concurrency-heavy code where Miri provides the most value.
MIRIFLAGS=-Zmiri-tree-borrows cargo +nightly miri test -p qsbr
MIRIFLAGS=-Zmiri-tree-borrows cargo +nightly miri test -p thin
MIRIFLAGS=-Zmiri-tree-borrows cargo +nightly miri test -p btree -- \
  pointers::atomic::tests \
  cursor::tests::test_interaction_between_mut_cursor_and_non_locking_cursor \
  cursor::tests::test_interaction_between_mut_cursor_and_shared_cursor \
  tree::tests::test_random_inserts_gets_and_removes_with_seed_multi_threaded \
  iter::tests::test_concurrent_iteration
