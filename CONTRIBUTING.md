# Contributing

## Prerequisites

- [rustup](https://rustup.rs/) (Homebrew's `rust` package won't work — it doesn't support `+toolchain` or `cargo miri`)
- The stable toolchain version is pinned in `rust-toolchain.toml` and installed automatically by rustup
- Nightly toolchain (for Miri): `rustup toolchain install nightly --component miri`

## Local Development

Format code:

```sh
./scripts/fmt.sh
```

Run lints (format check + clippy):

```sh
./scripts/lint.sh
```

Run tests (cargo test + Miri with tree borrows):

```sh
./scripts/test.sh
```

Run the full CI suite locally (lint + test):

```sh
./scripts/ci.sh
```

## CI

All of the above checks run automatically on pushes to `main` and on pull requests via GitHub Actions.
