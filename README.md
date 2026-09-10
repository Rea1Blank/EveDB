# EveDB

A database project written in Rust, developed in a single Cargo workspace.

**Status:** initial scaffold. No storage engine, query language, transactions,
or network service is implemented yet.

## Quick start

Install [Rust with rustup](https://rustup.rs/), then run:

```sh
git clone https://github.com/Rea1Blank/EveDB.git
cd EveDB
cargo run --locked -p evedb-cli -- --help
cargo run --locked -p evedb-cli -- --version
```

`rust-toolchain.toml` pins the development toolchain, including rustfmt and Clippy.

## Workspace

| Path | Purpose |
| --- | --- |
| `crates/evedb-core` | Core library; future home of the database engine |
| `crates/evedb-cli` | `evedb` command-line executable |
| `docs` | Architecture and development decisions |
| `legal` | Contributor agreement and licensing guidance |
| `.github` | CI, contribution checks, and repository ownership |

Add crates when a concrete component needs a separate boundary. See
[architecture](docs/architecture.md).

## Development

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo doc --workspace --no-deps --locked
```

All code, documentation, issues, pull requests, and commit messages must be in
English. Contributions require a CLA, a DCO sign-off, and verified commit
signatures. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

EveDB is **source-available** under the
[PolyForm Noncommercial License 1.0.0](LICENSE). It is not OSI open source.
Uses outside the purposes permitted by that license require a separate written
commercial license from the project owner, the individual operating
[@Rea1Blank](https://github.com/Rea1Blank).

Running EveDB as a commercial network service is not exempt merely because no
binary is distributed. Publishing source code alone does not grant permission
for commercial use. The license defines the permitted purposes, including its
specific provisions for noncommercial organizations.

See [licensing guidance](legal/LICENSING.md) and [NOTICE](NOTICE).
