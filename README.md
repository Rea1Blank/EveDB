# EveDB

EveDB is a general-purpose database with built-in event sourcing for typed
entities. It stores changes as events, maintains current state, and provides
historical reads and replay through a small operations API.

Create an entity, submit new field values, and read the state you need.
EveDB handles event history, versions, snapshots, and reconstruction with
minimal configuration.

Read the [product description](docs/product.md) for the data model, operations,
entity lifecycle, transactions, and history retention.

**Implementation status:** scaffold only; the database features described above
are not implemented yet.

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
| `src/evedb-core` | Core library; future home of the database engine |
| `src/evedb-cli` | `evedb` command-line executable |
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

EveDB is open-source software licensed under the
[GNU Affero General Public License version 3 only](LICENSE)
(`AGPL-3.0-only`). Commercial use is permitted.

If you modify EveDB and make that version available to users over a network,
you must offer those users its Corresponding Source under the AGPL.
Distributing binaries also carries source-availability obligations, even
without modifications. Independent applications are not automatically covered
merely because they communicate with EveDB over a network.

See [licensing guidance](legal/LICENSING.md) and [NOTICE](NOTICE).
