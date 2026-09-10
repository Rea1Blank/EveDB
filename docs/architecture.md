# Architecture

## Initial decision

EveDB uses a Cargo workspace with Rust edition 2024 and resolver 3. Package
version, license, toolchain requirements, and lint settings are shared at the
workspace root. Cargo.lock is committed. Packages are not published to crates.io.

The initial dependency direction is `evedb-cli -> evedb-core`. The core crate
must not depend on CLI code. Both crates use only the Rust standard library.

The executable currently exposes help and version information. The core crate
contains version metadata only. No storage format or public database API is
committed to by this scaffold.

## Growth

Keep closely related code in modules first. Add separate crates for storage,
protocols, or a server when their interfaces and dependency boundaries become
concrete. Keep benchmarks and integration tests beside the components they test.

Before implementing the engine, decide the data model, persistence and recovery
requirements, transaction guarantees, and whether the first interface is
embedded, client/server, or both. Record consequential decisions in this folder.
