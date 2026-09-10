# Contributing to EveDB

Use English for code, documentation, issues, pull requests, and commit messages.
Discuss substantial changes in an issue before implementation. Keep each pull
request focused and describe how the resulting behavior was checked.

## Local checks

Install Rust through rustup; the repository pins the toolchain.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo doc --workspace --no-deps --locked
```

Commit Cargo.lock. The current scaffold has no third-party Rust dependencies.

## CLA

Before a contribution can merge, each human author must complete the
[CLA acceptance form](legal/CLA-ACCEPTANCE.md). The
[CLA](legal/CLA.md) preserves your copyright and allows the owner to distribute
your contributions under commercial and other licenses.

The owner checks the signed acceptance and any employer authorization, retains
the evidence, and records approval on the default branch in
[the contributor registry](.github/cla/contributors.json). A pull request cannot
approve its own contributors by editing this registry. Registry changes must
be made by the project owner after reviewing the agreement.

The initial project owner does not need to grant a license to themself.
There are no automatic exemptions for bots. An authorized human must review
and adopt automated contributions and provide the required signature/sign-off.

For coauthored commits, every coauthor must also have an approved CLA and an
authorized commit email recorded as a SHA-256 digest in the registry. List all
actual contributors; do not invent coauthors or silently adopt someone else's
work.

## DCO and commit signatures

Every commit must contain a sign-off matching its author identity under the
[Developer Certificate of Origin 1.1](legal/DCO.txt). Coauthors must provide
matching sign-offs too. A sign-off is a certification of origin; a
cryptographic signature verifies the commit. They serve different purposes.

Configure an SSH or GPG signing key and register the public key with GitHub.
For example, with an existing SSH signing key:

```sh
git config --local gpg.format ssh
git config --local user.signingkey /absolute/path/to/signing_key
git config --local commit.gpgsign true
git config --local tag.gpgsign true
git commit -S -s -m "Describe the change"
```

Use an email associated with your GitHub account, including a GitHub noreply
address if preferred. GitHub must report each commit as verified. See
[GitHub's signing documentation](https://docs.github.com/en/authentication/managing-commit-signature-verification).

Do not commit private keys or credentials. Signed release tags should be
verified before publishing a release.

## Review and merge

Open a branch and pull request against main. All CI checks and the
`Contribution policy` check must pass. The policy workflow reads the registry
from the default branch and checks commit authors, coauthors, sign-offs, and
GitHub signature verification without checking out or running pull request code.

After a new registry entry is merged, rerun the Contribution policy workflow
with the pull request number. Split pull requests with more than 250 commits.

Merge without losing contribution metadata or signatures. GitHub rebase
merges are disabled. A maintainer may use a signed local merge that preserves
the reviewed commits. The solo owner may merge their own reviewed-by-CI pull
request; CODEOWNERS identifies the owner for contributions by others.

## Maintainer registry

Each approved contributor entry contains `github_id`, `login`,
`cla_version`, `accepted_at`, `evidence_sha256`, and `email_sha256` (an array).
Hash the completed signed acceptance for `evidence_sha256`. Hash each
trimmed, lowercase authorized email as UTF-8 for `email_sha256`.
Keep the original signed acceptance and any authority records privately.
Do not treat an unverified registry entry or a CI pass as proof of a legal grant.

The owner entry uses `role: "project-owner"` and is checked against the actual
GitHub repository owner ID. Review any ownership transfer before changing it.
