# Repository administration

The active GitHub rulesets are stored as importable JSON in
[.github/rulesets](../.github/rulesets). File changes do not automatically update
GitHub settings; the repository owner must apply and verify them.

## Main branch

- Pull requests are required, including for the owner.
- Commits entering main require GitHub-verified signatures.
- Quality, all three platform test jobs, and Contribution policy must pass.
- Required checks must come from the GitHub Actions app.
- Branches must be up to date and review threads resolved.
- Force pushes and deletion are blocked; no bypass actors are configured.

The review count is zero so a sole maintainer can merge their own pull request
after checks pass. CODEOWNERS routes ownership to Rea1Blank. Add required
independent reviews when another trusted maintainer joins.

Only merge commits are enabled in repository settings. Rebase and squash merges
are disabled to preserve reviewed commits and their signatures. GitHub web
commits require sign-offs. Ensure merge commits also have a sign-off; when
merging locally, use `git merge --no-ff -S --signoff` and preserve the reviewed
head. Automatic branch deletion after merge is enabled.

## Releases and security

Tags matching `v*` cannot be changed or deleted under the tag ruleset.
GitHub does not enforce signed tag creation through this ruleset. The local
repository enables `tag.gpgsign`; verify signed annotated tags before releases.

Private vulnerability reporting and Dependabot vulnerability alerts are enabled.
Dependency update pull requests receive the same contribution requirements.
An authorized human must review and adopt a bot change rather than bypassing
CLA and signature checks.

## Rights records

Only add a contributor to the CLA registry after reviewing and privately
retaining their signed acceptance and authority evidence. The CI registry is
an index of that review, not the agreement itself.

Changing the project owner or moving the repository to a company requires a
documented rights transfer and review of the CLA, registry owner ID, license
notices, repository URLs, and CODEOWNERS.

## Local Windows build tools

The MSVC Rust toolchain needs the Visual Studio C++ Build Tools, including the
MSVC compiler/linker and a Windows SDK. If Cargo reports that `link.exe` is
missing, install those components or build in a configured development
environment. GitHub's Windows runner provides them for CI.
