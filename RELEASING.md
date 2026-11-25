# Releasing mcpcli

Releases are built and packaged by [`.github/workflows/release.yml`](.github/workflows/release.yml)
and [`scripts/ci/package.py`](scripts/ci/package.py). Every archive bundles the compiled binary
together with `LICENSE`, `NOTICE`, `README.md`, `examples/mcpcli.json`, and
`docs/mcpcli-design.md`.

## 1. Update version and Cargo.lock

1. Set `[package].version` in `Cargo.toml` to the new `X.Y.Z` version (numeric dot-separated
   components only, enforced by `scripts/ci/package.py`).
2. Refresh the root package entry in `Cargo.lock` without unlocking transitive dependencies
   (update `Cargo.toml`, run `cargo check`, and inspect `git diff Cargo.lock` to confirm only
   the `mcpcli` package version changed).
3. Review `NOTICE` whenever `Cargo.lock` changes third-party crates or versions.
4. Run local preflight checks before committing:

~~~sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
python3 scripts/ci/test-release-archive.py
python3 scripts/ci/test-windows-pe.py
python3 scripts/ci/run-tests.py
~~~

## 2. Rehearse on `wip/release-checkpoint`

Before merging the release commit to `main` or pushing a version tag, push the candidate commit
to `wip/release-checkpoint` (or trigger `workflow_dispatch` on `release.yml`):

~~~sh
git push origin HEAD:refs/heads/wip/release-checkpoint --force-with-lease
~~~

Pushing `wip/release-checkpoint` runs `check`, `ci`, all seven native archive builds,
extracted-archive smoke tests, `gate`, and `bundle` (`SHA256SUMS`) while skipping GitHub
Release creation.

## 3. Tag on `main`

Once `wip/release-checkpoint` succeeds:

1. Merge or fast-forward `main` to the verified commit and push `origin/main`.
2. Create and push `vX.Y.Z` on that `main` commit:

~~~sh
git checkout main
git pull --ff-only origin main
git tag vX.Y.Z
git push origin vX.Y.Z
~~~

On `refs/tags/v*` pushes, `check` enforces two invariants before building:

1. `GITHUB_REF_NAME` equals `v<Cargo.toml package.version>`.
2. `HEAD` is an ancestor of `origin/main` (`git merge-base --is-ancestor HEAD origin/main`).

## 4. Seven-platform archive and smoke matrix

| Platform ID | Archive | Build environment | Extracted-archive smoke test |
| --- | --- | --- | --- |
| `linux-amd64` | `mcpcli-X.Y.Z-linux-amd64.tar.gz` | `rust:1.92.0-alpine3.23` on `ubuntu-24.04` (static musl) | `smoke-release.py` in `alpine:3.23` and `debian:bookworm-slim` |
| `linux-arm64` | `mcpcli-X.Y.Z-linux-arm64.tar.gz` | `rust:1.92.0-alpine3.23` on `ubuntu-24.04-arm` (static musl) | `smoke-release.py` in `alpine:3.23` and `debian:bookworm-slim` |
| `macos-amd64` | `mcpcli-X.Y.Z-macos-amd64.tar.gz` | `x86_64-apple-darwin` on `macos-15-intel` | `smoke-release.py` on runner |
| `macos-arm64` | `mcpcli-X.Y.Z-macos-arm64.tar.gz` | `aarch64-apple-darwin` on `macos-15` | `smoke-release.py` on runner |
| `windows-amd64` | `mcpcli-X.Y.Z-windows-amd64.zip` | `x86_64-pc-windows-msvc` on `windows-2022` | PE machine/import check + `windows-tests.ps1` `smoke-release.py` |
| `windows-arm64` | `mcpcli-X.Y.Z-windows-arm64.zip` | `aarch64-pc-windows-msvc` on `windows-11-arm` | PE machine/import check + `windows-tests.ps1` `smoke-release.py` |
| `freebsd-amd64` | `mcpcli-X.Y.Z-freebsd-amd64.tar.gz` | FreeBSD 15.1 VM (`vmactions/freebsd-vm`) on `ubuntu-24.04` | `smoke-release.py` inside FreeBSD 15.1 VM |

Every `cargo build --release --locked` step enforces exact `Cargo.lock` reproducibility.
`smoke-release.py` rejects archive path traversal, symlinks, hardlinks, FIFOs, and setuid bits
before exercising the extracted binary against live stdio and Streamable HTTP fixtures.

## 5. Verify and publish the draft GitHub Release

1. `gate` confirms `check`, `ci`, `build`, `linux`, and `freebsd` all succeeded.
2. `bundle` downloads all `release-archive-*` artifacts into `dist/`, asserts the exact set of
   seven platform archives, writes `dist/SHA256SUMS`, and uploads `release-bundle`.
3. On `refs/tags/v*` pushes, `release` queries the GitHub commits API to verify the live tag
   still points to `needs.check.outputs.commit`, then runs `gh release create` with `--draft`,
   `--verify-tag`, `--generate-notes`, and title `mcpcli X.Y.Z`.
4. Download the draft assets, verify `sha256sum -c SHA256SUMS` (or `shasum -a 256 -c SHA256SUMS`),
   check `NOTICE` inside one `.tar.gz` and one `.zip` archive, review the release notes, and
   publish the draft on GitHub.
5. Delete the remote preflight branch after publishing:

~~~sh
git push origin --delete wip/release-checkpoint
~~~
