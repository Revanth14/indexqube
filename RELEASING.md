# Releasing IndexQube

IndexQube releases are four self-contained bundles: Linux and macOS on amd64
and arm64. Each contains `iq`, the standalone `indexqube-gateway`, the installer,
and the README.

## Version and release names

Release tags are immutable SemVer tags such as `v0.2.0`. While IndexQube is in
alpha, ordinary `0.x` versions communicate that the public interface may still
change and keep GitHub's `/releases/latest` installer URL working. Do not move or
rename an existing tag.

For example, Git tag `v0.2.1` produces GitHub release title
`IndexQube v0.2.1`. Future releases follow the same `IndexQube vX.Y.Z` title
format without hard-coding a version in the workflow.

## Release contract

1. Update the tested Codex/Claude version fixtures and run `make check`.
2. Run the opt-in real-repository alpha lane documented in `scripts/real_repo_alpha.sh`.
3. Create and push an annotated `v*` tag. For example:

   ```bash
   version=v0.2.1
   git tag -a "$version" -m "IndexQube $version"
   git push origin "$version"
   ```

   The release workflow runs the race-enabled suite,
   tests installer update/rollback, cross-compiles all four bundles, writes
   SHA-256 checksums, creates a Sigstore-signed SLSA provenance attestation, and
   publishes immutable GitHub release assets.
4. Verify the published release and one downloaded bundle:

   ```bash
   gh release verify vX.Y.Z --repo Revanth14/indexqube
   gh attestation verify indexqube_vX.Y.Z_darwin_arm64.tar.gz \
     --repo Revanth14/indexqube
   ```

GitHub Actions obtains a short-lived signing certificate through OIDC; no
long-lived signing key is stored in the repository. Checksums protect the
installer download, and `gh attestation verify` binds an archive digest to this
repository's release workflow. `INDEXQUBE_REQUIRE_ATTESTATION=1` makes the
installer refuse installation when GitHub CLI provenance verification is not
available.

## Installer rollback

The installer validates the candidate binary before replacement, installs via
an atomic rename, and retains the prior regular binary as `iq.previous`.
Post-install validation restores the previous binary automatically on failure.
An explicit rollback swaps the current and previous versions:

```bash
curl -fsSL https://github.com/Revanth14/indexqube/releases/latest/download/install.sh | \
  sh -s -- --rollback
```

Do not delete or replace a user's `~/.indexqube` directory during install,
update, rollback, or uninstall. Database migrations create their own consistent
pre-migration backup and older binaries fail closed on newer schema versions.
