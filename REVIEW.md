# Gitcrawl Review

## Findings

### P2: Secrets are stored in plaintext config

The config file is at `~/.config/gitcrawl/config.json` and is written with restrictive file permissions, which is better than a repo-local `.env.local`. It is still plaintext storage. For a local-first maintainer tool this is acceptable for now, but it should be called out explicitly as a security tradeoff and eventually upgraded to OS keychain-backed storage if this becomes a broadly distributed tool.

### P3: Cross-platform support is code-aware but not CI-proven

The app now has explicit Windows and Linux code paths in [packages/api-core/src/config.ts](/Users/huntharo/github/gitcrawl/packages/api-core/src/config.ts) and [apps/cli/src/tui/app.ts](/Users/huntharo/github/gitcrawl/apps/cli/src/tui/app.ts), but CI still runs only on one platform. That means path handling, URL opening, terminal behavior, and config persistence are only partially verified outside macOS/Linux developer use. The remaining risk is more “untested edge behavior” than an obvious bug, but it is still a release-readiness gap.

## Strengths

- The local-first architecture is coherent: SQLite as the canonical store, direct CLI/TUI access, and clear package boundaries.
- The GitHub sync path now has useful backoff and operator-facing progress.
- The exact local vector path is a good pragmatic choice for the current scale.
- The TUI is already valuable for browsing clusters, switching repos, and kicking off the main local jobs.

## Recommended Work List

- [x] Make `gitcrawl tui` work without a repo argument by adding a repo picker from locally-known repositories.
- [x] Add in-TUI sync, embed, and cluster actions so the app can complete the main local workflow without dropping back to the shell.
- [x] Add in-TUI repo switching and a “sync a new repository” flow from the running app.
- [x] Escape Blessed tags in all user-provided text rendered into the detail pane.
- [x] Return 4xx for malformed request bodies and validation failures in the local HTTP API.
- [x] Decide whether `summarize` and `purge-comments` are advanced supported commands or internal maintenance commands, then align help/scripts/docs accordingly.
- [x] Add a cold-start onboarding flow in the TUI for repos with no local data yet.
- [ ] Consider keychain-backed secret storage for macOS/Linux/Windows as a future security upgrade.
- [ ] Add multi-OS CI coverage for packaging, config-path behavior, and installed CLI smoke tests.
