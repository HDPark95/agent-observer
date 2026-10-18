# Release preparation

## Source-first publication

The primary user path is Git clone, local Go build, and the embedded viewer. A public source repository with MIT and a verified quickstart is the release target. Prebuilt archives, Apple signing/notarization, and package-manager distribution are optional future channels, not prerequisites for source publication.

## Prepare the public source

The working checkout can contain private configuration and unpublished experiment documents. `release/source-files.txt` is the reviewed source allowlist. The packager does not consult Git staging or include Git history. It rejects unsafe paths, symlinks, state directories, raw JSONL, missing files, and several likely secret/personal-path patterns. This is a heuristic check, not proof that all sensitive data was found; manually review the allowlist and source.

```sh
python3 scripts/package_release.py source --output /absolute/new-output
python3 scripts/package_release.py verify /absolute/new-output/agent-observer-0.1.0-rc1-source.tar.gz
python3 scripts/package_release.py unpack /absolute/new-output/agent-observer-0.1.0-rc1-source.tar.gz --destination /absolute/new-source
```

From the unpacked source, run `make check`, `make build`, and both `python3 scripts/smoke_release.py ./bin/agent-observer` and `python3 scripts/smoke_release.py ./bin/agent-observer --clean-profile`. Use a new destination each time; packaging does not overwrite artifacts. The source archive is reproducible for identical selected bytes and metadata, with sorted files, normalized modes/timestamps, per-file SHA-256 manifest, and an archive checksum.

## Publication gates

`release/release.json` records `kochungcheon/agent-observer`, the selected MIT license and its SHA-256. LICENSE is included in both source and binary archives. `source --purpose publish` checks repository and license metadata against the allowlisted LICENSE. That eligibility check does not approve publication or prove that a remote exists; the packager never uploads anything.

Before publication:

1. Confirm the repository owner/name, code ownership, and disclosure channel. MIT has been selected; preserve LICENSE and review third-party notices. Update `license_sha256` if the reviewed license file changes.
2. Review the curated source, fixture provenance, documentation, and secret scan. Import that source into a fresh repository; do not push the current private checkout history. Stage only `release/source-files.txt` entries (`sed '/^#/d; /^$/d' release/source-files.txt | git add --pathspec-from-file=-`) and run `python3 scripts/package_release.py check-tree` there. The generated archive `SOURCE-MANIFEST.json` is verification output, not a public source input.
3. Run the pinned CI workflow from the public candidate. Confirm arm64 target in logs. CI definition alone is not a passing CI run.
4. Before publication, use a local fixture repository containing only the curated source to test clone, README build, demo, one explicit session, custom port, restart, and uninstall in a clean profile. This does not establish remote GitHub availability; agent-run checks do not replace an independent tester.
5. Authorize public repository creation/push explicitly and set the chosen URL in the guides. After authorized publication, verify the actual public HTTPS clone path and obtain an independent first-use check. Do not make binary archives mandatory for this source-only release. Signed/notarized distribution, Homebrew, Windows/Linux binaries, and live Claude worker support remain separate work.

The CI workflow pins [checkout v4.2.2](https://github.com/actions/checkout/releases/tag/v4.2.2) and [setup-go v5.5.0](https://github.com/actions/setup-go/releases/tag/v5.5.0) by commit. It uses the documented [macOS arm64 runner](https://docs.github.com/en/actions/reference/runners/github-hosted-runners) and asserts the actual architecture. It verifies the unpacked package, not a workspace with ignored files. A separate Go 1.22 job checks the declared minimum toolchain; it does not run the newer Staticcheck tool.

## Minimum toolchain caveat

The module language declaration remains Go 1.22. A local Go 1.22.12 test attempt on the current macOS aborted several test executables with `dyld: missing LC_UUID load command`. This is not a passing minimum-toolchain gate. Use the validated Go 1.26.4 toolchain on this target; the separate Linux CI job still needs an actual hosted run.

Binary packaging disables Go workspace files, persistent Go environment settings, and inherited build flags, and uses `-mod=readonly`. It still relies on the installed toolchain and the dependency versions/checksums in the curated module. Archive and checksum files must both verify before use; interruption can leave an incomplete output, so retry into a new directory.

`smoke_release.py --clean-profile` selects a separate first-install check. It copies the executable into an empty installation directory, uses a fresh child-process user profile and minimal PATH, and checks default `init`, saved-port discovery, repeat-init preservation, manual connection, restart and shutdown. It does not inherit application credentials or configuration. This is host-OS process isolation, not an independent machine or a downloaded-file Gatekeeper test. Normal `make check` runs with the repository's intended environment.

## Optional prebuilt binaries

On macOS Apple Silicon, build the binary from that validated source archive:

```sh
python3 scripts/package_release.py binary /absolute/new-output/agent-observer-0.1.0-rc1-source.tar.gz --output /absolute/new-binaries
```

The binary includes version metadata; the archive manifest records its exact source archive hash. This is a local, unsigned candidate, not an attestation or a reproducible-binary guarantee. Test the **unpacked binary** again, including demo in a browser. Do not use source-checkout success as a substitute.

A separate Chrome loopback-download test on macOS 26.3 propagated quarantine to the ad-hoc-signed executable; Gatekeeper assessment rejected it. Exact warning UI and execution after an exception were not verified. If a future release distributes prebuilt Mac binaries for ordinary download, prepare Developer ID signing and Apple notarization, then retest the final downloaded file. This does not make signing a prerequisite for users compiling the source locally.
