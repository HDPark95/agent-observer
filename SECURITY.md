# Security and sensitive data

Report potential vulnerabilities through [GitHub private vulnerability reporting](https://github.com/kochungcheon/agent-observer/security/advisories/new). Do not include transcripts, credentials, environment files, or private project paths in public issues. Include the version, platform, sanitized errors, and synthetic reproduction data in your private report.

Only loopback use is supported. Do not expose the observational API to untrusted networks. Managed messaging credentials are scoped but local filesystem access remains trusted. Export is explicitly configured and metadata-only. Known-token redaction is best-effort, not a general data-loss prevention system.

Only the latest reviewed release candidate is a validation target; no stable maintenance or security response SLA is established yet. The project is licensed under MIT; see LICENSE. Dependency notices are in THIRD_PARTY_NOTICES.md.
