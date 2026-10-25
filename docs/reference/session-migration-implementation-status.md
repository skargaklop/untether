# SSH session migration: implementation status

The tracked feature branch introduces a **disabled-by-default schema** and a **read-only, fail-closed SSH preflight**. It does not implement session transfer, receipt reconciliation, native Pi/OMP adapters, CLI or Telegram migration actions, or the receiver binary. **No engine or migration mode is supported end to end.** Do not activate the sample config and expect a migration.

Both `native` and `container-wrapper` speak protocol 1 with the same fixed `inspect` command. The latter additionally requires container readiness, UID, and mount attestations. An administrator must separately install and constrain the receiver; no root, systemd, Docker, Coolify, distro-specific path, or package-manager dependency is imposed by this schema. Refusal of a missing receiver is intentional.

The preflight requires: pinned host key in a verified known_hosts file; selected local key; installed protocol metadata; Linux architecture and execution-mode attestation; receiver health for installed executable, agent/version/capability/auth/provider connectivity, staging permissions, storage and inodes, Git parity, quiescence, distinct/enrolled Telegram destination, binding and resume probe. Container modes also require verified container state and UID/mount mapping. Reports only boolean checks; implementations must produce fresh truthful evidence, not static success JSON.

Do not use the endpoint health response as authorisation to perform a real transfer. Native adapters must prove persisted cross-host restart resume; context-copy must create and resume a real independent session. No SSH connection, process restart or VPS installation was performed as part of this code change.
