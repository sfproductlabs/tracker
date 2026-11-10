# Tracker CH-backed TLS + ACME support (source reference)

Durable reference for the Go tracker changes deployed to the ch cluster
(ch-1/2/3, `/opt/tracker-src/`). The deployed source is a plain copy (not git).

- `tls_certs.go` — `TlsCertManager`: passive, read-only ClickHouse-backed store
  of per-SNI TLS certs (`mcerts` object_type='tls'), the approved-domain
  allowlist (`site_configs.root_domain`, is_active=1), and pending ACME
  HTTP-01 tokens (`mcerts` object_type='acme_challenge'). x (the platform) is
  the sole writer / ACME client. `GetCertificate` serves per-SNI; `GetChallenge`
  queries CH directly (fresh, avoids cross-node cache lag); `IsApproved` is the
  dot-boundary suffix allowlist.
- `tracker.go` — wiring: `UseCHCerts`/`CHCertDB`/`CHCertTable`/
  `CHCertRefreshSeconds`/`UseTelemetryGate` config; the cert manager is created
  in main; the `:80` ACME-challenge handler; `GetCertificate` on the TLS server;
  the telemetry attribution gate in `trackWithArgs` (drops non-approved hosts
  when `UseTelemetryGate` is set).

See `plans/PLAN-TRACKER-TLS.md` for the design + rollout status.
