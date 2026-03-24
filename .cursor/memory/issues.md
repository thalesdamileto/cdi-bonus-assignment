# Issues

Known bugs, workarounds, and fragile areas. Remove entries when resolved.

| ID | Date | Area | Description | Impact | Status |
|----|------|------|-------------|--------|--------|
| ISS-001 | 2026-03-24 | Docker / Windows | `exec /docker-entrypoint.sh: no such file or directory` — CRLF em `docker-entrypoint.sh` quebra o shebang no Linux. Mitigação: `.gitattributes` (`*.sh` eol=lf), `sed` no Dockerfile, rebuild. | low | workaround |

<!-- ID: ISS-001, ISS-002... (for cross-reference in decisions.md and specs) -->
<!-- Date: YYYY-MM-DD (for pruning — issues with no activity >60 days should be reviewed) -->
<!-- Impact: high / medium / low -->
<!-- Status: open / workaround / investigating -->

## Active Workarounds

<!-- Temporary solutions in effect. Remove when the definitive fix is implemented. -->

-
