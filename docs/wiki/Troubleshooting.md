# Troubleshooting

## Guard rails that may block writes
- **Drop guard**: refuses suspect runs when a source suddenly shrinks.
- **Tombstones**: keep delete markers to prevent ping‑pong (`tombstone_ttl_days`).
- **Blackbox**: auto‑blocks flappy titles after repeated unresolved/fail events; decays after `cooldown_days`.
