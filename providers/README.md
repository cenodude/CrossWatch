# CrossWatch Providers

All pluggable integrations for CrossWatch live under the `providers/` namespace.

## Structure

```
providers/
  auth/        # Authentication providers (connect to external accounts)
  sync/        # Synchronization modules (move data between providers)
  metadata/    # Metadata sources (fetch posters, info, runtime, etc.)
```

## Auth Providers

- Location: `providers/auth/_auth_NAME.py`
- Must implement the `AuthProvider` protocol from `_auth_base.py`
- Export an instance as `PROVIDER`
- Define:
  - `manifest()` → UI metadata (label, flow type, fields, actions)
  - `capabilities()` → supported features
  - `get_status(config)`
  - `start(config, payload, save_config)`
  - `finish(config, payload, save_config)`
  - `refresh(config, save_config)`
  - `disconnect(config, save_config)`

## Sync Modules

- Location: `providers/sync/_mod_NAME.py`
- Must implement the `SyncModule` protocol from `_mod_base.py`
- Define:
  - `run_sync(ctx: SyncContext) -> SyncResult`
  - Optional: `supported_features()`
- Registered in `platform/orchestrator.py` by mapping `(source, target)` → `ModuleClass`

## Metadata Providers

- Location: `providers/metadata/_meta_NAME.py`
- Must expose either:
  - `PROVIDER = ProviderInstance` **or**
  - `def build(load_cfg, save_cfg) -> ProviderInstance`
- Define:
  - `manifest()` (label, supported entities, supported assets, needs API key?)
  - `capabilities()`
  - `fetch(entity, ids, locale, need) -> dict`

Returned metadata should be normalized to include:

```json
{
  "ids": {"tmdb": "123", "imdb": "tt123"},
  "title": "...",
  "overview": "...",
  "year": 2024,
  "images": {
    "poster": [{"url": "...", "w": 1000, "h": 1500, "lang": "en"}],
    "backdrop": [...]
  }
}
```

## Adding a New Provider

1. Create a new `_auth_NAME.py`, `_mod_NAME.py`, or `_meta_NAME.py` file in the proper folder.
2. Follow the conventions described above.
3. Test with `PlatformManager` or `MetadataManager`.
4. Update `orchestrator.py` if adding new sync pairs.

