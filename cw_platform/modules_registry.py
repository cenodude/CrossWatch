# cw_platform/modules_registry.py
MODULES = {
    "AUTH": {
        "_auth_PLEX": "providers.auth._auth_PLEX",
        "_auth_SIMKL": "providers.auth._auth_SIMKL",
        "_auth_TRAKT": "providers.auth._auth_TRAKT",
        "_auth_JELLYFIN": "providers.auth._auth_JELLYFIN",
    },
    "SYNC": {
        "_mod_PLEX": "providers.sync._mod_PLEX",
        "_mod_SIMKL": "providers.sync._mod_SIMKL",
        "_mod_TRAKT": "providers.sync._mod_TRAKT",
        "_mod_JELLYFIN": "providers.sync._mod_JELLYFIN",
    },
}
