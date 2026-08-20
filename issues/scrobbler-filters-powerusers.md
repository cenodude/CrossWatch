# Scrobbler filters - power users

Filters control whether a playback event is accepted before CrossWatch sends it to a scrobble destination.

Watcher routes use a route-level `filters` object.

Webhook source profiles use provider-specific filters, such as `filters_plex`, `filters_jellyfin`, and `filters_emby`.

## Matching behavior

Username matching ignores letter case. It also normalizes punctuation and spaces for normal names.

A nonmatching whitelist rejects the event. Without a username whitelist, the user filter passes.

Advanced identity values are supported when the source event supplies them:

```text
id:123
uuid:abcd1234
```

* `id:` compares account or user IDs.
* `uuid:` compares account or user UUID values.

Use advanced values only when normal username matching is ambiguous or unreliable.

Webhook implementations can expose different user fields. Use the event log's exact username when the picker is unavailable.

## Profiles, filters, and libraries

Profiles and filters solve different problems.

A source profile selects the connected server or account configuration. Filters select which events from that profile CrossWatch accepts.

Example:

* Profile: Plex Home Server
* Username whitelist: Pascal
* Server UUID allowlist: Home Server UUID

Use profiles for connection separation. Use filters for event separation.

Filters do not replace media server library whitelisting.

Library whitelisting determines which libraries CrossWatch may use. Filters determine which incoming playback events are accepted.

Changing a username or UUID filter cannot bypass a library whitelist rejection.

## Media filter behavior

Media ignore filters are case-insensitive.

Path prefixes are normalized so Windows and Linux style paths can both match.

Examples:

```text
Z:\data\media\placeholder
/data/media/placeholder
```

Filename patterns use simple contains matching. They are not regex.

Edition matching compares the detected edition or version name. `Trailer` matches `trailer`.

When a media ignore filter matches, CrossWatch drops the event before sending scrobbles to Trakt, SIMKL, MDBList, or other destinations.

## Agregarr placeholder trailers

The Agregarr toggle is a shortcut for the common placeholder trailer setup:

```json
{
  "ignore_agregarr_trailers": true
}
```

It checks the built-in defaults:

```text
filename contains: {edition-trailer}
edition equals: trailer
marker file: .comingsoon
```

This is intentionally not title based. A title like `Trailer Park Boys` is not ignored unless the file path, filename, edition, or marker file matches.

## Route filter example

Watcher route filters use the normal route `filters` object:

```json
{
  "filters": {
    "users": ["Pascal"],
    "ignore_agregarr_trailers": true,
    "ignored_path_prefixes": ["/data/media/placeholder"],
    "ignored_filename_patterns": ["{edition-Trailer}"],
    "ignored_editions": ["Trailer"]
  }
}
```

## Webhook filter example

Webhook filters are stored per source profile, so the provider key matters:

```json
{
  "filters_plex": {
    "ignore_agregarr_trailers": true,
    "ignored_filename_patterns": ["{edition-Trailer}"]
  },
  "filters_jellyfin": {
    "ignored_path_prefixes": ["/data/media/placeholder"]
  },
  "filters_emby": {
    "ignored_editions": ["Trailer"]
  }
}
```

Every destination attached to that webhook source profile uses the same provider filter.

Use Watcher routes when each destination needs its own filter set.

## Supported media keys

CrossWatch looks for path-like values in incoming payloads, including:

```text
path
file
filename
filepath
file_path
mediafile
media_file
```

It also checks internally enriched Plex values:

```text
_cw_file_path
_cw_file_paths
_cw_edition_title
```

Edition-like values include:

```text
edition
editionTitle
edition_title
version
versionTitle
version_title
```

Nested payloads are searched too, so Jellyfin and Emby `NowPlayingItem.Path` style data can be matched.

## Common mistakes

### Using a display name

The display name can differ from the event username. Use the picker or inspect incoming event logs.

### Adding a Plex account ID without a prefix

Use:

```text
id:123
```

Do not use:

```text
123
```

The `id:` prefix is required for an ID match.

### Expecting different webhook filters per destination

Webhook filters are shared by source profile. Use Watcher routes for destination-specific filtering.

### Adding one UUID to both lists

The blocklist wins. CrossWatch rejects the event.

### Expecting Scrob to always have file paths

Scrob can only apply path, filename, or edition filters when the incoming payload includes those fields.

If the payload only contains metadata IDs and title information, media-file filters cannot match.
