# Scrobbler filters - end users

Filters let you control which playback events CrossWatch accepts for Watcher routes and Webhook profiles.

Start with empty filters first. Confirm one movie or episode scrobbles correctly, then add only the filters you need.

## Filter dots

Filter rows use color hints:

* Green dots are allow filters. The event must match when values are set.
* Red dots are block or ignore filters. Matching events are skipped.

For example, **Username whitelist** is green because it allows selected users. **Ignored filename patterns** is red because matching media is ignored.

Route and webhook cards show a short filter summary, such as `1 user`, `1 UUID allow`, `Agregarr ignored`, `1 pattern ignored`, or `Live TV ignored`.

## Filter scope

Watcher and Webhooks store filters differently.

### Watcher filters

Watcher filters belong to each route.

Two routes from one media server profile can use different filters.

Example:

* Plex Home to Trakt Pascal, with username `Pascal`
* Plex Home to Trakt Family, with username `Family`

### Webhook filters

Webhook filters belong to the media server profile.

Every tracker destination attached to that profile uses the same webhook filter.

Example: Plex Home can send to both Trakt and SIMKL. Changing its webhook filter changes it for both destinations.

Use Watcher routes when each destination needs different filtering.

## Username whitelist

Use **Username whitelist** to accept playback only from selected users.

Leave it empty to accept all users.

Use the user picker when possible. It uses names returned by the selected media server profile.

## Plex Server UUID lists

Plex supports a Server UUID allowlist and blocklist.

Use the allowlist when only specific Plex servers may send events:

* Empty allowlist: every UUID is accepted unless blocked.
* Filled allowlist: only matching UUID values are accepted.
* Matching is exact.

Use the blocklist to reject specific Plex servers.

The blocklist wins over the allowlist. If a UUID appears in both lists, CrossWatch rejects the event.

## Ignore Plex Live TV and DVR

Enable **Ignore Plex Live TV and DVR** to skip Plex Live TV and DVR playback.

This option is available for Plex Watcher routes.

## Ignore Agregarr placeholder trailers

Enable **Ignore Agregarr placeholder trailers** when Agregarr creates placeholder movie folders with trailer files.

CrossWatch skips media that looks like an Agregarr trailer placeholder, including:

* files with `{edition-Trailer}` in the filename
* media with edition name `Trailer`
* media stored beside a `.comingsoon` marker file

This checks the file path, filename, edition, and marker files. It does not reject a movie just because the movie title contains the word trailer.

Example skipped file:

```text
/data/media/placeholder/movies/The Odyssey (2026)/The Odyssey (2026) {tmdb-1368337} {edition-Trailer}.mp4
```

## Ignored media filters

Use these when you want to skip files based on where they are stored or how they are named.

### Ignored path prefixes

Skips media under matching paths.

Good for placeholder folders, trailer folders, or temporary media roots.

Examples:

```text
/data/media/placeholder
Z:\data\media\placeholder
```

### Ignored filename patterns

Skips media when the filename contains one of the values.

Examples:

```text
{edition-Trailer}
-trailer
/sample
```

### Ignored editions

Skips media when the edition or version name matches.

Example:

```text
Trailer
```

## Provider support

These media ignore filters work for Watcher and Webhooks when the source sends enough media detail.

Supported well:

* Plex Watcher
* Jellyfin Watcher
* Emby Watcher
* Kodi Watcher
* Plex Webhooks
* Jellyfin Webhooks
* Emby Webhooks

Scrob can also be filtered when the incoming payload includes file path, filename, or edition fields.

## Troubleshooting

### No events pass

1. Clear **Username whitelist**.
2. Clear Plex UUID allowlist and blocklist.
3. Disable **Ignore Plex Live TV and DVR** for testing.
4. Disable media ignore filters for testing.
5. Start a new playback session.
6. Check the source profile connection.
7. Review debug logs for the rejected field.

### A trailer still scrobbles

1. Confirm the media source sends a file path or edition.
2. Add the placeholder folder to **Ignored path prefixes**.
3. Add `{edition-Trailer}` to **Ignored filename patterns**.
4. Add `Trailer` to **Ignored editions**.
5. Start a new playback session.

### Wrong user is scrobbled

1. Use the user picker.
2. Verify the route or webhook scope.
3. Confirm the destination profile belongs to that user.
4. Check for another route or webhook without filters.
