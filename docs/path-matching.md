# Match by file path

Match by file path is an optional setting for a sync pair between Plex, Jellyfin and Emby, including different instances of the same provider. Open the pair configuration and enable **General → Matching → Match by file path**. It is off by default and applies in both directions for two-way pairs.

The fallback supports movie and episode history, ratings and playback progress. It does not apply to watchlists, playlists, collections or tracker services such as Trakt and SIMKL. Kodi is not supported by this option yet.

CrossWatch first tries ID matching. If that cannot resolve an item, it compares normalized file-path endings within the destination libraries selected for that feature. The setting also permits this fallback when Strict ID matching is enabled; it does not enable title searches.

For example, these movie paths match:

~~~text
/mnt/media/movies/Alien (1979)/Alien.mkv
D:\Movies\Alien (1979)\Alien.mkv
~~~

Movie matching uses the last two path segments. Episode matching uses the last three segments plus season and episode numbers. Separators and case are normalized, and explicit multipart movie numbers remain distinct. File contents are not read. Different filenames, different trailing folders or different episode numbering do not match.

CrossWatch requires one matching destination item. Duplicate path matches and conflicting external IDs are skipped and logged. Successful fallback lookups are logged as `path_match` with `resolution_method=file_path`.

Enabling this option can require a full metadata catalogue read, including unwatched destination items. Large libraries may take longer to sync. CrossWatch stores the catalogue in its local SQLite database and reuses it across pairs and features with the same server, user credentials, and selected libraries. Credentials are hashed into the cache identity, not stored in the catalogue.

Catalogue reuse checks library scan and content-change timestamps on Plex, and item counts plus a verified metadata-change filter on Jellyfin/Emby. Jellyfin/Emby checks use the server HTTP date with a two-minute overlap instead of the CrossWatch clock. Servers that ignore or reject the filter, or omit the required timestamps, use the five-minute fallback. A changed signal or a catalogue older than 24 hours triggers a complete refresh. If the server offers no usable change signal, reuse is limited to five minutes. Within one adapter, checks are cached for up to 90 seconds. Cache-read and change-check errors trigger a full catalogue read. Cache-write errors still allow the freshly read catalogue to be used. Failed or incomplete full reads do not replace a complete catalogue or serve stale data. Unused catalogues are pruned after seven days when a new catalogue is saved.

This is a periodically refreshed index, not webhook-driven indexing: changes can remain unseen during the short cache window. Existing snapshots gain path metadata as they refresh; no manual database migration is needed.

Strict ID matching is also under **General → Matching**, with a separate control for each supported provider in the pair. Plex **Marked Watched** is under **History → Plex** and is saved for this pair only. Pairs without a Marked Watched override inherit the existing Plex default.

The file-path fallback was inspired by [WatchState’s path-matching feature](https://github.com/arabcoders/watchstate/blob/master/guides/path-match.md).

Maintenance → Retry provider items also clears saved file-path catalogues and invalidates their in-memory copies. The preview shows catalogue count and stored payload size. Catalogues rebuild when next needed; sync baselines remain intact.
