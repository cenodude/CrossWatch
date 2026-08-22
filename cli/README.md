# CrossWatch CLI

`cw` does what the web UI does, from a terminal. Status, config, provider logins, syncs, watcher, scheduler, logs, plus the analyzer, events, captures, backups, playlists, the editor and the rest.

It runs in the same container as CrossWatch and talks to the running service over the API. So a sync you start here shows up in the web UI, logs to the same place, and respects the sync already running guard. If the service is down, the read only stuff still works by reading the install directly.

```
docker exec -it crosswatch cw status
docker exec -it crosswatch cw sync run --follow
docker exec -it crosswatch cw shell
```

Inside the container, `cw` is installed on `PATH`, so the current directory does not matter:

```
cw status
cw auth token create --local
```

Outside a container, run it from the install root:

```
python cli/cw.py status
python -m cli status
```

## Tokens

No auth enabled in CrossWatch? Nothing to do, it just works.

With auth on you need a token. Use `--local` for the first one, that writes straight to the config so it works before you can authenticate at all:

```
cw --local auth token create --name cli
```

Shown once. Saved to `/config/.cw_cli/cli.json` in the container, `~/.crosswatch/cli.json` outside it. Every command after that is authenticated.

Per command or from the env if you prefer:

```
cw --token cwt_... status
CW_TOKEN=cwt_... cw status
```

```
cw auth token whoami
cw auth token list
cw auth token revoke <id>
```

Tokens are hashed at rest, inherit the permissions of the user they belong to, and land in the audit log like any UI action. Add `--expires-days` if you want them to lapse.

## Global options

These work anywhere on the line, before or after the subcommand.

```
-U, --url            base URL, falls back to CW_URL then the local install
-T, --token          API token, falls back to CW_TOKEN then the saved one
-o, --output         auto, table, json, yaml, plain
-L, --local          use this install, never the API
-k, --insecure       skip TLS checks, for self signed certs
    --http-timeout   seconds, default 30
-q, --quiet
    --no-color
```

`-o json` and `-o plain` are the script friendly ones. JSON on stdout, nothing decorative, errors on stderr.

```
cw pair list -o json | jq ".[] | select(.enabled) | .id"
cw auth token list -o plain | cut -f1
```

## Status

```
cw status                    everything at a glance
cw status --fresh            re-probe providers instead of the cached status
cw status --no-providers     skip the provider table
cw version                   version, plus every provider module version
cw health                    is anything answering
```

## Pairs

```
cw pair create plex trakt --feature watchlist --feature ratings
cw pair create simkl trakt --mode two-way
cw pair list [--enabled]
cw pair show <id>
cw pair enable <id>
cw pair disable <id>
cw pair feature <id> <feature> on|off
cw pair reorder <id> <id> ...
cw pair delete <id> [--yes]
```

Any unique id prefix works, so `cw pair show pair_07c3` is enough. The route name works too, `cw pair show "PLEX -> TRAKT"`.

## Sync

```
cw sync run                        every enabled pair
cw sync run --pair pair_07c3       just the one
cw sync run --follow               run it and stream the log until it finishes
cw sync status                     current or last run, with counts
cw sync follow                     attach to a run already going
cw sync cancel                     stop after the current step
cw sync unresolved                 what the last run could not match
cw sync providers [--counts]
```

`--follow` passes through the exit code from the sync, handy in cron or a health check.

## Config

```
cw config show [path]              all of it, or one subtree, secrets masked
cw config get sync.anime.enabled
cw config set sync.anime.enabled true
cw config set some.list "[1,2,3]" --json
cw config unset some.key
cw config edit                     opens $EDITOR
cw config meta                     the schema the UI uses
cw config migrate                  bring an old config up to date
cw config path                     where config, state and the db live
```

`set` is a merge patch, it only touches the key you name. Values get parsed, so `true`, `yes`, `on`, numbers and `null` all do the obvious thing, everything else stays a string. It will not let you clobber an object or a list with a scalar unless you pass `--json`.

`unset` actually removes the key. A merge patch cannot delete, so that one goes through its own endpoint. Anything under `app_auth` is off limits.

## Provider logins

All 18 providers work from here. Look before you leap:

```
cw auth providers                  everything, its flow and what it wants
cw auth show jellyfin              the fields for one provider
cw auth list [--fresh]             what is connected right now
```

`cw auth login <provider>` sorts out the flow itself, so the command is the same whichever kind it is:

```
cw auth login trakt                prints a code, waits for you to approve it
cw auth login plex --no-wait       same, without blocking
cw auth login jellyfin             asks for server, username, password
cw auth logout simkl
```

Plex, Trakt, SIMKL, MDBList, PunchPlay, BingeBase and Nuvio give you a code and a URL, then poll until they report connected or `--timeout` runs out.

Jellyfin, Emby, Kodi, Stremio, Floppy, Scrob, Tautulli, PublicMetaDB and TMDb ask for fields. Trakt, SIMKL and AniList want a client id and secret first. Pass them instead of typing them if you are scripting:

```
cw auth login jellyfin \
  --field jellyfin.server=http://192.168.2.100:8096 \
  --field jellyfin.username=pascal \
  --field jellyfin.password="$JF_PASSWORD" \
  --non-interactive
```

Field names come from the provider manifest, so `cw auth show <provider>` is the list that counts. The prefix is optional, `--field username=bob` is fine. `--non-interactive` exits 2 instead of prompting.

AniList is the odd one, it needs the client id and secret, then a browser to finish. The CLI prints the URL and waits for the callback.

`--instance <id>` everywhere, for multi instance setups.

## Analyzer

Finds items that are stuck or disagree between providers.

```
cw analyzer problems               what it thinks is wrong
cw analyzer attention              mismatches, pending retries, blocked
cw analyzer ratings                where ratings disagree
cw analyzer activity               per pair
cw analyzer detail <provider> <feature> <key>
cw analyzer suggest <provider> <feature> <key>
cw analyzer fix <provider> <feature> <key>
cw analyzer drop <provider> <feature> <key>
cw analyzer tracker
```

Most of them take `--pairs id1,id2` to narrow the scope.

## Events

```
cw events status
cw events recent [--domain scrobble] [--view events]
cw events search "dragon ball" --provider TRAKT
cw events groups
cw events show <group_id>
cw events run <run_id>             everything one sync run recorded
cw events item <item_key>          the history of one item
cw events stats --range 7d
cw events ack <group_id> [--undo]
cw events clear
```

## Captures

The rollback tool.

```
cw capture list
cw capture create [--provider PLEX] [--feature watchlist]
cw capture diff <older> <newer>
cw capture read <path>
cw capture restore <path> [--dry-run]
cw capture delete <path>
cw capture clear
```

`capture create` waits for the job and prints progress. `--no-wait` if you would rather not.

## Backups

```
cw backup list
cw backup create --note "before the big sync"
cw backup validate <path>
cw backup restore <path>
cw backup delete <path>
cw backup schedule [--enable] [--every-hours 24]
cw backup retention 10
```

## Watchlist and playback

```
cw watchlist list [--type movie] [--search dune]
cw watchlist remove <key> [<key> ...]

cw progress list [--provider PLEX] [--min 10]
cw progress providers
cw progress settings
cw progress watched <key>
cw progress set <key> 45
cw progress remove <key>
```

## Editor

```
cw editor list --kind watchlist [--provider PLEX]
cw editor sources
cw editor providers
cw editor send <key> --provider TRAKT
cw editor export
```

## Playlists

```
cw playlist overview
cw playlist providers
cw playlist resources PLEX
cw playlist activity

cw playlist endpoint list|add|sync|delete
cw playlist mapping list|add|run|preview|result|delete
cw playlist ruleset list|show|delete
```

`cw playlist mapping run <id> --dry-run` first, always.

## Import and export

```
cw export options
cw export preview --provider PLEX --feature watchlist
cw export file out.csv --provider PLEX --feature watchlist

cw import options
cw import preview letterboxd.csv
cw import commit <import_id> --features watchlist
```

## Metadata and manual entry

```
cw metadata search "blade runner" --year 1982
cw metadata resolve imdb=tt0083658
cw metadata providers

cw manual providers
cw manual watched --field imdb=tt0083658 --field type=movie --provider TRAKT
```

## Anime mapping

```
cw anime status
cw anime update [--rebuild]
cw anime overrides
cw anime add-override \
  --field match_provider=tvdb --field match_id=81472 \
  --field target_namespace=anidb --field target_id=4563
cw anime delete-override <rule_id>
cw anime search "dragon ball z"
cw anime export
```

## Instances and profiles

```
cw instance list [PLEX] [--configured]
cw instance add plex --field server=http://...
cw instance set plex PLEX-P01 --field label=Living room
cw instance delete plex PLEX-P01

cw user-profile list|show|create|set|delete
```

## Scrobbler

```
cw scrobbler overview
cw scrobbler event-routes
cw scrobbler route add|set|delete
cw scrobbler webhook urls
cw scrobbler webhook regenerate
cw scrobbler webhook cleanup-legacy
```

## Reporting

```
cw insights
cw stats [--raw]
cw activity recent
cw activity history [--type movie] [--search dune]
cw activity clear
```

## Maintenance

```
cw maintenance database            runtime db health
cw maintenance events              archive health, --optimize, --rebuild
cw maintenance cache <what>        all, metadata, provider-sync, activity-log, scrobbles, state
cw maintenance provider-cache
cw maintenance state-file --prune|--compact
cw maintenance tracker [--clear]
cw maintenance reset-stats
cw maintenance reset-watching
cw maintenance support [--scopes]
cw maintenance restart
```

## Watcher

```
cw watcher status
cw watcher start
cw watcher stop
cw watcher restart                 reload config and restart the routes
cw watcher now                     what is playing
cw watcher logs [-n 200]
```

## Scheduler

```
cw scheduler status
cw scheduler next
cw scheduler enable
cw scheduler disable
cw scheduler run-now               fire it now
cw scheduler replan                recompute the next run time
cw scheduler stop                  stop the worker, keep the config
cw scheduler show                  raw scheduling config
```

## Logs

```
cw logs tail                       last 200 lines of SYNC
cw logs tail -t WATCH -n 500
cw logs tail -f                    follow it live
cw logs tail -f --grep "ERROR|WARN"
cw logs channels
```

## Shell

```
cw shell
```

Groups nest, so you stop retyping the prefix:

```
cw> sync
cw(sync)> ?
cw(sync)> status
cw(sync)> run --follow
cw(sync)> !status          run something top level without leaving
cw(sync)> exit
cw> config
cw(config)> get sync.anime.enabled
cw(config)> exit
cw> exit
```

`?` shows what you can run here, `help <command>` gives the full help, `exit` or `end` or `..` steps back out. History goes to `~/.crosswatch/history`. Tab completion works where readline does.

## Exit codes

```
0   ok
1   failed
2   bad usage
3   cannot reach CrossWatch
4   not allowed
5   not found
6   busy, a sync is already running
```

`cw sync run --follow` passes through the exit code from the sync instead.

## When the service is down

Read only and repair commands keep going by reading the install. You get a note on stderr and the endpoint shows as `local (fallback)`:

```
! Cannot reach CrossWatch at http://127.0.0.1:8787 - answering from the local install instead
```

Works without the service: `status`, `health`, `version`, all of `config`, `pair list/show/enable/disable/delete`, `scheduler status/next/show`, and every `auth token` command.

Anything that needs the engine running, so starting a sync, poking the watcher, streaming logs, fails with exit 3 instead of doing something the UI cannot see. `--local` forces that mode and turns those into an error straight away.

## Layout

```
cli/
  cw.py            entry point
  _app.py          Typer app, argument hoisting, error handling
  _context.py      per invocation state, API first with local fallback
  _transport.py    HTTP client and SSE log streaming
  _local.py        in-process transport for the offline subset
  _settings.py     URL and token resolution, cli.json
  _render.py       table, key value, JSON, YAML and plain output
  _util.py         dotted paths, value parsing, formatting, pair lookup
  _errors.py       error types and exit codes
  commands/        one module per group
```

New command group means a `commands/<name>.py` with a `register(app)` function, listed in `_app._register_all`. Commands go through `Ctx` rather than a transport, that is what gets them the fallback for free.
