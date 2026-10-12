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

## CLI-only container mode

Set `CW_CLI_ONLY=1` when you want CrossWatch to keep the API, CLI, webhooks and auth callbacks available, but stop serving the web UI.

Outside a container, run it from the install root:

```
python cli/cw.py status
python -m cli status
```

## First Setup

Fresh install or deleted config? Set the CrossWatch admin password before using the API commands:

```
cw --local auth setup --username admin
```

By default this also creates and saves a CLI API token. Use `--no-token` if you only want to set the password.

## Tokens

Without app authentication configured, CrossWatch only exposes health/status/setup endpoints remotely. Use `cw --local auth setup` from the CrossWatch host or container for CLI-only installs.

With auth on you need a token. You can create another one locally from the CrossWatch host or container:

```
cw --local auth token create --name cli
```

Shown once. Saved to `/config/.cw_cli/cli.json` in the container, `~/.crosswatch/cli.json` outside it. Every command after that is authenticated.

For scripts and CI, keep the token out of the output by writing it to a file:

```
cw --local auth token create --name monitor --no-save --write-to /run/secrets/cw_token
cw --local auth token create --name monitor --no-save --write-to cw.env --format env
```

Only the id, name and expiry are printed. An existing file is left alone unless you pass `--force`, which replaces it in one step. The default layout is the bare token with no trailing newline, `--format env` writes `CW_TOKEN=<token>`.

On Linux and macOS the file is created with mode 0600. Windows has no such mode, the file gets the permissions of the folder it lands in, so pick a folder only you can read.

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

With `--local`, or when the service cannot be reached, the CLI cannot see live state. Sync, watcher, scheduler worker and ready to sync then show as unknown, and `watcher_alive`, `scheduler_running`, `sync_running`, `cancel_requested` and `can_run` are `null` in JSON.

## Pairs

```
cw pair create plex trakt --feature watchlist --feature ratings
cw pair create simkl trakt --mode two-way
cw pair create plex trakt --feature history --add-only
cw pair list [--enabled]
cw pair show <id>
cw pair enable <id>
cw pair disable <id>
cw pair feature <id> <feature> on|off
cw pair set <id> <feature>.<key> <value>
cw pair reorder <id> <id> ...
cw pair delete <id> [--yes]
```

Any unique id prefix works, so `cw pair show pair_07c3` is enough. The route name works too, `cw pair show "PLEX -> TRAKT"`.

`--add-only` creates every enabled feature with add on and remove off. `cw pair set` changes one option of a feature that already exists on the pair:

```
cw pair set <id> history.add on|off
cw pair set <id> history.remove on|off
cw pair set <id> history.remove_mode source_deletes|mirror
cw pair set <id> history.rewatches on|off
cw pair set <id> history.libraries PLEX=1,2        set the libraries of one provider
cw pair set <id> history.libraries PLEX=           clear them again
cw pair set <id> history.libraries '{"PLEX":["1"]}' replace the whole scope
```

`cw pair show` and `cw pair set` warn when a feature runs in mirror mode, also when it is only stored while Remove is off. The check follows the `sync.one_way_remove_mode` and `sync.enable_remove` defaults for features that do not set their own. In mirror mode Remove makes the target match the source, so everything missing from the source is removed from the target. With `-o json` the warnings are in the `warnings` list.

## Sync

```
cw sync list [--enabled]           run order, endpoints and enabled features
cw sync run                        every enabled pair
cw sync run 1                      just pair #1 from cw sync list
cw sync run pair_07c3              just the one by id or unique prefix
cw sync run --pair pair_07c3       same selector, option form
cw sync run --follow               run it and stream the log until it finishes
cw sync once                       local run, wait, return exit code
cw sync once --pair pair_07c3      local one-shot pair run
cw sync once --pair a --pair b     local one-shot selected pair run
cw sync once --feature watchlist   local one-shot feature-only run
cw sync once --dry-run --plan-out plan.json   write the planned changes per item
cw sync status                     current or last run, with counts
cw sync follow                     attach to a run already going
cw sync cancel                     stop after the current batch
cw sync unresolved                 what the last run could not match
cw sync providers [--counts]
```

`cw sync run` starts work through the running service. `cw sync once` runs in the current process, waits for completion, and exits with the sync result code, which is useful for `docker run --rm`, cron, Kubernetes CronJobs and other external schedulers. By default unresolved items do not fail the command; add `--fail-on-unresolved` for a stricter run.

`--plan-out PATH` works with `--dry-run` and writes every planned change to a JSON file without writing provider changes; providers are still read. If planning fails or is cancelled the file is not written and an existing one is kept. `--fail-on-skipped` cannot be combined with it, because a plan has no skipped count. The file has one entry per pair under `pairs`, and each entry is the same document as the JSON download in Interactive Sync, so `rows` holds the items with their `feature`, `operation`, `result` and `reason`. The counts in the command summary are taken from those rows.

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

Jellyfin, Emby, Kodi, Stremio, Floppy, Scrob, Tautulli, Tracearr, PublicMetaDB and TMDb ask for fields. Trakt, SIMKL and AniList want a client id and secret first. Pass them instead of typing them if you are scripting:

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
cw events calendar [--range 3m|6m|1y|60] [--metric changes|runs|failures]
cw events day [2026-09-05|today|yesterday]   the sync runs of one day
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
cw playlist setup --source-provider TRAKT --source-playlist LIST_ID --target-provider PLEX --create-target "Weekend"

cw playlist resource list PLEX
cw playlist resource create PLEX --name Weekend
cw playlist resource rename PLEX LIST_ID --name NewName
cw playlist resource delete PLEX LIST_ID

cw playlist endpoint list
cw playlist endpoint add PLEX LIST_ID --name PlexFavs
cw playlist endpoint edit EP-01 --playlist LIST_ID
cw playlist endpoint sync EP-01
cw playlist endpoint delete EP-01

cw playlist mapping list
cw playlist mapping add --source EP-01 --target EP-02 --name Movies
cw playlist mapping edit MAP-01 --membership mirror --order preserve
cw playlist mapping enable MAP-01
cw playlist mapping disable MAP-01
cw playlist mapping run MAP-01
cw playlist mapping preview MAP-01
cw playlist mapping result MAP-01
cw playlist mapping delete MAP-01

cw playlist ruleset list
cw playlist ruleset show trakt_free_account
cw playlist ruleset clone trakt_free_account --name MyRules
cw playlist ruleset add ruleset.json
cw playlist ruleset validate ruleset.json
cw playlist ruleset delete RULESET_ID
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
cw maintenance tools               show modal-style maintenance actions
cw maintenance database            runtime db health
cw maintenance events              archive health, --optimize, --purge, --rebuild
cw maintenance cache <what>        all, metadata, provider-sync, activity-log, scrobbles, state
cw maintenance provider-cache
cw maintenance provider-cleanup    clear provider watchlist, ratings, history, progress or collection
cw maintenance state-file --prune|--compact
cw maintenance tracker [--clear]
cw maintenance reset-pair [id]     start one sync pair from fresh baselines, -F to pick features
cw maintenance reset-stats
cw maintenance reset-watching
cw maintenance support [--scopes]
cw maintenance factory-reset       requires --confirm RESET
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
cw scheduler list                  advanced jobs
cw scheduler set hourly
cw scheduler set every 6h
cw scheduler set daily 03:30
cw scheduler set interval 45m
cw scheduler add PAIR_ID --at 00:00 --at 12:00
cw scheduler setup --kind pair --pair 1,3 --at 03:30
cw scheduler edit JOB_ID --at 03:30
cw scheduler pause JOB_ID
cw scheduler resume JOB_ID
cw scheduler delete JOB_ID
cw scheduler setup                 guided setup
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

Works without the service: `status`, `health`, `version`, all of `config`, `pair list/show/enable/disable/delete`, `sync list`, `scheduler status/next/show`, and every `auth token` command.

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
