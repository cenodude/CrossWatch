# Scripts

The folder `/scripts/` contains **standalone helper scripts** for CrossWatch.

They are **not used by the CrossWatch app or sync engine**.  
You run them manually when you need to inspect, fix, clean, or snapshot a provider.

> **Important:** The “backup” feature in these scripts is **not a full server backup**.  
> It only exports user-facing state (history/ratings/watchlist) via the provider APIs.  
> It does **not** touch or dump provider databases, metadata stores, or server config.  
> Think of it as a *CrossWatch-level snapshot*, not a disaster-recovery backup.

Sometimes you want a clean provider state without touching CrossWatch itself.

Examples:
- You tested syncs and want to wipe the mess.
- You’re switching your source of truth and want a fresh start.
- Your watched / rating data got corrupted and you’d rather reset than debug.
- You want a quick snapshot before doing something destructive.

These scripts are the “do it once, manually, carefully” tools.

## Requirements

Scripts read your existing CrossWatch config:

`/config/config.json`

So you don’t need to re-enter credentials, **but provider blocks must be present**.

You’ll get a menu with numbered actions.
