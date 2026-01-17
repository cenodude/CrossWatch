# Scheduling

Enable periodic runs in **Settings → Scheduling**.

## Simple plan (default)
- **enabled**: `true|false`
- **mode**: `hourly | every_n_hours | daily_time`
- **every_n_hours**: integer `1–24` (used when `mode=every_n_hours`)
- **daily_time**: `HH:MM` (24-hour) in your configured timezone
- **Time (anchor)**: when combined with `every_n_hours`, this is the **first** run of the day; repeats every *N* hours from that anchor.  
  *Example:* `N=6`, `Time=03:30` → runs at **03:30, 09:30, 15:30, 21:30** local time.

**Recommendation:** start with every 24 hours.

---
## Advanced plan (sequential)
Toggle **Use advanced plan** to replace the simple cadence with an explicit, step-by-step schedule.

- **Pair** – which sync pair to run. Only **enabled** pairs are selectable; disabled are greyed out.
- **Time** – start time for this step (24-hour).  
  *Ignored* when **After** is set (see below).
- **Days** – check the weekdays this step may run.
- **After** – make this step start **immediately after** another step finishes. When set, the **Time** field is ignored.
- **Active** – per-step enable/disable without removing it.

Behavior:
- Steps run **top-to-bottom**, never in parallel. No overlap, no surprises.
- “Always sequential. Times are user-defined.” If two steps share the same time, order still follows the list.
- **Auto-create from enabled pairs** – generates a skeleton plan (one step per enabled pair) you can edit.
- **Add step** – append a new row to the plan.

Tips:
- Keep long-running pairs earlier; use **After** to chain dependent pairs.
- Use per-step **Active** to pause experiments without touching the global toggle.
