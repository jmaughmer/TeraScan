# cosched.py

Merges one or more TeraScan satellite pass schedule files, deduplicates overlapping passes, and distributes the resulting passes across one or more gap-constrained output channels. In file mode, channel 1 is pushed locally via `clearsched` + `mansched` and additional channels are pushed over SSH according to `--remote-host`. In fetch mode, each successfully fetched channel is pushed back to the source it was fetched from, so partial fetch failures do not remap channels onto the wrong targets. A fetched schedule that contains only the header and no pass entries is treated as a failed fetch and is not scheduled.

Passes whose effective duration would floor or trim to zero seconds are treated as unscheduled and written to `/tmp/cosched_not_scheduled` instead of remaining on an output channel.

Remote scheduling is submitted in a single SSH batch per remote channel to reduce connection overhead and timeout risk on high-latency links. The batch stops before any `mansched` runs if the TeraScan profile cannot be sourced, and stops at the first `mansched` that fails.

## Dependencies

| Command / Module | Purpose |
|-----------------|---------|
| Python 3.6+ | Runtime (compatible with RHEL 7 system Python or SCL `rh-python36`) |
| `resched` | TeraScan binary – refreshes schedule state before fetching (fetch mode; enabled by default) |
| `listsched` | TeraScan binary – reads the current pass schedule (fetch mode only) |
| `clearsched` | TeraScan binary – clears the existing schedule before loading a new one |
| `mansched` | TeraScan binary – loads individual passes into TeraScan |
| `ssh` | OpenSSH client – used to fetch from, read `system.config` on, and push to remote hosts |

All TeraScan binaries are expected at `/opt/terascan/bin/`. `resched` and `listsched` (locally and over SSH) and the remote `clearsched` and `mansched` run through `bash` after sourcing `/opt/terascan/etc/tscan.bash_profile`. The local `clearsched` and `mansched` are run directly, without sourcing the profile.

> **RHEL 6 note:** Python 3 requires Software Collections.  
> `yum install rh-python36 && scl enable rh-python36 bash`  
> Change the shebang to `#!/usr/bin/env /opt/rh/rh-python36/root/usr/bin/python3`.

## Usage

### File mode

Provide one or more pre-existing schedule files as positional arguments. One output channel is created per input file:

```
cosched.py <input1> [<input2> ...] [OPTIONS]
```

> **Tip:** Use `--sat-priority SAT=PRIORITY` to override the scheduled priority for specific satellites (see [Satellite priority overrides](#satellite-priority-overrides)).

### Fetch mode

Automatically retrieve schedules by running `resched` then `listsched` locally and on each `--remote-host`, then schedule the combined result:

```
cosched.py --fetch [--remote-host user@host] [--remote-host user@host2] [OPTIONS]
```

In fetch mode, each fetched schedule is written under `/tmp/` with a source-prefixed label
(`local_<hostname>.sched` for local fetches and `remote_<host>.sched` for remote fetches)
before being used as an input. If labels would collide, a numeric suffix is appended to keep
them unique.
Use `--no-resched` to skip running `resched` before `listsched`. If `resched` is missing or not executable on a host, that host's fetch fails and it does not become a channel, so use `--no-resched` on systems without it. On each host, `resched` and `listsched` run in a single call and share one `--timeout-secs` timeout.

Output paths in fetch mode follow the source each channel came from, not the channel's position after failed fetches are dropped: the first `--out` (or default `cosched_out_1`) is always the local schedule, and each further `--out` (or `cosched_out_2`, `cosched_out_3`, …) matches a `--remote-host` in the order given. For example, with one `--remote-host`, if the local fetch fails, the remote schedule is still written to the second `--out` path (or `cosched_out_2`), never to the local one.

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `--fetch` | off | Fetch schedules via `listsched` instead of reading files |
| `--no-resched` | off | In fetch mode, skip running `resched` before `listsched` |
| `--out <path>` | `cosched_out_N` next to first input (`/tmp` in fetch mode) | Output path for channel N (repeat once per channel). In fetch mode, the first is the local schedule and each further one matches a `--remote-host` in order, even if some fetches fail |
| `--gap <int>` | `190` | Minimum gap in seconds between consecutive passes on a channel |
| `--max-trim <int>` | `180` | Maximum total duration reduction (seconds) allowed per pass |
| `--max-start-delay <int>` | `180` | Maximum start delay (seconds) allowed when pushing a pass later |
| `--timeout-secs <int>` | `120` | Timeout in seconds for each subprocess call (`listsched`, `clearsched`, `mansched`, `ssh`); remote `mansched` batches get more (see [Subprocess timeouts](#subprocess-timeouts)) |
| `--ssh-connect-timeout <int>` | `30` | SSH connection timeout in seconds |
| `--ssh-keepalive-interval <int>` | `30` | SSH `ServerAliveInterval` in seconds |
| `--ssh-keepalive-count-max <int>` | `3` | SSH `ServerAliveCountMax` |
| `--remote-batch-secs-per-pass <int>` | `10` | Extra timeout in seconds per pass for each remote `mansched` batch, added to `--timeout-secs` (must be ≥ 0) |
| `--remote-host <host>` | — | Remote station as an SSH target (repeat for each station). File mode: push target for channel 2, 3, … in order. Fetch mode: also a schedule source |
| `--exclude-sat <sat>` | — | Exclude a satellite from **all** channels (repeat for multiple) |
| `--local-exclude-sat <sat>` | — | Exclude a satellite from the local target only (repeat for multiple) |
| `--remote-exclude-sat <sat>` | — | Exclude a satellite from remote targets only (repeat for multiple) |
| `--sat-priority <sat>=<pri>` | — | Override the priority recorded for a satellite across **all** channels (repeat for multiple; case-insensitive). Applied after scheduling; see [Satellite priority overrides](#satellite-priority-overrides) |

### Remote host targets

`--remote-host` accepts any destination `ssh` accepts: `host`, `user@host`, or a `Host` alias from `~/.ssh/config`. `ssh` does not accept `host:port`; to use a non-default port, key or user, define a `Host` alias in `~/.ssh/config` and pass the alias. SSH runs with `BatchMode=yes`, so key-based login must already work without any prompt.

## Satellite exclusion

Passes for specific satellites can be excluded from scheduling on a per-channel basis. Exclusions are case-insensitive and are applied during the scheduling phase, so a pass excluded from one channel can still be placed on a channel where it is not excluded.

| Option | Scope |
|--------|-------|
| `--exclude-sat <sat>` | Excluded from all channels |
| `--local-exclude-sat <sat>` | Excluded from the local target only |
| `--remote-exclude-sat <sat>` | Excluded from remote targets only |

All three options may be repeated to exclude multiple satellites.

In file mode, the local target is channel 1 and remote targets are channels 2+. In fetch mode,
the local/remote exclusion scope follows the actual source of each successfully fetched channel,
so a surviving remote channel still gets remote exclusions even if an earlier local fetch failed.

## Satellite priority overrides

Use `--sat-priority SAT=PRIORITY` to override the priority recorded for a specific satellite. Overrides are applied to all channels after scheduling and before writing output files and submitting to `mansched`. Multiple satellites may be overridden by repeating the option.

Because overrides are applied *after* scheduling, they do not change which passes are scheduled or which channel they land on. The scheduler always uses the priorities from the input schedules. An override only changes the priority written to the channel output files and passed to `mansched`, which TeraScan then uses for its own conflict handling. Passes written to `/tmp/cosched_not_scheduled` keep their input priority.

```bash
# Record metop-3 as priority 2 and aqua as priority 1 in the outputs and in mansched
python3 cosched.py --fetch \
  --sat-priority metop-3=2 \
  --sat-priority aqua=1
```

Priority values follow TeraScan conventions (lower number = higher priority). Satellite names are matched case-insensitively.

## Input file format

Space-separated columns; lines beginning with `#` are headers or comments.

```
#  state  pri  satel    telem       date    day    time    durat  post_process
 1  sched   3  metop-3   ahrpt    2025/08/18 230  19:08:50  12:50
```

| Column | Description |
|--------|-------------|
| index | Entry number |
| state | Schedule state (`sched`, `confl`, etc.) – all states are treated as candidates |
| pri | Priority (lower value = higher priority) |
| satel | Satellite name |
| telem | Telemetry type |
| date | `YYYY/MM/DD` |
| day | Day of year |
| time | `HH:MM:SS` |
| durat | Duration as `MM:SS` |
| post_process | Ignored; not carried to output files or `mansched` |

Lines that do not match this layout are skipped without a warning.

Duplicate passes are identified by matching satellite and telemetry with start times within 30 seconds of each other. The pass with the earlier start time is kept; if start times are equal, the lower priority value (higher priority) wins.

## Scheduling algorithm

Passes are assigned to channels using a **priority-first, earliest-start** greedy strategy:

1. Passes whose original start time is in the past are skipped. "Now" is the host's local clock, so the host's time zone must match the time zone of the schedule times.
2. Passes are sorted by priority (lower value = higher priority), then by start time within each priority level, so higher-priority passes claim channel slots first.
3. For each pass, every eligible channel is evaluated for appending after its last scheduled pass:
   - If the required gap is already satisfied, the pass start is used as-is (ceiled to 10 s).
   - If the gap is short by ≤ `--max-start-delay`, the pass start is pushed forward.
   - Otherwise, the previous pass's duration is trimmed (in 10-second steps) to create the gap. If trimming would exceed `--max-trim`, that channel is skipped.
   - Only a pass that starts before the new pass's latest allowed start (original start + `--max-start-delay`) counts as the previous pass. If the channel's last pass starts later than that, it is never trimmed; the new pass is left to the insertion fallback in step 5.
   - If trimming reduces the previous pass to zero seconds, that already-scheduled pass is removed from the channel and written to `/tmp/cosched_not_scheduled`.
4. The channel that yields the **earliest adjusted start** is chosen; ties are broken by longest adjusted duration, then lowest channel index.
5. If no channel can accept the pass via append, an **insertion fallback** is attempted: every position within every channel is tested, optionally trimming the preceding pass to create the required leading gap (again, only if it starts before the new pass's latest allowed start), and splitting the trailing-gap adjustment between trimming the new pass's stop time and delaying the first following pass's start, so neither pass bears the full cost. The first following pass absorbs as much of the shortfall as its remaining start-delay budget allows; the new pass is trimmed for the remainder. If cascading the first following pass's delay into later passes would exceed any later pass's start-delay budget, the algorithm falls back to cascade-delaying all following passes without trimming the new pass. If all channels reject all insertion positions, the pass is unscheduled. Insertion positions that require **no changes to already-scheduled passes** are preferred over those that modify surrounding passes; within each group the earliest adjusted start wins, then longest duration.
6. If no insertion position is feasible on any channel, or if a pass would be reduced to zero duration after 10-second flooring/trim, the pass is written to `/tmp/cosched_not_scheduled`.
7. Start times are rounded up to the nearest 10-second boundary; durations are rounded down to the nearest 10-second boundary.

## Channel-to-host mapping

### File mode

| Channel | Push target |
|---------|------------|
| 1 | Local (`clearsched` + `mansched`) |
| 2 | `--remote-host` \#1 (SSH) |
| 3 | `--remote-host` \#2 (SSH) |
| … | … |

Channels without a corresponding `--remote-host` are written to the output file but not pushed remotely. The number of channels is the number of input files, so any `--remote-host` entries beyond the number of input files minus one are ignored.

### Fetch mode

Each successfully fetched schedule keeps the push target it came from.

- A locally fetched schedule is pushed locally.
- A schedule fetched from `--remote-host user@host` is pushed back to `user@host`.
- A fetched schedule with no pass entries is treated as a failed fetch and does not become a channel.
- If some fetches fail, the remaining channels are still pushed to their original sources rather than being remapped by their compressed channel number.

## Telemetry-to-chain mapping

Telemetry chain mapping is loaded from `/opt/terascan/pass/config/system.config`, separately for each station:

- The local channel uses the local `system.config`.
- Each remote channel uses that remote host's own `system.config`, read over SSH before any schedule is changed. Stations can number their chains differently, so a remote host's passes are never loaded with the local station's chain numbers.
- If a remote host cannot be reached while its `system.config` is being read, the run aborts before any station (local or remote) is cleared.
- Numeric section names (for example `[1]`, `[2]`, ...) are treated as chain IDs.
- `telemetry.name` values in those sections are mapped (case-insensitive) to chain IDs.
- If a station's `system.config` is missing or has no usable mapping, `cosched.py` falls back to the built-in defaults below for that station.

| Telemetry | Chain |
|-----------|-------|
| `aquadb` | 1 |
| `nppdb` | 2 |
| `jpssdb` | 3 |
| `jpss2db` | 4 |
| `ahrpt` | 5 |
| `rtd` | 6 |

Unrecognized telemetry strings default to chain 1, and a warning naming the telemetry is printed to stderr.

## Output file format

Each channel is written as a text file using the same column layout as the input (output files are overwritten on each run):

```
#  state  pri  satel    telem       date    day    time    durat  post_process

 1  sched   3  metop-3  ahrpt    2025/08/18 230  19:08:50  12:50
```

- Entries are renumbered from 1, pass states are normalized to `sched`, and no `post_process` value is written.
- Start times are rounded up and durations rounded down to 10-second boundaries, then any scheduling adjustments are applied. For example, a pass requested at `19:15:05` for `13:12` is written as `19:15:10` for `13:10`.
- Passes whose effective duration reaches zero are omitted from channel outputs and treated as unscheduled instead.

### Unscheduled passes

After each run, passes that were not placed on any channel are written to `/tmp/cosched_not_scheduled` in the same format, replacing the previous run's file. A pass ends up in this file when:

- Its original start time is already in the past, **or**
- Every channel rejects both the append attempt and all insertion positions because the required start delay would exceed `--max-start-delay` and/or trimming surrounding passes on every channel would exceed `--max-trim`, **or**
- Its effective duration would be reduced to zero after 10-second flooring or trimming, **or**
- It had already been placed, but was then trimmed to zero seconds as the previous pass of a later-processed pass.

In the last two cases, the unscheduled output preserves the original requested start time and duration.

## Examples

### Single-host fetch and schedule

```bash
python3 cosched.py --fetch
```

Fetches the local schedule, builds one channel, clears the local TeraScan schedule, and loads it.

If the fetched output contains only the header and no pass entries, the fetch is treated as failed and no local channel is scheduled.

### Two-host fetch and co-schedule

```bash
python3 cosched.py --fetch \
  --remote-host user@antenna2.example.com
```

Fetches schedules from the local host and `antenna2`, merges and deduplicates them, builds two channels, loads channel 1 locally, and loads channel 2 on `antenna2` via SSH.

If one fetch fails, only the successfully fetched schedules become channels, and each surviving channel is still pushed back to the source it came from.

The same rule applies when a fetch returns only the header with no pass entries.

### Increase SSH/subprocess timeouts for slow links

```bash
python3 cosched.py --fetch \
  --remote-host user@antenna2.example.com \
  --timeout-secs 300 \
  --ssh-connect-timeout 60 \
  --ssh-keepalive-interval 30 \
  --ssh-keepalive-count-max 5 \
  --remote-batch-secs-per-pass 20
```

### File mode with custom gap

```bash
python3 cosched.py /tmp/host1.sched /tmp/host2.sched \
  --gap 240 \
  --max-trim 120 \
  --max-start-delay 120 \
  --out /tmp/ch1_out \
  --out /tmp/ch2_out \
  --remote-host user@antenna2.example.com
```

### Override output paths in fetch mode

```bash
python3 cosched.py --fetch \
  --remote-host ops@remote-gs \
  --out /opt/schedules/local.sched \
  --out /opt/schedules/remote.sched
```

`local.sched` always receives the local schedule and `remote.sched` the schedule for `ops@remote-gs`. If the local fetch fails, nothing is written to `local.sched`; the remote schedule still goes to `remote.sched`.

### Exclude satellites from specific channels

```bash
# Exclude NOAA-20 from all channels, and metop-3 only from the local channel
python3 cosched.py --fetch \
  --remote-host user@antenna2.example.com \
  --exclude-sat noaa-20 \
  --local-exclude-sat metop-3
```

```bash
# Exclude a satellite from remote channels only (e.g. antenna2 cannot receive it)
python3 cosched.py --fetch \
  --remote-host user@antenna2.example.com \
  --remote-exclude-sat aqua
```

### Override satellite priorities

```bash
python3 cosched.py --fetch \
  --sat-priority metop-3=2 \
  --sat-priority noaa-20=1 \
  --sat-priority aqua=3
```

## Subprocess timeouts

All calls to `listsched`, `clearsched`, `mansched`, and remote SSH commands (including reading each remote `system.config`) use `--timeout-secs` (default: 120).

The one exception is the remote `mansched` batch. All of a remote host's passes are loaded in one SSH session, so a single 120-second budget would be shared by every `mansched` call, and a long schedule could time out after `clearsched` had already emptied that host. The batch timeout is therefore `--timeout-secs` + `--remote-batch-secs-per-pass` × number of passes. With the defaults, a 30-pass batch gets 120 + 10 × 30 = 420 seconds. Set `--remote-batch-secs-per-pass 0` to use the fixed `--timeout-secs` budget.

SSH behavior is additionally controlled by:

- `--ssh-connect-timeout` (default: 30)
- `--ssh-keepalive-interval` (default: 30)
- `--ssh-keepalive-count-max` (default: 3)

Timeouts and subprocess failures are logged to stdout/stderr. If any remote host cannot be reached when its `system.config` is read (just before the push phase), the run aborts without changing any schedule. Once the push phase begins, a `clearsched` or `mansched` failure aborts the run immediately so later channels are not modified after a failed push step.

Channels are pushed one after another and there is no rollback. If a push fails partway, channels pushed before it keep their new schedules, the failing station may be left partly loaded, and later channels keep their old schedules. The output files and, in fetch mode, the fetched `/tmp/*.sched` files remain available for recovery.

## Exit status

| Status | Meaning |
|--------|---------|
| `0` | All channels were written and every push target was updated (channels without a push target are only written) |
| `1` | No schedule could be fetched, a remote `system.config` could not be read, a `clearsched`/`mansched` step failed, or another error occurred (for example, an unreadable input file) |
| `2` | Invalid command-line arguments |
