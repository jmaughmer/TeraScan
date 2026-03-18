# cosched.py

Merges one or more TeraScan satellite pass schedule files, deduplicates overlapping passes, and distributes the resulting passes across one or more gap-constrained output channels. Each output channel is then pushed to TeraScan via `clearsched` + `mansched` (locally for channel 1, over SSH for additional channels).

Remote scheduling is submitted in a single SSH batch per remote channel to reduce connection overhead and timeout risk on high-latency links.

## Dependencies

| Command / Module | Purpose |
|-----------------|---------|
| Python 3.6+ | Runtime (compatible with RHEL 7 system Python or SCL `rh-python36`) |
| `listsched` | TeraScan binary – reads the current pass schedule (fetch mode only) |
| `clearsched` | TeraScan binary – clears the existing schedule before loading a new one |
| `mansched` | TeraScan binary – loads individual passes into TeraScan |
| `ssh` | OpenSSH client – used when pushing to remote channels |

All TeraScan binaries are expected at `/opt/terascan/bin/`. Remote SSH commands source `/opt/terascan/etc/tscan.bash_profile` before executing.

> **RHEL 6 note:** Python 3 requires Software Collections.  
> `yum install rh-python36 && scl enable rh-python36 bash`  
> Change the shebang to `#!/usr/bin/env /opt/rh/rh-python36/root/usr/bin/python3`.

## Usage

### File mode

Provide one or more pre-existing schedule files as positional arguments:

```
cosched.py <input1> [<input2> ...] [OPTIONS]
```

> **Tip:** Use `--sat-priority SAT=PRIORITY` to override the scheduled priority for specific satellites (see [Satellite priority overrides](#satellite-priority-overrides)).

Automatically retrieve schedules by running `listsched` locally and on each `--remote-host`, then schedule the combined result:

```
cosched.py --fetch [--remote-host user@host] [--remote-host user@host2] [OPTIONS]
```

In fetch mode, each fetched schedule is written to `/tmp/<hostname>.sched` before being used as an input.

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `--fetch` | off | Fetch schedules via `listsched` instead of reading files |
| `--out <path>` | `cosched_out_N` next to first input | Output path for channel N (repeat once per channel) |
| `--gap <int>` | `190` | Minimum gap in seconds between consecutive passes on a channel |
| `--max-trim <int>` | `180` | Maximum total duration reduction (seconds) allowed per pass |
| `--max-start-delay <int>` | `180` | Maximum start delay (seconds) allowed when pushing a pass later |
| `--timeout-secs <int>` | `120` | Timeout in seconds for each subprocess call (`listsched`, `clearsched`, `mansched`, `ssh`) |
| `--ssh-connect-timeout <int>` | `30` | SSH connection timeout in seconds |
| `--ssh-keepalive-interval <int>` | `30` | SSH `ServerAliveInterval` in seconds |
| `--ssh-keepalive-count-max <int>` | `3` | SSH `ServerAliveCountMax` |
| `--remote-host <host>` | — | SSH host for channel 2, 3, … (repeat for each additional channel) |
| `--exclude-sat <sat>` | — | Exclude a satellite from **all** channels (repeat for multiple) |
| `--local-exclude-sat <sat>` | — | Exclude a satellite from the local channel (channel 1) only (repeat for multiple) |
| `--remote-exclude-sat <sat>` | — | Exclude a satellite from all remote channels (channels 2+) only (repeat for multiple) |
| `--sat-priority <sat>=<pri>` | — | Override the priority for a satellite across **all** channels (repeat for multiple; case-insensitive) |

## Satellite exclusion

Passes for specific satellites can be excluded from scheduling on a per-channel basis. Exclusions are case-insensitive and are applied during the scheduling phase, so a pass excluded from one channel can still be placed on a channel where it is not excluded.

| Option | Scope |
|--------|-------|
| `--exclude-sat <sat>` | Excluded from all channels |
| `--local-exclude-sat <sat>` | Excluded from channel 1 (local) only |
| `--remote-exclude-sat <sat>` | Excluded from channels 2+ (remote) only |

All three options may be repeated to exclude multiple satellites.

## Satellite priority overrides

Use `--sat-priority SAT=PRIORITY` to override the priority recorded in the input schedule for a specific satellite. Overrides are applied to all channels after scheduling and before writing output files and submitting to `mansched`. Multiple satellites may be overridden by repeating the option.

```bash
# Lower metop-3 to priority 2 and raise aqua to priority 1
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

Duplicate passes (same date, time, satellite, and telemetry) are deduplicated by keeping the entry with the lowest priority value.

## Scheduling algorithm

Passes are assigned to channels using a **greedy, earliest-start** strategy:

1. Passes whose original start time is in the past are skipped.
2. For each pass (processed in start-time order), every channel is evaluated.
3. For a channel whose previous pass ends too close to the new pass:
   - If the required delay is ≤ `--max-start-delay`, the pass start is pushed forward.
   - Otherwise, the previous pass duration is trimmed (in 10-second steps) to create the gap. If the required trim would exceed `--max-trim`, the pass is skipped on that channel.
4. The channel that yields the **earliest adjusted start** is chosen; ties are broken by longest adjusted duration, then lowest channel index.
5. Start times are rounded up to the nearest 10-second boundary; durations are rounded down to the nearest 10-second boundary.

## Channel-to-host mapping

| Channel | Push target |
|---------|------------|
| 1 | Local (`clearsched` + `mansched`) |
| 2 | `--remote-host` \#1 (SSH) |
| 3 | `--remote-host` \#2 (SSH) |
| … | … |

Channels without a corresponding `--remote-host` are written to the output file but not pushed remotely.

## Telemetry-to-chain mapping

Telemetry chain mapping is loaded from `/opt/terascan/pass/config/system.config` when available.

- Numeric section names (for example `[1]`, `[2]`, ...) are treated as chain IDs.
- `telemetry.name` values in those sections are mapped (case-insensitive) to chain IDs.
- If no usable mapping is found, `cosched.py` falls back to the built-in defaults below.

| Telemetry | Chain |
|-----------|-------|
| `aquadb` | 1 |
| `nppdb` | 2 |
| `jpssdb` | 3 |
| `jpss2db` | 4 |
| `ahrpt` | 5 |
| `rtd` | 6 |

Unrecognized telemetry strings default to chain 1.

## Output file format

Each channel is written as a text file using the same column layout as the input:

```
#  state  pri  satel    telem       date    day    time    durat  post_process

 1  sched   3  metop-3   ahrpt    2025/08/18 230  19:09:00  12:50
```

Pass states are normalized to `sched`; start times and durations reflect any scheduling adjustments.

### Unscheduled passes

After each run, passes that were not placed on any channel are written to `/tmp/cosched_not_scheduled` in the same format. A pass ends up in this file when:

- Its original start time is already in the past, **or**
- Every channel rejects it because the required start delay would exceed `--max-start-delay` and trimming the previous pass on every channel would exceed `--max-trim`.

## Examples

### Single-host fetch and schedule

```bash
python3 cosched.py --fetch
```

Fetches the local schedule, builds one channel, clears the local TeraScan schedule, and loads it.

### Two-host fetch and co-schedule

```bash
python3 cosched.py --fetch \
  --remote-host user@antenna2.example.com
```

Fetches schedules from the local host and `antenna2`, merges and deduplicates them, builds two channels, loads channel 1 locally, and loads channel 2 on `antenna2` via SSH.

### Increase SSH/subprocess timeouts for slow links

```bash
python3 cosched.py --fetch \
  --remote-host user@antenna2.example.com \
  --timeout-secs 300 \
  --ssh-connect-timeout 60 \
  --ssh-keepalive-interval 30 \
  --ssh-keepalive-count-max 5
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

All calls to `listsched`, `clearsched`, `mansched`, and remote SSH commands use `--timeout-secs` (default: 120).

SSH behavior is additionally controlled by:

- `--ssh-connect-timeout` (default: 30)
- `--ssh-keepalive-interval` (default: 30)
- `--ssh-keepalive-count-max` (default: 3)

Timeouts and subprocess failures are logged to stdout/stderr. The scheduler continues where possible and reports per-host failures.
