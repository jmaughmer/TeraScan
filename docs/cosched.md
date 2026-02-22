# cosched.py

Merges one or more TeraScan satellite pass schedule files, deduplicates overlapping passes, and distributes the resulting passes across one or more gap-constrained output channels. Each output channel is then pushed to TeraScan via `clearsched` + `mansched` (locally for channel 1, over SSH for additional channels).

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

### Fetch mode

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
| `--remote-host <host>` | — | SSH host for channel 2, 3, … (repeat for each additional channel) |

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

The following default mapping is used when constructing `mansched` commands. Adjust in `telemetry_to_chain()` for site-specific configurations.

| Telemetry | Chain |
|-----------|-------|
| `teradb` | 1 |
| `aquadb` | 2 |
| `nppdb` | 3 |
| `jpssdb` | 4 |
| `mpt` | 5 |
| `hrtp` | 6 |
| `ahrpt` | 7 |
| `rtd` | 8 |

Unrecognized telemetry strings default to chain 1.

## Output file format

Each channel is written as a text file using the same column layout as the input:

```
#  state  pri  satel    telem       date    day    time    durat  post_process

 1  sched   3  metop-3   ahrpt    2025/08/18 230  19:09:00  12:50
```

Pass states are normalized to `sched`; start times and durations reflect any scheduling adjustments.

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

## Subprocess timeouts

All calls to `listsched`, `clearsched`, `mansched`, and remote SSH commands time out after 120 seconds (`TIMEOUT_SECS`). Timeouts are logged to stdout but do not abort the entire run.
