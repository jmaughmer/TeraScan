# get_terascan_schedules.py

Retrieves the current TeraScan pass schedule from the local host and, optionally, from a remote host over SSH. Each schedule is written to `/tmp/<hostname>.sched` for inspection or use as input to other tools (e.g., [cosched.py](cosched.md)).

Compatible with Python 3.6+ (RHEL 7).

## Dependencies

| Command / Module | Purpose |
|-----------------|---------|
| Python 3.6+ | Runtime |
| `listsched` | TeraScan binary at `/opt/terascan/bin/listsched` – dumps the current schedule |
| `ssh` | OpenSSH client – required only when `--remote` is supplied |

The local invocation sources `/opt/terascan/etc/tscan.bash_profile` via a `bash -c` wrapper so that TeraScan environment variables are available. The remote invocation uses the same profile source over SSH.

## Usage

```
get_terascan_schedules.py [--remote <host>]
```

### Options

| Option | Description |
|--------|-------------|
| `-r, --remote <host>` | SSH target (`host` or `user@host`) to also retrieve a schedule from |

## Behavior

1. Runs `listsched` locally (via `bash -c 'source …; listsched'`) and writes output to `/tmp/<local_hostname>.sched`.
2. If `--remote` is given, runs the same command on the remote host via SSH (key-based auth, `BatchMode=yes`) and writes output to `/tmp/<remote_hostname>.sched`.
3. Prints the path of each written file to stdout.
4. Exits with status `1` if any invocation fails; status `0` if all succeed.

Each call times out after 120 seconds.

## Output files

| File | Contents |
|------|---------|
| `/tmp/<local_hostname>.sched` | Raw `listsched` output from the local host |
| `/tmp/<remote_hostname>.sched` | Raw `listsched` output from the remote host (if `--remote` used) |

The hostname component is derived from the host string by stripping any `user@` prefix and `:port` suffix, then replacing characters outside `[A-Za-z0-9_.-]` with `_`.

## Examples

### Local schedule only

```bash
python3 get_terascan_schedules.py
# Wrote local schedule to /tmp/antenna1.sched
```

### Local and remote schedule

```bash
python3 get_terascan_schedules.py --remote ops@antenna2.example.com
# Wrote local schedule to /tmp/antenna1.sched
# Wrote remote schedule to /tmp/antenna2.example.com.sched
```

### Use with cosched.py

```bash
python3 get_terascan_schedules.py --remote ops@antenna2.example.com
python3 cosched.py /tmp/antenna1.sched /tmp/antenna2.example.com.sched \
  --remote-host ops@antenna2.example.com
```

## Error handling

| Error | Cause |
|-------|-------|
| `listsched not found at /opt/terascan/bin/listsched` | Binary missing or wrong path |
| `listsched timed out after 120s` | `listsched` did not complete within the timeout |
| `listsched failed (exit N): <stderr>` | `listsched` exited non-zero |
| `ssh not found on PATH` | OpenSSH client unavailable |
| `ssh to <host> timed out after 120s` | SSH connection or remote command timed out |
| `ssh to <host> failed (exit N): <stderr>` | Remote command exited non-zero |

Errors are printed to `stderr`. When one invocation fails the other still runs; the script exits `1` if any failure occurred.
