# get_terascan_schedules.py

Retrieves the current TeraScan pass schedule from the local host and, optionally, from a remote host over SSH. Each schedule is written to `/tmp/<hostname>.sched` for inspection or use as input to other tools (e.g., [cosched.py](cosched.md)).

Compatible with Python 3.6+ (RHEL 7).

> **Relation to `cosched.py --fetch`:** `cosched.py` fetches schedules itself in fetch mode, runs `resched` first, and uses its own file names (`/tmp/local_<hostname>.sched`, `/tmp/remote_<host>.sched`). Use this script when you only want to look at the current schedules, or to prepare input files for `cosched.py` file mode.

## Dependencies

| Command / Module | Purpose |
|-----------------|---------|
| Python 3.6+ | Runtime |
| `listsched` | TeraScan binary at `/opt/terascan/bin/listsched` – dumps the current schedule |
| `ssh` | OpenSSH client – required only when `--remote` is supplied |

The local invocation runs `bash -c 'source /opt/terascan/etc/tscan.bash_profile; /opt/terascan/bin/listsched'` so that TeraScan environment variables are available. Because of the `;`, `listsched` still runs if sourcing the profile fails.

The remote invocation sends `source /opt/terascan/etc/tscan.bash_profile && /opt/terascan/bin/listsched` over SSH. It runs in the remote user's login shell, not a `bash` wrapper, so that shell must support `source` (bash, zsh and tcsh all do). Because of the `&&`, `listsched` does not run there if sourcing fails.

## Usage

```
get_terascan_schedules.py [--remote <host>]
```

### Options

| Option | Description |
|--------|-------------|
| `-r, --remote <host>` | SSH target (`host`, `user@host`, or a `Host` alias from `~/.ssh/config`) to also retrieve a schedule from |

`ssh` does not accept `host:port`; to use a non-default port, key or user, define a `Host` alias in `~/.ssh/config` and pass the alias.

## Behavior

1. Runs `listsched` locally (via `bash -c 'source …; listsched'`) and writes output to `/tmp/<local_hostname>.sched`.
2. If `--remote` is given, runs `source … && listsched` on the remote host via SSH and writes output to `/tmp/<remote_hostname>.sched`. SSH uses key-based login with `BatchMode=yes` (no prompts) and `ConnectTimeout=30`.
3. Prints the path of each written file to stdout.
4. Exits with status `1` if any invocation fails; status `0` if all succeed.

Each call times out after 120 seconds; the timeouts are fixed and have no command-line options. Unlike `cosched.py --fetch`, this script does not run `resched` first, and it does not treat a schedule with no pass entries as a failure.

## Output files

| File | Contents |
|------|---------|
| `/tmp/<local_hostname>.sched` | Raw `listsched` output from the local host |
| `/tmp/<remote_hostname>.sched` | Raw `listsched` output from the remote host (if `--remote` used) |

The hostname component is derived from the host string by stripping any `user@` prefix and `:port` suffix, then replacing characters outside `[A-Za-z0-9_.-]` with `_`.

Existing files are overwritten. If the remote label works out to the same name as the local hostname (for example, `--remote ops@antenna1` run on `antenna1`), the remote schedule overwrites the local one.

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

This is `cosched.py` file mode: `resched` is not run first. To fetch and co-schedule in one step, use `python3 cosched.py --fetch --remote-host ops@antenna2.example.com` instead.

## Error handling

| Error | Cause |
|-------|-------|
| `listsched failed (exit 127): … No such file or directory` | `listsched` is missing from `/opt/terascan/bin/` |
| `listsched failed (exit N): <stderr>` | `listsched` exited non-zero |
| `listsched timed out after 120s` | `listsched` did not complete within the timeout |
| `listsched not found at /opt/terascan/bin/listsched` | `bash` itself could not be started (despite the wording, this is not about `listsched`) |
| `ssh not found on PATH` | OpenSSH client unavailable |
| `ssh to <host> timed out after 120s` | SSH connection or remote command timed out |
| `ssh to <host> failed (exit N): <stderr>` | Remote command exited non-zero |

Errors are printed to `stderr`. When one invocation fails the other still runs; the script exits `1` if any failure occurred.
