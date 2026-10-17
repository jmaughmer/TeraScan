# TeraScan

A collection of scripts for controlling and testing TeraScan ground station antenna systems.

## Scripts

| Script | Description |
|--------|-------------|
| [antenna_move_test.sh](scripts/antenna_move_test.sh) | Sweeps an antenna through a configurable azimuth/elevation grid, logging position and AGC signal level at each step to a CSV file. |
| [cosched.py](scripts/cosched.py) | Merges TeraScan pass schedules from one or more stations (from files, or fetched with `--fetch`), deduplicates passes, and distributes them across gap-constrained channels that are loaded back onto the local and remote stations via `clearsched` + `mansched`. |
| [get_terascan_schedules.py](scripts/get_terascan_schedules.py) | Runs `listsched` locally (and optionally on a remote host via SSH) and writes each schedule to `/tmp/<hostname>.sched` for inspection or as `cosched.py` input. |

## Documentation

- [antenna_move_test.sh](docs/antenna_move_test.md)
- [cosched.py](docs/cosched.md)
- [get_terascan_schedules.py](docs/get_terascan_schedules.md)

## Requirements

**antenna_move_test.sh**

- Bash 4+ and GNU `date`
- `ac` – TeraScan antenna controller CLI. Make sure it comes before any unrelated `ac` command on `PATH`; see the [script docs](docs/antenna_move_test.md#dependencies).
- `bc` – arbitrary-precision arithmetic
- `setqdc` – downconverter configuration CLI

**cosched.py and get_terascan_schedules.py**

- Python 3.6+ (RHEL 7 system Python; see [cosched.md](docs/cosched.md#dependencies) for RHEL 6)
- TeraScan installation at `/opt/terascan` with `listsched` (both scripts) and `resched`, `clearsched` and `mansched` (`cosched.py`)
- `/opt/terascan/etc/tscan.bash_profile` on every station
- `/opt/terascan/pass/config/system.config` on every station for telemetry-to-chain mapping (`cosched.py` falls back to built-in defaults if it is missing)
- OpenSSH client (`ssh`) with key-based login to each remote station; SSH runs in batch mode, so it must not prompt

## Testing

Run the regression tests (Python 3.6+) from the repository root with:

```bash
python -m unittest discover -s tests
```

Tests that run shell code (the `antenna_move_test.sh` tests and the `cosched.py` remote-batch script tests) need `bash`. They use stub commands and temporary files, so they don't need antenna hardware or TeraScan, and they are skipped when `bash` is not available.

## License

GNU General Public License v3.0; see [LICENSE](LICENSE).
