# TeraScan

A collection of scripts for controlling and testing TeraScan ground station antenna systems.

## Scripts

| Script | Description |
|--------|-------------|
| [antenna_move_test.sh](scripts/antenna_move_test.sh) | Sweeps an antenna through a configurable azimuth/elevation grid, logging position and AGC signal level at each step to a CSV file. |
| [cosched.py](scripts/cosched.py) | Merges one or more TeraScan pass schedule files, deduplicates passes, and distributes them across gap-constrained channels pushed to TeraScan via `mansched`. |
| [get_terascan_schedules.py](scripts/get_terascan_schedules.py) | Runs `listsched` locally (and optionally on a remote host via SSH) and writes each schedule to `/tmp/<hostname>.sched`. |

## Documentation

- [antenna_move_test.sh](docs/antenna_move_test.md)
- [cosched.py](docs/cosched.md)
- [get_terascan_schedules.py](docs/get_terascan_schedules.md)

## Requirements

- Bash 4+
- `bc` – arbitrary-precision arithmetic (antenna_move_test.sh)
- `ac` – antenna controller CLI (antenna_move_test.sh)
- `setqdc` – downconverter configuration CLI (antenna_move_test.sh)
- Python 3.6+ (cosched.py, get_terascan_schedules.py)
- TeraScan installation at `/opt/terascan` with `listsched`, `clearsched`, and `mansched` binaries
- OpenSSH client (`ssh`) for remote operations
