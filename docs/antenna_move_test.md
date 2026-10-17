# antenna_move_test.sh

Sweeps an antenna through a configurable azimuth/elevation grid and records the actual position and AGC signal level at each commanded step. Results are written to a timestamped CSV file. All activity is also written to a timestamped log file under `~/logs/`.

## Dependencies

| Command | Purpose |
|---------|---------|
| `ac` | Antenna controller CLI – reads position and issues move commands |
| `bc` | Arbitrary-precision arithmetic – used for degree-to-tenths conversion |
| `setqdc` | Downconverter configuration CLI – sets the telemetry chain data type |

All three must be available on `PATH` before the script is run. The script will exit immediately with an error if any are missing.

> **Check which `ac` you get:** many Linux systems also have an unrelated `ac` command (connect-time accounting from the `psacct`/`acct` package). The script only checks that *an* `ac` exists, so make sure the TeraScan antenna controller `ac` comes first on `PATH` (`command -v ac`).

## Usage

```
antenna_move_test.sh [OPTIONS]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-c, --telemetry-chain <int>` | `3` | Telemetry chain number passed to `ac` and `setqdc` |
| `-t, --telemetry-data <string>` | `npp` | Data type identifier passed to `setqdc` |
| `--az-start <int>` | `0` | Starting azimuth in tenths of a degree (0–3600) |
| `--az-end <int>` | `3600` | Ending azimuth in tenths of a degree (0–3600) |
| `--az-step <int>` | `10` | Azimuth increment between steps in tenths of a degree (must be > 0) |
| `--el-start <int>` | `0` | Starting elevation in tenths of a degree (0–900) |
| `--el-end <int>` | `0` | Ending elevation in tenths of a degree (0–900) |
| `--el-step <int>` | `0` | Elevation increment between steps in tenths of a degree (required > 0 when el-start ≠ el-end) |
| `--position-timeout <int>` | `120` | Maximum seconds to wait for the antenna to reach each target position |
| `-o, --csv-output <path>` | `~/antenna_move_test_output_<STAMP>.csv` | Path for the CSV output file |
| `-h, --help` | | Print help and exit |

> **Note on units:** All azimuth and elevation values are in **tenths of a degree**.  
> For example, `--az-start 900` means 90.0° and `--el-end 450` means 45.0°.

## Outputs

### CSV file

Written to the path given by `--csv-output`; an existing file at that path is overwritten. One row is appended for each commanded azimuth position. If the position can't be parsed from `ac` after the antenna settles, that row is skipped and a warning is logged.

```
Timestamp,Configured Azimuth,Configured Elevation,Actual Azimuth,Actual Elevation,AGC
2026-02-22T18:00:00Z,900,0,902,1,87
```

| Column | Description |
|--------|-------------|
| `Timestamp` | UTC timestamp in ISO-8601 format at the time of the reading |
| `Configured Azimuth` | The azimuth value commanded (tenths of a degree) |
| `Configured Elevation` | The elevation value in effect at the time (tenths of a degree) |
| `Actual Azimuth` | Azimuth reported by `ac`, converted to tenths of a degree |
| `Actual Elevation` | Elevation reported by `ac`, converted to tenths of a degree |
| `AGC` | AGC level reported by `ac` |

### Log file

Written to `~/logs/antenna_move_test-<STAMP>.log`. All `f_log` messages (including command output piped through `tee`) are recorded here. The same messages are also echoed to the terminal. Error messages are written both to the log and to the terminal's stderr.

## Behavior

1. **Validation** – All inputs are validated before any antenna commands are issued.
2. **Current position** – The starting position and AGC level are read (`ac <chain> P` and `ac <chain> %`) and logged. This does not move the antenna.
3. **Downconverter configuration** – `setqdc` is called once to configure the telemetry chain.
4. **Initial positioning** – The antenna is commanded to `AZ_START` / `EL_START` and the script waits until it arrives (within ±2.0° in both azimuth and elevation).
5. **Sweep loop** – If elevation range is non-trivial (`EL_START ≠ EL_END`), the outer loop steps through elevation; the inner loop sweeps azimuth at each elevation. If elevation is fixed, only the azimuth sweep runs.
   - Azimuth is swept **clockwise** when `AZ_START < AZ_END`, **counterclockwise** otherwise.
   - Elevation is stepped **up** when `EL_START < EL_END`, **down** otherwise.
   - Each sweep visits the start value, then every step up to and including the end value. If the range is not a multiple of the step, the last position is the last step before the end value. For example, `--az-start 0 --az-end 3600 --az-step 70` stops at 3570.
   - In a 2-D sweep, every elevation row restarts at `AZ_START`, so the antenna slews back across the whole azimuth range between rows. Allow for that slew in `--position-timeout`.
6. **Position check** – After each azimuth command the script waits for the antenna to settle within tolerance (±2.0°; the azimuth check handles the 0°/360° wrap), then reads back the actual position and AGC level and appends a row to the CSV. If the antenna does not settle within `--position-timeout` seconds the script aborts. The timeout counts one-second polls, and each poll also runs `ac`, so the real wait can be somewhat longer.

## Safety notes

- The script moves the antenna but does not check the TeraScan schedule. Make sure no pass is scheduled on the chain for the duration of the test.
- If the script is interrupted (for example with Ctrl-C) or stops on an error, the antenna stays at its last commanded position. Nothing returns it to a stow or starting position.

## Examples

### Scan full azimuth range at a fixed elevation

```bash
./antenna_move_test.sh \
  --az-start 0 \
  --az-end 3600 \
  --az-step 100
```

### Scan a 90°–180° azimuth window at 30° elevation

```bash
./antenna_move_test.sh \
  --az-start 900 \
  --az-end 1800 \
  --az-step 50 \
  --el-start 300 \
  --el-end 300
```

### 2-D azimuth/elevation grid sweep

```bash
./antenna_move_test.sh \
  --az-start 0 \
  --az-end 3600 \
  --az-step 100 \
  --el-start 0 \
  --el-end 450 \
  --el-step 50 \
  --telemetry-chain 2 \
  --telemetry-data noaa19 \
  --csv-output /tmp/grid_sweep.csv
```

## Error handling

The script uses `set -o errexit` and `set -o pipefail`, so any unexpected command failure will abort execution immediately. The `die` helper prints an `ERROR:`-prefixed message to the terminal's `stderr` before exiting with status 1. Errors raised after logging starts (everything except option-parsing errors such as `Missing value for <opt>`) are also recorded in the log file.

Common error messages:

| Message | Cause |
|---------|-------|
| `'<cmd>' command not found in PATH` | `ac`, `bc` or `setqdc` is not installed or not on `PATH` |
| `<NAME> must be an integer` | A numeric option was given a non-integer (or negative) value |
| `AZ_START must be between 0 and 3600` (and similar) | An azimuth value is above 3600 or an elevation value is above 900 |
| `AZ_START and AZ_END must be different` | Both values are identical; no movement would occur |
| `AZ_STEP must be greater than 0` | Zero or negative step would cause an infinite loop |
| `EL_STEP must be greater than 0 when EL_START and EL_END differ` | Elevation range specified but no step size given |
| `Antenna did not reach target position within <N>s` | The antenna failed to settle within the `--position-timeout` window |
| `Missing value for <opt>` | An option flag was provided without a following value |
| `Unknown option: <opt>` | An unrecognized option was given |

The script exits with status `0` on success and `1` on any `die` error. If a command such as `ac` or `setqdc` fails unexpectedly, `errexit` stops the script with that command's exit status.
