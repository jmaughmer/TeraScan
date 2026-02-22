#!/bin/bash
set -o errexit
set -o pipefail

## Variables ##
# Timestamp for log and output files
STAMP=$(date +'%Y%m%d_%H%M%S')

# Default values
DEFAULT_TELEMETRY_CHAIN=3
DEFAULT_TELEMETRY_DATA=npp
DEFAULT_AZ_START=0
DEFAULT_AZ_END=3600
DEFAULT_AZ_STEP=10
DEFAULT_EL_START=0
DEFAULT_EL_END=0
DEFAULT_EL_STEP=0
DEFAULT_CSV_OUTPUT="${HOME}/antenna_move_test_output_${STAMP}.csv"

# Initialize with defaults (so validation/parsing never sees empty values)
TELEMETRY_CHAIN="$DEFAULT_TELEMETRY_CHAIN"
TELEMETRY_DATA="$DEFAULT_TELEMETRY_DATA"
AZ_START="$DEFAULT_AZ_START"
AZ_END="$DEFAULT_AZ_END"
AZ_STEP="$DEFAULT_AZ_STEP"
EL_START="$DEFAULT_EL_START"
EL_END="$DEFAULT_EL_END"
EL_STEP="$DEFAULT_EL_STEP"
CSV_OUTPUT="$DEFAULT_CSV_OUTPUT"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

require_arg() {
  local opt="$1"
  local val="${2-}"
  if [[ -z "$val" ]]; then
    die "Missing value for $opt (run with --help)"
  fi
}

is_uint() {
  [[ "${1-}" =~ ^[0-9]+$ ]]
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -c|--telemetry-chain)
      require_arg "$1" "${2-}"
      TELEMETRY_CHAIN="$2"
      shift 2
      ;;
    -o|--csv-output)
      require_arg "$1" "${2-}"
      CSV_OUTPUT="$2"
      shift 2
      ;;
    -t|--telemetry-data)
      require_arg "$1" "${2-}"
      TELEMETRY_DATA="$2"
      shift 2
      ;;
    --az-start)
      require_arg "$1" "${2-}"
      AZ_START="$2"
      shift 2
      ;;
    --az-end)
      require_arg "$1" "${2-}"
      AZ_END="$2"
      shift 2
      ;;
    --az-step)
      require_arg "$1" "${2-}"
      AZ_STEP="$2"
      shift 2
      ;;
    --el-start)
      require_arg "$1" "${2-}"
      EL_START="$2"
      shift 2
      ;;
    --el-end)
      require_arg "$1" "${2-}"
      EL_END="$2"
      shift 2
      ;;
    --el-step)
      require_arg "$1" "${2-}"
      EL_STEP="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [OPTIONS]"
      echo "Options:"
      echo "  -c, --telemetry-chain  Telemetry chain (default: $DEFAULT_TELEMETRY_CHAIN)"
      echo "  -o, --csv-output       CSV output file (default: $DEFAULT_CSV_OUTPUT)"
      echo "  -t, --telemetry-data   Telemetry data (default: $DEFAULT_TELEMETRY_DATA)"
      echo "      --az-start         Azimuth start (default: $DEFAULT_AZ_START)"
      echo "      --az-end           Azimuth end (default: $DEFAULT_AZ_END)"
      echo "      --az-step          Azimuth step (default: $DEFAULT_AZ_STEP)"
      echo "      --el-start         Elevation start (default: $DEFAULT_EL_START)"
      echo "      --el-end           Elevation end (default: $DEFAULT_EL_END)"
      echo "      --el-step          Elevation step (default: $DEFAULT_EL_STEP)"
      echo "  -h, --help             Show this help message"
      exit 0
      ;;
    *)
      die "Unknown option: $1 (run with --help)"
      ;;
  esac
done
## End Variables ##

## Logging ##
mkdir -p "${HOME}/logs"
LOG="antenna_move_test-${STAMP}.log"
MAINLOG="${HOME}/logs/${LOG}"

# send all output to the console and log file
exec 3>&1 1>>"$MAINLOG" 2>&1

# logging function
f_log () {
  local NOW
  NOW=$(date +'%Y-%m-%d %H:%M:%S.%3N %Z')
  echo "${NOW} - ${1-}" | tee /dev/fd/3
}
## End Logging ##

## Input Validation ##
command -v ac >/dev/null 2>&1 || die "'ac' command not found in PATH"
command -v bc >/dev/null 2>&1 || die "'bc' command not found in PATH"
command -v setqdc >/dev/null 2>&1 || die "'setqdc' command not found in PATH"

is_uint "$TELEMETRY_CHAIN" || die "TELEMETRY_CHAIN must be an integer"
[[ -n "${TELEMETRY_DATA:-}" ]] || die "TELEMETRY_DATA must be a non-empty string"
is_uint "$AZ_START" || die "AZ_START must be an integer"
is_uint "$AZ_END" || die "AZ_END must be an integer"
is_uint "$AZ_STEP" || die "AZ_STEP must be an integer"
is_uint "$EL_START" || die "EL_START must be an integer"
is_uint "$EL_END" || die "EL_END must be an integer"
is_uint "$EL_STEP" || die "EL_STEP must be an integer"

if (( AZ_START > 3600 )); then
  die "AZ_START must be between 0 and 3600"
fi
if (( AZ_END > 3600 )); then
  die "AZ_END must be between 0 and 3600"
fi
if (( AZ_START == AZ_END )); then
  die "AZ_START and AZ_END must be different"
fi
if (( AZ_STEP <= 0 )); then
  die "AZ_STEP must be greater than 0"
fi

if (( EL_START > 900 )); then
  die "EL_START must be between 0 and 900"
fi
if (( EL_END > 900 )); then
  die "EL_END must be between 0 and 900"
fi

# Only require a non-zero EL_STEP when we actually intend to move elevation
if (( EL_START != EL_END && EL_STEP <= 0 )); then
  die "EL_STEP must be greater than 0 when EL_START and EL_END differ"
fi
## End Input Validation ##

## Functions ##
to_tenths() {
  # Convert a possibly-decimal degree value (e.g. "123.4") to integer tenths (1234).
  local val="${1-}"
  echo "scale=0; ($val*10)/1" | bc
}

abs_i() {
  local n="$1"
  if (( n < 0 )); then
    echo $(( -n ))
  else
    echo "$n"
  fi
}

# check_current_position sets three implicit globals used by callers:
#   CUR_AZ    – current azimuth reading (empty string if unparseable)
#   CUR_EL    – current elevation reading (empty string if unparseable)
#   AGC_LEVEL – current AGC level (empty string if unparseable)
check_current_position () {
  local AGC POS
  POS=$(ac "$TELEMETRY_CHAIN" P || true)
  AGC=$(ac "$TELEMETRY_CHAIN" % || true)
  CUR_AZ=$(echo "$POS" | grep -o 'Az:[[:space:]]*[0-9.]*' | grep -o '[0-9.]*' || true)
  CUR_EL=$(echo "$POS" | grep -o 'El:[[:space:]]*[0-9.]*' | grep -o '[0-9.]*' || true)
  AGC_LEVEL=$(echo "$AGC" | grep -o 'Reply:L[0-9]*' | grep -o '[0-9]*' || true)
  f_log "Current Position: Az: ${CUR_AZ:-N/A} El: ${CUR_EL:-N/A} AGC: ${AGC_LEVEL:-N/A}"
}

check_test_position () {
  while true; do
    sleep 1

    local POS AZ EL
    POS=$(ac "$TELEMETRY_CHAIN" P || true)
    AZ=$(echo "$POS" | grep -o 'Az:[[:space:]]*[0-9.]*' | grep -o '[0-9.]*' || true)
    EL=$(echo "$POS" | grep -o 'El:[[:space:]]*[0-9.]*' | grep -o '[0-9.]*' || true)

    if [[ -z "${AZ:-}" || -z "${EL:-}" ]]; then
      f_log "Could not parse position from telemetry output; retrying"
      continue
    fi

    local AZ_TENTHS EL_TENTHS
    AZ_TENTHS=$(to_tenths "$AZ")
    EL_TENTHS=$(to_tenths "$EL")

    # Calculate azimuth difference considering circular nature (0-3600 tenths)
    local AZ_DIFF_RAW AZ_DIFF_ABS AZ_DIFF_WRAP AZ_DIFF
    AZ_DIFF_RAW=$(( AZ_TENTHS - AZ_TEST_START ))
    AZ_DIFF_ABS=$(abs_i "$AZ_DIFF_RAW")
    AZ_DIFF_WRAP=$(( 3600 - AZ_DIFF_ABS ))
    if (( AZ_DIFF_WRAP < AZ_DIFF_ABS )); then
      AZ_DIFF="$AZ_DIFF_WRAP"
    else
      AZ_DIFF="$AZ_DIFF_ABS"
    fi

    # Calculate elevation difference
    local EL_DIFF_RAW EL_DIFF
    EL_DIFF_RAW=$(( EL_TENTHS - EL_TEST_START ))
    EL_DIFF=$(abs_i "$EL_DIFF_RAW")

    f_log "Current position - Az: $AZ (${AZ_TENTHS} tenths), El: $EL (${EL_TENTHS} tenths)"
    f_log "Differences - Az: $AZ_DIFF tenths, El: $EL_DIFF tenths"

    # Check if within tolerance
    if (( AZ_DIFF <= 20 && EL_DIFF <= 20 )); then
      f_log "Antenna is in position within tolerance"
      break
    fi
  done
}

configure_downconverter () {
  f_log "Configuring downconverter on chain $TELEMETRY_CHAIN for data type ${TELEMETRY_DATA}"
  setqdc "$TELEMETRY_CHAIN" "$TELEMETRY_DATA"  | tee /dev/fd/3
}

init_csv_output () {
  f_log "Creating CSV output file: $CSV_OUTPUT"
  echo "Timestamp,Configured Azimuth,Configured Elevation,Actual Azimuth,Actual Elevation,AGC" > "$CSV_OUTPUT"
}

test_azimuth_movement () {
  local i
  if (( AZ_START < AZ_END )); then
    f_log "Moving azimuth clockwise"
    i=$AZ_START
    while (( i <= AZ_END )); do
      AZ_TEST_START="$i"
      f_log "Setting azimuth: $i"
      ac "$TELEMETRY_CHAIN" "A$i" | tee /dev/fd/3
      f_log "Checking current position"
      check_current_position
      if [[ -z "${CUR_AZ:-}" || -z "${CUR_EL:-}" ]]; then
        f_log "WARNING: Could not parse position; skipping CSV row for azimuth $i"
      else
        echo "$(date +'%FT%TZ'),$i,$EL_TEST_START,$(to_tenths "$CUR_AZ"),$(to_tenths "$CUR_EL"),${AGC_LEVEL:-}" >> "$CSV_OUTPUT"
      fi
      i=$(( i + AZ_STEP ))
    done
  else
    f_log "Moving azimuth counterclockwise"
    i=$AZ_START
    while (( i >= AZ_END )); do
      AZ_TEST_START="$i"
      f_log "Setting azimuth: $i"
      ac "$TELEMETRY_CHAIN" "A$i" | tee /dev/fd/3
      f_log "Checking current position"
      check_current_position
      if [[ -z "${CUR_AZ:-}" || -z "${CUR_EL:-}" ]]; then
        f_log "WARNING: Could not parse position; skipping CSV row for azimuth $i"
      else
        echo "$(date +'%FT%TZ'),$i,$EL_TEST_START,$(to_tenths "$CUR_AZ"),$(to_tenths "$CUR_EL"),${AGC_LEVEL:-}" >> "$CSV_OUTPUT"
      fi
      i=$(( i - AZ_STEP ))
    done
  fi
}

## End Functions ##

## Main ##
f_log "Logging to $MAINLOG"
f_log ""
f_log "Begin antenna movement test with the following parameters"
f_log "Telemetry Chain = $TELEMETRY_CHAIN"
f_log "Telemetry Data = $TELEMETRY_DATA"
f_log "Azimuth Start = $AZ_START"
f_log "Azimuth End = $AZ_END"
f_log "Azimuth Movement = $AZ_STEP"
f_log "Elevation Start = $EL_START"
f_log "Elevation End = $EL_END"
f_log "Elevation Movement = $EL_STEP"
f_log ""
f_log "Checking current position"
check_current_position
f_log "Configuring downconverter for telemetry chain $TELEMETRY_CHAIN and data type $TELEMETRY_DATA"
configure_downconverter
f_log "Setting starting azimuth, AZ=$AZ_START"
ac "$TELEMETRY_CHAIN" "A$AZ_START"
f_log "Setting starting elevation, EL=$EL_START"
ac "$TELEMETRY_CHAIN" "E$EL_START"
f_log "Starting movement test and checking background AGC signal levels."
init_csv_output
AZ_TEST_START=$AZ_START
EL_TEST_START=$EL_START
check_test_position

if (( EL_START < EL_END )); then
  i=$EL_START
  while (( i <= EL_END )); do
    f_log "Moving elevation to $i (stepping up towards $EL_END)"
    ac "$TELEMETRY_CHAIN" "E$i" | tee /dev/fd/3
    EL_TEST_START="$i"
    f_log "Getting current position"
    check_test_position
    test_azimuth_movement
    i=$(( i + EL_STEP ))
  done
elif (( EL_START > EL_END )); then
  i=$EL_START
  while (( i >= EL_END )); do
    f_log "Moving elevation to $i (stepping down towards $EL_END)"
    ac "$TELEMETRY_CHAIN" "E$i" | tee /dev/fd/3
    EL_TEST_START="$i"
    f_log "Getting current position"
    check_test_position
    test_azimuth_movement
    i=$(( i - EL_STEP ))
  done
else
  f_log "Elevation start and end are the same; skipping elevation movement."
  test_azimuth_movement
fi

f_log "Antenna movement test complete."
## End Main ##
