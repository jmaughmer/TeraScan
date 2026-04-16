#!/usr/bin/env python3
"""
Co-scheduler: Read one or more satellite schedule files, deduplicate passes, and build
one or more new schedules maximizing the number of passes while ensuring at least
190 seconds between passes on each schedule. Start times may only move forward
(in 10-second steps) and durations may only be reduced (in 10-second steps).

Scheduling rules:
- Do not delay a pass start by more than --max-start-delay seconds (default: 180).
    If more delay would be required to satisfy the gap to the previous pass, trim the
    previous pass duration (in 10-second steps) to create the gap. If even trimming the
    previous pass within the allowed trim cap cannot create the required gap without
    exceeding the start-delay cap, the pass is skipped.
- Do not reduce any pass's duration by more than --max-trim seconds in total
    relative to its original duration (default: 180 seconds).
- When inserting a pass between two existing passes, the required trailing gap is created
    by splitting the adjustment between trimming the new pass's stop time and delaying the
    first following pass's start, so neither pass bears the full cost. The first following
    pass absorbs as much as its remaining start-delay budget allows; the new pass is
    trimmed for the remainder. If the cascade from delaying the first following pass
    exceeds any later pass's start-delay budget, the algorithm falls back to cascade-
    delaying all following passes without trimming the new pass.
- Passes whose original start time is in the past are skipped.
- Assignment is greedy: passes are processed in priority order (lower number = higher
    priority), then by start time within each priority level. Each pass is placed on the
    channel that allows the earliest adjusted start via greedy append; ties resolved by
    longest duration, then lowest channel index. If append fails on all channels, an
    insertion fallback tries every position within each channel, adjusting surrounding
    passes only when necessary and preferring positions that require no such adjustments.

Input format (space-separated, header lines start with '#'):
#  state  pri  satel    telem       date    day    time    durat  post_process
 1  sched   3  metop-3   ahrpt    2025/08/18 230  19:08:50  12:50

Assumptions:
- Duplicate passes are identified by (satellite, telemetry) with start times within 30 seconds
  of each other. The earlier start time is kept; the lower priority value wins on ties.
- All passes (sched/confl/etc.) are candidates for scheduling.
- When adjusting, we adhere to 10-second granularity; times are ceiled, durations are floored.

Usage (file mode):
    cosched.py <input1> [<input2> ...] [--out <path>] [--out <path>] ... \
        [--gap 190] [--max-trim 180] [--max-start-delay 180] \
        [--timeout-secs 120] [--ssh-connect-timeout 30] \
        [--ssh-keepalive-interval 30] [--ssh-keepalive-count-max 3] \
        [--remote-host user@host] [--remote-host user@host2] ... \
        [--exclude-sat SAT] [--local-exclude-sat SAT] [--remote-exclude-sat SAT] ... \
        [--sat-priority SAT=PRIORITY] ...

Satellite exclusion:
- --exclude-sat SAT        Exclude a satellite from ALL channels (repeat for multiple).
- --local-exclude-sat SAT  Exclude a satellite from the local channel (channel 1) only.
- --remote-exclude-sat SAT Exclude a satellite from all remote channels (channels 2+) only.

Exclusions are case-insensitive and are applied during scheduling so excluded passes on
one channel can still be placed on channels where they are not excluded.

Satellite priority overrides:
- --sat-priority SAT=PRIORITY  Override the scheduled priority for a satellite across all
    channels (repeat for multiple; case-insensitive). Applied after scheduling, before
    writing output files and submitting to mansched. Takes precedence over priorities in
    the input schedule.

Usage (fetch mode):
    cosched.py --fetch [--remote-host user@host] [--remote-host user@host2] ...

In fetch mode, listsched is run locally and on each --remote-host. The results are written
to /tmp/<hostname>.sched and then used as the channel inputs automatically. The number of
channels equals the number of successfully fetched schedules that contain at least one
pass entry. Fetches that return only the header are treated as failures.

In both modes:
- Outputs default to sibling files next to the first input: cosched_out_1 ... cosched_out_N.
- In file mode, channel 1 is pushed locally via clearsched + mansched.
- In file mode, each additional channel maps to the corresponding --remote-host (in order)
    and is pushed over SSH. If more channels than --remote-host entries, the extra channels
    are written locally but not pushed remotely.
- In fetch mode, each successfully fetched channel is pushed back to the source it was
    fetched from, so partial fetch failures do not remap remote channels onto the wrong
    targets.
- clearsched/mansched failures abort the run immediately so later channels are not
    modified after a failed push step.
- Passes that could not be placed on any channel (past passes, or passes blocked by gap/trim
  constraints on all channels) are written to /tmp/cosched_not_scheduled in the same
  schedule file format.
- Remote mansched submissions are batched per host into a single SSH session to reduce
    connection overhead and timeout risk.

All subprocess calls (clearsched, mansched, listsched, remote SSH) use
--timeout-secs (default: 120). SSH connection and keepalive behavior are configurable
with --ssh-connect-timeout, --ssh-keepalive-interval, and
--ssh-keepalive-count-max. Remote commands source
/opt/terascan/etc/tscan.bash_profile before executing.

Compatible with Python 3.6+ (RHEL 7).

RHEL 6 Python 3 installation notes:
- Requires Software Collections (SCL) for Python 3.6+.
yum install rh-python36
scl enable rh-python36 bash

Change shebang to: #!/usr/bin/env /opt/rh/rh-python36/root/usr/bin/python3

"""

import argparse
import configparser
import os
import re
import shlex
import socket
import subprocess
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple


HEADER = "#  state  pri  satel    telem       date    day    time    durat  post_process"
TIMEOUT_SECS = 120  # per subprocess call
SSH_CONNECT_TIMEOUT = 30
SSH_SERVER_ALIVE_INTERVAL = 30
SSH_SERVER_ALIVE_COUNT_MAX = 3
LISTSCHED = "/opt/terascan/bin/listsched"   # listsched binary path
SCHED_DIR = "/tmp"                           # directory for fetched .sched files
SYSTEM_CONFIG = "/opt/terascan/pass/config/system.config"  # TeraScan system config
DEFAULT_CHAIN = 1

_FALLBACK_CHAIN_MAP = {
    "aquadb": 1,
    "nppdb": 2,
    "jpssdb": 3,
    "jpss2db": 4,
    "ahrpt": 5,
    "rtd": 6,
}


def load_telemetry_chain_map(path=SYSTEM_CONFIG, default=None):
    # type: (str, Optional[Dict[str, int]]) -> Dict[str, int]
    """Build a telemetry-name -> chain-number map from a TeraScan system.config file.

    Reads the INI-format system.config and returns a dict mapping lowercase
    telemetry names to their integer chain numbers derived from numeric section
    headers (e.g. ``[1]``, ``[2]``).  Falls back to *default* when the file
    cannot be read, is missing, or yields no usable entries.

    Args:
        path: Path to the system.config file (default: SYSTEM_CONFIG).
        default: Mapping returned when the file is unavailable or empty.
                 Defaults to _FALLBACK_CHAIN_MAP when not supplied.

    Returns:
        A dict mapping telemetry name (str, lowercase) -> chain number (int).
    """
    if default is None:
        default = _FALLBACK_CHAIN_MAP
    try:
        cfg = configparser.RawConfigParser()
        cfg.read(path)
        result = {}  # type: Dict[str, int]
        for section in cfg.sections():
            try:
                chain = int(section)
            except ValueError:
                continue  # skip non-numeric sections (e.g. [antenna-1], [system])
            if cfg.has_option(section, "telemetry.name"):
                telem = cfg.get(section, "telemetry.name").strip().lower()
                if telem:
                    result[telem] = chain
        return result if result else default
    except Exception as exc:
        print("WARNING: could not load telemetry chain map from {}: {}".format(path, exc),
              file=sys.stderr)
        return default


TELEMETRY_CHAIN_MAP = load_telemetry_chain_map()

class Pass:
    def __init__(
        self,
        idx,               # type: int
        state,             # type: str
        pri,               # type: int
        sat,               # type: str
        telem,             # type: str
        date_str,          # type: str
        doy,               # type: int
        time_str,          # type: str
        dur_str,           # type: str
        start,             # type: datetime
        dur_s,             # type: int
        out_start=None,    # type: Optional[datetime]
        out_dur_s=None,    # type: Optional[int]
    ):
        self.idx = idx
        self.state = state
        self.pri = pri
        self.sat = sat
        self.telem = telem
        self.date_str = date_str  # YYYY/MM/DD
        self.doy = doy
        self.time_str = time_str  # HH:MM:SS
        self.dur_str = dur_str    # MM:SS

        # Derived/parsed
        self.start = start        # type: datetime
        self.dur_s = dur_s        # type: int

        # For output after scheduling
        self.out_start = out_start      # type: Optional[datetime]
        self.out_dur_s = out_dur_s      # type: Optional[int]


_LINE_RE = re.compile(
    r"^\s*(?P<idx>\d+)\s+"
    r"(?P<state>\S+)\s+"
    r"(?P<pri>\d+)\s+"
    r"(?P<sat>\S+)\s+"
    r"(?P<telem>\S+)\s+"
    r"(?P<date>\d{4}/\d{2}/\d{2})\s+"
    r"(?P<doy>\d{1,3})\s+"
    r"(?P<time>\d{2}:\d{2}:\d{2})\s+"
    r"(?P<dur>\d{1,2}:\d{2})"
)


def parse_duration_to_seconds(s: str) -> int:
    """Convert a 'MM:SS' duration string to an integer number of seconds."""
    mm, ss = s.split(":")
    return int(mm) * 60 + int(ss)


def seconds_to_mmss(total_seconds: int) -> str:
    """Convert an integer number of seconds to a 'MM:SS' string. Clamps negative values to 0."""
    if total_seconds < 0:
        total_seconds = 0
    mm = total_seconds // 60
    ss = total_seconds % 60
    return f"{mm:02d}:{ss:02d}"


def ceil_to_next_10s(dt: datetime) -> datetime:
    """Round a datetime up to the nearest 10-second boundary (no-op if already on one)."""
    remainder = dt.second % 10
    if remainder == 0 and dt.microsecond == 0:
        return dt.replace(microsecond=0)
    add = 10 - remainder
    base = dt.replace(microsecond=0)
    return base + timedelta(seconds=add)


def floor_to_10s_seconds(x: int) -> int:
    """Round an integer number of seconds down to the nearest 10-second boundary."""
    return (x // 10) * 10


def floor_to_prev_10s(dt: datetime) -> datetime:
    """Round a datetime down to the previous 10-second boundary."""
    base = dt.replace(microsecond=0)
    return base - timedelta(seconds=base.second % 10)


def start_delay_seconds(original_start: datetime, adjusted_start: datetime) -> int:
    """Return the delay in seconds between the original and adjusted start times."""
    return int((adjusted_start - original_start).total_seconds())


def ceil_start_within_delay(
    original_start: datetime,
    earliest_start: datetime,
    max_start_delay: int,
) -> Optional[datetime]:
    """Return the earliest 10-second-aligned start that stays within the delay cap."""
    adjusted_start = ceil_to_next_10s(earliest_start)
    if start_delay_seconds(original_start, adjusted_start) > max_start_delay:
        return None
    return adjusted_start


def latest_start_within_delay(original_start: datetime, max_start_delay: int) -> Optional[datetime]:
    """Return the latest 10-second-aligned start that stays within the delay cap."""
    latest_start = floor_to_prev_10s(original_start + timedelta(seconds=max_start_delay))
    if latest_start < original_start:
        return None
    return latest_start


def parse_schedule(path: str) -> List[Pass]:
    """
    Parse a schedule file into a list of Pass objects.

    Reads the file at the given path line by line, ignoring blank lines and
    comments. Each matching line is parsed using the schedule line regex to
    extract fields such as index, state, priority, satellite, telemetry, date,
    day-of-year, time, and duration. The function computes the start datetime
    and duration in seconds, then constructs a Pass for each valid line.

    Args:
        path: Path to the schedule file.

    Returns:
        A list of Pass instances parsed from the file.
    """
    passes = []  # type: List[Pass]
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            m = _LINE_RE.search(line)
            if not m:
                continue
            idx = int(m.group("idx"))
            state = m.group("state")
            pri = int(m.group("pri"))
            sat = m.group("sat")
            telem = m.group("telem")
            date_str = m.group("date")
            doy = int(m.group("doy"))
            time_str = m.group("time")
            dur_str = m.group("dur")
            start = datetime.strptime(f"{date_str} {time_str}", "%Y/%m/%d %H:%M:%S")
            dur_s = parse_duration_to_seconds(dur_str)
            passes.append(Pass(idx, state, pri, sat, telem, date_str, doy, time_str, dur_str, start, dur_s))
    return passes


def schedule_content_has_passes(content: str) -> bool:
    """Return True when raw schedule content contains at least one pass entry."""
    for line in content.splitlines():
        if _LINE_RE.search(line):
            return True
    return False


def dedupe_passes(passes: List[Pass]) -> List[Pass]:
    """Deduplicate passes by satellite, telemetry, and approximate start time.

    Two passes are considered duplicates if they share the same satellite and
    telemetry and their start times differ by at most 30 seconds. When
    duplicates are found the pass with the earlier start time is kept; ties
    on start time are broken by lower priority value (higher priority), then
    first encountered.

    Returns the deduplicated passes sorted by start time.
    """
    NEAR_SECONDS = 30
    sorted_passes = sorted(passes, key=lambda x: x.start)
    result = []  # type: List[Pass]

    for p in sorted_passes:
        matched = False
        # Scan backward through already-accepted passes; once the time gap
        # exceeds NEAR_SECONDS all earlier entries are too far away.
        for i in range(len(result) - 1, -1, -1):
            existing = result[i]
            if (p.start - existing.start).total_seconds() > NEAR_SECONDS:
                break
            if existing.sat.lower() == p.sat.lower() and existing.telem.lower() == p.telem.lower():
                # Duplicate: existing has the earlier start (sorted order);
                # adopt p's priority if it is higher (lower number).
                if p.pri < existing.pri:
                    existing.pri = p.pri
                matched = True
                break
        if not matched:
            result.append(p)

    return sorted(result, key=lambda x: x.start)


def format_output_line(idx: int, state: str, pri: int, sat: str, telem: str, dt: datetime, dur_s: int) -> str:
    """
    Format a single schedule entry as a fixed-width output line.

    Args:
        idx: The entry index for ordering.
        state: The schedule state label.
        pri: The priority value.
        sat: The satellite identifier.
        telem: The telemetry mode/identifier.
        dt: The datetime of the scheduled start.
        dur_s: The duration in seconds.

    Returns:
        A formatted string containing index, state, priority, satellite, telemetry,
        date, day-of-year, time, and duration in mm:ss.
    """
    date_str = dt.strftime("%Y/%m/%d")
    time_str = dt.strftime("%H:%M:%S")
    doy = dt.timetuple().tm_yday
    dur_str = seconds_to_mmss(dur_s)
    return (
        f"{idx:>2}  {state:<6} {pri:>2}  "
        f"{sat:<8} {telem:<7}  "
        f"{date_str} {doy:>3}  {time_str}  {dur_str}  "
    )


def _find_insertion(
    ch_passes,        # type: List[Pass]
    p,                # type: Pass
    gap_seconds,      # type: int
    max_trim_10,      # type: int
    max_start_delay,  # type: int
):
    # type: (...) -> Optional[Tuple[int, datetime, int, List[Tuple[int, Optional[datetime], Optional[int]]]]]
    """Find a valid mid-channel insertion position for pass p.

    Tries each insertion index from 0 to len(ch_passes)-1 (appending after the last
    pass is already handled by the primary loop and is therefore skipped here).  For
    each candidate position the function:

    - Optionally trims the immediately preceding pass (within its remaining trim budget)
      to create the required leading gap.
    - Splits the trailing-gap adjustment between trimming the new pass's stop time and
      delaying the first following pass's start time, so neither pass bears the full
      cost.  The new pass's end is trimmed by the portion of the shortfall that cannot
      be absorbed by the first following pass's remaining start-delay budget.  Falls back
      to cascade-delaying following passes only when the combined budgets are exhausted.

    Args:
        ch_passes: Already-scheduled passes on this channel.
        p: The candidate pass to insert.
        gap_seconds: Minimum gap required between consecutive passes.
        max_trim_10: Maximum duration trim allowed per pass (floored to 10 s).
        max_start_delay: Maximum start delay allowed per pass.

    Returns:
        ``(insert_idx, adj_start, adj_dur, side_effects)`` when a valid position is
        found, or ``None`` otherwise.  *side_effects* is a list of
        ``(ch_idx, new_out_start_or_None, new_out_dur_s_or_None)`` describing in-place
        changes to already-scheduled passes; a ``None`` field value means no change.
        Positions that require no side effects (no disruption to already-scheduled passes)
        are preferred over those that do; within each group the earliest adjusted start
        wins, then longest duration.
    """
    adj_dur = floor_to_10s_seconds(p.dur_s)
    best = None  # type: Optional[Tuple[int, datetime, int, List]]

    for j in range(len(ch_passes)):  # j=len (append) already tried by the main loop
        p_prev = ch_passes[j - 1] if j > 0 else None
        following = ch_passes[j:]

        side_effects = []  # type: List[Tuple[int, Optional[datetime], Optional[int]]]

        # --- Step 1: adjusted start for new pass, optionally trimming p_prev ---
        if p_prev is None:
            adj_start = ceil_start_within_delay(p.start, p.start, max_start_delay)
            if adj_start is None:
                continue
        else:
            prev_end = p_prev.out_start + timedelta(seconds=p_prev.out_dur_s)
            min_start = prev_end + timedelta(seconds=gap_seconds)
            adj_start = ceil_start_within_delay(p.start, max(p.start, min_start), max_start_delay)
            if adj_start is not None:
                pass
            else:
                latest_start = latest_start_within_delay(p.start, max_start_delay)
                if latest_start is None:
                    continue
                adj_start = latest_start
                target_prev_end = adj_start - timedelta(seconds=gap_seconds)
                target_prev_dur_raw = int((target_prev_end - p_prev.out_start).total_seconds())
                target_prev_dur = max(0, floor_to_10s_seconds(target_prev_dur_raw))
                min_prev_dur_allowed = max(0, floor_to_10s_seconds(p_prev.dur_s) - max_trim_10)
                if target_prev_dur < min_prev_dur_allowed:
                    continue  # can't trim p_prev enough
                side_effects.append((j - 1, None, min(p_prev.out_dur_s, target_prev_dur)))

        if start_delay_seconds(p.start, adj_start) > max_start_delay:
            continue

        # --- Step 2: create required trailing gap ---
        # Split the gap deficit between trimming the new pass's stop time and delaying
        # the first following pass's start, so neither pass bears the full cost.  The
        # first following pass absorbs as much of the shortfall as its remaining
        # start-delay budget allows; the new pass is trimmed for what remains.  If the
        # cascade from delaying the first following pass exceeds any later pass's budget,
        # fall back to cascade-delaying all following passes without trimming the new pass.
        use_dur = adj_dur
        new_end = adj_start + timedelta(seconds=use_dur)
        feasible = True
        prev_end_dt = new_end

        if following:
            fp0 = following[0]
            min_fp0_start = ceil_to_next_10s(new_end + timedelta(seconds=gap_seconds))
            if fp0.out_start < min_fp0_start:
                shortfall = int((min_fp0_start - fp0.out_start).total_seconds())
                fp0_already_delayed = int((fp0.out_start - fp0.start).total_seconds())
                fp0_avail_delay = max(0, max_start_delay - fp0_already_delayed)

                # Give fp0 as much of the shortfall as its delay budget allows;
                # trim the new pass's end for the remainder.
                fp0_delay = min(shortfall, floor_to_10s_seconds(fp0_avail_delay))
                remaining = shortfall - fp0_delay
                trimmed_dur = floor_to_10s_seconds(adj_dur - remaining)
                trim_amount = adj_dur - trimmed_dur
                split_ok = trimmed_dur >= 0 and trim_amount <= max_trim_10

                split_side_effects = []
                if split_ok and fp0_delay > 0:
                    new_fp0_start = fp0.out_start + timedelta(seconds=fp0_delay)
                    split_side_effects.append((j, new_fp0_start, None))
                    # Cascade fp0's delay into fp1, fp2, ... as needed.
                    prev_csc = new_fp0_start + timedelta(seconds=fp0.out_dur_s)
                    for k, fp in enumerate(following[1:], start=1):
                        min_fp_start = ceil_to_next_10s(prev_csc + timedelta(seconds=gap_seconds))
                        if fp.out_start >= min_fp_start:
                            break
                        ad = int((fp.out_start - fp.start).total_seconds())
                        en = int((min_fp_start - fp.out_start).total_seconds())
                        if ad + en > max_start_delay:
                            split_ok = False
                            break
                        split_side_effects.append((j + k, min_fp_start, None))
                        prev_csc = min_fp_start + timedelta(seconds=fp.out_dur_s)

                if split_ok:
                    use_dur = trimmed_dur
                    side_effects.extend(split_side_effects)
                else:
                    # Split infeasible; fall back to cascade-delaying following passes.
                    for k, fp in enumerate(following):
                        min_fp_start = ceil_to_next_10s(prev_end_dt + timedelta(seconds=gap_seconds))
                        if fp.out_start >= min_fp_start:
                            break  # sufficient gap; subsequent passes also unaffected
                        already_delayed = int((fp.out_start - fp.start).total_seconds())
                        extra_needed = int((min_fp_start - fp.out_start).total_seconds())
                        if already_delayed + extra_needed > max_start_delay:
                            feasible = False
                            break
                        side_effects.append((j + k, min_fp_start, None))
                        prev_end_dt = min_fp_start + timedelta(seconds=fp.out_dur_s)

        if feasible:
            # Prefer slots with no side effects (original times preserved),
            # then earliest adjusted start, then longest duration.
            key = (bool(side_effects), adj_start, -use_dur)
            best_key = (bool(best[3]), best[1], -best[2]) if best is not None else None
            if best is None or key < best_key:
                best = (j, adj_start, use_dur, side_effects)

    return best


def schedule_n_channels(
    passes: List[Pass],
    n_channels: int,
    gap_seconds: int = 190,
    max_trim_seconds: int = 180,
    max_start_delay: int = 180,
    channel_exclude_sats: Optional[List[set]] = None,
) -> Tuple[List[List[Pass]], List[Pass]]:
    """Assign passes to N channels while enforcing gaps, start delays, and trim limits.

    Passes are sorted by priority (lower number = higher priority) then start time so
    that higher-priority passes claim channel slots first.  For each pass, every channel
    is evaluated for appending and the one yielding the earliest adjusted start is chosen
    (ties broken by longest adjusted duration, then lowest channel index).  When all
    channels reject the append attempt, a secondary insertion pass tries to slot the pass
    at any earlier position within each channel by trimming the preceding scheduled pass
    and/or forward-shifting following scheduled passes, within their respective trim and
    start-delay budgets.  Passes whose original start time is already in the past are
    silently skipped.

    Args:
        passes: List of Pass objects to schedule.
        n_channels: Number of output channels to fill.
        gap_seconds: Minimum gap between consecutive passes on a channel.
        max_trim_seconds: Maximum total trim allowed on a previous pass duration.
        max_start_delay: Maximum delay allowed to push a pass start later.
        channel_exclude_sats: Optional per-channel exclusion sets. If provided,
            channel_exclude_sats[i] is a set of lowercase satellite names that
            must not be assigned to channel i. A pass excluded from one channel
            may still be placed on a channel where it is not excluded.

    Returns:
        A tuple of (channels, unscheduled) where channels is a list of n_channels
        lists each containing scheduled Pass objects with adjusted out_start and
        out_dur_s fields, and unscheduled is a list of Pass objects that could not
        be placed on any channel (including passes whose start time is in the past).
    """
    ch = [[] for _ in range(n_channels)]  # type: List[List[Pass]]
    unscheduled = []  # type: List[Pass]
    max_trim_10 = floor_to_10s_seconds(max_trim_seconds)
    now = datetime.now()

    # Process higher-priority passes first (lower pri number = higher priority);
    # within the same priority level, preserve start-time order.
    for p in sorted(passes, key=lambda p: (p.pri, p.start)):
        # Skip passes whose original start time is already in the past
        if p.start < now:
            unscheduled.append(p)
            continue

        candidates = []  # tuples: (i, adj_start, adj_dur, prev_new_dur_or_None)
        for i in range(n_channels):
            if channel_exclude_sats and p.sat.lower() in channel_exclude_sats[i]:
                continue
            prev = ch[i][-1] if ch[i] else None
            adj_dur = floor_to_10s_seconds(p.dur_s)

            if prev is None:
                adj_start = ceil_start_within_delay(p.start, p.start, max_start_delay)
                if adj_start is None:
                    continue
                candidates.append((i, adj_start, adj_dur, None))
                continue

            prev_end = prev.out_start + timedelta(seconds=prev.out_dur_s)
            min_start = prev_end + timedelta(seconds=gap_seconds)

            adj_start = ceil_start_within_delay(p.start, max(p.start, min_start), max_start_delay)
            if adj_start is not None:
                candidates.append((i, adj_start, adj_dur, None))
            else:
                latest_start = latest_start_within_delay(p.start, max_start_delay)
                if latest_start is None:
                    continue
                adj_start = latest_start

                target_prev_end = adj_start - timedelta(seconds=gap_seconds)
                target_prev_dur_raw = int((target_prev_end - prev.out_start).total_seconds())
                target_prev_dur = max(0, floor_to_10s_seconds(target_prev_dur_raw))

                base_prev_dur = floor_to_10s_seconds(prev.dur_s)
                min_prev_dur_allowed = max(0, base_prev_dur - max_trim_10)

                if target_prev_dur < min_prev_dur_allowed:
                    continue

                new_prev_dur = min(prev.out_dur_s, target_prev_dur)
                candidates.append((i, adj_start, adj_dur, new_prev_dur))

        if not candidates:
            # Greedy append failed on all channels; try inserting at an earlier
            # position within each channel, adjusting surrounding passes as needed.
            best_insert = None
            for i in range(n_channels):
                if channel_exclude_sats and p.sat.lower() in channel_exclude_sats[i]:
                    continue
                result = _find_insertion(ch[i], p, gap_seconds, max_trim_10, max_start_delay)
                if result is None:
                    continue
                ins_idx, adj_start, adj_dur, side_effects = result
                key = (bool(side_effects), adj_start, -adj_dur, i)
                best_key = (bool(best_insert[4]), best_insert[2], -best_insert[3], best_insert[0]) if best_insert is not None else None
                if best_insert is None or key < best_key:
                    best_insert = (i, ins_idx, adj_start, adj_dur, side_effects)

            if best_insert is None:
                unscheduled.append(p)
                continue

            i, ins_idx, adj_start, adj_dur, side_effects = best_insert
            for ch_idx, new_start, new_dur in side_effects:
                if new_start is not None:
                    ch[i][ch_idx].out_start = new_start
                if new_dur is not None:
                    ch[i][ch_idx].out_dur_s = new_dur
            q = Pass(
                idx=p.idx,
                state="sched",
                pri=p.pri,
                sat=p.sat,
                telem=p.telem,
                date_str=p.date_str,
                doy=p.doy,
                time_str=p.time_str,
                dur_str=p.dur_str,
                start=p.start,
                dur_s=p.dur_s,
                out_start=adj_start,
                out_dur_s=adj_dur,
            )
            ch[i].insert(ins_idx, q)
            continue

        # Choose earliest start, then longer duration, then lower channel index
        candidates.sort(key=lambda x: (x[1], -x[2], x[0]))
        i, adj_start, adj_dur, prev_new_dur = candidates[0]

        if prev_new_dur is not None:
            prev = ch[i][-1]
            if prev_new_dur < prev.out_dur_s:
                prev.out_dur_s = prev_new_dur

        q = Pass(
            idx=p.idx,
            state="sched",
            pri=p.pri,
            sat=p.sat,
            telem=p.telem,
            date_str=p.date_str,
            doy=p.doy,
            time_str=p.time_str,
            dur_str=p.dur_str,
            start=p.start,
            dur_s=p.dur_s,
            out_start=adj_start,
            out_dur_s=adj_dur,
        )
        ch[i].append(q)

    return ch, unscheduled


def write_schedule(path: str, passes: List[Pass]) -> None:
    """Write a scheduling file to the given path.

    Builds a list of output lines starting with the global header, then formats one
    line per pass using `format_output_line`. For each pass, it uses `out_start`
    and `out_dur_s` when available, otherwise falls back to `start` and `dur_s`.
    The file is written as UTF-8 with a trailing newline.

    Args:
        path: Destination file path for the schedule output.
        passes: Iterable of Pass objects to serialize.

    Returns:
        None
    """
    lines = [HEADER, ""]  # type: List[str]
    for i, p in enumerate(passes, start=1):
        dt = p.out_start or p.start
        dur_s = p.out_dur_s if p.out_dur_s is not None else p.dur_s
        lines.append(format_output_line(i, p.state, p.pri, p.sat, p.telem, dt, dur_s))
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).rstrip() + "\n")


def clear_tschedule() -> None:
    """Purge the local TeraScan schedule by running clearsched.

    Checks that the clearsched binary exists and is executable before running.
    Prints stdout on success. Raises RuntimeError on failure or timeout.
    """
    cmd = "/opt/terascan/bin/clearsched"
    if not (os.path.isfile(cmd) and os.access(cmd, os.X_OK)):
        raise RuntimeError("Command not found or not executable: {}".format(cmd))
    try:
        print(f"Running: {cmd}")
        result = subprocess.run(
            [cmd],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=TIMEOUT_SECS,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("Command timed out after {}s: {}".format(TIMEOUT_SECS, cmd))
    except Exception as e:
        raise RuntimeError("Error running command: {}".format(e))

    if result.stdout:
        print(result.stdout.strip())
    if result.returncode != 0:
        err = result.stderr.strip() if result.stderr else ""
        raise RuntimeError(
            "Command exited with status {}{}".format(
                result.returncode,
                ": {}".format(err) if err else "",
            )
        )
    print("Cleared terascan schedule")


def clear_remote_tschedule(host: Optional[str] = None) -> None:
    """Purge terascan schedule on a remote host via ssh.

    Raises RuntimeError on failure or timeout.
    """
    if not host:
        raise RuntimeError("Remote host not provided; cannot run remote clearsched.")
    cmd = "/opt/terascan/bin/clearsched"
    try:
        remote_cmd = "bash -lc " + shlex.quote(
            "source /opt/terascan/etc/tscan.bash_profile && " + shlex.quote(cmd)
        )
        ssh_args = [
            "ssh",
            "-n",
            "-q",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout={}".format(SSH_CONNECT_TIMEOUT),
            "-o",
            "ServerAliveInterval={}".format(SSH_SERVER_ALIVE_INTERVAL),
            "-o",
            "ServerAliveCountMax={}".format(SSH_SERVER_ALIVE_COUNT_MAX),
            host,
            remote_cmd,
        ]
        print("Running:", " ".join(ssh_args))
        result = subprocess.run(
            ssh_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=TIMEOUT_SECS,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("Remote clearsched timed out after {}s on {}".format(TIMEOUT_SECS, host))
    except Exception as e:
        raise RuntimeError("Error running remote clearsched: {}".format(e))

    if result.stdout:
        print(result.stdout.strip())
    if result.returncode != 0:
        err = result.stderr.strip() if result.stderr else ""
        raise RuntimeError(
            "remote clearsched exited with status {}{}".format(
                result.returncode,
                ": {}".format(err) if err else "",
            )
        )
    print("Cleared remote terascan schedule")


def telemetry_to_chain(telem: str) -> int:
    """Map telemetry to a TeraScan chain number."""
    key = (telem or "").strip().lower()
    return TELEMETRY_CHAIN_MAP.get(key, DEFAULT_CHAIN)


def build_mansched_args(p: Pass, overrides: Optional[Dict[str, str]] = None) -> List[str]:
    """Build mansched k=v args with defaults from pass data, allowing overrides."""
    overrides = overrides or {}
    start_dt = p.out_start or p.start
    dur_s = p.out_dur_s if p.out_dur_s is not None else p.dur_s

    telemetry = overrides.get("telemetry", p.telem)
    satellite = overrides.get("satellite", p.sat)
    chain = overrides.get("chain")
    if chain is None:
        chain = telemetry_to_chain(telemetry)
    # Ensure chain is str for command assembly
    chain_str = str(chain)

    start_date = overrides.get("start_date", start_dt.strftime("%Y/%m/%d"))
    start_time = overrides.get("start_time", start_dt.strftime("%H:%M:%S"))
    duration = overrides.get("duration", seconds_to_mmss(dur_s))
    priority = overrides.get("priority", str(p.pri))
    orbit_number = overrides.get("orbit_number", "0")

    return [
        f"telemetry={telemetry}",
        f"satellite={satellite}",
        f"chain={chain_str}",
        f"start_date={start_date}",
        f"start_time={start_time}",
        f"duration={duration}",
        f"priority={priority}",
        f"orbit_number={orbit_number}",
    ]


def push_schedule_to_mansched(passes: List[Pass], overrides: Optional[Dict[str, str]] = None) -> None:
    """Invoke mansched once per pass using defaults/overrides.

    Raises RuntimeError if mansched cannot be executed successfully.
    """
    cmd = "/opt/terascan/bin/mansched"
    if not (os.path.isfile(cmd) and os.access(cmd, os.X_OK)):
        raise RuntimeError("Command not found or not executable: {}".format(cmd))

    ordered = sorted(passes, key=lambda x: x.out_start or x.start)
    for p in ordered:
        dur_s = p.out_dur_s if p.out_dur_s is not None else p.dur_s
        if dur_s <= 0:
            continue  # skip zero-length passes
        args = [cmd] + build_mansched_args(p, overrides)
        print("Running:", " ".join(args))
        try:
            result = subprocess.run(
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                timeout=TIMEOUT_SECS,
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError("mansched timed out after {}s".format(TIMEOUT_SECS))
        except Exception as e:
            raise RuntimeError("Error running mansched: {}".format(e))

        if result.stdout:
            print(result.stdout.strip())
        if result.returncode != 0:
            err = result.stderr.strip() if result.stderr else ""
            raise RuntimeError(
                "mansched exited with status {}{}".format(
                    result.returncode,
                    ": {}".format(err) if err else "",
                )
            )


def push_schedule_to_remote_mansched(passes: List[Pass], host: str, overrides: Optional[Dict[str, str]] = None) -> None:
    """Invoke mansched over ssh on the given host once per pass using defaults/overrides.

    Raises RuntimeError if the remote batch cannot be executed successfully.
    """
    if not host:
        raise RuntimeError("Remote host not provided; cannot run remote mansched.")
    cmd = "/opt/terascan/bin/mansched"
    try:
        # Sort by start time to submit in time order, then run in a single SSH session
        # to avoid per-pass connection overhead on high-latency links.
        ordered = sorted(passes, key=lambda x: x.out_start or x.start)
        remote_lines = []
        for p in ordered:
            dur_s = p.out_dur_s if p.out_dur_s is not None else p.dur_s
            if dur_s <= 0:
                continue  # skip zero-length passes
            tokens = [cmd] + build_mansched_args(p, overrides)
            remote_lines.append(" ".join(shlex.quote(t) for t in tokens))

        if not remote_lines:
            print("No remote passes to schedule; skipping remote mansched.")
            return

        remote_script = "source /opt/terascan/etc/tscan.bash_profile && set -e\n" + "\n".join(remote_lines)
        remote_cmd = "bash -lc " + shlex.quote(remote_script)
        ssh_args = [
            "ssh",
            "-n",
            "-q",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout={}".format(SSH_CONNECT_TIMEOUT),
            "-o",
            "ServerAliveInterval={}".format(SSH_SERVER_ALIVE_INTERVAL),
            "-o",
            "ServerAliveCountMax={}".format(SSH_SERVER_ALIVE_COUNT_MAX),
            host,
            remote_cmd,
        ]
        print("Running remote mansched batch for {} pass(es) on {}".format(len(remote_lines), host))
        result = subprocess.run(
            ssh_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=TIMEOUT_SECS,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("remote mansched timed out after {}s on {}".format(TIMEOUT_SECS, host))
    except Exception as e:
        raise RuntimeError("Error running remote mansched: {}".format(e))

    if result.stdout:
        print(result.stdout.strip())
    if result.returncode != 0:
        err = result.stderr.strip() if result.stderr else ""
        raise RuntimeError(
            "remote mansched exited with status {}{}".format(
                result.returncode,
                ": {}".format(err) if err else "",
            )
        )


def default_output_paths(input1: str, n: int) -> List[str]:
    """Return n default output paths (cosched_out_1 .. cosched_out_N) next to input1."""
    base_dir = os.path.dirname(os.path.abspath(input1))
    return [os.path.join(base_dir, f"cosched_out_{i + 1}") for i in range(n)]


def sanitize_label(host: str) -> str:
    """Return a safe filename label derived from a host string.

    Strips a leading 'user@' prefix and a trailing ':port' suffix, then
    replaces any character outside [A-Za-z0-9_.-] with '_'.

    Examples::

        'user@host.example.com' -> 'host.example.com'
        'host:2222'             -> 'host'
    """
    if "@" in host:
        host = host.split("@", 1)[1]
    if ":" in host:
        host = host.split(":", 1)[0]
    return re.sub(r"[^A-Za-z0-9_.-]", "_", host)


def fetch_local_schedule() -> str:
    """Run listsched locally, sourcing the TeraScan profile, and return its stdout.

    Raises:
        RuntimeError: If the binary is not found, the call times out, or
            listsched exits with a non-zero status.
    """
    try:
        proc = subprocess.run(
            ["bash", "-c", "source /opt/terascan/etc/tscan.bash_profile; " + LISTSCHED],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=TIMEOUT_SECS,
            check=False,
        )
    except FileNotFoundError:
        raise RuntimeError("listsched not found at {}".format(LISTSCHED))
    except subprocess.TimeoutExpired:
        raise RuntimeError("listsched timed out after {}s".format(TIMEOUT_SECS))
    if proc.returncode != 0:
        raise RuntimeError("listsched failed (exit {}): {}".format(proc.returncode, proc.stderr.strip()))
    return proc.stdout


def fetch_remote_schedule(host: str) -> str:
    """Run listsched on a remote host via SSH and return its stdout.

    The entire remote command is passed as a single string to SSH so the
    remote shell receives both the profile source and the binary invocation
    intact (splitting into separate argv elements causes SSH to join them
    with spaces, breaking shell parsing of the operator between them).

    Args:
        host: SSH target in 'host' or 'user@host' form.

    Raises:
        RuntimeError: If ssh is not on PATH, the call times out, or the
            remote command exits with a non-zero status.
    """
    remote_cmd = "bash -lc " + shlex.quote(
        "source /opt/terascan/etc/tscan.bash_profile && {}".format(shlex.quote(LISTSCHED))
    )
    try:
        proc = subprocess.run(
            [
                "ssh",
                "-n",
                "-q",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout={}".format(SSH_CONNECT_TIMEOUT),
                "-o",
                "ServerAliveInterval={}".format(SSH_SERVER_ALIVE_INTERVAL),
                "-o",
                "ServerAliveCountMax={}".format(SSH_SERVER_ALIVE_COUNT_MAX),
                host,
                remote_cmd,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=TIMEOUT_SECS,
            check=False,
        )
    except FileNotFoundError:
        raise RuntimeError("ssh not found on PATH")
    except subprocess.TimeoutExpired:
        raise RuntimeError("ssh to {} timed out after {}s".format(host, TIMEOUT_SECS))
    if proc.returncode != 0:
        raise RuntimeError("ssh to {} failed (exit {}): {}".format(host, proc.returncode, proc.stderr.strip()))
    return proc.stdout


def write_raw_schedule(label: str, content: str) -> str:
    """Write raw listsched output to SCHED_DIR/<label>.sched and return the path.

    The label is sanitized to [A-Za-z0-9_.-] to prevent path traversal even
    when a pre-sanitized label is passed.

    Args:
        label: A hostname or other identifier used as the filename stem.
        content: Raw text content to write (listsched stdout).

    Returns:
        Absolute path of the written file.

    Raises:
        ValueError: If the sanitized label is empty.
    """
    safe_label = re.sub(r"[^A-Za-z0-9_.-]", "_", label)
    if not safe_label:
        raise ValueError("label must contain at least one valid character")
    path = os.path.join(SCHED_DIR, "{}.sched".format(safe_label))
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path


def main():
    global TIMEOUT_SECS, SSH_CONNECT_TIMEOUT, SSH_SERVER_ALIVE_INTERVAL, SSH_SERVER_ALIVE_COUNT_MAX
    ap = argparse.ArgumentParser(
        description=(
            "Co-schedule satellite pass lists into one or more gap-constrained schedules. "
            "In file mode, channel 1 is pushed locally and additional channels map to "
            "--remote-host entries in order. In fetch mode, each successfully fetched "
            "channel is pushed back to the source it was fetched from. Use --fetch to "
            "retrieve schedules automatically via listsched before scheduling."
        )
    )
    ap.add_argument(
        "inputs",
        nargs="*",
        help="One or more schedule files to merge and co-schedule. Required unless --fetch is used.",
    )
    ap.add_argument(
        "--fetch",
        action="store_true",
        default=False,
        help=(
            "Fetch schedules by running listsched locally and on each --remote-host, "
            "write them to {}/".format(SCHED_DIR) + "<hostname>.sched, "
            "then use those files as inputs."
        ),
    )
    ap.add_argument(
        "--out",
        dest="outputs",
        action="append",
        default=[],
        metavar="PATH",
        help="Output path for each channel (repeat for each channel; defaults to cosched_out_N next to the first input)",
    )
    ap.add_argument("--gap", type=int, default=190, help="Minimum gap in seconds between passes on a channel (default: 190)")
    ap.add_argument("--max-trim", type=int, default=180, help="Maximum total duration reduction allowed per pass in seconds (default: 180)")
    ap.add_argument("--max-start-delay", type=int, default=180, help="Maximum start delay allowed for a pass in seconds (default: 180)")
    ap.add_argument("--timeout-secs", type=int, default=TIMEOUT_SECS, help="Subprocess timeout in seconds for clearsched/mansched/listsched/ssh calls (default: 120)")
    ap.add_argument("--ssh-connect-timeout", type=int, default=SSH_CONNECT_TIMEOUT, help="SSH connection timeout in seconds (default: 30)")
    ap.add_argument("--ssh-keepalive-interval", type=int, default=SSH_SERVER_ALIVE_INTERVAL, help="SSH ServerAliveInterval in seconds (default: 30)")
    ap.add_argument("--ssh-keepalive-count-max", type=int, default=SSH_SERVER_ALIVE_COUNT_MAX, help="SSH ServerAliveCountMax (default: 3)")
    ap.add_argument(
        "--remote-host",
        dest="remote_hosts",
        action="append",
        default=[],
        metavar="HOST",
        help="Remote host for channel 2, 3, ... (repeat for each additional channel; e.g. user@host)",
    )
    ap.add_argument(
        "--exclude-sat",
        dest="exclude_sats",
        action="append",
        default=[],
        metavar="SAT",
        help="Satellite name to exclude from ALL channels (repeat for multiple; case-insensitive)",
    )
    ap.add_argument(
        "--local-exclude-sat",
        dest="local_exclude_sats",
        action="append",
        default=[],
        metavar="SAT",
        help="Satellite name to exclude from the local channel only (repeat for multiple; case-insensitive)",
    )
    ap.add_argument(
        "--remote-exclude-sat",
        dest="remote_exclude_sats",
        action="append",
        default=[],
        metavar="SAT",
        help="Satellite name to exclude from all remote channels (repeat for multiple; case-insensitive)",
    )
    ap.add_argument(
        "--sat-priority",
        dest="sat_priorities",
        action="append",
        default=[],
        metavar="SAT=PRIORITY",
        help=(
            "Override the priority for a specific satellite (e.g. metop-3=2). "
            "Repeat for multiple satellites. Case-insensitive. "
            "Applies to all channels and takes precedence over priorities in the input schedule."
        ),
    )
    args = ap.parse_args()

    # Parse --sat-priority SAT=PRIORITY entries
    sat_priority_map = {}  # type: Dict[str, int]
    for entry in args.sat_priorities:
        if "=" not in entry:
            ap.error("--sat-priority must be in SAT=PRIORITY format, got: {}".format(entry))
        sat_raw, _, pri_str = entry.partition("=")
        try:
            sat_priority_map[sat_raw.strip().lower()] = int(pri_str.strip())
        except ValueError:
            ap.error("--sat-priority priority must be an integer, got: {!r}".format(pri_str.strip()))

    if args.timeout_secs <= 0:
        ap.error("--timeout-secs must be > 0")
    if args.ssh_connect_timeout <= 0:
        ap.error("--ssh-connect-timeout must be > 0")
    if args.ssh_keepalive_interval <= 0:
        ap.error("--ssh-keepalive-interval must be > 0")
    if args.ssh_keepalive_count_max <= 0:
        ap.error("--ssh-keepalive-count-max must be > 0")

    TIMEOUT_SECS = args.timeout_secs
    SSH_CONNECT_TIMEOUT = args.ssh_connect_timeout
    SSH_SERVER_ALIVE_INTERVAL = args.ssh_keepalive_interval
    SSH_SERVER_ALIVE_COUNT_MAX = args.ssh_keepalive_count_max

    if args.fetch:
        # Fetch mode: run listsched locally and on each remote host, write raw
        # output to SCHED_DIR, then use those paths as inputs for scheduling.
        input_paths = []             # type: List[str]
        channel_targets = []         # type: List[Tuple[str, Optional[str]]]
        failures = 0

        # Local
        local_label = sanitize_label(socket.gethostname())
        try:
            content = fetch_local_schedule()
            if not schedule_content_has_passes(content):
                raise RuntimeError("local schedule fetch returned no pass entries")
            path = write_raw_schedule(local_label, content)
            print(f"Fetched local schedule to {path}")
            input_paths.append(path)
            channel_targets.append(("local", None))
        except Exception as e:
            print(f"ERROR fetching local schedule: {e}", file=sys.stderr)
            failures += 1

        # Remotes
        for host in args.remote_hosts:
            label = sanitize_label(host)
            try:
                content = fetch_remote_schedule(host)
                if not schedule_content_has_passes(content):
                    raise RuntimeError("remote schedule fetch returned no pass entries")
                path = write_raw_schedule(label, content)
                print(f"Fetched remote schedule from {host} to {path}")
                input_paths.append(path)
                channel_targets.append(("remote", host))
            except Exception as e:
                print(f"ERROR fetching schedule from {host}: {e}", file=sys.stderr)
                failures += 1

        if not input_paths:
            print("ERROR: no schedules could be fetched; aborting.", file=sys.stderr)
            sys.exit(1)
        if failures:
            print(f"WARNING: {failures} fetch(es) failed; continuing with {len(input_paths)} schedule(s).")

    else:
        if not args.inputs:
            ap.error("at least one input file is required (or use --fetch to retrieve schedules automatically)")
        input_paths = args.inputs
        channel_targets = [("local", None)]  # type: List[Tuple[str, Optional[str]]]
        for i in range(1, len(input_paths)):
            host_idx = i - 1
            if host_idx < len(args.remote_hosts):
                channel_targets.append(("remote", args.remote_hosts[host_idx]))
            else:
                channel_targets.append(("unmapped", None))

    n_channels = len(input_paths)

    # Parse and merge all input schedules, then deduplicate
    all_passes = []  # type: List[Pass]
    for path in input_paths:
        all_passes.extend(parse_schedule(path))
    uniq = dedupe_passes(all_passes)

    # Build per-channel satellite exclusion sets
    global_excl = {s.lower() for s in args.exclude_sats}
    local_excl = {s.lower() for s in args.local_exclude_sats}
    remote_excl = {s.lower() for s in args.remote_exclude_sats}

    channel_exclude_sats = []
    for i in range(n_channels):
        excl = set(global_excl)
        if i == 0:
            excl |= local_excl
        else:
            excl |= remote_excl
        channel_exclude_sats.append(excl)

    for i, excl in enumerate(channel_exclude_sats, start=1):
        if excl:
            print(f"Channel {i}: excluding satellites: {', '.join(sorted(excl))}")

    if sat_priority_map:
        for sat, pri in sorted(sat_priority_map.items()):
            print("Satellite priority override: {} -> {}".format(sat, pri))

    # Schedule across N channels
    channels, unscheduled = schedule_n_channels(
        uniq,
        n_channels=n_channels,
        gap_seconds=args.gap,
        max_trim_seconds=args.max_trim,
        max_start_delay=args.max_start_delay,
        channel_exclude_sats=channel_exclude_sats,
    )

    # Apply per-satellite priority overrides to all scheduled passes
    if sat_priority_map:
        for ch in channels:
            for p in ch:
                override = sat_priority_map.get(p.sat.lower())
                if override is not None:
                    p.pri = override

    # Resolve output paths (use supplied --out values, fall back to defaults)
    defaults = default_output_paths(input_paths[0], n_channels)
    out_paths = [args.outputs[i] if i < len(args.outputs) else defaults[i] for i in range(n_channels)]

    # Write output files
    for i, (ch, path) in enumerate(zip(channels, out_paths), start=1):
        write_schedule(path, ch)
        print(f"Wrote {len(ch)} passes to channel {i}: {path}")

    # Write unscheduled passes
    not_sched_path = "/tmp/cosched_not_scheduled"
    write_schedule(not_sched_path, unscheduled)
    print(f"Wrote {len(unscheduled)} unscheduled passes to {not_sched_path}")

    # Push each channel back to its configured target.
    for i, ch in enumerate(channels, start=1):
        target_kind, target_host = channel_targets[i - 1]
        try:
            if target_kind == "local":
                clear_tschedule()
                push_schedule_to_mansched(ch)
            elif target_kind == "remote":
                if target_host is None:
                    raise RuntimeError("No remote host configured for channel {}".format(i))
                clear_remote_tschedule(target_host)
                push_schedule_to_remote_mansched(ch, target_host)
            else:
                print(f"No push target configured for channel {i}; skipping push.")
        except RuntimeError as exc:
            print("ERROR pushing channel {}: {}".format(i, exc), file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
