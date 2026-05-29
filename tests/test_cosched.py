"""Regression tests for the coscheduler entrypoint and core scheduling helpers.

The suite is intentionally split into three groups:
- main() routing and push behavior in file and fetch modes
- delay/rounding edge cases inside the scheduler
- near-duplicate pass deduplication rules
"""

import importlib.util
import sys
import tempfile
import unittest
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, call, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
COSCHED_PATH = REPO_ROOT / "scripts" / "cosched.py"
SCHEDULE_WITH_PASS = """#  state  pri  satel    telem       date    day    time    durat  post_process
 1  sched   3  metop-3   ahrpt    2026/04/17 107  19:08:50  12:50
"""
HEADER_ONLY_SCHEDULE = "#  state  pri  satel    telem       date    day    time    durat  post_process\n"


def load_cosched_module():
    """Import scripts/cosched.py under a throwaway module name for each test."""
    spec = importlib.util.spec_from_file_location("cosched_under_test", str(COSCHED_PATH))
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load cosched module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_pass(module, start, dur_s, idx, sat=None, telem="ahrpt", pri=1):
    """Build a Pass instance with just the fields each test needs to vary."""
    sat = sat or "sat-{}".format(idx)
    return module.Pass(
        idx=idx,
        state="sched",
        pri=pri,
        sat=sat,
        telem=telem,
        date_str=start.strftime("%Y/%m/%d"),
        doy=start.timetuple().tm_yday,
        time_str=start.strftime("%H:%M:%S"),
        dur_str=module.seconds_to_mmss(dur_s),
        start=start,
        dur_s=dur_s,
    )


class CoschedMainRoutingTests(unittest.TestCase):
    """Exercise CLI routing, fetch-source mapping, and push abort behavior."""

    def test_fetch_mode_preserves_remote_targets_when_local_fetch_fails(self):
        cosched = load_cosched_module()
        schedule_n_channels = Mock(return_value=([
            ["remote-a-pass"],
            ["remote-b-pass"],
        ], []))

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    sys,
                    "argv",
                    [
                        "cosched.py",
                        "--fetch",
                        "--remote-host",
                        "remote-a",
                        "--remote-host",
                        "remote-b",
                    ],
                )
            )
            stack.enter_context(
                patch.object(cosched, "fetch_local_schedule", Mock(side_effect=RuntimeError("local fetch failed")))
            )
            stack.enter_context(
                patch.object(
                    cosched,
                    "fetch_remote_schedule",
                    Mock(side_effect=lambda host: SCHEDULE_WITH_PASS),
                )
            )
            stack.enter_context(
                patch.object(
                    cosched,
                    "write_raw_schedule",
                    Mock(side_effect=lambda label, content: "/tmp/{}.sched".format(label)),
                )
            )
            stack.enter_context(patch.object(cosched, "parse_schedule", Mock(return_value=[])))
            stack.enter_context(patch.object(cosched, "dedupe_passes", Mock(return_value=[])))
            stack.enter_context(patch.object(cosched, "schedule_n_channels", schedule_n_channels))
            stack.enter_context(patch.object(cosched, "write_schedule", Mock()))
            clear_local = stack.enter_context(patch.object(cosched, "clear_tschedule", Mock()))
            push_local = stack.enter_context(patch.object(cosched, "push_schedule_to_mansched", Mock()))
            clear_remote = stack.enter_context(patch.object(cosched, "clear_remote_tschedule", Mock()))
            push_remote = stack.enter_context(
                patch.object(cosched, "push_schedule_to_remote_mansched", Mock())
            )

            cosched.main()

        clear_local.assert_not_called()
        push_local.assert_not_called()
        self.assertEqual(schedule_n_channels.call_args[1]["n_channels"], 2)
        self.assertEqual(clear_remote.call_args_list, [call("remote-a"), call("remote-b")])
        self.assertEqual(
            push_remote.call_args_list,
            [
                call(["remote-a-pass"], "remote-a"),
                call(["remote-b-pass"], "remote-b"),
            ],
        )

    def test_fetch_mode_uses_remote_exclusions_for_surviving_remote_channels(self):
        cosched = load_cosched_module()
        schedule_n_channels = Mock(return_value=([
            ["remote-a-pass"],
            ["remote-b-pass"],
        ], []))

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    sys,
                    "argv",
                    [
                        "cosched.py",
                        "--fetch",
                        "--remote-host",
                        "remote-a",
                        "--remote-host",
                        "remote-b",
                        "--local-exclude-sat",
                        "local-only",
                        "--remote-exclude-sat",
                        "remote-only",
                        "--exclude-sat",
                        "global-only",
                    ],
                )
            )
            stack.enter_context(
                patch.object(cosched, "fetch_local_schedule", Mock(side_effect=RuntimeError("local fetch failed")))
            )
            stack.enter_context(
                patch.object(
                    cosched,
                    "fetch_remote_schedule",
                    Mock(side_effect=lambda host: SCHEDULE_WITH_PASS),
                )
            )
            stack.enter_context(
                patch.object(
                    cosched,
                    "write_raw_schedule",
                    Mock(side_effect=lambda label, content: "/tmp/{}.sched".format(label)),
                )
            )
            stack.enter_context(patch.object(cosched, "parse_schedule", Mock(return_value=[])))
            stack.enter_context(patch.object(cosched, "dedupe_passes", Mock(return_value=[])))
            stack.enter_context(patch.object(cosched, "write_schedule", Mock()))
            stack.enter_context(patch.object(cosched, "clear_tschedule", Mock()))
            stack.enter_context(patch.object(cosched, "push_schedule_to_mansched", Mock()))
            stack.enter_context(patch.object(cosched, "clear_remote_tschedule", Mock()))
            stack.enter_context(patch.object(cosched, "push_schedule_to_remote_mansched", Mock()))
            stack.enter_context(patch.object(cosched, "schedule_n_channels", schedule_n_channels))

            cosched.main()

        self.assertEqual(
            schedule_n_channels.call_args[1]["channel_exclude_sats"],
            [
                {"global-only", "remote-only"},
                {"global-only", "remote-only"},
            ],
        )

    def test_fetch_mode_skips_header_only_remote_schedule(self):
        cosched = load_cosched_module()
        schedule_n_channels = Mock(return_value=([
            ["local-pass"],
            ["remote-pass"],
        ], []))

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    sys,
                    "argv",
                    [
                        "cosched.py",
                        "--fetch",
                        "--remote-host",
                        "remote-a",
                        "--remote-host",
                        "remote-b",
                    ],
                )
            )
            stack.enter_context(
                patch.object(cosched, "fetch_local_schedule", Mock(return_value=SCHEDULE_WITH_PASS))
            )
            stack.enter_context(
                patch.object(
                    cosched,
                    "fetch_remote_schedule",
                    Mock(side_effect=[HEADER_ONLY_SCHEDULE, SCHEDULE_WITH_PASS]),
                )
            )
            stack.enter_context(
                patch.object(
                    cosched,
                    "write_raw_schedule",
                    Mock(side_effect=lambda label, content: "/tmp/{}.sched".format(label)),
                )
            )
            stack.enter_context(patch.object(cosched, "parse_schedule", Mock(return_value=[])))
            stack.enter_context(patch.object(cosched, "dedupe_passes", Mock(return_value=[])))
            stack.enter_context(patch.object(cosched, "schedule_n_channels", schedule_n_channels))
            stack.enter_context(patch.object(cosched, "write_schedule", Mock()))
            clear_local = stack.enter_context(patch.object(cosched, "clear_tschedule", Mock()))
            push_local = stack.enter_context(patch.object(cosched, "push_schedule_to_mansched", Mock()))
            clear_remote = stack.enter_context(patch.object(cosched, "clear_remote_tschedule", Mock()))
            push_remote = stack.enter_context(
                patch.object(cosched, "push_schedule_to_remote_mansched", Mock())
            )

            cosched.main()

        self.assertEqual(schedule_n_channels.call_args[1]["n_channels"], 2)
        clear_local.assert_called_once_with()
        push_local.assert_called_once_with(["local-pass"])
        clear_remote.assert_called_once_with("remote-b")
        push_remote.assert_called_once_with(["remote-pass"], "remote-b")

    def test_fetch_mode_uses_unique_raw_schedule_labels_for_colliding_sources(self):
        cosched = load_cosched_module()
        schedule_n_channels = Mock(return_value=([
            ["local-pass"],
            ["remote-a-pass"],
            ["remote-b-pass"],
        ], []))

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    sys,
                    "argv",
                    [
                        "cosched.py",
                        "--fetch",
                        "--remote-host",
                        "ops@station-a:2222",
                        "--remote-host",
                        "station-a",
                    ],
                )
            )
            stack.enter_context(
                patch.object(cosched.socket, "gethostname", Mock(return_value="station-a"))
            )
            stack.enter_context(
                patch.object(cosched, "fetch_local_schedule", Mock(return_value=SCHEDULE_WITH_PASS))
            )
            stack.enter_context(
                patch.object(
                    cosched,
                    "fetch_remote_schedule",
                    Mock(side_effect=lambda host: SCHEDULE_WITH_PASS),
                )
            )
            write_raw = stack.enter_context(
                patch.object(
                    cosched,
                    "write_raw_schedule",
                    Mock(side_effect=lambda label, content: "/tmp/{}.sched".format(label)),
                )
            )
            stack.enter_context(patch.object(cosched, "parse_schedule", Mock(return_value=[])))
            stack.enter_context(patch.object(cosched, "dedupe_passes", Mock(return_value=[])))
            stack.enter_context(patch.object(cosched, "schedule_n_channels", schedule_n_channels))
            stack.enter_context(patch.object(cosched, "write_schedule", Mock()))
            clear_local = stack.enter_context(patch.object(cosched, "clear_tschedule", Mock()))
            push_local = stack.enter_context(patch.object(cosched, "push_schedule_to_mansched", Mock()))
            clear_remote = stack.enter_context(patch.object(cosched, "clear_remote_tschedule", Mock()))
            push_remote = stack.enter_context(
                patch.object(cosched, "push_schedule_to_remote_mansched", Mock())
            )

            cosched.main()

        labels = [c.args[0] for c in write_raw.call_args_list]
        self.assertEqual(len(labels), 3)
        self.assertEqual(len(set(labels)), 3)
        self.assertEqual(schedule_n_channels.call_args[1]["n_channels"], 3)
        clear_local.assert_called_once_with()
        push_local.assert_called_once_with(["local-pass"])
        self.assertEqual(
            clear_remote.call_args_list,
            [call("ops@station-a:2222"), call("station-a")],
        )
        self.assertEqual(
            push_remote.call_args_list,
            [
                call(["remote-a-pass"], "ops@station-a:2222"),
                call(["remote-b-pass"], "station-a"),
            ],
        )

    def test_file_mode_keeps_local_then_remote_mapping(self):
        cosched = load_cosched_module()
        schedule_n_channels = Mock(return_value=([
            ["local-pass"],
            ["remote-pass"],
        ], []))

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    sys,
                    "argv",
                    [
                        "cosched.py",
                        "input1.sched",
                        "input2.sched",
                        "--remote-host",
                        "remote-a",
                    ],
                )
            )
            stack.enter_context(patch.object(cosched, "parse_schedule", Mock(return_value=[])))
            stack.enter_context(patch.object(cosched, "dedupe_passes", Mock(return_value=[])))
            stack.enter_context(patch.object(cosched, "schedule_n_channels", schedule_n_channels))
            stack.enter_context(patch.object(cosched, "write_schedule", Mock()))
            clear_local = stack.enter_context(patch.object(cosched, "clear_tschedule", Mock()))
            push_local = stack.enter_context(patch.object(cosched, "push_schedule_to_mansched", Mock()))
            clear_remote = stack.enter_context(patch.object(cosched, "clear_remote_tschedule", Mock()))
            push_remote = stack.enter_context(
                patch.object(cosched, "push_schedule_to_remote_mansched", Mock())
            )

            cosched.main()

        self.assertEqual(schedule_n_channels.call_args[1]["n_channels"], 2)
        clear_local.assert_called_once_with()
        push_local.assert_called_once_with(["local-pass"])
        clear_remote.assert_called_once_with("remote-a")
        push_remote.assert_called_once_with(["remote-pass"], "remote-a")

    def test_file_mode_aborts_after_local_clear_failure(self):
        cosched = load_cosched_module()
        schedule_n_channels = Mock(return_value=([
            ["local-pass"],
            ["remote-pass"],
        ], []))

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    sys,
                    "argv",
                    [
                        "cosched.py",
                        "input1.sched",
                        "input2.sched",
                        "--remote-host",
                        "remote-a",
                    ],
                )
            )
            stack.enter_context(patch.object(cosched, "parse_schedule", Mock(return_value=[])))
            stack.enter_context(patch.object(cosched, "dedupe_passes", Mock(return_value=[])))
            stack.enter_context(patch.object(cosched, "schedule_n_channels", schedule_n_channels))
            stack.enter_context(patch.object(cosched, "write_schedule", Mock()))
            clear_local = stack.enter_context(
                patch.object(cosched, "clear_tschedule", Mock(side_effect=RuntimeError("local clear failed")))
            )
            push_local = stack.enter_context(patch.object(cosched, "push_schedule_to_mansched", Mock()))
            clear_remote = stack.enter_context(patch.object(cosched, "clear_remote_tschedule", Mock()))
            push_remote = stack.enter_context(
                patch.object(cosched, "push_schedule_to_remote_mansched", Mock())
            )

            with self.assertRaises(SystemExit) as exc:
                cosched.main()

        self.assertEqual(exc.exception.code, 1)
        clear_local.assert_called_once_with()
        push_local.assert_not_called()
        clear_remote.assert_not_called()
        push_remote.assert_not_called()

    def test_file_mode_aborts_after_local_mansched_failure(self):
        cosched = load_cosched_module()
        schedule_n_channels = Mock(return_value=([
            ["local-pass"],
            ["remote-pass"],
        ], []))

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    sys,
                    "argv",
                    [
                        "cosched.py",
                        "input1.sched",
                        "input2.sched",
                        "--remote-host",
                        "remote-a",
                    ],
                )
            )
            stack.enter_context(patch.object(cosched, "parse_schedule", Mock(return_value=[])))
            stack.enter_context(patch.object(cosched, "dedupe_passes", Mock(return_value=[])))
            stack.enter_context(patch.object(cosched, "schedule_n_channels", schedule_n_channels))
            stack.enter_context(patch.object(cosched, "write_schedule", Mock()))
            clear_local = stack.enter_context(patch.object(cosched, "clear_tschedule", Mock()))
            push_local = stack.enter_context(
                patch.object(cosched, "push_schedule_to_mansched", Mock(side_effect=RuntimeError("mansched failed")))
            )
            clear_remote = stack.enter_context(patch.object(cosched, "clear_remote_tschedule", Mock()))
            push_remote = stack.enter_context(
                patch.object(cosched, "push_schedule_to_remote_mansched", Mock())
            )

            with self.assertRaises(SystemExit) as exc:
                cosched.main()

        self.assertEqual(exc.exception.code, 1)
        clear_local.assert_called_once_with()
        push_local.assert_called_once_with(["local-pass"])
        clear_remote.assert_not_called()
        push_remote.assert_not_called()

    def test_negative_scheduling_params_rejected(self):
        cosched = load_cosched_module()
        for flag in ["--gap", "--max-trim", "--max-start-delay"]:
            with self.subTest(flag=flag):
                with ExitStack() as stack:
                    stack.enter_context(
                        patch.object(sys, "argv", ["cosched.py", "--fetch", flag, "-1"])
                    )
                    with self.assertRaises(SystemExit) as exc:
                        cosched.main()
                    self.assertEqual(exc.exception.code, 2)

    def test_sat_priority_arg_validation_rejected(self):
        cosched = load_cosched_module()
        for entry in ["metop-3", "metop-3=high"]:
            with self.subTest(entry=entry):
                with ExitStack() as stack:
                    stack.enter_context(
                        patch.object(sys, "argv", ["cosched.py", "--fetch", "--sat-priority", entry])
                    )
                    with self.assertRaises(SystemExit) as exc:
                        cosched.main()
                    self.assertEqual(exc.exception.code, 2)

    def test_nonpositive_timeout_and_ssh_params_rejected(self):
        cosched = load_cosched_module()
        for flag in [
            "--timeout-secs",
            "--ssh-connect-timeout",
            "--ssh-keepalive-interval",
            "--ssh-keepalive-count-max",
        ]:
            for value in ["0", "-1"]:
                with self.subTest(flag=flag, value=value):
                    with ExitStack() as stack:
                        stack.enter_context(
                            patch.object(sys, "argv", ["cosched.py", "--fetch", flag, value])
                        )
                        with self.assertRaises(SystemExit) as exc:
                            cosched.main()
                        self.assertEqual(exc.exception.code, 2)

    def test_fetch_mode_exits_when_all_fetches_fail(self):
        cosched = load_cosched_module()
        schedule_n_channels = Mock()

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    sys,
                    "argv",
                    [
                        "cosched.py",
                        "--fetch",
                        "--remote-host",
                        "remote-a",
                    ],
                )
            )
            stack.enter_context(
                patch.object(cosched, "fetch_local_schedule", Mock(side_effect=RuntimeError("local fetch failed")))
            )
            stack.enter_context(
                patch.object(cosched, "fetch_remote_schedule", Mock(side_effect=RuntimeError("remote fetch failed")))
            )
            stack.enter_context(patch.object(cosched, "schedule_n_channels", schedule_n_channels))

            with self.assertRaises(SystemExit) as exc:
                cosched.main()

        self.assertEqual(exc.exception.code, 1)
        schedule_n_channels.assert_not_called()

    def test_file_mode_unmapped_channel_is_written_but_not_pushed(self):
        cosched = load_cosched_module()
        schedule_n_channels = Mock(return_value=([
            ["local-pass"],
            ["remote-pass"],
            ["unmapped-pass"],
        ], []))

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    sys,
                    "argv",
                    [
                        "cosched.py",
                        "input1.sched",
                        "input2.sched",
                        "input3.sched",
                        "--remote-host",
                        "remote-a",
                    ],
                )
            )
            stack.enter_context(patch.object(cosched, "parse_schedule", Mock(return_value=[])))
            stack.enter_context(patch.object(cosched, "dedupe_passes", Mock(return_value=[])))
            stack.enter_context(patch.object(cosched, "schedule_n_channels", schedule_n_channels))
            write_schedule = stack.enter_context(patch.object(cosched, "write_schedule", Mock()))
            clear_local = stack.enter_context(patch.object(cosched, "clear_tschedule", Mock()))
            push_local = stack.enter_context(patch.object(cosched, "push_schedule_to_mansched", Mock()))
            clear_remote = stack.enter_context(patch.object(cosched, "clear_remote_tschedule", Mock()))
            push_remote = stack.enter_context(
                patch.object(cosched, "push_schedule_to_remote_mansched", Mock())
            )

            cosched.main()

        self.assertEqual(schedule_n_channels.call_args[1]["n_channels"], 3)
        self.assertEqual(write_schedule.call_count, 4)
        clear_local.assert_called_once_with()
        push_local.assert_called_once_with(["local-pass"])
        clear_remote.assert_called_once_with("remote-a")
        push_remote.assert_called_once_with(["remote-pass"], "remote-a")

    def test_sat_priority_override_applies_case_insensitively(self):
        cosched = load_cosched_module()
        scheduled = make_pass(cosched, datetime(2099, 1, 1, 12, 0, 0), 100, 1, sat="MeToP-3", pri=9)
        schedule_n_channels = Mock(return_value=([[scheduled]], []))

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    sys,
                    "argv",
                    [
                        "cosched.py",
                        "input1.sched",
                        "--sat-priority",
                        "metop-3=2",
                    ],
                )
            )
            stack.enter_context(patch.object(cosched, "parse_schedule", Mock(return_value=[])))
            stack.enter_context(patch.object(cosched, "dedupe_passes", Mock(return_value=[])))
            stack.enter_context(patch.object(cosched, "schedule_n_channels", schedule_n_channels))
            stack.enter_context(patch.object(cosched, "write_schedule", Mock()))
            stack.enter_context(patch.object(cosched, "clear_tschedule", Mock()))
            push_local = stack.enter_context(patch.object(cosched, "push_schedule_to_mansched", Mock()))

            cosched.main()

        self.assertEqual(scheduled.pri, 2)
        push_local.assert_called_once_with([scheduled])

    def test_fetch_local_schedule_runs_resched_before_listsched_by_default(self):
        cosched = load_cosched_module()
        run_mock = Mock(return_value=Mock(returncode=0, stdout=SCHEDULE_WITH_PASS, stderr=""))

        original_flag = cosched.RUN_RESCHED_BEFORE_LISTSCHED
        cosched.RUN_RESCHED_BEFORE_LISTSCHED = True
        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(cosched.os.path, "isfile", Mock(return_value=True)))
                stack.enter_context(patch.object(cosched.os, "access", Mock(return_value=True)))
                stack.enter_context(patch.object(cosched.subprocess, "run", run_mock))

                cosched.fetch_local_schedule()
        finally:
            cosched.RUN_RESCHED_BEFORE_LISTSCHED = original_flag

        command = run_mock.call_args[0][0][2]
        self.assertIn(cosched.RESCHED, command)
        self.assertIn(cosched.LISTSCHED, command)

    def test_fetch_local_schedule_fails_when_resched_missing(self):
        cosched = load_cosched_module()

        original_flag = cosched.RUN_RESCHED_BEFORE_LISTSCHED
        cosched.RUN_RESCHED_BEFORE_LISTSCHED = True
        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(cosched.os.path, "isfile", Mock(return_value=False)))
                stack.enter_context(patch.object(cosched.subprocess, "run", Mock()))

                with self.assertRaises(RuntimeError) as exc:
                    cosched.fetch_local_schedule()
        finally:
            cosched.RUN_RESCHED_BEFORE_LISTSCHED = original_flag

        self.assertIn("resched not found", str(exc.exception))

    def test_no_resched_switch_skips_resched_in_fetch(self):
        cosched = load_cosched_module()
        run_mock = Mock(return_value=Mock(returncode=0, stdout=SCHEDULE_WITH_PASS, stderr=""))

        with ExitStack() as stack:
            stack.enter_context(patch.object(sys, "argv", ["cosched.py", "--fetch", "--no-resched"]))
            stack.enter_context(patch.object(cosched.subprocess, "run", run_mock))
            stack.enter_context(patch.object(cosched, "write_raw_schedule", Mock(return_value="/tmp/local.sched")))
            stack.enter_context(patch.object(cosched, "parse_schedule", Mock(return_value=[])))
            stack.enter_context(patch.object(cosched, "dedupe_passes", Mock(return_value=[])))
            stack.enter_context(patch.object(cosched, "schedule_n_channels", Mock(return_value=([[]], []))))
            stack.enter_context(patch.object(cosched, "write_schedule", Mock()))
            stack.enter_context(patch.object(cosched, "clear_tschedule", Mock()))
            stack.enter_context(patch.object(cosched, "push_schedule_to_mansched", Mock()))

            cosched.main()

        command = run_mock.call_args[0][0][2]
        self.assertIn(cosched.LISTSCHED, command)
        self.assertNotIn(cosched.RESCHED, command)


class CoschedDelayRoundingTests(unittest.TestCase):
    """Cover rounding-sensitive scheduling cases near trim and delay caps."""

    def test_append_trims_previous_pass_to_keep_delay_within_cap(self):
        cosched = load_cosched_module()
        prev = make_pass(cosched, datetime(2099, 1, 1, 12, 0, 0), 170, 1, sat="prev")
        current = make_pass(cosched, datetime(2099, 1, 1, 12, 2, 59), 120, 2, sat="current")

        channels, unscheduled = cosched.schedule_n_channels(
            [prev, current],
            n_channels=1,
            gap_seconds=190,
            max_trim_seconds=180,
            max_start_delay=180,
        )

        self.assertEqual(unscheduled, [])
        self.assertEqual(len(channels[0]), 2)
        self.assertEqual(channels[0][0].out_dur_s, 160)
        self.assertEqual(channels[0][1].out_start, datetime(2099, 1, 1, 12, 5, 50))
        self.assertLessEqual(
            int((channels[0][1].out_start - current.start).total_seconds()),
            180,
        )

    def test_insertion_split_does_not_round_following_pass_past_delay_cap(self):
        cosched = load_cosched_module()
        inserted = make_pass(cosched, datetime(2099, 1, 1, 12, 8, 20), 100, 1, sat="inserted")
        following = make_pass(cosched, datetime(2099, 1, 1, 12, 10, 1), 100, 2, sat="following")
        following.out_start = datetime(2099, 1, 1, 12, 13, 0)
        following.out_dur_s = 100

        result = cosched._find_insertion(
            [following],
            inserted,
            gap_seconds=190,
            max_trim_10=180,
            max_start_delay=180,
        )

        self.assertIsNotNone(result)
        insert_idx, adj_start, adj_dur, side_effects = result
        self.assertEqual(insert_idx, 0)
        self.assertEqual(adj_start, inserted.start)
        self.assertEqual(adj_dur, 90)
        self.assertEqual(side_effects, [])

    def test_excluded_pass_on_all_channels_is_unscheduled(self):
        cosched = load_cosched_module()
        blocked = make_pass(cosched, datetime(2099, 1, 1, 12, 0, 0), 100, 1, sat="blocked")

        channels, unscheduled = cosched.schedule_n_channels(
            [blocked],
            n_channels=2,
            channel_exclude_sats=[{"blocked"}, {"blocked"}],
        )

        self.assertEqual(channels, [[], []])
        self.assertEqual(len(unscheduled), 1)
        self.assertIs(unscheduled[0], blocked)

    def test_insertion_split_and_fallback_reject_when_later_delay_budget_exhausted(self):
        cosched = load_cosched_module()
        inserted = make_pass(cosched, datetime(2099, 1, 1, 12, 0, 0), 100, 1, sat="inserted")
        following_1 = make_pass(cosched, datetime(2099, 1, 1, 12, 3, 0), 100, 2, sat="following-1")
        following_2 = make_pass(cosched, datetime(2099, 1, 1, 12, 6, 30), 100, 3, sat="following-2")
        following_1.out_start = datetime(2099, 1, 1, 12, 3, 0)
        following_1.out_dur_s = 100
        following_2.out_start = datetime(2099, 1, 1, 12, 9, 0)
        following_2.out_dur_s = 100

        result = cosched._find_insertion(
            [following_1, following_2],
            inserted,
            gap_seconds=190,
            max_trim_10=0,
            max_start_delay=180,
        )

        self.assertIsNone(result)


class CoschedPushAndIoTests(unittest.TestCase):
    """Validate command assembly/order and output-path sanitation helpers."""

    def test_push_schedule_to_mansched_orders_by_time_and_skips_zero_duration(self):
        cosched = load_cosched_module()
        late = make_pass(cosched, datetime(2099, 1, 1, 12, 20, 0), 120, 1, sat="late")
        zero = make_pass(cosched, datetime(2099, 1, 1, 12, 10, 0), 0, 2, sat="zero")
        early = make_pass(cosched, datetime(2099, 1, 1, 12, 0, 0), 120, 3, sat="early")
        run_mock = Mock(return_value=Mock(returncode=0, stdout="", stderr=""))

        with ExitStack() as stack:
            stack.enter_context(patch.object(cosched.os.path, "isfile", Mock(return_value=True)))
            stack.enter_context(patch.object(cosched.os, "access", Mock(return_value=True)))
            stack.enter_context(patch.object(cosched.subprocess, "run", run_mock))

            cosched.push_schedule_to_mansched([late, zero, early])

        self.assertEqual(run_mock.call_count, 2)
        first_args = run_mock.call_args_list[0][0][0]
        second_args = run_mock.call_args_list[1][0][0]
        self.assertIn("satellite=early", first_args)
        self.assertIn("satellite=late", second_args)

    def test_push_schedule_to_remote_mansched_requires_host(self):
        cosched = load_cosched_module()

        with self.assertRaises(RuntimeError) as exc:
            cosched.push_schedule_to_remote_mansched([], "")

        self.assertIn("Remote host not provided", str(exc.exception))

    def test_push_schedule_to_remote_mansched_skips_subprocess_for_empty_batch(self):
        cosched = load_cosched_module()
        zero = make_pass(cosched, datetime(2099, 1, 1, 12, 0, 0), 0, 1, sat="zero")

        with ExitStack() as stack:
            run_mock = stack.enter_context(patch.object(cosched.subprocess, "run", Mock()))
            cosched.push_schedule_to_remote_mansched([zero], "remote-a")

        run_mock.assert_not_called()

    def test_write_raw_schedule_sanitizes_label_and_rejects_empty(self):
        cosched = load_cosched_module()

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(cosched, "SCHED_DIR", tmpdir):
                path = cosched.write_raw_schedule("ops@host:2222", "payload")

            self.assertTrue(path.endswith("ops_host_2222.sched"))
            self.assertEqual(Path(path).read_text(encoding="utf-8"), "payload")

        with self.assertRaises(ValueError):
            cosched.write_raw_schedule("", "payload")


class CoschedDedupeTests(unittest.TestCase):
    """Verify duplicate selection keeps the intended pass and priority."""

    def test_near_duplicate_keeps_earlier_priority_when_starts_differ(self):
        cosched = load_cosched_module()
        earlier = make_pass(cosched, datetime(2099, 1, 1, 12, 0, 0), 100, 1, sat="metop-3", pri=5)
        later = make_pass(cosched, datetime(2099, 1, 1, 12, 0, 20), 100, 2, sat="metop-3", pri=1)

        deduped = cosched.dedupe_passes([later, earlier])

        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0].start, earlier.start)
        self.assertEqual(deduped[0].pri, 5)

    def test_exact_start_tie_adopts_lower_priority_value(self):
        cosched = load_cosched_module()
        first = make_pass(cosched, datetime(2099, 1, 1, 12, 0, 0), 100, 1, sat="metop-3", pri=5)
        tied = make_pass(cosched, datetime(2099, 1, 1, 12, 0, 0), 100, 2, sat="metop-3", pri=1)

        deduped = cosched.dedupe_passes([first, tied])

        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0].start, first.start)
        self.assertEqual(deduped[0].pri, 1)

    def test_exact_start_tie_keeps_full_payload_from_lower_priority_value(self):
        cosched = load_cosched_module()
        first = make_pass(cosched, datetime(2099, 1, 1, 12, 0, 0), 100, 1, sat="metop-3", pri=5)
        tied = make_pass(cosched, datetime(2099, 1, 1, 12, 0, 0), 240, 2, sat="metop-3", pri=1)

        deduped = cosched.dedupe_passes([first, tied])

        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0].idx, tied.idx)
        self.assertEqual(deduped[0].dur_s, tied.dur_s)
        self.assertEqual(deduped[0].pri, tied.pri)


if __name__ == "__main__":
    unittest.main()