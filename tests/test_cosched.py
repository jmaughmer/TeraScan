import importlib.util
import sys
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import Mock, call, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
COSCHED_PATH = REPO_ROOT / "scripts" / "cosched.py"
SCHEDULE_WITH_PASS = """#  state  pri  satel    telem       date    day    time    durat  post_process
 1  sched   3  metop-3   ahrpt    2026/04/17 107  19:08:50  12:50
"""
HEADER_ONLY_SCHEDULE = "#  state  pri  satel    telem       date    day    time    durat  post_process\n"


def load_cosched_module():
    spec = importlib.util.spec_from_file_location("cosched_under_test", str(COSCHED_PATH))
    module = importlib.util.module_from_spec(spec)
    if spec.loader is None:
        raise RuntimeError("Unable to load cosched module")
    spec.loader.exec_module(module)
    return module


class CoschedMainRoutingTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()