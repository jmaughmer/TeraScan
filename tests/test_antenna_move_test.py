"""Regression tests for antenna_move_test.sh error reporting.

The script is run with stub ac/bc/setqdc commands, so no antenna hardware or
TeraScan installation is needed. The tests are skipped when bash is unavailable.
"""

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "antenna_move_test.sh"


def find_bash():
    """Return a usable bash path, or None. Skips the WSL launcher on Windows."""
    bash = shutil.which("bash")
    if bash and os.name == "nt":
        windows_dir = os.path.normcase(os.environ.get("SystemRoot", r"C:\Windows"))
        if os.path.normcase(bash).startswith(windows_dir):
            return None
    return bash


BASH = find_bash()


@unittest.skipIf(BASH is None, "bash is not available")
class AntennaMoveTestErrorOutputTests(unittest.TestCase):
    """Errors must reach the terminal both before and after logging starts."""

    def run_script(self, *args):
        """Run the script with stub dependencies; return (process, log text)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            home = Path(tmpdir) / "home"
            stub_bin = Path(tmpdir) / "bin"
            home.mkdir()
            stub_bin.mkdir()
            for name in ("ac", "bc", "setqdc"):
                stub = stub_bin / name
                stub.write_text("#!/bin/sh\nexit 0\n")
                stub.chmod(0o755)
            env = dict(os.environ)
            env["HOME"] = str(home)
            env["PATH"] = str(stub_bin) + os.pathsep + env.get("PATH", "")
            proc = subprocess.run(
                [BASH, str(SCRIPT_PATH)] + list(args),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                env=env,
                timeout=60,
            )
            logs = list((home / "logs").glob("*.log"))
            log_text = logs[0].read_text() if logs else ""
        return proc, log_text

    def test_validation_error_after_log_redirect_reaches_terminal_stderr(self):
        # Regression: die() wrote only to stderr, which the logging redirect sends
        # to the log file, so validation and timeout errors never reached the screen.
        proc, log_text = self.run_script("--az-start", "100", "--az-end", "100")

        self.assertEqual(proc.returncode, 1)
        self.assertIn("ERROR: AZ_START and AZ_END must be different", proc.stderr)
        self.assertIn("ERROR: AZ_START and AZ_END must be different", log_text)

    def test_option_error_before_log_redirect_is_printed_once(self):
        proc, _ = self.run_script("--bogus")

        self.assertEqual(proc.returncode, 1)
        self.assertEqual(proc.stderr, "ERROR: Unknown option: --bogus (run with --help)\n")


if __name__ == "__main__":
    unittest.main()
