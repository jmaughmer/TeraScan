#!/usr/bin/env python3
"""
Get TeraScan schedules locally (and optionally from a remote host) and write them to /tmp.

Behavior:
- Runs /opt/terascan/bin/listsched locally and writes to /tmp/<hostname>.sched
- If --remote <host> is provided, also runs the same command via SSH on that host and writes to /tmp/<remotehost>.sched

Notes:
- Python 3.6 compatible (RHEL7)
- Exits non-zero if any requested invocation fails
"""

import argparse
import os
import re
import socket
import subprocess
import sys
from typing import Optional


LISTSCHED = "/opt/terascan/bin/listsched"
OUTPUT_DIR = "/tmp"
TIMEOUT_SECS = 120  # reasonable default timeout per call


def sanitize_label(host: str) -> str:
	"""Return a safe filename label derived from a host string.
	Examples:
	  'user@host.example.com' -> 'host.example.com'
	  'host:2222' -> 'host'
	Only allow [A-Za-z0-9_.-] in the result; replace others with '_'.
	"""
	# strip user@
	if "@" in host:
		host = host.split("@", 1)[1]
	# strip :port
	if ":" in host:
		host = host.split(":", 1)[0]
	# sanitize
	return re.sub(r"[^A-Za-z0-9_.-]", "_", host)


def run_local() -> str:
	"""Run listsched locally and return its stdout as text."""
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


def run_remote(host: str) -> str:
	"""Run listsched on a remote host via SSH and return stdout as text."""
	# Pass the entire remote command as a single string so the remote shell
	# receives it intact; splitting it into separate argv elements causes SSH
	# to join them with spaces, which breaks shell parsing of the semicolon.
	remote_cmd = "source /opt/terascan/etc/tscan.bash_profile && {}".format(LISTSCHED)
	try:
		proc = subprocess.run(
			["ssh", "-q", "-o", "BatchMode=yes", "-o", "ConnectTimeout=30", host, remote_cmd],
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


def write_output(label: str, content: str) -> str:
	"""Write schedule content to a file under OUTPUT_DIR using a sanitized label.

	The label is constrained to characters in [A-Za-z0-9_.-]; all other characters
	are replaced with '_'. This prevents path traversal and other unsafe filenames
	even if an unsanitized label is passed by callers.
	"""
	# Sanitize label to ensure it cannot escape OUTPUT_DIR or create unexpected paths.
	safe_label = re.sub(r"[^A-Za-z0-9_.-]", "_", label)
	if not safe_label:
		raise ValueError("label must contain at least one valid character")
	path = os.path.join(OUTPUT_DIR, "{}.sched".format(safe_label))
	with open(path, "w", encoding="utf-8") as f:
		f.write(content)
	return path


def main(argv: Optional[list] = None) -> int:
	parser = argparse.ArgumentParser(description="Run TeraScan listsched locally and optionally on a remote host, writing outputs to /tmp.")
	parser.add_argument("--remote", "-r", help="Remote host to SSH into and run listsched as well (e.g., host or user@host)")
	args = parser.parse_args(argv)

	failures = 0

	# Local
	local_host = socket.gethostname()
	try:
		out = run_local()
		path = write_output(sanitize_label(local_host), out)
		print("Wrote local schedule to {}".format(path))
	except Exception as e:  # noqa: BLE001 (broad except acceptable for CLI reporting)
		failures += 1
		print("ERROR: {}".format(e), file=sys.stderr)

	# Remote
	if args.remote:
		try:
			out = run_remote(args.remote)
			path = write_output(sanitize_label(args.remote), out)
			print("Wrote remote schedule to {}".format(path))
		except Exception as e:  # noqa: BLE001
			failures += 1
			print("ERROR: {}".format(e), file=sys.stderr)

	return 1 if failures else 0


if __name__ == "__main__":
	sys.exit(main())

