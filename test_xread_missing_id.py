#!/usr/bin/env python3
"""Populate a stream and use redis-cli to read from a missing ID in its middle."""

import argparse
import shutil
import subprocess
import sys
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    args = parser.parse_args()

    if shutil.which("redis-cli") is None:
        sys.exit("redis-cli was not found on PATH. Install it before running this script.")

    # A fresh key keeps repeated runs independent without deleting existing data.
    stream = f"xread-missing-id:{uuid.uuid4().hex}"
    cli = ["redis-cli", "-h", args.host, "-p", str(args.port), "--raw"]

    def run(*command):
        print(f"> redis-cli {' '.join(command)}", flush=True)
        subprocess.run(cli + list(command), check=True, timeout=5)

    for number in range(1, 11):
        run("XADD", stream, f"{number * 1000}-0", "value", f"entry-{number}")

    print("\n5500-0 does not exist; it falls between 5000-0 and 6000-0.")
    print("Expected: entries 6000-0 through 10000-0 (values entry-6 through entry-10).")
    run("XREAD", "STREAMS", stream, "3200-0")
    print(f"\nStream retained for inspection: {stream}")


if __name__ == "__main__":
    try:
        main()
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        sys.exit(str(error))
