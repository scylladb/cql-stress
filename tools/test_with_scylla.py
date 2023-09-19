#! /usr/bin/env python3

import argparse
import os
import subprocess

from util.scylla_docker import *


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-filter",
                        help="filter tests by name")
    parser.add_argument("--teardown",
                        choices=["always", "on-success", "never"],
                        default="always",
                        help="configure teardown behavior")

    args = parser.parse_args()

    # Build the tests now. This will allow us to skip setting up the Scylla Docker
    # in case tests don't compile.
    print("Building tests")
    subprocess.run(args=["cargo", "build", "--tests"], check=True)

    with scylla_docker(args.teardown) as node:
        print("Running tests")
        test_env = os.environ.copy()
        test_env["SCYLLA_URI"] = f"{node.ip}:{node.port}"

        command = ["cargo", "test"]
        if args.test_filter is not None:
            command.append(args.test_filter)
        command += ["--", "--test-threads=1"]

        subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
