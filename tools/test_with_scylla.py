#! /usr/bin/env python3

import argparse
from collections import namedtuple
from contextlib import contextmanager
import os
import subprocess


ScyllaDockerNode = namedtuple("ScyllaDockerNode", ["ip", "port"])


@contextmanager
def scylla_docker(teardown_behavior, compose_yaml="docker/scylla_test/compose.yml"):
    print("Setting up Scylla")
    print("Starting the container and waiting for it to be healthy")

    subprocess.run(args=["docker", "compose", "-f",
                   compose_yaml, "up", "-d", "--wait"], check=True)

    success = False
    try:

        yield ScyllaDockerNode(ip="127.0.0.1", port="9042")
        success = True
    finally:
        if teardown_behavior == "always":
            do_teardown = True
        elif teardown_behavior == "on-success":
            do_teardown = success
        else:
            do_teardown = False

        if do_teardown:
            print("Tearing down")
            subprocess.run(args=["docker", "compose", "-f",
                           compose_yaml, "stop"], check=True)
            subprocess.run(args=["docker", "compose", "-f",
                           compose_yaml, "down"], check=True)
            print("Teardown successful")


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
