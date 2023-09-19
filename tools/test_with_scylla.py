#! /usr/bin/env python3

import argparse
from collections import namedtuple
from contextlib import contextmanager
import os
import subprocess
import time


ScyllaDockerNode = namedtuple("ScyllaDockerNode", ["ip", "port"])


def wait_until_scylla_responds(container_id, max_attempts):
    for i in range(max_attempts):
        try:
            print(
                f"Waiting until Scylla responds to queries (attempt #{i}/{max_attempts})")
            subprocess.run(["docker", "exec", container_id,
                            "cqlsh", "-e", "select * from system.local"],
                           check=True, stdout=subprocess.DEVNULL)
            # If we are here, this means success
            return
        except KeyboardInterrupt:
            # Propagate it further
            raise
        except:
            time.sleep(5)

    raise RuntimeError(
        "Scylla didn't start responding within the configured amount of retries")


@contextmanager
def scylla_docker(teardown_behavior, max_healthcheck_attempts):
    print("Setting up Scylla")
    cp = subprocess.run(args=["docker", "run", "-d", "-p", "9042:9042", "scylladb/scylla"],
                        capture_output=True, check=True)
    container_id = cp.stdout.decode('utf-8').strip()
    print(f"Container ID: {container_id}")

    success = False
    try:
        # Wait until Scylla starts responding to queries
        wait_until_scylla_responds(container_id, max_healthcheck_attempts)

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
            subprocess.run(args=["docker", "stop", container_id], check=True)
            subprocess.run(args=["docker", "rm", container_id], check=True)
            print("Teardown successful")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-filter",
                        help="filter tests by name")
    parser.add_argument("--teardown",
                        choices=["always", "on-success", "never"],
                        default="always",
                        help="configure teardown behavior")
    parser.add_argument("--max-healthcheck-attempts",
                        type=int,
                        default=20,
                        help="maximum number of attempts to connect to Scylla " +
                        "before running the tests")

    args = parser.parse_args()

    # Build the tests now. This will allow us to skip setting up the Scylla Docker
    # in case tests don't compile.
    print("Building tests")
    subprocess.run(args=["cargo", "build", "--tests"], check=True)

    with scylla_docker(args.teardown, args.max_healthcheck_attempts) as node:
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
