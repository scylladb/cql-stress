from collections import namedtuple
from contextlib import contextmanager
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
