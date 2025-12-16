import subprocess
import os
import random
from datetime import datetime
from os.path import dirname
from collections import namedtuple


DEFAULT_CASSANDRA_STRESS_VERSION = "v3.17.3"
ROOT_DIRECTORY = dirname(dirname(dirname(__file__)))
DOWNLOAD_DIRECTORY_NAME = os.path.join(ROOT_DIRECTORY, "cassandra-download")

DEFAULT_TIMESTAMP_FORMAT = "%Y%m%d_%Hh%Mm%Ss"

TEST_PROFILES_DIRECTORY = os.path.join(
    ROOT_DIRECTORY, "tools", "util", "profiles")


Keyspaces = namedtuple("Keyspaces", ["ks_cassandra", "ks_cqlstress"])


CSCliRuntimeArguments = namedtuple("CSCliRuntimeArguments", [
                                   "workload_size", "concurrency", "hdr_log_file", "log_interval", "throttle"])
DEFAULT_RUNTIME_ARGUMENTS = CSCliRuntimeArguments(
    workload_size=100, concurrency=1, hdr_log_file=None, log_interval=1, throttle=None)


def prepare_args(command, node_ip, keyspace, runtime_args: CSCliRuntimeArguments = DEFAULT_RUNTIME_ARGUMENTS):
    args = [command, "no-warmup", f"n={runtime_args.workload_size}",
            "-node", node_ip]
    
    # Add rate arguments with optional throttle
    rate_args = [f"threads={runtime_args.concurrency}"]
    if runtime_args.throttle:
        rate_args.append(f"throttle={runtime_args.throttle}")
    args.extend(["-rate"] + rate_args)
    
    args.extend(["-schema", f"keyspace={keyspace}"])

    # Add HDR logging options if specified
    if runtime_args.hdr_log_file:
        log_args = ["-log", f"hdrfile={runtime_args.hdr_log_file}"]
        if runtime_args.log_interval != 1:  # Only add if not the default
            log_args.append(f"interval={runtime_args.log_interval}")
        args.extend(log_args)

    return args


# Default query name used in test profiles.
DEFAULT_TEST_QUERY_NAME = "test_query"


def prepare_user_args(node_ip, profile_name, runtime_args: CSCliRuntimeArguments = DEFAULT_RUNTIME_ARGUMENTS):
    rate_args = [f"threads={runtime_args.concurrency}"]
    if runtime_args.throttle:
        rate_args.append(f"throttle={runtime_args.throttle}")
    
    return ["user", f"profile={profile_name}",
            "no-warmup", f"n={runtime_args.workload_size}",
            "-node", node_ip,
            "-rate"] + rate_args


def generate_random_keyspaces(timestamp_format=DEFAULT_TIMESTAMP_FORMAT):
    r = random.randint(0, 100000)
    now = datetime.now().strftime(timestamp_format)
    ks_cassandra = f"ks_cassandra_{r}_{now}"
    ks_cqlstress = f"ks_cqlstress_{r}_{now}"
    return Keyspaces(ks_cassandra=ks_cassandra, ks_cqlstress=ks_cqlstress)


class CSCliRunner:
    def __init__(self, stress_cmd):
        self.stress_cmd = stress_cmd

    def run(self, command, node_ip, keyspace, runtime_args: CSCliRuntimeArguments):
        args = self.stress_cmd + \
            prepare_args(command, node_ip, keyspace, runtime_args)
        subprocess.run(args=args, check=True)

    def prepare_user_args(self, node_ip, profile_name, query_name=DEFAULT_TEST_QUERY_NAME, runtime_args: CSCliRuntimeArguments = DEFAULT_RUNTIME_ARGUMENTS):
        full_profile_name = os.path.join(TEST_PROFILES_DIRECTORY, profile_name)
        rate_args = [f"threads={runtime_args.concurrency}"]
        if runtime_args.throttle:
            rate_args.append(f"throttle={runtime_args.throttle}")
        
        return ["user", f"profile={full_profile_name}", f"ops({query_name}=1)",
                "no-warmup", f"n={runtime_args.workload_size}",
                "-node", node_ip,
                "-rate"] + rate_args + [
                f"-pop seq=1..{runtime_args.workload_size}"]

    def run_user(self, node_ip, profile_name, runtime_args: CSCliRuntimeArguments):
        args = self.stress_cmd + \
            self.prepare_user_args(
                node_ip=node_ip, profile_name=profile_name, runtime_args=runtime_args)
        subprocess.run(args=args, check=True)


class CassandraStress(CSCliRunner):
    def __init__(self, cassandra_stress_version=DEFAULT_CASSANDRA_STRESS_VERSION):
        cassandra_stress_dir = "cassandra-stress"
        cassandra_stress_tar = "cassandra-stress-bin.tar.gz"

        abs_cassandra_dir = os.path.join(DOWNLOAD_DIRECTORY_NAME, cassandra_stress_dir)
        abs_cassandra_tar = os.path.join(DOWNLOAD_DIRECTORY_NAME, cassandra_stress_tar)

        super().__init__(
            stress_cmd=[os.path.join(abs_cassandra_dir, "bin", "cassandra-stress")]
        )

        if os.path.exists(abs_cassandra_dir):
            # Cassandra already fetched.
            print(
                f"Cassandra {cassandra_stress_version} already installed. Skipping the download phase."
            )
            return

        if not os.path.exists(abs_cassandra_tar):
            os.makedirs(DOWNLOAD_DIRECTORY_NAME, exist_ok=True)
            # Fetch cassandra.
            print(
                f"Fetching cassandra {cassandra_stress_version} to {DOWNLOAD_DIRECTORY_NAME}"
            )
            cassandra_stress_url = f"https://github.com/scylladb/cassandra-stress/releases/download/{cassandra_stress_version}/cassandra-stress-bin.tar.gz"
            subprocess.run(
                args=[
                    "wget",
                    "-c",
                    "-P",
                    DOWNLOAD_DIRECTORY_NAME,
                    "-N",
                    "--no-verbose",
                    cassandra_stress_url,
                ],
                check=True,
            )

        # Extract cassandra
        print(f"Extracting cassandra {cassandra_stress_version}")
        subprocess.run(
            args=[
                "tar",
                "-xzf",
                abs_cassandra_tar,
                "--directory",
                DOWNLOAD_DIRECTORY_NAME,
            ],
            check=True,
        )
        print(f"Extracted cassandra to {abs_cassandra_dir}")


class CqlStressCassandraStress(CSCliRunner):
    def __init__(self):
        super().__init__(stress_cmd=["cql-stress-cassandra-stress"])


if __name__ == "__main__":
