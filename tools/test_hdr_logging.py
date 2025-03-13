#! /usr/bin/env python3

import os
from util.cassandra_stress import CqlStressCassandraStress
from util.cassandra_stress import generate_random_keyspaces, CSCliRuntimeArguments
from util.scylla_docker import ScyllaDockerNode

# This test verifies that HDR logging functionality works correctly


def check_hdr_file_valid(file_path, expected_interval=1):
    """Check if HDR log file has been created and has valid content."""
    print(f"Checking HDR file: {file_path}")
    if not os.path.exists(file_path):
        print(f"ERROR: HDR file {file_path} does not exist!")
        return False

    # Check file size - should be non-empty
    file_size = os.path.getsize(file_path)
    print(f"HDR file size: {file_size} bytes")
    if file_size == 0:
        print(f"ERROR: HDR file {file_path} is empty!")
        return False

    # Validate file contents
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        if len(lines) < 5:
            print(f"ERROR: HDR file has too few lines ({len(lines)})")
            return False
        
        # Validate header comments
        expected_headers = [
            "Logged with Cql-stress",
            "#[StartTime:",
            "#[BaseTime:",
            "#[MaxValueDivisor:",
        ]
        
        for i, expected in enumerate(expected_headers):
            if i >= len(lines):
                print(f"ERROR: Missing header line: {expected}")
                return False
            if expected not in lines[i]:
                print(f"ERROR: Header mismatch. Expected '{expected}' in line: {lines[i].strip()}")
                return False
                
        print("All header lines validated successfully")
        
        # Validate data rows
        if len(lines) <= 4:
            print("WARNING: No data rows found in HDR file")
            return True  # Still return True as the file format is valid, just empty
            
        data_rows = lines[4:]
        last_timestamp = None
        
        for i, row in enumerate(data_rows):
            # Skip empty lines
            if not row.strip():
                continue
                
            # Example row: Tag=write,5.449,5.002,4.944,<data>
            parts = row.split(',')
            
            # Last row might be shorter due to end of the test
            is_last_row = (i == len(data_rows) - 1)
            if not is_last_row and len(parts) < 4:
                print(f"ERROR: Invalid data row format: {row.strip()}")
                return False
                
            # Validate tag presence
            if not parts[0].startswith("Tag="):
                print(f"ERROR: Missing tag in row: {row.strip()}")
                return False
                
            try:
                # Get timestamp and interval length
                timestamp = float(parts[1])
                interval_length = float(parts[2])
                tolerance = 0.1  # 0.1 second tolerance
                
                # First row validation
                if i == 0:
                    # First timestamp should be around expected_interval 
                    if timestamp < expected_interval - tolerance or timestamp > expected_interval + tolerance:
                        print(f"ERROR: First timestamp {timestamp} is not close to expected interval {expected_interval}")
                        return False
                
                # Subsequent rows validation - skip validation for last row
                if last_timestamp is not None and not is_last_row:
                    # Check if this timestamp is approximately last_timestamp + interval_length
                    expected_timestamp = last_timestamp + expected_interval
                    
                    if abs(timestamp - expected_timestamp) > tolerance:
                        print(f"ERROR: Timestamp {timestamp} does not match expected {expected_timestamp} (previous + interval)")
                        return False
                
                # Check if interval length is close to expected interval - skip validation for last row
                if not is_last_row and abs(interval_length - expected_interval) > tolerance:
                    print(f"ERROR: Interval length {interval_length} is not close to expected {expected_interval}")
                    return False
                    
                last_timestamp = timestamp
                
            except ValueError as e:
                print(f"ERROR: Invalid numeric data in row: {row.strip()} - {e}")
                return False
                
        print(f"Successfully validated {len(data_rows)} data rows")
        return True
        
    except Exception as e:
        print(f"ERROR: Failed to validate HDR file contents: {e}")
        return False


def run(runtime_args: CSCliRuntimeArguments, node: ScyllaDockerNode,
        cql_stress: CqlStressCassandraStress):
    keyspaces = generate_random_keyspaces()
    ks_cqlstress = keyspaces.ks_cqlstress

    # Create a temporary directory for HDR log files if no hdr_log_file provided
    hdr_file = runtime_args.hdr_log_file

    print("\n=== Starting the HDR logging test... ===")
    print(f"\n=== Running cql-stress with log interval ({runtime_args.log_interval}s) ===\n")
    
    # Run the stress test
    cql_stress.run(
        command="write",
        node_ip=node.ip,
        keyspace=ks_cqlstress,
        runtime_args=runtime_args
    )
    
    # Verify HDR file was created
    if not check_hdr_file_valid(hdr_file, expected_interval=runtime_args.log_interval):
        raise RuntimeError("HDR log test failed")

    print("\n=== HDR logging test successful ===\n")

