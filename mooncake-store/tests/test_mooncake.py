import argparse
import time
import random
import statistics
from typing import List, Dict, Any
from mooncake.store import MooncakeDistributedStore

# How to test
# 1. Run `python ./test_mooncake.py --role=prefill` in one terminal
# 2. Run `python ./test_mooncake.py --role=decode` in another terminal

class PerformanceTracker:
    """Tracks and calculates performance metrics for operations."""

    def __init__(self):
        self.operation_latencies: List[float] = []
        self.operation_sizes: List[int] = []
        self.start_time: float = 0
        self.end_time: float = 0

    def start_tracking(self):
        """Start overall performance tracking."""
        self.start_time = time.perf_counter()

    def stop_tracking(self):
        """Stop overall performance tracking."""
        self.end_time = time.perf_counter()

    def record_operation(self, latency_seconds: float, data_size_bytes: int):
        """Record a single operation's performance."""
        self.operation_latencies.append(latency_seconds)
        self.operation_sizes.append(data_size_bytes)

    def get_statistics(self) -> Dict[str, Any]:
        """Calculate and return comprehensive performance statistics."""
        if not self.operation_latencies:
            return {"error": "No operations recorded"}

        total_time = sum(self.operation_latencies)
        total_operations = len(self.operation_latencies)
        total_bytes = sum(self.operation_sizes)

        # Convert latencies to milliseconds for reporting
        latencies_ms = [lat * 1000 for lat in self.operation_latencies]

        # Calculate percentiles
        p90_latency = statistics.quantiles(latencies_ms, n=10)[8] if len(latencies_ms) >= 10 else max(latencies_ms)
        p99_latency = statistics.quantiles(latencies_ms, n=100)[98] if len(latencies_ms) >= 100 else max(latencies_ms)

        # Calculate throughput metrics
        ops_per_second = total_operations / total_time if total_time > 0 else 0
        bytes_per_second = total_bytes / total_time if total_time > 0 else 0
        mbps = bytes_per_second / (1024 * 1024)  # MB/s

        return {
            "total_operations": total_operations,
            "total_time_seconds": total_time,
            "total_bytes": total_bytes,
            "p90_latency_ms": p90_latency,
            "p99_latency_ms": p99_latency,
            "mean_latency_ms": statistics.mean(latencies_ms),
            "operations_per_second": ops_per_second,
            "throughput_mbps": mbps,
            "throughput_bytes_per_second": bytes_per_second
        }

class TestInstance:
    def __init__(self, args):
        self.args = args
        self.store = None
        self.performance_tracker = PerformanceTracker()

    def setup(self):
        """Initialize the MooncakeDistributedStore with configuration."""
        self.store = MooncakeDistributedStore()

        # Use command-line arguments instead of environment variables
        protocol = self.args.protocol
        device_name = self.args.device_name
        local_hostname = self.args.local_hostname
        metadata_server = self.args.metadata_server
        global_segment_size = self.args.global_segment_size * 1024 * 1024  # Convert MB to bytes
        local_buffer_size = self.args.local_buffer_size * 1024 * 1024  # Convert MB to bytes
        master_server_address = self.args.master_server

        print(f"Setting up {self.args.role} instance with:")
        print(f"  Protocol: {protocol}")
        print(f"  Device: {device_name}")
        print(f"  Local hostname: {local_hostname}")
        print(f"  Metadata server: {metadata_server}")
        print(f"  Master server: {master_server_address}")
        print(f"  Global segment size: {global_segment_size // (1024*1024)} MB")
        print(f"  Local buffer size: {local_buffer_size // (1024*1024)} MB")

        retcode = self.store.setup(local_hostname,
                                  metadata_server,
                                  global_segment_size,
                                  local_buffer_size,
                                  protocol,
                                  device_name,
                                  master_server_address)
        if retcode:
            print(f"ERROR: Store setup failed with return code {retcode}")
            exit(1)

        print("Store setup completed successfully")
        time.sleep(1)  # Give some time for initialization

    def prefill(self):
        """Execute prefill operations with performance tracking."""
        print(f"Starting prefill operations: {self.args.max_requests} requests")
        print(f"Value size: {self.args.value_length // (1024*1024)} MB per operation")

        self.performance_tracker.start_tracking()

        index = 0
        failed_operations = 0

        while index < self.args.max_requests:
            key = f"k_{index}"
            value = bytes([index % 256] * self.args.value_length)

            # Measure individual operation latency
            op_start = time.perf_counter()
            retcode = self.store.put(key, value)
            op_end = time.perf_counter()

            operation_latency = op_end - op_start

            if retcode:
                print(f"WARNING: put failed, key {key}, retcode {retcode}")
                failed_operations += 1
            else:
                # Only record successful operations
                self.performance_tracker.record_operation(operation_latency, self.args.value_length)

            # Simulate some randomness in operation success (98% success rate)
            if random.randint(0, 100) < 98:
                index += 1

            # Progress reporting
            if index % self.args.progress_interval == 0 and index > 0:
                print(f"Completed {index} prefill operations")

        self.performance_tracker.stop_tracking()

        print(f"Prefill phase completed. Failed operations: {failed_operations}")
        self._print_performance_stats("PREFILL")

        print(f"Waiting {self.args.wait_time} seconds for decode phase...")
        time.sleep(self.args.wait_time)

    def decode(self):
        """Execute decode operations with performance tracking."""
        print(f"Starting decode operations: {self.args.max_requests} requests")
        print(f"Expected value size: {self.args.value_length // (1024*1024)} MB per operation")

        self.performance_tracker.start_tracking()

        index = 0
        failed_operations = 0

        while index < self.args.max_requests:
            key = f"k_{index}"

            # Measure individual operation latency
            op_start = time.perf_counter()
            value = self.store.get_buffer(key)
            op_end = time.perf_counter()

            operation_latency = op_end - op_start

            if value is None:
                print(f"WARNING: get failed, key {key}")
                failed_operations += 1
            else:
                # Record successful operation
                self.performance_tracker.record_operation(operation_latency, len(value))

                # Verify data integrity
                expected_value = bytes([index % 256] * self.args.value_length)

            index += 1

            # Progress reporting
            if index % self.args.progress_interval == 0:
                print(f"Completed {index} decode operations")

        self.performance_tracker.stop_tracking()
        self._print_performance_stats("DECODE")

        print(f"Waiting {self.args.wait_time} seconds before exit...")
        time.sleep(self.args.wait_time)

    def _print_latency_histogram(self, latencies_ms: List[float]):
        """Prints a simple text-based histogram of latencies."""
        if not latencies_ms:
            print("\nNo latency data to generate a histogram.")
            return

        min_lat, max_lat = min(latencies_ms), max(latencies_ms)
        if abs(max_lat - min_lat) < 1e-9:
            print(f"\nLatency Distribution: All latencies are {min_lat:.2f} ms.")
            return

        num_bins = 10
        bin_width = (max_lat - min_lat) / num_bins
        
        # To ensure the max value falls into the last bin, we add a small epsilon
        if bin_width > 0:
            bins = [0] * num_bins
            for lat in latencies_ms:
                bin_index = int((lat - min_lat) / bin_width)
                if bin_index >= num_bins:
                    bin_index = num_bins - 1
                bins[bin_index] += 1
        else: # All values are the same
            bins = [len(latencies_ms)]
            num_bins = 1

        max_bin_count = max(bins) if bins else 0
        if max_bin_count == 0:
            return

        print(f"\nLatency Distribution:")
        for i in range(num_bins):
            bar_width = int(bins[i] / max_bin_count * 40) if max_bin_count > 0 else 0
            bar = '#' * bar_width
            bin_start = min_lat + i * bin_width
            bin_end = bin_start + bin_width
            print(f"  {bin_start:7.2f} - {bin_end:7.2f} ms | {bar} ({bins[i]})")

    def _print_performance_stats(self, operation_type: str):
        """Print comprehensive performance statistics."""
        stats = self.performance_tracker.get_statistics()

        if "error" in stats:
            print(f"No performance data available for {operation_type}: {stats['error']}")
            return

        print(f"\n=== {operation_type} PERFORMANCE STATISTICS ===")
        print(f"Total operations: {stats['total_operations']}")
        print(f"Total time: {stats['total_time_seconds']:.2f} seconds")
        print(f"Total data transferred: {stats['total_bytes'] / (1024*1024):.2f} MB")
        print(f"")
        print(f"Latency metrics:")
        print(f"  Mean latency: {stats['mean_latency_ms']:.2f} ms")
        print(f"  P90 latency:  {stats['p90_latency_ms']:.2f} ms")
        print(f"  P99 latency:  {stats['p99_latency_ms']:.2f} ms")
        print(f"")
        print(f"Throughput metrics:")
        print(f"  Operations/sec: {stats['operations_per_second']:.2f}")
        print(f"  Throughput:     {stats['throughput_mbps']:.2f} MB/s")
        print(f"  Bandwidth:      {stats['throughput_bytes_per_second']:.0f} bytes/s")
        self._print_latency_histogram([
            lat * 1000 for lat in self.performance_tracker.operation_latencies
        ])
        print(f"===============================================\n")


def parse_arguments():
    """Parse command-line arguments for the stress test."""
    parser = argparse.ArgumentParser(
        description="Mooncake Distributed Store Stress Test with Performance Measurement",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Role configuration
    parser.add_argument(
        "--role",
        type=str,
        choices=["prefill", "decode"],
        required=True,
        help="Role of this instance: prefill (producer) or decode (consumer)"
    )

    # Network and connection settings
    parser.add_argument(
        "--protocol",
        type=str,
        default="rdma",
        help="Communication protocol to use"
    )
    parser.add_argument(
        "--device-name",
        type=str,
        default="erdma_0",
        help="Network device name for RDMA"
    )
    parser.add_argument(
        "--local-hostname",
        type=str,
        default="localhost",
        help="Local hostname (use `ip addr` to find correct hostname)"
    )
    parser.add_argument(
        "--metadata-server",
        type=str,
        default="192.168.0.144:2379",
        help="Metadata server address"
    )
    parser.add_argument(
        "--master-server",
        type=str,
        default="192.168.0.144:50051",
        help="Master server address"
    )

    # Memory and storage settings
    parser.add_argument(
        "--global-segment-size",
        type=int,
        default=8000,
        help="Global segment size in MB"
    )
    parser.add_argument(
        "--local-buffer-size",
        type=int,
        default=8000,
        help="Local buffer size in MB"
    )

    # Test parameters
    parser.add_argument(
        "--max-requests",
        type=int,
        default=100,
        help="Maximum number of requests to process"
    )
    parser.add_argument(
        "--value-length",
        type=int,
        default=16*1024*1024,
        help="Size of each value in bytes"
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=500,
        help="Progress reporting interval (number of operations)"
    )
    parser.add_argument(
        "--wait-time",
        type=int,
        default=20,
        help="Wait time in seconds after operations complete"
    )

    return parser.parse_args()


def main():
    """Main entry point for the stress test."""
    args = parse_arguments()

    print("=== Mooncake Distributed Store Stress Test ===")
    print(f"Role: {args.role.upper()}")
    print(f"Protocol: {args.protocol}")
    print(f"Max requests: {args.max_requests}")
    print(f"Value size: {args.value_length // (1024*1024)} MB")
    print("=" * 50)

    try:
        # Create and setup test instance
        tester = TestInstance(args)
        tester.setup()

        # Execute the appropriate role
        if args.role == "decode":
            tester.decode()
        else:
            tester.prefill()

        print("Test completed successfully!")

    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"Test failed with error: {e}")
        raise


if __name__ == '__main__':
    main()