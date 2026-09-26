#!/usr/bin/env python3
"""
Borges latency analysis script for parsing and organizing logs from different components.
Parses logs from client, server, and shard components to organize by request ID.
Based on Borges's shared log architecture.
"""

import re
import os
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from collections import defaultdict


class LogEntry:
    """Represents a single log entry with timestamp and content"""

    def __init__(
        self,
        timestamp: str,
        component: str,
        content: str,
        request_id: Optional[int] = None,
    ):
        self.timestamp = timestamp
        self.component = component
        self.content = content
        self.request_id = request_id
        self.datetime = self._parse_timestamp(timestamp)

    def _parse_timestamp(self, timestamp: str) -> datetime:
        """Parse timestamp string to datetime object"""
        try:
            # Remove brackets and parse the timestamp
            clean_timestamp = timestamp.strip("[]")
            return datetime.strptime(clean_timestamp, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            # Fallback for different timestamp formats
            return datetime.now()

    def __str__(self):
        return f"[{self.timestamp}] [{self.component}] {self.content}"


class BorgesLogAnalyzer:
    """Main class for analyzing Borges system logs"""

    def __init__(self, log_dir: str):
        self.log_dir = log_dir
        self.request_logs = defaultdict(list)  # request_id -> list of LogEntry

    def parse_client_logs(self, filename: str) -> None:
        """Parse client logs - append requests and responses"""
        filepath = os.path.join(self.log_dir, filename)
        if not os.path.exists(filepath):
            print(f"Warning: {filepath} not found")
            return

        with open(filepath, "r") as f:
            lines = f.readlines()

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Extract request ID and timestamp
            request_match = re.search(r"request id[:\s]+(\d+)", line)
            timestamp_match = re.search(r"\[([\d\-\s:.]+)\]", line)

            if request_match and timestamp_match:
                request_id = int(request_match.group(1))
                timestamp = timestamp_match.group(1)
                content = line.split("] ", 2)[-1] if "] " in line else line

                entry = LogEntry(timestamp, "client", content, request_id)
                self.request_logs[request_id].append(entry)

    def parse_server0_log(self, filename: str) -> None:
        """Parse server logs - storage and processing operations"""
        filepath = os.path.join(self.log_dir, filename)
        if not os.path.exists(filepath):
            print(f"Warning: {filepath} not found")
            return

        with open(filepath, "r") as f:
            lines = f.readlines()

        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if not line:
                continue

            # Extract request ID and timestamp
            request_match = re.search(
                r"receive request, request id[:\s]+(\d+)", line)

            if request_match:
                request_id = int(request_match.group(1))
                j = 0
                while i + j < len(lines):
                    current_line = lines[i + j].strip()
                    if "receive request" in current_line and j > 0:
                        # Found the end of this block
                        break
                    timestamp_match = re.search(
                        r"\[([\d\-\s:.]+)\]", current_line)
                    if timestamp_match and 'write index update' not in current_line:
                        timestamp = timestamp_match.group(1)
                        content = (
                            current_line.split("] ", 2)[-1]
                            if "] " in current_line
                            else current_line
                        )
                        entry = LogEntry(timestamp, "server0",
                                         content, request_id)
                        self.request_logs[request_id].append(entry)
                    j += 1
            i += 1

    def parse_sequencer_logs(self, filename: str) -> None:
        """Parse sequencer logs - global ordering and coordination"""
        filepath = os.path.join(self.log_dir, filename)
        if not os.path.exists(filepath):
            print(f"Warning: {filepath} not found")
            return

        with open(filepath, "r") as f:
            lines = f.readlines()

        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if not line:
                continue

            # Extract request ID and timestamp
            request_match = re.search(
                r"receive local cut of request id[:\s]+(\d+)", line
            )

            if request_match:
                request_id = int(request_match.group(1))
                j = 0
                has_update_hash_table = False
                while i + j < len(lines):
                    current_line = lines[i + j].strip()
                    if "receive local cut of request" in current_line and j > 0:
                        # Found the end of this block
                        break
                    if "update hash table, request id" in current_line:
                        if has_update_hash_table:
                            # Skip duplicate update hash table entries
                            j += 1
                            continue
                        has_update_hash_table = True
                    timestamp_match = re.search(
                        r"\[([\d\-\s:.]+)\]", current_line)
                    if timestamp_match:
                        timestamp = timestamp_match.group(1)
                        content = (
                            current_line.split("] ", 2)[-1]
                            if "] " in current_line
                            else current_line
                        )
                        entry = LogEntry(
                            timestamp, "sequencer", content, request_id)
                        self.request_logs[request_id].append(entry)
                    j += 1
            i += 1

    def parse_all_logs(self) -> None:
        """Parse all log files"""
        print("Parsing client logs...")
        self.parse_client_logs("client_log_")

        print("Parsing server logs...")
        self.parse_server0_log("server0_log_")
        # self.parse_server1_log("server1_log_")

        print("Parsing sequencer logs...")
        self.parse_sequencer_logs("sequencer_log_")

        print(f"Parsed logs for {len(self.request_logs)} requests/sequences")

    def _normalize_log_content(self, content: str) -> str:
        """Normalize log content for merging similar actions across all log files"""
        normalized = content

        # Remove component prefixes like [async_logger], [Client, 0], [DataServer, 0], [Sequencer, 0]
        normalized = re.sub(r"\[async_logger\]\s*", "", normalized)
        normalized = re.sub(r"\[[\w\s,]+\]\s*", "", normalized)

        # Normalize client operations
        if "send write request" in normalized:
            normalized = re.sub(
                r"send write request,\s*request id:\s*\d+", "send write request", normalized
            )
        elif "receive response" in normalized:
            normalized = re.sub(
                r"receive response,\s*request id:\s*\d+", "receive response", normalized
            )

        # Normalize server/DataServer operations
        elif "receive request" in normalized:
            normalized = re.sub(
                r"receive request,\s*request id:\s*\d+", "receive request", normalized
            )
        # Normalize sequencer operations
        elif "receive local cut of request id" in normalized:
            normalized = re.sub(
                r"receive local cut of request id:\s*\d+",
                "receive local cut of request",
                normalized,
            )
        elif "update hash table, request id" in normalized:
            normalized = re.sub(
                r"update hash table, request id:\s*\d+",
                "update hash table",
                normalized,
            )
        elif "commit gsn of request id" in normalized:
            normalized = re.sub(
                r"commit gsn of request id:\s*\d+",
                "commit gsn",
                normalized,
            )
        elif "copy_to_cxl_buffer" in normalized:
            normalized = re.sub(
                r"copy_to_cxl_buffer, request id:\s*\d+",
                "copy_to_cxl_buffer",
                normalized,
            )
        elif "write index update" in normalized:
            normalized = re.sub(
                r"write index update, request id:\s*\d+",
                "write index update",
                normalized,
            )
        elif "receive global cut" in normalized:
            normalized = re.sub(
                r"receive global cut, request id:\s*\d+",
                "receive global cut",
                normalized,
            )
        elif "send response" in normalized:
            normalized = re.sub(
                r"send response request id:\s*\d+",
                "send response",
                normalized,
            )

        return normalized.strip()

    def _merge_similar_logs(self, logs: List[LogEntry]) -> List[LogEntry]:
        """Merge logs that represent the same action, keeping the one with latest timestamp"""
        if not logs:
            return logs

        # Group logs by normalized content and request_id
        grouped_logs = defaultdict(list)

        for log in logs:
            # Create a key based on normalized content and request_id
            normalized_content = self._normalize_log_content(log.content)
            key = f"{normalized_content}_{log.request_id}"
            grouped_logs[key].append(log)

        merged_logs = []

        for group in grouped_logs.values():
            if len(group) == 1:
                # Only one log, no merging needed
                merged_logs.append(group[0])
            else:
                # Multiple logs with same normalized content, keep the latest one
                latest_log = max(group, key=lambda x: x.datetime)

                # Create merged content showing all components involved
                components = sorted(set(log.component for log in group))
                if len(components) > 1:
                    # Update the component to show all involved components
                    latest_log.component = f"{'+'.join(components)}"

                merged_logs.append(latest_log)

        return merged_logs

    def get_request_logs(
        self, request_id: int, merge_similar: bool = True
    ) -> List[LogEntry]:
        """Get all logs for a specific request, sorted by timestamp"""
        if request_id not in self.request_logs:
            return []

        # Sort logs by timestamp
        logs = self.request_logs[request_id]
        logs.sort(key=lambda x: x.datetime)

        if merge_similar:
            logs = self._merge_similar_logs(logs)
            # Sort again after merging
            logs.sort(key=lambda x: x.datetime)

        return logs

    def print_request_logs(self, request_id: int) -> None:
        """Print all logs for a specific request in chronological order"""
        logs = self.get_request_logs(request_id)

        if not logs:
            print(f"No logs found for request {request_id}")
            return

        print(f"\n=== Request {request_id} Logs ===")
        for log in logs:
            print(f"[{log.timestamp}] [{log.component}] {log.content}")
        print(f"=== End Request {request_id} ===\n")

    def get_available_requests(self) -> List[int]:
        """Get list of all available request IDs"""
        return sorted(self.request_logs.keys())

    def analyze_request_latency(self, request_id: int) -> Dict[str, float]:
        """Analyze latency breakdown for a specific request"""
        logs = self.get_request_logs(request_id)
        if not logs:
            return {}

        latency_info = {}

        # Find key timestamps for Borges operations
        client_send_time = None
        client_receive_time = None
        server_receive_time = None
        replicate_time = None
        commit_time = None
        ack_time = None

        for log in logs:
            normalized_content = self._normalize_log_content(log.content)

            if log.component == "client" and "send request" in normalized_content:
                client_send_time = log.datetime
            elif log.component == "client" and "receive response" in normalized_content:
                client_receive_time = log.datetime
            elif "server" in log.component and "receive request" in normalized_content:
                if server_receive_time is None:
                    server_receive_time = log.datetime
            elif "send replicate to shard" in normalized_content:
                if replicate_time is None:
                    replicate_time = log.datetime
            elif "receive committed cut" in normalized_content:
                if commit_time is None:
                    commit_time = log.datetime
            elif "send ack to client" in normalized_content:
                if ack_time is None:
                    ack_time = log.datetime

        # Calculate latencies
        if client_send_time and client_receive_time:
            latency_info["total_latency_ms"] = (
                client_receive_time - client_send_time
            ).total_seconds() * 1000

        if client_send_time and server_receive_time:
            latency_info["client_to_server_ms"] = (
                server_receive_time - client_send_time
            ).total_seconds() * 1000

        if server_receive_time and replicate_time:
            latency_info["server_to_replicate_ms"] = (
                replicate_time - server_receive_time
            ).total_seconds() * 1000

        if replicate_time and commit_time:
            latency_info["replicate_to_commit_ms"] = (
                commit_time - replicate_time
            ).total_seconds() * 1000

        if ack_time and client_receive_time:
            latency_info["ack_to_response_ms"] = (
                client_receive_time - ack_time
            ).total_seconds() * 1000

        return latency_info

    def calculate_message_intervals(self, request_id: int) -> List[Tuple[str, float]]:
        """Calculate time intervals between consecutive messages for a request"""
        logs = self.get_request_logs(request_id, merge_similar=True)

        if len(logs) < 2:
            return []

        intervals = []
        for i in range(len(logs) - 1):
            current_log = logs[i]
            next_log = logs[i + 1]

            # Calculate interval in milliseconds
            interval_ms = (
                next_log.datetime - current_log.datetime
            ).total_seconds() * 1000

            # Create interval name
            current_action = self._normalize_log_content(current_log.content)
            next_action = self._normalize_log_content(next_log.content)
            interval_name = f"{current_action} -> {next_action}"

            intervals.append((interval_name, interval_ms))

        return intervals

    def _validate_message_sequence(self, logs: List[LogEntry]) -> bool:
        """Validate that logs follow expected Borges message flow"""

        expected_actions = [
            "send write request",
            "receive request",
            # "write index update",
            "copy_to_cxl_buffer",
            "receive local cut of request",
            "update hash table",
            "commit gsn",
            "switch hash table",
            "receive global cut",
            "send response",
            "receive response",
        ]
        # expected_actions2 = [
        #     "send write request",
        #     "receive request",
        #     "copy_to_cxl_buffer",
        #     "write index update",
        #     "receive local cut of request",
        #     "update hash table",
        #     "commit gsn",
        #     "receive global cut",
        #     "switch hash table",
        #     "send response",
        #     "receive response",
        # ]

        if len(logs) != len(expected_actions):
            return False

        actions = [self._normalize_log_content(log.content) for log in logs]

#    1. [2025-08-10 02:51:12.767407] [client] send write request
#    2. [2025-08-10 02:51:12.767549] [server0] receive request
#    3. [2025-08-10 02:51:12.767578] [server0] write index update
#    4. [2025-08-10 02:51:12.767604] [server0] copy_to_cxl_buffer
#    5. [2025-08-10 02:51:12.767640] [sequencer] receive local cut of request
#    6. [2025-08-10 02:51:12.767674] [sequencer] switch hash table
#    7. [2025-08-10 02:51:12.767682] [sequencer] commit gsn
#    8. [2025-08-10 02:51:12.767690] [sequencer] update hash table
#    9. [2025-08-10 02:51:12.767703] [server0] receive global cut
#   10. [2025-08-10 02:51:12.767709] [server0] send response
#   11. [2025-08-10 02:51:12.767784] [client] receive response

        for i in range(len(expected_actions)):
            if actions[i] != expected_actions[i]:
                return False
        return True

    def analyze_all_intervals(
        self, start_id=1, max_request_id: int = 10000
    ) -> Dict[str, List[float]]:
        """Analyze intervals for all requests up to max_request_id with valid message sequences"""
        print(
            f"Analyzing intervals for requests {start_id} to {max_request_id} (valid message sequences only)..."
        )

        # Dictionary to store all interval times by interval name
        all_intervals = defaultdict(list)

        valid_requests = 0
        skipped_invalid = 0

        for request_id in range(start_id, max_request_id + 1):
            if request_id in self.request_logs:
                logs = self.get_request_logs(request_id, merge_similar=True)

                # Only process requests with valid sequences
                if not self._validate_message_sequence(logs):
                    skipped_invalid += 1
                    continue

                # Calculate intervals for valid requests
                intervals = self.calculate_message_intervals(request_id)

                if len(intervals) >= 1:  # At least one interval
                    valid_requests += 1
                    for interval_name, interval_time in intervals:
                        all_intervals[interval_name].append(interval_time)

        print(f"Processed {valid_requests} valid requests")
        print(f"Skipped {skipped_invalid} requests (invalid sequences)")
        return dict(all_intervals)

    def calculate_percentiles(
        self, intervals_data: Dict[str, List[float]]
    ) -> Dict[str, Dict[str, float]]:
        """Calculate P50 and P99 percentiles for each interval"""
        statistics = {}

        for interval_name, times in intervals_data.items():
            if len(times) > 0:
                times_array = np.array(times)
                statistics[interval_name] = {
                    "count": len(times),
                    "p50": np.percentile(times_array, 50),
                    "p99": np.percentile(times_array, 99),
                    "mean": np.mean(times_array),
                    "min": np.min(times_array),
                    "max": np.max(times_array),
                }

        return statistics

    def print_e2e_statistics(self, start_id=1, max_request_id: int = 10000):
        """Print end-to-end latency statistics (client send to client receive)"""
        e2e_latencies = []

        for request_id in range(start_id, max_request_id + 1):
            if request_id in self.request_logs:
                logs = self.get_request_logs(request_id, merge_similar=True)

                # Only process requests with valid sequences
                if self._validate_message_sequence(logs):
                    # Calculate e2e latency: client send to client receive
                    client_send = None
                    client_receive = None

                    for log in logs:
                        normalized = self._normalize_log_content(log.content)
                        if log.component == "client" and "send write request" in normalized:
                            client_send = log.datetime
                        elif (
                            log.component == "client"
                            and "receive response" in normalized
                        ):
                            client_receive = log.datetime

                    if client_send and client_receive:
                        e2e_latency_ms = (
                            client_receive - client_send
                        ).total_seconds() * 1000
                        e2e_latencies.append(e2e_latency_ms)

        if e2e_latencies:
            e2e_array = np.array(e2e_latencies)

            print(f"\n=== End-to-End Latency (client send -> client receive) ===")
            print(f"{'Count':<12} {'P50 (ms)':<12} {'P99 (ms)':<12}")
            print("=" * 36)
            print(
                f"{len(e2e_latencies):<12} {np.percentile(e2e_array, 50):<12.3f} {np.percentile(e2e_array, 99):<12.3f}"
            )
        else:
            print("No valid end-to-end latency data found!")

    def print_interval_statistics(self, start_id=0, max_request_id: int = 10000):
        """Print comprehensive interval statistics"""
        print(
            f"\n=== Borges Interval Statistics for Requests {start_id}-{max_request_id} ===\n"
        )

        # Get all intervals data
        intervals_data = self.analyze_all_intervals(start_id, max_request_id)

        if not intervals_data:
            print("No interval data found!")
            return

        # Calculate statistics
        statistics = self.calculate_percentiles(intervals_data)

        # Sort intervals by frequency (most common first)
        sorted_intervals = sorted(
            statistics.items(), key=lambda x: x[1]["count"], reverse=True
        )

        print(
            f"{'Interval':<70} {'Count':<8} {'P50 (ms)':<12} {'P99 (ms)':<12} {'Mean (ms)':<12} {'Min (ms)':<12} {'Max (ms)':<12}"
        )
        print("=" * 140)

        for interval_name, stats in sorted_intervals:
            print(
                f"{interval_name:<70} {stats['count']:<8} {stats['p50']:<12.3f} {stats['p99']:<12.3f} {stats['mean']:<12.3f} {stats['min']:<12.3f} {stats['max']:<12.3f}"
            )

        # Summary statistics
        print(f"\n=== Summary ===")
        print(f"Total unique intervals: {len(statistics)}")
        total_samples = sum(stats["count"] for stats in statistics.values())
        print(f"Total interval samples: {total_samples}")

        # Calculate end-to-end latency statistics
        self.print_e2e_statistics(start_id, max_request_id)

        # Find most critical intervals (highest P99)
        print(f"\n=== Top 5 Highest P99 Intervals ===")
        top_p99 = sorted(statistics.items(), key=lambda x: x[1]["p99"], reverse=True)[
            :5
        ]
        for interval_name, stats in top_p99:
            print(f"{interval_name:<70} P99: {stats['p99']:.3f}ms")


def main():
    """Main function to analyze Borges log intervals"""
    log_dir = "logs"
    analyzer = BorgesLogAnalyzer(log_dir)

    # Parse all logs
    analyzer.parse_all_logs()

    # Show available requests
    requests = analyzer.get_available_requests()
    print(f"Available requests: {len(requests)} total")

    if len(requests) == 0:
        return

    # Show first 10 requests with their intervals
    print(f"\n=== First 10 Requests Analysis ===")
    for request_id in requests[:10]:
        print(f"\n--- Request {request_id} ---")
        logs = analyzer.get_request_logs(request_id, merge_similar=True)

        print(f"Messages ({len(logs)} total):")
        for i, log in enumerate(logs):
            action = analyzer._normalize_log_content(log.content)
            print(f"  {i+1:2d}. [{log.timestamp}] [{log.component}] {action}")

        intervals = analyzer.calculate_message_intervals(request_id)
        if intervals:
            print(f"\nIntervals ({len(intervals)} total):")
            for i, (interval_name, interval_time) in enumerate(intervals):
                print(f"  {i+1:2d}. {interval_name}: {interval_time:.3f}ms")
    # Analyze message sequences
    print(f"\n=== Message Sequence Analysis ===")
    # Count different message sequences
    sequence_counts = defaultdict(int)
    message_count_distribution = defaultdict(int)

    for request_id in requests[:5000]:  # Check first 1000 requests
        logs = analyzer.get_request_logs(request_id, merge_similar=True)
        message_count = len(logs)
        message_count_distribution[message_count] += 1

        # Create sequence signature
        actions = [analyzer._normalize_log_content(
            log.content) for log in logs]
        sequence_signature = " -> ".join(actions)
        sequence_counts[sequence_signature] += 1

    print(f"Message count distribution in first 1000 requests:")
    for count, frequency in sorted(message_count_distribution.items()):
        print(f"  {count} messages: {frequency} requests")

    print(f"\nTop 5 most common sequences:")
    top_sequences = sorted(sequence_counts.items(), key=lambda x: x[1], reverse=True)[
        :5
    ]
    for i, (sequence, count) in enumerate(top_sequences):
        print(
            f"\n{i+1}. ({count} requests) Sequence with {len(sequence.split(' -> '))} messages:"
        )
        for j, action in enumerate(sequence.split(" -> ")):
            print(f"   {j+1:2d}. {action}")

    # Calculate and print interval statistics for all requests
    analyzer.print_interval_statistics(10, 5000)


if __name__ == "__main__":
    main()