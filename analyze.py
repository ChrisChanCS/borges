import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
import json
# Plotting is disabled; keep analysis CLI free of matplotlib/seaborn dependencies.
# import matplotlib.pyplot as plt
# import seaborn as sns


def numeric_folder_candidates(value):
    """
    Return directory-name candidates for numeric experiment parameters.
    This lets CLI inputs such as 0.0 match folders named 0.
    """
    raw = str(value)
    candidates = [raw]
    try:
        candidates.append(f"{float(raw):g}")
    except ValueError:
        pass
    return list(dict.fromkeys(candidates))


def numeric_path_sort_key(path):
    try:
        return float(path.name)
    except ValueError:
        return float("inf")


def analyze_performance(workload, client_num, ycsb_option=None, server_num=None,
                        scale_out_rate=None, segment_size=None,
                        zipf_theta=None):
    """
    Analyze performance data for the specified workload and client count

    Args:
        workload (str): workload type (append, ycsb-a, ycsb-b, ycsb-c, ycsb-d,
            lock, retwis, scale, stream_scale_out, segment)
        client_num (int): client count
        ycsb_option (int, optional): YCSB option (0-3), used only when workload is ycsb
        server_num (int, optional): server count, used only when workload is scale
        scale_out_rate (float, optional): scale-out rate, used only when workload is stream_scale_out
        segment_size (int, optional): configured segment size, used only when workload is segment
        zipf_theta (str/float, optional): Zipf skewness, used only when workload is skewness
    """
    # Build the folder path
    if workload == "skewness":
        # Skewness workload: result/skewness/Z/client_N/
        if zipf_theta is None:
            print("Error: skewness workload requires the zipf_theta argument")
            return
        folder_path = None
        for theta_name in numeric_folder_candidates(zipf_theta):
            candidate = Path(f"result/skewness/{theta_name}/client_{client_num}")
            if candidate.exists():
                folder_path = candidate
                zipf_theta = theta_name
                break
        if folder_path is None:
            folder_path = Path(f"result/skewness/{zipf_theta}/client_{client_num}")
    elif workload == "segment":
        # Segment workload: result/segment/segment_size_X/client_N/
        if segment_size is None:
            print("Error: segment workload requires the segment_size argument")
            return
        folder_path = Path(
            f"result/segment/segment_size_{segment_size}/client_{client_num}")
    elif workload == "stream_scale_out":
        # Stream scale out workload: result/stream_scale_out/rate_X.X/client_N/
        if scale_out_rate is None:
            print("Error: stream_scale_out workload requires the scale_out_rate argument")
            return
        folder_path = Path(
            f"result/stream_scale_out/rate_{scale_out_rate}/client_{client_num}")
    elif workload == "scale":
        # Scale workload: result/scale/server_count/client_count/
        if server_num is None:
            print("Error: scale workload requires the server_num argument")
            return
        folder_path = Path(f"result/scale/{server_num}/{client_num}")
    elif workload.startswith("ycsb"):
        # YCSB workload: result/ycsb-a/client_2/
        folder_path = Path(f"result/{workload}/client_{client_num}")
    else:
        # Other workloads: result/append/client_2/
        folder_path = Path(f"result/{workload}/client_{client_num}")

    if not folder_path.exists():
        print(f"Error: directory {folder_path} does not exist")
        return

    # Collect all CSV files
    csv_files = list(folder_path.glob("*.csv"))

    if not csv_files:
        print(f"Error: no CSV files found in {folder_path}")
        return

    print(f"Found {len(csv_files)} CSV files")

    # Read all CSV files and merge data
    all_latencies = []
    all_start_times = []
    all_end_times = []

    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file, header=None, names=[
                             'start_time', 'end_time', 'latency'])
            # Ensure data types are correct
            df['latency'] = pd.to_numeric(df['latency'], errors='coerce')
            df['start_time'] = pd.to_numeric(df['start_time'], errors='coerce')
            df['end_time'] = pd.to_numeric(df['end_time'], errors='coerce')

            # Remove invalid data
            df = df.dropna()

            all_latencies.extend(df['latency'].tolist())
            all_start_times.extend(df['start_time'].tolist())
            all_end_times.extend(df['end_time'].tolist())
            print(f"Read {csv_file.name}: {len(df)} records")
        except Exception as e:
            print(f"Failed to read {csv_file.name}: {e}")

    if not all_latencies:
        print("Error: No data was read")
        return

    # Compute latency statistics
    latencies = np.array(all_latencies, dtype=float)
    p50_latency = np.percentile(latencies, 50)
    p99_latency = np.percentile(latencies, 99)

    # Compute throughput
    start_times = np.array(all_start_times, dtype=float)
    end_times = np.array(all_end_times, dtype=float)

    # Find the earliest start time and latest end time.
    earliest_start = np.min(start_times)
    latest_end = np.max(end_times)

    # Compute total time in seconds
    # Timestamps are absl::ToUnixMicros and are already in microseconds
    total_time_seconds = (latest_end - earliest_start) / 1e6

    # Compute throughput(requests/sec)
    total_requests = len(latencies)
    throughput = total_requests / total_time_seconds if total_time_seconds > 0 else 0

    # Output results
    print(f"\n=== Performance analysis results ===")
    print(f"Workload: {workload}")
    print(f"Client count: {client_num}")
    if server_num is not None:
        print(f"Server count: {server_num}")
    if scale_out_rate is not None:
        print(f"Scale Out rate: {scale_out_rate}")
    if segment_size is not None:
        print(f"Configured segment size: {segment_size} bytes")
        print(f"Data segment size: {segment_size - 64} bytes")
    if zipf_theta is not None:
        print(f"Zipf theta: {zipf_theta}")
    if ycsb_option is not None:
        print(f"YCSBOptions: {ycsb_option}")
    print(f"Total requests: {total_requests}")
    print(f"Total time: {total_time_seconds:.2f} seconds")
    print(f"P50 latency: {p50_latency:.2f} us")
    print(f"P99 latency: {p99_latency:.2f} us")
    print(f"Throughput: {throughput:.2f} requests/sec")
    print(f"Average latency: {np.mean(latencies):.2f} us")
    print(f"Minimum latency: {np.min(latencies):.2f} us")
    print(f"Maximum latency: {np.max(latencies):.2f} us")

    # Return the result dictionary for batch analysis
    return {
        'workload': workload,
        'client_num': client_num,
        'server_num': server_num,
        'scale_out_rate': scale_out_rate,
        'segment_size': segment_size,
        'data_segment_size': segment_size - 64 if segment_size is not None else None,
        'zipf_theta': float(zipf_theta) if zipf_theta is not None else None,
        'ycsb_option': ycsb_option,
        'total_requests': total_requests,
        'total_time_seconds': total_time_seconds,
        'p50_latency': p50_latency,
        'p99_latency': p99_latency,
        'throughput': throughput,
        'mean_latency': np.mean(latencies),
        'min_latency': np.min(latencies),
        'max_latency': np.max(latencies)
    }


def analyze_segment_comprehensive():
    """
    Comprehensively analyze all segment size test results
    """
    print("=== Segment Size Comprehensive Analysis ===")

    base_path = Path("result/segment")
    if not base_path.exists():
        print(f"Error: directory {base_path} does not exist")
        return

    all_results = []

    for segment_folder in sorted(base_path.glob("segment_size_*"),
                                 key=lambda p: int(p.name.split("_")[-1])):
        try:
            segment_size = int(segment_folder.name.split("_")[-1])
        except ValueError:
            print(f"Skip invalid segment folder: {segment_folder}")
            continue

        for client_folder in sorted(segment_folder.glob("client_*"),
                                    key=lambda p: int(p.name.split("_")[-1])):
            try:
                client_num = int(client_folder.name.split("_")[-1])
            except ValueError:
                print(f"Skip invalid client folder: {client_folder}")
                continue

            result = analyze_performance(
                "segment", client_num, segment_size=segment_size)
            if result:
                all_results.append(result)
            print("-" * 50)

    if not all_results:
        print("No test results found")
        return

    df = pd.DataFrame(all_results)

    print("\n=== Analysis grouped by segment size ===")
    for segment_size in sorted(df['segment_size'].unique()):
        segment_data = df[df['segment_size'] == segment_size]
        data_segment_size = segment_size - 64
        print(f"\nConfigured segment size: {segment_size} bytes")
        print(f"Data segment size: {data_segment_size} bytes")
        print(f"Test configuration count: {len(segment_data)}")
        print(f"Average throughput: {segment_data['throughput'].mean():.2f} requests/sec")
        print(f"Average p50 latency: {segment_data['p50_latency'].mean():.2f} us")
        print(f"Average p99 latency: {segment_data['p99_latency'].mean():.2f} us")

    print("\n=== Analysis grouped by client count ===")
    for client_count in sorted(df['client_num'].unique()):
        client_data = df[df['client_num'] == client_count]
        print(f"\nClient count: {client_count}")
        print(f"Test configuration count: {len(client_data)}")
        print(f"Average throughput: {client_data['throughput'].mean():.2f} requests/sec")
        print(f"Average p50 latency: {client_data['p50_latency'].mean():.2f} us")
        print(f"Average p99 latency: {client_data['p99_latency'].mean():.2f} us")

    # Plot generation is disabled.
    # try:
    #     generate_segment_plots(df)
    # except Exception as e:
    #     print(f"Failed to generate plot: {e}")

    return df


def generate_segment_plots(df):
    """
    Plot generation is disabled.
    """
    # Plot generation is disabled.
    return


def analyze_skewness_comprehensive():
    """
    Comprehensively analyze all skewness test results
    """
    print("=== Skewness Comprehensive Analysis ===")

    base_path = Path("result/skewness")
    if not base_path.exists():
        print(f"Error: directory {base_path} does not exist")
        return

    all_results = []

    for theta_folder in sorted(base_path.iterdir(), key=numeric_path_sort_key):
        if not theta_folder.is_dir():
            continue
        try:
            zipf_theta = float(theta_folder.name)
        except ValueError:
            print(f"Skip invalid skewness folder: {theta_folder}")
            continue

        for client_folder in sorted(theta_folder.glob("client_*"),
                                    key=lambda p: int(p.name.split("_")[-1])):
            try:
                client_num = int(client_folder.name.split("_")[-1])
            except ValueError:
                print(f"Skip invalid client folder: {client_folder}")
                continue

            result = analyze_performance(
                "skewness", client_num, zipf_theta=theta_folder.name)
            if result:
                all_results.append(result)
            print("-" * 50)

    if not all_results:
        print("No test results found")
        return

    df = pd.DataFrame(all_results)

    print("\n=== Analysis grouped by Zipf theta ===")
    for theta in sorted(df['zipf_theta'].unique()):
        theta_data = df[df['zipf_theta'] == theta]
        print(f"\nZipf theta: {theta:g}")
        print(f"Test configuration count: {len(theta_data)}")
        print(f"Average throughput: {theta_data['throughput'].mean():.2f} requests/sec")
        print(f"Average p50 latency: {theta_data['p50_latency'].mean():.2f} us")
        print(f"Average p99 latency: {theta_data['p99_latency'].mean():.2f} us")

    print("\n=== Analysis grouped by client count ===")
    for client_count in sorted(df['client_num'].unique()):
        client_data = df[df['client_num'] == client_count]
        print(f"\nClient count: {client_count}")
        print(f"Test configuration count: {len(client_data)}")
        print(f"Average throughput: {client_data['throughput'].mean():.2f} requests/sec")
        print(f"Average p50 latency: {client_data['p50_latency'].mean():.2f} us")
        print(f"Average p99 latency: {client_data['p99_latency'].mean():.2f} us")

    # Plot generation is disabled.
    # try:
    #     generate_skewness_plots(df)
    # except Exception as e:
    #     print(f"Failed to generate plot: {e}")

    return df


def generate_skewness_plots(df):
    """
    Plot generation is disabled.
    """
    # Plot generation is disabled.
    return


def analyze_stream_scale_out_comprehensive():
    """
    Comprehensively analyze all stream scale out test results
    """
    print("=== Stream Scale Out Comprehensive Analysis ===")

    base_path = Path("result/stream_scale_out")
    if not base_path.exists():
        print(f"Error: directory {base_path} does not exist")
        return

    all_results = []

    # Visit each rate directory.
    for rate_folder in base_path.glob("rate_*"):
        scale_out_rate = float(rate_folder.name.split("_")[1])

        # Visit each client directory.
        for client_folder in rate_folder.glob("client_*"):
            client_num = int(client_folder.name.split("_")[1])

            # Analyze this configuration
            result = analyze_performance(
                "stream_scale_out", client_num, scale_out_rate=scale_out_rate)
            if result:
                all_results.append(result)
            print("-" * 50)

    if not all_results:
        print("No test results found")
        return

    # Create a DataFrame for analysis
    df = pd.DataFrame(all_results)

    # Analyze grouped by scale_out_rate
    print("\n=== Analysis grouped by scale-out rate ===")
    for rate in sorted(df['scale_out_rate'].unique()):
        rate_data = df[df['scale_out_rate'] == rate]
        print(f"\nScale Out rate: {rate}")
        print(f"Test configuration count: {len(rate_data)}")
        print(f"Average throughput: {rate_data['throughput'].mean():.2f} requests/sec")
        print(f"Average p50 latency: {rate_data['p50_latency'].mean():.2f} us")
        print(f"Average p99 latency: {rate_data['p99_latency'].mean():.2f} us")

    # Analyze grouped by client count
    print("\n=== Analysis grouped by client count ===")
    for client_count in sorted(df['client_num'].unique()):
        client_data = df[df['client_num'] == client_count]
        print(f"\nClient count: {client_count}")
        print(f"Test configuration count: {len(client_data)}")
        print(f"Average throughput: {client_data['throughput'].mean():.2f} requests/sec")
        print(f"Average p50 latency: {client_data['p50_latency'].mean():.2f} us")
        print(f"Average p99 latency: {client_data['p99_latency'].mean():.2f} us")

    # Plot generation is disabled.
    # try:
    #     generate_stream_scale_out_plots(df)
    # except Exception as e:
    #     print(f"Failed to generate plot: {e}")

    return df


def generate_stream_scale_out_plots(df):
    """
    Plot generation is disabled.
    """
    # Plot generation is disabled.
    return


def show_usage():
    """Show usage information"""
    print("Usage: python analyze.py <workload> <client_num> [options]")
    print("")
    print("Arguments:")
    print("  workload     workload type:")
    print("               - append: append only")
    print("               - rdma: append only (RDMA transport)")
    print("               - counter: counter workload")
    print("               - ycsb-a: YCSB-A (balanced)")
    print("               - ycsb-b: YCSB-B (read-heavy)")
    print("               - ycsb-c: YCSB-C (read-only)")
    print("               - ycsb-d: YCSB-D (read-insert)")
    print("               - lock: lock workload")
    print("               - retwis: retwis workload")
    print("               - scale: scale test workload")
    print("               - segment: segment size test workload")
    print("               - skewness: counter skewness test workload")
    print("               - stream_scale_out: stream scale out test workload")
    print("  client_num   client count")
    print("")
    print("Optional arguments:")
    print("  ycsb_option  YCSB option (0-3), used only when workload is ycsb")
    print("  server_num   server count, used only when workload is scale")
    print("  segment_size configured segment size, used only when workload is segment")
    print("  zipf_theta   Zipf theta, used only when workload is skewness")
    print("  scale_out_rate Scale Out rate, used only when workload is stream_scale_out")
    print("")
    print("Special commands:")
    print("  segment_all: analyze all segment size test results")
    print("  skewness_all: analyze all skewness test results")
    print("  stream_scale_out_all: analyze all stream scale out test results")
    print("")
    print("Examples:")
    print("  python analyze.py append 4")
    print("  python analyze.py rdma 40  # RDMA results in result/append_rdma/client_40/")
    print("  python analyze.py counter 4")
    print("  python analyze.py ycsb-a 2")
    print("  python analyze.py lock 6")
    print("  python analyze.py retwis 8")
    print("  python analyze.py scale 12 3     # 12clients, 3servers")
    print("  python analyze.py segment 1 320  # 1client, configured segment size 320")
    print("  python analyze.py segment_all    # Analyze all segment-size results")
    print("  python analyze.py skewness 96 0.99  # 96clients, Zipf theta 0.99")
    print("  python analyze.py skewness_all       # Analyze all skewness results")
    print("  python analyze.py stream_scale_out 8 0.5  # 8clients, 0.5scale-out rate")
    print("  python analyze.py stream_scale_out_all    # Analyze all stream scale-out results")


def main():
    if len(sys.argv) < 2:
        show_usage()
        sys.exit(1)

    # Handle special commands.
    if sys.argv[1] == "segment_all":
        analyze_segment_comprehensive()
        return

    if sys.argv[1] == "skewness_all":
        analyze_skewness_comprehensive()
        return

    if sys.argv[1] == "stream_scale_out_all":
        analyze_stream_scale_out_comprehensive()
        return

    if len(sys.argv) < 3:
        show_usage()
        sys.exit(1)

    try:
        workload = sys.argv[1]
        client_num = int(sys.argv[2])

        if client_num <= 0:
            print("Error: client_num must be a positive integer")
            sys.exit(1)

        # Validate the workload type.
        valid_workloads = ['append', 'rdma', 'counter', 'ycsb-a', 'ycsb-b',
                           'ycsb-c', 'ycsb-d', 'lock', 'retwis', 'scale',
                           'segment', 'skewness', 'stream_scale_out']
        if workload not in valid_workloads:
            print(f"Error: Invalid workload type '{workload}'")
            print(f"Valid workload types: {', '.join(valid_workloads)}")
            sys.exit(1)

        # Handle different workload types
        ycsb_option = None
        server_num = None
        scale_out_rate = None
        segment_size = None
        zipf_theta = None

        if workload == 'stream_scale_out':
            # The stream scale-out workload requires scale_out_rate.
            if len(sys.argv) < 4:
                print("Error: stream_scale_out workload requires the scale_out_rate argument")
                print(
                    "Usage: python analyze.py stream_scale_out <client_num> <scale_out_rate>")
                sys.exit(1)
            scale_out_rate = float(sys.argv[3])
            if scale_out_rate < 0 or scale_out_rate > 1:
                print("Error: scale_out_rate must be between 0 and 1")
                sys.exit(1)
        elif workload == 'scale':
            # The scale workload requires server_num.
            if len(sys.argv) < 4:
                print("Error: scale workload requires the server_num argument")
                print("Usage: python analyze.py scale <client_num> <server_num>")
                sys.exit(1)
            server_num = int(sys.argv[3])
            if server_num <= 0 or server_num > 6:
                print("Error: server_num must be between 1 and 6")
                sys.exit(1)
        elif workload == 'segment':
            # The segment workload requires segment_size.
            if len(sys.argv) < 4:
                print("Error: segment workload requires the segment_size argument")
                print("Usage: python analyze.py segment <client_num> <segment_size>")
                sys.exit(1)
            segment_size = int(sys.argv[3])
            if segment_size <= 64:
                print("Error: segment_size must be larger than 64 bytes")
                sys.exit(1)
        elif workload == 'skewness':
            # The skewness workload requires zipf_theta.
            if len(sys.argv) < 4:
                print("Error: skewness workload requires the zipf_theta argument")
                print("Usage: python analyze.py skewness <client_num> <zipf_theta>")
                sys.exit(1)
            zipf_theta = sys.argv[3]
            theta_value = float(zipf_theta)
            if theta_value < 0 or theta_value >= 1:
                print("Error: zipf_theta must be in [0, 1)")
                sys.exit(1)
        elif workload.startswith('ycsb') and len(sys.argv) >= 4:
            # Optional ycsb_option argument for YCSB.
            ycsb_option = int(sys.argv[3])
            if ycsb_option < 0 or ycsb_option > 3:
                print("Error: ycsb_option must be between 0 and 3")
                sys.exit(1)

    except ValueError as e:
        print(f"Error: Invalid argument type - {e}")
        show_usage()
        sys.exit(1)

    analyze_performance(workload, client_num, ycsb_option,
                        server_num, scale_out_rate, segment_size, zipf_theta)


if __name__ == "__main__":
    main()
