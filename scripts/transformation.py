# scripts/transformation.py

import pandas as pd

def transform_data(perf_df, meta_df):
    """Simple transformation for performance data.

    Steps:
    1) Compute CPU, Memory, Disk I/O, and Network metrics.
    2) Aggregate windowed metrics (1min, 5min, hourly).
    3) Merge with metadata on Server_ID.
    4) Add anomaly flag for high CPU utilization (>85%).
    """
    try:
        # --- Step 1: Compute core metrics ---
        print("Starting transformation...")

        # CPU & Memory utilization already in percentage form
        if 'CPU_Utilization (%)' in perf_df.columns:
            perf_df.rename(columns={'CPU_Utilization (%)': 'CPU_Utilization'}, inplace=True)
            print("CPU Utilization column ready.")
        else:
            perf_df['CPU_Utilization'] = 0
            print("CPU Utilization column missing; filled with 0.")

        if 'Memory_Usage (%)' in perf_df.columns:
            perf_df.rename(columns={'Memory_Usage (%)': 'Memory_Utilization'}, inplace=True)
            print("Memory Utilization column ready.")
        else:
            perf_df['Memory_Utilization'] = 0
            print("Memory Utilization column missing; filled with 0.")

        # Disk I/O Rate
        if 'Disk_IO (%)' in perf_df.columns:
            perf_df.rename(columns={'Disk_IO (%)': 'Disk_IO_Rate'}, inplace=True)
            print("Disk I/O Rate column ready.")
        else:
            perf_df['Disk_IO_Rate'] = 0
            print("Disk I/O column missing; filled with 0.")

        # Network Throughput
        if {'Network_Traffic_In (MB/s)', 'Network_Traffic_Out (MB/s)'}.issubset(perf_df.columns):
            perf_df['Network_Throughput_MBps'] = (
                perf_df['Network_Traffic_In (MB/s)'] + perf_df['Network_Traffic_Out (MB/s)']
            ) / 2
            print("Calculated Network Throughput (average of In/Out).")
        else:
            perf_df['Network_Throughput_MBps'] = 0
            print("Network columns missing; throughput set to 0.")

        # --- Step 2: Aggregate time windows ---
        # if 'Timestamp' in perf_df.columns:
        #     perf_df['Timestamp'] = pd.to_datetime(perf_df['Timestamp'], errors='coerce')
        #     perf_df = perf_df.sort_values(by='Timestamp')
        #     perf_df.set_index('Timestamp', inplace=True)

        #     agg_1min = perf_df.resample('1min').mean(numeric_only=True).reset_index()
        #     agg_5min = perf_df.resample('5min').mean(numeric_only=True).reset_index()
        #     agg_hourly = perf_df.resample('1H').mean(numeric_only=True).reset_index()

        #     print("Aggregated metrics into 1min, 5min, and hourly windows.")
        # else:
        #     agg_hourly = pd.DataFrame()
        #     print("No Timestamp column found; skipped aggregation.")

        # perf_df.reset_index(inplace=True)

        # --- Step 3: Merge with metadata ---
        if 'Server_ID' in perf_df.columns and 'Server_ID' in meta_df.columns:
            final_df = perf_df.merge(meta_df, on='Server_ID', how='left')
            print("Merged performance data with metadata on Server_ID.")
        else:
            final_df = perf_df.copy()
            print("Server_ID missing in one of the tables; skipped merge.")

        # --- Step 4: Add anomaly flag ---
        if 'CPU_Utilization' in final_df.columns:
            final_df['Anomaly_Flag'] = final_df['CPU_Utilization'].apply(
                lambda x: 'High Load' if x > 85 else 'Normal'
            )
            print("Added Anomaly_Flag based on CPU Utilization > 85%.")
        else:
            final_df['Anomaly_Flag'] = 'Unknown'
            print("No CPU_Utilization column; anomaly flag set to 'Unknown'.")

        # --- Final summary ---
        # Persist hourly aggregation for inspection/PowerBI (if produced)
        # try:
        #     out_dir = '../output'
        #     import os
        #     os.makedirs(out_dir, exist_ok=True)
        #     if 'agg_1min' in locals() and not agg_1min.empty:
        #         agg_1min.to_csv(os.path.join(out_dir, 'aggregates_1min.csv'), index=False)
        #     if 'agg_5min' in locals() and not agg_5min.empty:
        #         agg_5min.to_csv(os.path.join(out_dir, 'aggregates_5min.csv'), index=False)
        #     if 'agg_hourly' in locals() and not agg_hourly.empty:
        #         agg_hourly.to_csv(os.path.join(out_dir, 'aggregates_hourly.csv'), index=False)
        #         print(f'Wrote hourly aggregates to {os.path.join(out_dir, "aggregates_hourly.csv")}')
        # except Exception:
        #     # non-fatal if write fails
        #     pass

        print('Transformation complete: rows =', final_df.shape[0], 'columns =', final_df.shape[1])
        return final_df

    except Exception as e:
        print('Error transforming data.')
        print('Reason:', e)
        return perf_df

    

