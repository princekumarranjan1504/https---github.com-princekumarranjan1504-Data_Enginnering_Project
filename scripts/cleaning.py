import pandas as pd

def clean_performance_data(df):
    """
    Cleans and validates server performance logs from Station 1 & 2.

    Steps:
    1. Drop irrelevant columns.
    2. Handle missing/null numeric values.
    3. Convert Timestamp to datetime.
    4. Detect and handle anomalies:
        - Missing timestamps
        - Negative metric values
        - Duplicate records
    5. Return cleaned DataFrame + print summary of anomalies.
    """
    try:
        # Step 1: Drop irrelevant columns
        drop_cols = ['Config_Version', 'Last_Patch_Date', 'Deployment_Token']
        df = df.drop(columns=drop_cols, errors='ignore')
        print("Dropped irrelevant columns (if any).")

        # Step 2: Fill missing numeric values with 0
        num_cols = ['CPU_Utilization (%)', 'Memory_Usage (%)', 'Disk_IO (%)', 'Network_Traffic_In (MB/s)', 'Network_Traffic_Out (MB/s)']
        for col in num_cols:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                df[col] = df[col].fillna(0)
                if null_count > 0:
                    print(f"Filled {null_count} missing values in {col} with 0.")

        # Step 3: Convert Timestamp to datetime
        if 'Log_Timestamp' in df.columns:
            df.rename(columns={'Log_Timestamp': 'Timestamp'}, inplace=True)
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
            invalid_timestamps = df['Timestamp'].isnull().sum()
            if invalid_timestamps > 0:
                print(f"Found {invalid_timestamps} invalid/missing timestamps.")
        else:
            print("No Timestamp column found.")

        # Step 4: Detect anomalies
        anomalies = {}

        # (a) Negative metrics (updated columns)
        neg_mask = (df[['CPU_Utilization (%)', 'Memory_Usage (%)', 'Disk_IO (%)', 'Network_Traffic_In (MB/s)', 'Network_Traffic_Out (MB/s)']] < 0).any(axis=1)
        neg_count = neg_mask.sum()
        if neg_count > 0:
            anomalies['negative_metrics'] = neg_count
            df.loc[neg_mask, ['CPU_Utilization (%)','Memory_Usage (%)','Disk_IO (%)','Network_Traffic_In (MB/s)','Network_Traffic_Out (MB/s)']] = df.loc[neg_mask, ['CPU_Utilization (%)','Memory_Usage (%)','Disk_IO (%)','Network_Traffic_In (MB/s)','Network_Traffic_Out (MB/s)']].abs()
            print(f"Found and corrected {neg_count} negative metric values.")

        # (b) Duplicates
        dup_count = df.duplicated().sum()
        if dup_count > 0:
            anomalies['duplicates'] = dup_count
            df = df.drop_duplicates()
            print(f"Dropped {dup_count} duplicate records.")

        # Step 5: Enforce unified schema
        df.columns = [c.strip() for c in df.columns]  # remove any extra spaces
        df = df.sort_index(axis=1)  # alphabetic order for consistency

        print("Unified schema and sorted columns.")
        print(f"Final cleaned data: {df.shape[0]} rows, {df.shape[1]} columns")

        # Step 6: Print anomaly summary
        if anomalies:
            print("Anomaly Summary:")
            for k, v in anomalies.items():
                print(f"   - {k}: {v}")
        else:
            print("No major anomalies detected.")

        return df

    except Exception as e:
        print("Error cleaning performance data.")
        print("Reason:", e)
        return df

    
    
