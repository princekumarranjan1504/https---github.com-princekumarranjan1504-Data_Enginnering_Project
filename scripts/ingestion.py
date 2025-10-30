# scripts/ingestion.py

import pandas as pd

def load_excel(file_path):
    # Reads all Excel sheets and returns them as DataFrames.
    try:
        # Read the metadata sheet
        metadata = pd.read_excel(file_path, sheet_name='Server_Metadata')

        # Read performance data for two stations
        station1 = pd.read_excel(file_path, sheet_name='Server_Performance_Station1')
        station2 = pd.read_excel(file_path, sheet_name='Server_Performance_Station2')

        # Print a success message and the shapes of the loaded tables
        print('Loaded successfully:')
        print(' - Metadata shape:', metadata.shape)
        print(' - Station1 shape:', station1.shape)
        print(' - Station2 shape:', station2.shape)

        return metadata, station1, station2

    except Exception as e:
        # print a error message output
        print('Error reading Excel file.')
        print('Reason:', e)
        return None, None, None
    