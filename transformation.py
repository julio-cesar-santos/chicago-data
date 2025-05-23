import pandas as pd

def read_datasources(source_name):
    return pd.read_csv(source_name)

def rename_columns(df):
    df.columns = df.columns.str.strip().str.upper()
    return df

def drop_rows_with_null_values(df):
    return df.dropna(axis=1, thresh=900)

def fill_missing_values(df):
    num_cols = df.select_dtypes(include=['int64', 'float64']).columns
    df[num_cols] = df[num_cols].fillna(df[num_cols].mean())
    return df

def merge_dataframes(df_crashes, df_vehicles):
    return pd.merge(df_crashes, df_vehicles, on="CRASH_RECORD_ID", how="inner")

def read_data_pipeline(crash_file, vehicle_file):
    df_crash = read_datasources(crash_file)
    df_vehicle = read_datasources(vehicle_file)
    
    df_crash = rename_columns(df_crash)
    df_vehicle = rename_columns(df_vehicle)
    
    df_crash = drop_rows_with_null_values(df_crash)
    df_vehicle = drop_rows_with_null_values(df_vehicle)
    
    df_merged = merge_dataframes(df_crash, df_vehicle)
    
    df_merged = fill_missing_values(df_merged)
    
    return df_merged