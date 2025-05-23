from transformation import read_data_pipeline

crash_file = "data/traffic_crashes2.csv"
vehicle_file = "data/traffic_crashes-vehicles2.csv"

df = read_data_pipeline(crash_file, vehicle_file)

print(df)