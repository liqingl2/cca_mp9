import pandas as pd
import glob

# Path to the directory containing your CSV files
csv_files_path = 'redo.csv'

# List to store individual DataFrames from each CSV file
dfs = []

# Read each CSV file into a DataFrame and append to the list
for file in glob.glob(csv_files_path):
    df = pd.read_csv(file)
    dfs.append(df)

# Concatenate all DataFrames into a single DataFrame
concatenated_df = pd.concat(dfs, ignore_index=True)

# Filter the DataFrame based on the specified conditions
filtered_df = concatenated_df[
    (concatenated_df['scheduled_departure'].between(800, 1200, inclusive='left')) &
    (concatenated_df['origin_airport'] == 'ORD') &
    (concatenated_df['cancelled'] == 0) &
    (concatenated_df['month'] == 12) &
    (concatenated_df['day'] == 25)
][['airline', 'origin_airport', 'destination_airport']]

# Add a new column 'time_zone_difference' to the filtered DataFrame
filtered_df['time_zone_difference'] = (
    (filtered_df['scheduled_arrival'] // 100) * 60 +
    (filtered_df['scheduled_arrival'] % 100) -
    ((filtered_df['scheduled_departure'] // 100) * 60 +
     (filtered_df['scheduled_departure'] % 100) +
     filtered_df['scheduled_time']) % (24 * 60)
)

# Print the resulting DataFrame
print(filtered_df)

output_csv_path = 'query2.csv'
filtered_df.to_csv(output_csv_path, index=False)
print(f"Filtered data saved to {output_csv_path}")