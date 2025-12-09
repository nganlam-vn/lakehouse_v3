import pandas as pd

# Load the CSV file
df = pd.read_csv('ncr_ride_bookings.csv')

# Replace all 'null' strings with empty strings
df = df.replace('null', '')

# Save the modified DataFrame back to a new CSV file
df.to_csv('modified_ncr_ride_bookings.csv', index=False)

# Optionally, print the first few rows to verify
print(df.head())