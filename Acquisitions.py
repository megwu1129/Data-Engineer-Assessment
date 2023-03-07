import pandas as pd

df = pd.read_csv('People.csv')
df['created_dt'] = pd.to_datetime(df['created_dt'])

# Use created_dt to do the partition and count the number of rows per date
count_by_date = df.groupby(df['created_dt'].dt.date).size()
acquisition_df = count_by_date.to_frame(name='count').reset_index()

# Rename the column to 'acquisitions'
acquisition_df.rename(columns={'count': 'acquisitions'}, inplace=True)

# Convert back to datetime type
acquisition_df['created_dt'] = pd.to_datetime(acquisition_df['created_dt'])

# Rename the column
acquisition_df = acquisition_df.rename(columns={'created_dt':'acquisition_date'})


# Export to a new CSV file
acquisition_df.to_csv('acquisition_facts.csv', index=False)