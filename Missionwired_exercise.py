import pandas as pd

cons_df = pd.read_csv('data/cons.csv')
email_df = pd.read_csv('data/cons_email.csv')
subcription_df = pd.read_csv('data/cons_email_chapter_subscription.csv')

# Drop rows with null values in the 'id' column
cons_df.dropna(subset=['cons_id'], inplace=True)
email_df.dropna(subset=['cons_email_id'], inplace=True)
subcription_df.dropna(subset=['cons_email_chapter_subscription_id'], inplace=True)

# Drop duplicated rows
cons_df.drop_duplicates(subset=['cons_id'],inplace=True)
email_df.drop_duplicates(subset=['cons_email_id'],inplace=True)
subcription_df.drop_duplicates(subset=['cons_email_chapter_subscription_id'],inplace=True)

# Remove rows where is_banned is 1 for cons to keep only valid constituents
condition = cons_df['is_banned'] != 1
filtered_cons_df = cons_df[condition]

# Keep needed columns for cons
cons_selected_columns = ['cons_id','source','create_dt', 'modified_dt']
filtered_cons_df = filtered_cons_df.loc[:, cons_selected_columns]

# Keep needed columns for emails
email_selected_columns = ['cons_email_id','cons_id', 'is_primary', 'email', 'create_dt', 'modified_dt']
filtered_email_df = email_df.loc[:, email_selected_columns]

# Keep email data that is primary
email_condition = filtered_email_df['is_primary'] == 1
filtered_email_df = filtered_email_df[email_condition]

# Keep only needed columns for subscriptions
subs_selected_columns = ['cons_email_chapter_subscription_id','cons_email_id', 'chapter_id', 'isunsub']
filtered_subs_df = subcription_df.loc[:, subs_selected_columns]

# Keep subscription statuses where chapter_id is 1
sub_status = filtered_subs_df['chapter_id'] == 1
filtered_subs_df = filtered_subs_df[sub_status]

# Join the 3 datasets together
merged_cons_email_df = pd.merge(filtered_email_df, filtered_cons_df, on='cons_id', how='left')
merged_email_subs_df = pd.merge(merged_cons_email_df, filtered_subs_df, on='cons_email_id', how='left')

# Replace NaN values of column isunsub and chapter_id with 0 and 1
merged_email_subs_df['isunsub'] = merged_email_subs_df['isunsub'].fillna(0)
merged_email_subs_df['chapter_id'] = merged_email_subs_df['chapter_id'].fillna(1)


# NEED CONFIRMATION: what is person created_at and updated_at?
# Rename the columns
merged_email_subs_df = merged_email_subs_df.rename(columns={'create_dt_x': 'create_dt', 'modified_dt_x': 'updated_dt', 'isunsub':'is_unsub'})
selected_columns = ['email', 'is_unsub', 'create_dt', 'updated_dt']
final_df = merged_email_subs_df.loc[:, selected_columns]

# Revise data types
final_df = final_df.astype({'email': 'str', 'is_unsub': 'boolean'})
final_df['create_dt'] = pd.to_datetime(final_df['create_dt'])
final_df['updated_dt'] = pd.to_datetime(final_df['updated_dt'])

# Export the modified DataFrame to a new CSV file
final_df.to_csv('People.csv', index=False)