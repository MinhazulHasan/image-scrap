import pandas as pd
import os

# Set the path to your data directory
data_dir = 'ailed-data/db2'

# Function to safely read CSV and print column names
def read_csv_safe(filepath):
    df = pd.read_csv(filepath)
    print(f"Columns in {os.path.basename(filepath)}:")
    print(df.columns.tolist())
    return df

# Read the CSV files
objects_df = read_csv_safe(os.path.join(data_dir, 'response_questionnaires_detail_objects.csv'))
details_df = read_csv_safe(os.path.join(data_dir, 'response_questionnaires_detail.csv'))
questions_df = read_csv_safe(os.path.join(data_dir, 'questionnaire_questions.csv'))

# Merge the dataframes
merged_df = objects_df.merge(
    details_df,
    left_on='response_questionnaires_detail_id',
    right_on='id',
    how='left',
    suffixes=('_obj', '_det')
)

merged_df = merged_df.merge(
    questions_df,
    left_on='questionnaire_question_id',
    right_on='id',
    how='left',
    suffixes=('', '_que')
)

print("\nColumns in merged_df:")
print(merged_df.columns.tolist())

# Check if 'response' and 'field_name' columns exist
if 'response' not in merged_df.columns:
    print("Error: 'response' column not found in merged DataFrame")
    # Try to find a column that might contain the response
    possible_response_columns = [col for col in merged_df.columns if 'response' in col.lower()]
    if possible_response_columns:
        print(f"Possible response columns: {possible_response_columns}")
        response_column = possible_response_columns[3]
    else:
        raise KeyError("Cannot find a suitable 'response' column")
else:
    response_column = 'response'

if 'field_name' not in merged_df.columns:
    print("Error: 'field_name' column not found in merged DataFrame")
    raise KeyError("Cannot find 'field_name' column")

# Filter rows where field_name ends with 'image' or 'photos'
filtered_df = merged_df[
    merged_df['field_name'].notna() & 
    (merged_df['field_name'].str.endswith('_image') | 
     merged_df['field_name'].str.endswith('_photos'))
]


# Function to construct full image URL
def construct_full_url(path):
    return f'https://rooms.ailed.ai/image.php?path={path}'


# Select only the required columns
result_df = filtered_df[['field_name', response_column]]
result_df['full_image_url'] = result_df['response_det'].apply(construct_full_url)


# Remove any rows with null values
result_df = result_df.dropna()

# Save the result to a new CSV file
result_df.to_csv(os.path.join('output', 'result.csv'), index=False)

print(f"\nProcessed {len(result_df)} rows. Result saved to 'result.csv'.")