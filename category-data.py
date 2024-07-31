import pandas as pd


def filter_records(csv_file, output_csv_file):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file)
    
    # Drop rows where 'field_name' is NaN
    df = df.dropna(subset=['field_name'])
    
    # Filter the DataFrame for rows where 'field_name' ends with '_image' or '_photo'
    filtered_df = df[df['field_name'].str.endswith(('_image', '_photo'))]
    
    # Write the filtered records to a new CSV file
    filtered_df.to_csv(output_csv_file, index=False)

# Specify the path to your input CSV file and the output CSV file
input_csv_file = './ailed-data/db1/questionnaire_questions.csv'
output_csv_file = 'filtered_records.csv'

# Filter records and create a new CSV file
filter_records(input_csv_file, output_csv_file)

print(f"Filtered records have been saved to {output_csv_file}")
