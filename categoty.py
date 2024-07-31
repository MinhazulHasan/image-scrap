import pandas as pd

def get_distinct_values(csv_file):
    df = pd.read_csv(csv_file)
    # Drop rows where 'field_name' is NaN
    df = df.dropna(subset=['field_name'])
    # Filter the DataFrame for rows where 'field_name' ends with '_image' or '_photo'
    filtered_df = df[df['field_name'].str.endswith(('_image', '_photo'))]
    # Get distinct values from the 'field_name' column
    distinct_values = filtered_df['field_name'].unique().tolist()
    return distinct_values


csv_file = './ailed-data/db1/questionnaire_questions.csv'
distinct_values = get_distinct_values(csv_file)
for value in distinct_values:
    print(value)

# make a CSV file with the distinct values
df = pd.DataFrame(distinct_values, columns=['field_name'])
df.to_csv('distinct_field_names.csv', index=False)
print("Distinct values have been saved to distinct_field_names.csv")


