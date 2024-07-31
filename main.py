from services.db_service import DatabaseService
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()
# Database connection details
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name1 = os.getenv("DB_NAME1")
db_name2 = os.getenv("DB_NAME2")
db_port = os.getenv("DB_PORT")


# Query to fetch data
query = """
    SELECT * FROM response_questionnaires_detail AS rqd
    INNER JOIN questionnaire_questions AS qq
    ON rqd.questionnaire_question_id = qq.id
    LIMIT 10;
"""

# Initialize database service
db_service = DatabaseService(db_host, db_user, db_password, int(db_port))

# Fetch data from both databases
data1 = db_service.fetch_data(db_name1, query)
# data2 = db_service.fetch_data(db_name2, query)

if data1:
    df1 = pd.DataFrame(data1)
    print(f"Data fetched from {db_name1}. Shape: {df1.shape}")
else:
    print(f"Failed to fetch data from {db_name1}")


# Combine data from both databases
# combined_data = pd.concat([data1, data2], ignore_index=True)

# Save the combined data to a CSV file
# combined_data.to_csv("combined_data.csv", index=False)

# print("Data has been fetched and saved to combined_data.csv")
