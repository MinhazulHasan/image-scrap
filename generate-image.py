import pandas as pd
import os
import requests
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Read the CSV file
df = pd.read_csv('./output/result.csv')

# Create a set of unique field names
field_names = set(df['field_name'])

# Create folders based on field names
for field in field_names:
    os.makedirs(os.path.join('data', field), exist_ok=True)

# Function to download an image
def download_image(url, folder):
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            file_name = os.path.basename(urlparse(url).path)
            file_path = os.path.join(folder, file_name)
            with open(file_path, 'wb') as file:
                for chunk in response.iter_content(1024):
                    file.write(chunk)
            print(f"Downloaded: {file_path}")
        else:
            print(f"Failed to download: {url}")
    except Exception as e:
        print(f"Error downloading {url}: {str(e)}")

# Use ThreadPoolExecutor for parallel downloads
with ThreadPoolExecutor(max_workers=10) as executor:
    future_to_url = {executor.submit(download_image, row['full_image_url'], row['field_name']): row for _, row in df.iterrows()}
    for future in as_completed(future_to_url):
        future.result()  # This will raise any exceptions that occurred during download

print("Download complete!")