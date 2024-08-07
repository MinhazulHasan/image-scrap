import pandas as pd
import os
import requests
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import base64
from bs4 import BeautifulSoup
import re

# Create base directory
base_dir = 'data'
os.makedirs(base_dir, exist_ok=True)

def create_category_folders(categories):
    for category in categories:
        folder_path = os.path.join(base_dir, category)
        os.makedirs(folder_path, exist_ok=True)
    print(f"Created folders for {len(categories)} unique categories.")

def download_and_save_image(row):
    category = row['field_name']
    s3_path = row['response']
    
    if not s3_path.startswith('vacam/'):
        return f"Skipped {s3_path}: Not a valid S3 path"
    
    image_url = f'https://rooms.ailed.ai/image.php?path={s3_path}'
    image_name = s3_path.split('/')[-1]
    save_path = os.path.join(base_dir, category, image_name)
    
    try:
        response = requests.get(image_url, timeout=10)
        response.raise_for_status()
        
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        img_tag = soup.find('img')
        
        if img_tag and img_tag.has_attr('src'):
            # Extract the base64 data
            base64_data = img_tag['src'].split(',')[1]
            
            # Decode the base64 data
            image_data = base64.b64decode(base64_data)
            
            # Save the image
            with open(save_path, 'wb') as f:
                f.write(image_data)
            
            return f"Successfully downloaded and saved: {image_name}"
        else:
            return f"Error with {image_name}: No valid image data found"
    
    except requests.RequestException as e:
        return f"Error downloading {image_name}: {str(e)}"
    except Exception as e:
        if os.path.exists(save_path):
            os.remove(save_path)
        return f"Error processing {image_name}: {str(e)}"

# Load and prepare data
df1 = pd.read_csv('output/Responses of Images with label DB2.csv')
df2 = pd.read_csv('output/Responses of Images with label.csv')

# Combine dataframes
df = pd.concat([df1, df2], ignore_index=True)
df = df[df['field_name'].str.endswith(('_image', '_photo', '_photos'))]

# Rename columns for clarity
df.columns = ['order_id', 'response', 'field_name']

# Get unique categories
categories = df['field_name'].unique()

print(f"Loaded {len(df)} rows of data with {len(categories)} unique categories.")
print(df.head())  # Display the first few rows to verify the data

# Create category folders
create_category_folders(categories)

# Download images
results = []

with ThreadPoolExecutor(max_workers=10) as executor:
    future_to_row = {executor.submit(download_and_save_image, row): row for _, row in df.iterrows()}
    for future in as_completed(future_to_row):
        result = future.result()
        results.append(result)
        print(result)  # Print result for each image

print(f"Finished processing {len(results)} images.")

# Summary
success_count = sum(1 for r in results if r.startswith("Successfully"))
error_count = len(results) - success_count

print(f"Total images processed: {len(results)}")
print(f"Successfully downloaded: {success_count}")
print(f"Errors encountered: {error_count}")

# Display some of the error messages if any
if error_count > 0:
    print("\nSample of errors encountered:")
    for result in results:
        if not result.startswith("Successfully"):
            print(result)
            if error_count <= 5 or len(results) <= 20:
                continue
            break