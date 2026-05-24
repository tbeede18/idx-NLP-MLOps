import os
import json
import boto3
import requests
from datetime import datetime

# Load local .env variables if running on your laptop. 
# (GitHub Actions will ignore this and use your Secrets automatically)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def fetch_and_store_properties(zip_code):
    print(f"Starting ingestion for ZIP: {zip_code}")
    
    # 1. Setup API Connection
    url = f"https://{os.getenv('RAPIDAPI_HOST')}/search"
    querystring = {"location": zip_code, "status": "for_sale"}
    headers = {
        "X-RapidAPI-Key": os.getenv("RAPIDAPI_KEY"),
        "X-RapidAPI-Host": os.getenv("RAPIDAPI_HOST")
    }
    
    print("Fetching data from RapidAPI...")
    response = requests.get(url, headers=headers, params=querystring)
    
    if response.status_code != 200:
        print(f"API Error: {response.status_code}")
        print(response.text)
        return
        
    raw_data = response.json()
    
    # 2. Establish unique filename for the Bronze Data Lake
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"raw_listings_{zip_code}_{timestamp}.json"
    
    # 3. Connect to AWS S3 and upload raw JSON payload
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    
    # *** REPLACE THIS WITH YOUR EXACT BUCKET NAME ***
    bucket_name = "idx-dev-bronze-950639281924-us-west-1-an" 
    
    print(f"Uploading {filename} to S3 Bronze layer ({bucket_name})...")
    s3.put_object(
        Bucket=bucket_name,
        Key=filename,
        Body=json.dumps(raw_data, indent=2),
        ContentType="application/json"
    )
    print("Ingestion successful. Data secured in S3.")

if __name__ == "__main__":
    # Test market - you can change this to any zip code you want
    fetch_and_store_properties("90007")