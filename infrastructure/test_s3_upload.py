import os
import boto3
import json
import requests
import time
from datetime import datetime

# 1. Plug in the keys from your .csv file
ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = 'idx-dev-bronze-950639281924-us-west-1-an' 

CLIENT_ID = os.getenv('CRMLS_CLIENT_ID')
CLIENT_SECRET = os.getenv('CRMLS_CLIENT_SECRET')

TOKEN_URL = "https://api-prod.corelogic.com/trestle/oidc/connect/token"
API_URL = "https://api-prod.corelogic.com/trestle/odata/Property?$top=200&$filter=StandardStatus eq 'Active'&$orderby=ModificationTimestamp desc"

# 2. Log in to AWS
s3 = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

# # 4. GENERATE A FRESH TOKEN (Translating that PHP script into Python)
# print("Generating fresh API access token...")
# token_payload = {
#     "grant_type": "client_credentials",
#     "client_id": CLIENT_ID,
#     "client_secret": CLIENT_SECRET
# }
# token_response = requests.post(TOKEN_URL, data=token_payload)
# token_response.raise_for_status() # This will throw an error if the credentials fail
# fresh_access_token = token_response.json().get("access_token")

# # 5. Pull ACTUAL data from the API using the fresh token
# print("Fetching live data from Trestle API...")
# api_headers = {
#     "Authorization": f"Bearer {fresh_access_token}",
#     "Accept": "application/json"
# }
# response = requests.get(API_URL, headers=api_headers)
# response.raise_for_status()
# real_estate_data = response.json()

# # Convert the Python dictionary into a formatted JSON string
# json_string = json.dumps(real_estate_data, indent=2)

# # 6. Upload directly to S3
# print("Uploading JSON data to S3...")
# s3.put_object(
#     Bucket=BUCKET_NAME,
#     Key='property_data_batch_2.json', # The name of the file in the cloud
#     Body=json_string,                 # The actual JSON data
#     ContentType='application/json'    # Tells S3 this is a JSON file
# )
# print("Upload successful! Your JSON data is now in the cloud.")



def run_ingestion():
    try:
        # 4. GENERATE A FRESH TOKEN 
        print(f"\n[{datetime.now().strftime('%X')}] Generating fresh API access token...")
        token_payload = {
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET
        }
        token_response = requests.post(TOKEN_URL, data=token_payload)
        token_response.raise_for_status() # This will throw an error if the credentials fail
        fresh_access_token = token_response.json().get("access_token")

        # 5. Pull ACTUAL data from the API using the fresh token
        print("Fetching live data from Trestle API...")
        api_headers = {
            "Authorization": f"Bearer {fresh_access_token}",
            "Accept": "application/json"
        }
        response = requests.get(API_URL, headers=api_headers)
        response.raise_for_status()
        real_estate_data = response.json()

        # Convert the Python dictionary into a formatted JSON string
        json_string = json.dumps(real_estate_data, indent=2)

        # 6. Generate a unique filename using the current date and time
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"property_data_batch_{timestamp}.json"

        # 7. Upload directly to S3
        print(f"Uploading {filename} to S3...")
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=filename,                 
            Body=json_string,                 
            ContentType='application/json'    
        )
        print("✅ Upload successful! JSON data is in the cloud.")

        
    except Exception as e:
        print(f"❌ Error during ingestion: {str(e)}")

if __name__ == "__main__":
    print("🚀 Starting 5-minute ingestion timer...")
    
    # Run an initial batch immediately
    run_ingestion()
    
    # Keep the script running forever, executing every 5 minutes (300 seconds)
    while True:
        print("\n⏳ Waiting 5 minutes for the next market update...")
        time.sleep(300)
        run_ingestion()