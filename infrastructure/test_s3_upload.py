import os
import boto3
import json
import requests

# 1. Plug in the keys from your .csv file
ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = 'idx-dev-bronze-950639281924-us-west-1-an' 

CLIENT_ID = os.getenv('CRMLS_CLIENT_ID')
CLIENT_SECRET = os.getenv('CRMLS_CLIENT_SECRET')

TOKEN_URL = "https://api-prod.corelogic.com/trestle/oidc/connect/token"
API_URL = "https://api-prod.corelogic.com/trestle/odata/Property?$top=200&$filter=StandardStatus eq 'Active'"

# 2. Log in to AWS
s3 = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

# 4. GENERATE A FRESH TOKEN (Translating that PHP script into Python)
print("Generating fresh API access token...")
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

# 6. Upload directly to S3
print("Uploading JSON data to S3...")
s3.put_object(
    Bucket=BUCKET_NAME,
    Key='property_data_batch_1.json', # The name of the file in the cloud
    Body=json_string,                 # The actual JSON data
    ContentType='application/json'    # Tells S3 this is a JSON file
)
print("Upload successful! Your JSON data is now in the cloud.")