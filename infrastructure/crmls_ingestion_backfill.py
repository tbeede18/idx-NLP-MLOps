import time
import boto3
import requests
import pandas as pd
import os
import json
import requests
import time
from datetime import datetime, timedelta

ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME = 'idx-raw-rec-engine-01' 
CLIENT_ID = os.getenv('CRMLS_CLIENT_ID')
CLIENT_SECRET = os.getenv('CRMLS_CLIENT_SECRET')

TOKEN_URL = "https://api-prod.corelogic.com/trestle/oidc/connect/token"
API_BASE_URL = "https://api-prod.corelogic.com/trestle/odata/Property"

s3 = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

# Dynamically query parameters inside the loop
def get_backfill_params(skip_count, start_date, end_date):
    return {
        "$top": "200",
        "$skip": str(skip_count),
        "$filter": f"ModificationTimestamp ge {start_date} and ModificationTimestamp le {end_date}",
        "$orderby": "ModificationTimestamp asc"
    }

def run_historical_backfill():
    # Define your overall backfill window
    start_string = "2026-06-29"
    end_string = "2026-07-13"

    # Convert strings to datetime objects so Python can do math on them
    start_date = datetime.strptime(start_string, "%Y-%m-%d")
    end_date = datetime.strptime(end_string, "%Y-%m-%d")

    # Generate the list of days
    days_to_backfill = []
    current = start_date

    while current <= end_date:
        days_to_backfill.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1) # Adds exactly 1 day per loop

    print(f"Generated {len(days_to_backfill)} days to process.")

    # 1. Initialize your token tracking variables outside the loop
    last_token_time = 0
    api_headers = {}

    for current_date in days_to_backfill:
        print(f"--- Starting backfill for {current_date} ---")

        # 2. Check if the token is missing OR older than 50 minutes (3000 seconds)
        current_time = time.time()
        
        if (current_time - last_token_time) > 3000:
            print("Token missing or expiring soon. Generating a fresh one...")
            token_payload = {
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET
            }
            token_response = requests.post(TOKEN_URL, data=token_payload)
            token_response.raise_for_status()
            
            api_headers = {
                "Authorization": f"Bearer {token_response.json().get('access_token')}",
                "Accept": "application/json"
            }
            
            # 3. Update the timestamp to reset the 50-minute clock
            last_token_time = current_time
        
        # 2. Reset skip to 0 for the new day
        skip = 0 
        start_time = f"{current_date}T00:00:00Z"
        end_time = f"{current_date}T23:59:59Z"

        
        while True:
            try:
                # Fetch using our new dynamic parameters
                response = requests.get(
                    API_BASE_URL, 
                    headers=api_headers, 
                    params=get_backfill_params(skip, start_time, end_time)
                )
                
                # Smart backoff if CoreLogic rate limits you (HTTP 429)
                if response.status_code == 429:
                    print("⚠️ Hit rate limits. Sleeping for 15 seconds...")
                    time.sleep(15)
                    continue
                    
                response.raise_for_status()
                batch_data = response.json()
                records = batch_data.get("value", [])
                
                # If the value array is empty, we reached the end of the backlog!
                if not records:
                    print(f"Finished {current_date}!\n")
                    break

                print(f"Pulling records from skip offset: {skip}...")

                # 1. Look at the oldest record in this batch to determine its real date
                sample_timestamp = records[0].get("ModificationTimestamp") # e.g., "2026-05-01T14:15:58Z"

                # Slice the first 10 characters to get just 'YYYY-MM-DD'
                date_only = sample_timestamp[:10]

                # Split into your partition variables
                s3_year, s3_month, s3_day = date_only.split('-')

                # 4. Format the filename matching your current convention (using the data's true timestamp)
                file_timestamp = s3_year + s3_month + s3_day
                filename = f"property_data_batch_{file_timestamp}_skip_{skip}.json" 
                # Note: Adding the '_skip_' variable guarantees uniqueness if multiple batches land in the same second

                # 5. Assemble the final path structure
                s3_key = f"year={s3_year}/month={s3_month}/day={s3_day}/{filename}"

                # 6. Push the raw text straight into that folder structure
                s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=s3_key,
                    Body=json.dumps(batch_data, indent=2),
                    ContentType='application/json'
                )
                
                print(f"✅ Successfully loaded {len(records)} records into S3.")
                
                # Advance to the next page
                skip += 200
                
                # Short courtesy pause so CoreLogic doesn't block your connection
                time.sleep(1.0)
            
            except Exception as e:
                print(f"❌ FATAL ERROR during backfill slice at skip {skip}: {str(e)}")
                print("Skipping the rest of this day to avoid infinite retry loop...")
                break # Moves to the next day instead of looping forever

if __name__ == "__main__":
    print("🚀 Starting historical data backfill processing...")
    run_historical_backfill()