import os
import requests

# 1. Load credentials securely
CLIENT_ID = os.environ.get("CRMLS_CLIENT_ID")
CLIENT_SECRET = os.environ.get("CRMLS_CLIENT_SECRET")

print(f"DEBUG: Client ID exists? {'YES' if CLIENT_ID else 'NO - ITS EMPTY'}")
print(f"DEBUG: Client Secret exists? {'YES' if CLIENT_SECRET else 'NO - ITS EMPTY'}")

# Trestle-specific Endpoints
TOKEN_URL = "https://api-prod.corelogic.com/trestle/oidc/connect/token"
BASE_API_URL = "https://api-prod.corelogic.com/trestle/odata/Property"

def get_access_token():
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "api"  # Trestle uses 'api' instead of 'OData'
    }
    
    # Use the form-encoded headers we added earlier!
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    print("Requesting Trestle Access Token...")
    response = requests.post(TOKEN_URL, data=payload, headers=headers)
    
    if response.status_code == 200:
        print("Success! Token retrieved.")
        return response.json().get("access_token")
    else:
        raise Exception(f"Failed to get token: {response.status_code} - {response.text}")
    
def fetch_sample_property(token):
    """Uses the token to pull a single property listing to audit the schema."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    # OData syntax: $top=1 gets just one record, $filter limits to active listings
    query_url = f"{BASE_API_URL}?$top=1&$filter=StandardStatus eq 'Active'"
    
    print("Fetching sample listing...")
    response = requests.get(query_url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        print("\n--- Live Data Schema Retrieved ---")
        # Print the keys (columns) to see what data we actually have to work with
        if 'value' in data and len(data['value']) > 0:
            sample_property = data['value'][0]
            for key, val in sample_property.items():
                print(f"{key}: {type(val).__name__} (e.g., {val})")
        else:
            print("Query successful, but no active properties returned.")
    else:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")

if __name__ == "__main__":
    try:
        access_token = get_access_token()
        fetch_sample_property(access_token)
    except Exception as e:
        print(e)