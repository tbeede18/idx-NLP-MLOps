Engineering Log: Tyler Beede MLOps Real Estate Pipeline

Role: MLOps & Data Infrastructure Engineer
Goal: Build a real-time ingestion, validation, and automated retraining pipeline for California real estate data.

June 22, 2026 - Week 1

Goal: Establish the foundational API connection and extract the data contract.

What I did today:

Received the Client ID and Secret for the CRMLS / CoreLogic Trestle API.

Set up a local Python environment and securely exported credentials as environment variables to prevent leaking them to GitHub.

Built the initial OAuth2 Client Credentials script to request an access token.

Challenges & Debugging:

Obstacle 1: Hit a 500 Internal Server Error on the identity token endpoint.

Solution 1: Realized the initial CRMLS URLs were generic. Based on my Client ID, I deduced the gateway was actually managed by CoreLogic Trestle.

Obstacle 2: Got through Server Error, but hit a NameResolutionError and then a 400 invalid_client.

Solution 2: Updated the endpoints to point specifically to Trestle's production identity server (api-prod.corelogic.com).

The Win:
Successfully authenticated and pulled a sample property record using OData ($top=1&$filter=StandardStatus eq 'Active'). I funneled the terminal output into a new file (trestle_schema_sample.txt) and pushed it to the repo.

Next Steps:
Now that we have the exact data contract (schema) from the live feed, the AI/ML engineers on my team can begin writing their TFX validation rules and prompt logic, while I begin configuring the AWS Kinesis stream to ingest this data at scale.

June 25, 2026 - Week 1 (Continued)

Goal: Discover how IAM keys work and connect AWS S3 cloud infrastructure to previously created python scripts.

What I did today:

Designed the AWS S3 storage strategy using the Medallion Architecture:

Bronze (Raw): Stores the immutable, raw JSON payloads directly from the API.

Silver (Cleaned): Will store cleaned data post-TFX (TensorFlow Data Validation) processing.

Gold (Features): Possibly useful for specific engineered features (e.g., aggregated neighborhood metrics) optimized for model training.

Provisioned the initial Bronze tier S3 bucket with strict public access blocks.

Generated AWS IAM programmatic access keys to connect the python code and the S3 Buckets.

Upgraded the Python ingestion script to dynamically generate its own 1-hour OAuth2 token on the fly, making it fully autonomous.

Forecasted the AWS cost for the project based on a real sample weight (5.3 MB per 200 properties), running the script 24/7 (8,640 batch requests/month) with a 50 GB storage buffer will cost up to $1.35/month, but the 50 GB storage buffer is ther intentionally, it will likely be less, in the $1.20-$1.30 range.

Challenges & Debugging:

Obstacle 1: Hardcoding the AWS Access Keys and Secret Keys in the Python script is a massive security risk for hackers/bots.

Solution 1: Utilized an OS trick (os.getenv()) to keep the .csv keys strictly local. It's highly unlikely others will need these keys because the pipeline should be automated anyways. For our production roadmap, I established that we should deprecate these static keys entirely and attach passwordless IAM Roles directly to our cloud compute instances (Also free I believe!).

Obstacle 2: Uploading a .txt file to the cloud is inefficient and slow, we want JSON for the computer's efficiency while remaining human-readable.

Solution 2: Used boto3.put_object() and the json library to extract the API response and stream the JSON data directly into the S3 bucket from memory.

The Win:
Successfully executed an end-to-end, fully automated ETL run. The script generated its own token, pulled 200 active records from the live API, and routed the 5.3 MB JSON payload directly into the secure Bronze S3 bucket in the cloud.

Next Steps:
Make the script automated to truly run every 5 minutes until our data is populated enough for the model (Look into AWS Lambda).

Sync with the team to hand off the populated Bronze bucket. The ML engineers can now connect their TFX validation scripts to this cloud bucket to begin cleaning the data and building out the Silver layer.