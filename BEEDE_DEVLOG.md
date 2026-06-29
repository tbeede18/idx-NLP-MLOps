# Engineering Log: Tyler Beede MLOps Real Estate Pipeline

## Role: MLOps & Data Infrastructure Engineer
## Goal: Build a real-time ingestion, validation, and automated retraining pipeline for California real estate data.

-----------------------------------------------------------------------------------------------------------------------------------------

## June 22, 2026 - Week 1

### Goal: Establish the foundational API connection and extract the data contract.

### What I did today:

* Received the Client ID and Secret for the CRMLS / CoreLogic Trestle API.

* Set up a local Python environment and securely exported credentials as environment variables to prevent leaking them to GitHub.

* Built the initial OAuth2 Client Credentials script to request an access token.

### Challenges & Debugging:

* Obstacle 1: Hit a 500 Internal Server Error on the identity token endpoint.

* Solution 1: Realized the initial CRMLS URLs were generic. Based on my Client ID, I deduced the gateway was actually managed by CoreLogic Trestle.

* Obstacle 2: Got through Server Error, but hit a NameResolutionError and then a 400 invalid_client.

* Solution 2: Updated the endpoints to point specifically to Trestle's production identity server (api-prod.corelogic.com).

### The Win:
Successfully authenticated and pulled a sample property record using OData ($top=1&$filter=StandardStatus eq 'Active'). I funneled the terminal output into a new file (trestle_schema_sample.txt) and pushed it to the repo.

### Next Steps:
Now that we have the exact data contract (schema) from the live feed, the AI/ML engineers on my team can begin writing their TFX validation rules and prompt logic, while I begin configuring the AWS Kinesis stream to ingest this data at scale.

-----------------------------------------------------------------------------------------------------------------------------------------

## June 25, 2026 - Week 1 (Continued)

### Goal: Discover how IAM keys work and connect AWS S3 cloud infrastructure to previously created python scripts.

### What I did today:

* Designed the AWS S3 storage strategy using the Medallion Architecture:

    * Bronze (Raw): Stores the immutable, raw JSON payloads directly from the API.

    * Silver (Cleaned): Will store cleaned data post-TFX (TensorFlow Data Validation) processing.

    * Gold (Features): Possibly useful for specific engineered features (e.g., aggregated neighborhood metrics) optimized for model training.

* Provisioned the initial Bronze tier S3 bucket with strict public access blocks.

* Generated AWS IAM programmatic access keys to connect the python code and the S3 Buckets.

* Upgraded the Python ingestion script to dynamically generate its own 1-hour OAuth2 token on the fly, making it fully autonomous.

* Forecasted the AWS cost for the project based on a real sample weight (5.3 MB per 200 properties), running the script 24/7 (8,640 batch requests/month) with a 50 GB storage buffer will cost up to $1.35/month, but the 50 GB storage buffer is ther intentionally, it will likely be less, in the $1.20-$1.30 range.

### Challenges & Debugging:

* Obstacle 1: Hardcoding the AWS Access Keys and Secret Keys in the Python script is a massive security risk for hackers/bots.

* Solution 1: Utilized an OS trick (os.getenv()) to keep the .csv keys strictly local. It's highly unlikely others will need these keys because the pipeline should be automated anyways. For our production roadmap, I established that we should deprecate these static keys entirely and attach passwordless IAM Roles directly to our cloud compute instances (Also free I believe!).

* Obstacle 2: Uploading a .txt file to the cloud is inefficient and slow, we want JSON for the computer's efficiency while remaining human-readable.

* Solution 2: Used boto3.put_object() and the json library to extract the API response and stream the JSON data directly into the S3 bucket from memory.

### The Win:
Successfully executed an end-to-end, fully automated ETL run. The script generated its own token, pulled 200 active records from the live API, and routed the 5.3 MB JSON payload directly into the secure Bronze S3 bucket in the cloud.

### Next Steps:
* Make the script automated to truly run every 5 minutes until our data is populated enough for the model (Look into AWS Lambda).

* Sync with the team to hand off the populated Bronze bucket. The ML engineers can now connect their TFX validation scripts to this cloud bucket to begin cleaning the data and building out the Silver layer.

## June 28, 2026 - Week 2

### Goal: Automate the 5-minute data ingestion pipeline using serverless cloud infrastructure.

### What I did today:
* Architected the Serverless Pipeline: Migrated the local Python ingestion script into an AWS Lambda function. Refactored the code from the requests library to use urllib to eliminate external dependency requirements within the Lambda environment.

* Infrastructure-as-a-Service Configuration:
    * Configured Environment Variables for secure management of Trestle API credentials within the AWS Console.

    * Attached an IAM Role with AmazonS3FullAccess to the Lambda function, moving away from the static access key on my laptop to a passwordless, secure cloud permissions setup.

    * Established an EventBridge (CloudWatch Events) trigger with a 5-minute rate expression to fully automate the 24/7 ingestion loop.

* Optimized API Interaction: Added OData parameters ($orderby=ModificationTimestamp desc) to the API request string to ensure the ingestion engine is consistently capturing the most recent market activity rather than static index records.

### Challenges & Debugging:
* Obstacle 1: Initial testing in AWS Lambda triggered Timeout errors.

* Solution 1: Identified that the default 3-second timeout was insufficient for the round-trip network latency between AWS and the CoreLogic/Trestle API. Increased the function timeout to 60 seconds to provide a safe operational buffer.

* Obstacle 2: URL encoding errors triggered by spaces in the OData query string.

* Solution 2: Resolved by explicitly URL-encoding the API query string to ensure compatibility with the web server, preventing control character errors.

* Obstacle 3: Lambda execution resulted in "Hello World" boilerplate output.

* Solution 3: Identified a disconnect between the "Code" editor and the "Deployment" trigger; confirmed that every code iteration requires an explicit Deploy action to update the production runtime.

### The Win:
The pipeline is now officially fully autonomous. The Lambda function successfully authenticates, pulls live market data, and writes unique, timestamped JSON payloads to the Bronze S3 bucket every 5 minutes without local client intervention. The architecture is now resilient, scalable, and cost-effective.

### Next Steps:
* Monitor ingestion logs overnight to ensure trigger stability.

* Begin the historical "Catch-Up" backfill to bridge the data gap between the original historical dataset and the start of live ingestion.

* Coordinate with the ML team to begin the Bronze-to-Silver ETL job using AWS Glue to deduplicate the incoming streams.