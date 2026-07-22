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

-----------------------------------------------------------------------------------------------------------------------------------------

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

-----------------------------------------------------------------------------------------------------------------------------------------

## July 2, 2026 - Week 2

### Goal: Establish cloud cost governance, role-based access controls, and automated budget enforcement for team infrastructure.

### What I did today:
* **Architected Cost-Tracking Guardrails:** Formulate a strict resource-tagging protocol to map infrastructure spend directly to specific teams and presented this structure to my boss (e.g., `Key: Group` | `Value: TestGroupNum`). Provisions were made to anchor all pipeline assets to a single region (`us-west-1`) to eliminate cross-region data transfer fees.

* **Configured Automated Budget Fail-Safes:**
    * Navigated the AWS Budgets console to construct a project-specific cost tracking model with a hard enforcement threshold.
    * Engineered a real-time **Budget Action** to attach a restrictive `ReadOnlyAccess` IAM policy to the target user group the exact millisecond a financial threshold is breached, completely freezing unauthorized resource provisioning.

* **Delegated Cloud Administration:** Structured a shared administration workflow enabling designated platforms admins/coaches to independently manage IAM onboarding, cost allocation tag activation, and budget overrides without relying on root account credentials.

### Challenges & Debugging:
* **Obstacle 1:** Attempting to access the **Cost Allocation Tags** page resulted in an immediate "Access Denied: IAM user access not activated" error, despite operating with administrative permissions.

* **Solution 1:** Identified that AWS inherently locks the financial vault from all non-root users. Resolved by executing a one-time root account configuration update to check the box for **Activate IAM Access** under the account's Billing preferences, unlocking the dashboard for the administrative team.

* **Obstacle 2:** Newly applied user-defined tags on S3 resource layers failed to populate immediately within the AWS Budgets dropdown menu.

* **Solution 2:** Recognized the underlying architectural delay of the AWS billing console; the system requires a 24-hour cycle to run a global resource sweep and ingest active metadata tags. Left the tagged infrastructure static to allow the automated daily system sweep to register the keys.

* **Obstacle 3:** The AWS Budgets automated action engine could not be executed due to missing role identity permissions in the dropdown menu.

* **Solution 3:** Discovered that the budget automation engine operates as an independent "robot" requiring explicit service-linked permissions. Formulated a standalone IAM service role (`Budget-Actions-Role`) utilizing the managed policy `AWSBudgetsActions_RolePolicyForResourceAdministrationWithSSM` to grant the budget engine the authority to pass and apply "Deny" rules mid-month.

### The Win:
The cloud infrastructure is now completely insulated from runaway spend. A robust, multi-tenant administrative structure is in place, and if an engineer accidentally leaves a heavy, unapproved data science resource running over a weekend, the automated budget robot will instantly drop their access to Read-Only before any catastrophic credit burning can occur.

### Next Steps:
* Log back into the AWS console after the 24-hour data sweep to verify that the `Group` tag key has populated and is ready for activation.
* Finalize mapping the team groups and hand off the Day 1 deployment instructions to the platform users.
* Shift focus to the data engineering layer to map out the AWS Glue ETL workflow for turning the raw Bronze JSON strings into clean relational tables.

-----------------------------------------------------------------------------------------------------------------------------------------

## July 6, 2026 - Week 2

### Goal: Establish cloud cost governance, role-based access controls, and automated budget enforcement for team infrastructure.

### What I did today:
* **Architected Cost-Tracking Guardrails:** Formulate a strict resource-tagging protocol to map infrastructure spend directly to specific teams and presented this structure to my boss (e.g., `Key: Group` | `Value: TestGroupNum`). Provisions were made to anchor all pipeline assets to a single region (`us-west-1`) to eliminate cross-region data transfer fees.

* **Configured Automated Budget Fail-Safes:**
    * Navigated the AWS Budgets console to construct a project-specific cost tracking model with a hard enforcement threshold.
    * Engineered a real-time **Budget Action** to attach a restrictive `ReadOnlyAccess` IAM policy to the target user group the exact millisecond a financial threshold is breached, completely freezing unauthorized resource provisioning.

* **Delegated Cloud Administration:** Structured a shared administration workflow enabling designated platforms admins/coaches to independently manage IAM onboarding, cost allocation tag activation, and budget overrides without relying on root account credentials.

* **Engineered Silver-Layer Data Pipeline:** Successfully implemented a robust ETL workflow using AWS Glue/Spark to process raw Trestle API JSON data.
    * Configured Spark to handle multi-line, nested API structures with `multiLine` and `PERMISSIVE` modes.
    * Implemented structural flattening of API `value` arrays using `explode`.
    * Standardized data quality by casting raw string fields to appropriate `DoubleType` and `IntegerType` formats to enable math and filtering.
    * Integrated automated deduplication logic using `dropDuplicates(["ListingId"])` to ensure data integrity.
    * Optimized storage by partitioning output data by `year`, `month`, and `day` and writing to S3 in Parquet format with a specific path-overwrite strategy.

### Challenges & Debugging:
* **Obstacle 1:** Attempting to access the **Cost Allocation Tags** page resulted in an immediate "Access Denied: IAM user access not activated" error, despite operating with administrative permissions.

* **Solution 1:** Identified that AWS inherently locks the financial vault from all non-root users. Resolved by executing a one-time root account configuration update to check the box for **Activate IAM Access** under the account's Billing preferences, unlocking the dashboard for the administrative team.

* **Obstacle 2:** Newly applied user-defined tags on S3 resource layers failed to populate immediately within the AWS Budgets dropdown menu.

* **Solution 2:** Recognized the underlying architectural delay of the AWS billing console; the system requires a 24-hour cycle to run a global resource sweep and ingest active metadata tags. Left the tagged infrastructure static to allow the automated daily system sweep to register the keys.

* **Obstacle 3:** The AWS Budgets automated action engine could not be executed due to missing role identity permissions in the dropdown menu.

* **Solution 3:** Discovered that the budget automation engine operates as an independent "robot" requiring explicit service-linked permissions. Formulated a standalone IAM service role (`Budget-Actions-Role`) utilizing the managed policy `AWSBudgetsActions_RolePolicyForResourceAdministrationWithSSM` to grant the budget engine the authority to pass and apply "Deny" rules mid-month.

* **Obstacle 4:** Spark JSON parsing resulted in a `_corrupt_record` error due to API multi-line responses. **Solution:** Enabled `multiLine("true")` and `PERMISSIVE` mode.
* **Obstacle 5:** Quantile calculations failed due to `StringType` casting and empty DataFrame/null issues. **Solution:** Implemented explicit `DoubleType` casting for prices and area fields, and added validation checks for row counts before running statistical operations.
* **Obstacle 6:** `mode("overwrite")` with `partitionBy` triggered "empty string" path errors. **Solution:** Switched to explicit path-based overwrites for specific partitions.

### The Win:
The cloud infrastructure is now completely insulated from runaway spend. Additionally, the data engineering pipeline is functional; raw, nested API responses are now being cleaned, deduplicated, and stored as optimized, partitioned Parquet files, creating a reliable foundation for downstream machine learning tasks.

### Next Steps:
* Log back into the AWS console after the 24-hour data sweep to verify that the `Group` tag key has populated and is ready for activation.
* Finalize mapping the team groups and hand off the Day 1 deployment instructions to the platform users.
* Perform a full historical backfill of all remaining data in the Bronze S3 bucket to populate the entire Silver data lake.
* Begin initial feature engineering on the cleaned Parquet data for the NFL/Real Estate prediction models.