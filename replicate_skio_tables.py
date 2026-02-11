import os
import yaml
import json
import requests
import datetime
import logging
import re
import google.auth
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import secretmanager
from google.cloud.exceptions import NotFound

# Import our custom modules
from data_quality import QualityChecker
from slack_reporter import SlackReporter

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================

# Defaults (Override via Cloud Function Environment Variables)
SOURCE_PROJECT = os.getenv("SOURCE_PROJECT", "largedata-380204")
SOURCE_DATASET = os.getenv("SOURCE_DATASET", "skio_dosedaily_co")
SOURCE_REGION = os.getenv("SOURCE_REGION", "us-west1")

DEST_PROJECT = os.getenv("DEST_PROJECT", "dosedaily-raw")
DEST_DATASET = os.getenv("DEST_DATASET", "skio_subscriptions")
DEST_REGION = os.getenv("DEST_REGION", "US")

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET", "dosedaily-skio-bridge-west1-v4") 
YAML_PATH = os.getenv("YAML_PATH", "skio_schema_with_keys.yml")
SECRET_NAME = os.getenv("SECRET_NAME", "SLACK_WEBHOOK_URL")

# 🚫 TABLES TO IGNORE
TABLES_TO_SKIP = ["CancelFlowV2Session"] 

# ==========================================
# 🛠️ HELPER FUNCTIONS
# ==========================================

def get_secret(secret_name, project_id, version_id="latest", logger=None):
    """Fetch secret from Secret Manager. Returns None on failure."""
    log = logger or logging.getLogger(__name__)
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_name}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        value = response.payload.data.decode("UTF-8")
        log.info("Secret '%s' fetched successfully (length=%d)", secret_name, len(value))
        return value
    except Exception as e:
        log.warning("Could not fetch secret '%s' from project %s: %s", secret_name, project_id, e)
        return None

def to_snake_case(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

# ==========================================
# 🚀 CORE LOGIC
# ==========================================

def replicate_skio_data(request):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    dry_run = False
    if request and request.args.get('dry_run', '').lower() == 'true':
        dry_run = True
        logger.info("🔍 DRY RUN MODE ENABLED")

    # 1. Setup Clients
    client_source = bigquery.Client(project=DEST_PROJECT, location=SOURCE_REGION)
    client_dest = bigquery.Client(project=DEST_PROJECT, location=DEST_REGION)
    storage_client = storage.Client(project=DEST_PROJECT)
    
    quality_checker = QualityChecker(client_dest)

    # 2. Bridge Bucket
    try:
        bucket = storage_client.get_bucket(GCS_BUCKET_NAME)
    except NotFound:
        if not dry_run: storage_client.create_bucket(GCS_BUCKET_NAME, location=SOURCE_REGION)

    # 3. Load Schema
    if not os.path.exists(YAML_PATH):
        return f"Schema {YAML_PATH} not found", 500
    with open(YAML_PATH, 'r') as f:
        schema_data = yaml.safe_load(f) or {}

    tables_to_sync = []
    processed = set()
    for src in schema_data.get('sources', []):
        for t in src.get('tables', []):
            if t['name'] not in processed and t['name'] not in TABLES_TO_SKIP:
                tables_to_sync.append(t)
                processed.add(t['name'])

    logger.info(f"📊 Processing {len(tables_to_sync)} tables...")
    
    # 4. Ensure Dataset
    try:
        client_dest.get_dataset(f"{DEST_PROJECT}.{DEST_DATASET}")
    except NotFound:
        if not dry_run:
            ds = bigquery.Dataset(f"{DEST_PROJECT}.{DEST_DATASET}")
            ds.location = DEST_REGION
            client_dest.create_dataset(ds)

    sync_results = []

    # 5. Sync Loop
    for idx, table_info in enumerate(tables_to_sync, 1):
        t_name = table_info['name']
        t_desc = table_info.get('description', '')
        yaml_columns = table_info.get('columns', [])
        
        logger.info(f"[{idx}/{len(tables_to_sync)}] {t_name}")

        try:
            # --- EXTRACT ---
            gcs_uri = f"gs://{GCS_BUCKET_NAME}/skio_export/{t_name}/*.avro"
            if not dry_run:
                extract_config = bigquery.ExtractJobConfig(destination_format="AVRO")
                blobs = list(storage_client.list_blobs(GCS_BUCKET_NAME, prefix=f"skio_export/{t_name}/"))
                if blobs: 
                    b = storage_client.bucket(GCS_BUCKET_NAME)
                    b.delete_blobs(blobs)
                client_source.extract_table(f"{SOURCE_PROJECT}.{SOURCE_DATASET}.{t_name}", gcs_uri, job_config=extract_config).result()

            # --- LOAD STAGING ---
            staging_id = f"{DEST_PROJECT}.{DEST_DATASET}.{t_name}_staging"
            if not dry_run:
                job_config = bigquery.LoadJobConfig(source_format="AVRO", write_disposition="WRITE_TRUNCATE", use_avro_logical_types=True)
                client_dest.load_table_from_uri(gcs_uri, staging_id, job_config=job_config).result()

            # --- PREPARE FINAL & MAP TYPES ---
            dest_id = f"{DEST_PROJECT}.{DEST_DATASET}.{t_name}"
            
            # Fetch Staging Schema
            staging_map = {} 
            if not dry_run:
                staging_table = client_dest.get_table(staging_id)
                for f in staging_table.schema:
                    staging_map[f.name] = f.field_type
            else:
                for c in yaml_columns:
                    staging_map[c['name']] = "STRING"

            pks = [c['name'] for c in yaml_columns if c.get('primary_key') is True]
            if not pks:
                sync_results.append({"table": t_name, "status": "SKIPPED", "message": "No PK"})
                continue
            pk_col = pks[0]

            valid_cols = []
            source_map = {}
            col_types = {}
            bq_schema = []
            
            # Freshness Tracking Candidates
            created_col_name = None
            updated_col_name = None

            for col in yaml_columns:
                c_name = col['name']
                
                # Identify Freshness Columns
                if c_name in ['createdAt', 'created_at']:
                    created_col_name = c_name
                if c_name in ['updatedAt', 'updated_at']:
                    updated_col_name = c_name

                # Resolve Source Column Name
                src_name = None
                if c_name in staging_map:
                    src_name = c_name
                elif to_snake_case(c_name) in staging_map:
                    src_name = to_snake_case(c_name)
                
                if not src_name:
                    if c_name == pk_col: raise ValueError(f"PK {pk_col} missing")
                    continue
                
                source_map[c_name] = src_name
                valid_cols.append(c_name)
                
                dtype = col.get('data_type', 'STRING').upper()
                mapped_type = {'VARCHAR': 'STRING', 'DATETIME': 'TIMESTAMP', 'BOOLEAN': 'BOOL', 'INTEGER': 'INT64', 'FLOAT': 'FLOAT64', 'JSON': 'JSON'}.get(dtype, dtype)
                col_types[c_name] = mapped_type
                bq_schema.append(bigquery.SchemaField(c_name, mapped_type, description=col.get('description')))

            # Create/Update Final Table
            if not dry_run:
                try:
                    table = client_dest.get_table(dest_id)
                    table.schema = bq_schema
                    client_dest.update_table(table, ["schema"])
                except NotFound:
                    table = bigquery.Table(dest_id, schema=bq_schema)
                    table.description = t_desc
                    if table_info.get('partition_by') and table_info.get('partition_by') in valid_cols:
                        table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field=table_info.get('partition_by'))
                    if table_info.get('cluster_by'):
                        table.clustering_fields = [c for c in table_info.get('cluster_by') if c in valid_cols]
                    client_dest.create_table(table)
                    if pks:
                        try: client_dest.query(f"ALTER TABLE `{dest_id}` ADD PRIMARY KEY ({', '.join(pks)}) NOT ENFORCED").result()
                        except: pass

            # --- MERGE ---
            def safe_cast(dest_col):
                src_col = source_map[dest_col]
                target_type = col_types[dest_col]
                source_type = staging_map.get(src_col, "STRING") 

                # 1. JSON -> JSON
                if target_type == 'JSON' and source_type == 'JSON':
                    return f"S.`{src_col}`"
                # 2. String -> JSON
                if target_type == 'JSON':
                    return f"SAFE.PARSE_JSON(SAFE_CAST(S.`{src_col}` AS STRING))"
                # 3. JSON -> String
                if target_type == 'STRING' and source_type == 'JSON':
                    return f"TO_JSON_STRING(S.`{src_col}`)"
                # 4. Standard Cast
                return f"SAFE_CAST(S.`{src_col}` AS {target_type})"

            update_sql = ", ".join([f"`{c}`={safe_cast(c)}" for c in valid_cols if c != pk_col])
            insert_cols = ", ".join([f"`{c}`" for c in valid_cols])
            insert_vals = ", ".join([f"{safe_cast(c)}" for c in valid_cols])
            
            pk_cast_str = safe_cast(pk_col)

            merge_sql = f"""
                MERGE `{dest_id}` T USING `{staging_id}` S
                ON T.`{pk_col}` = {pk_cast_str}
                WHEN MATCHED THEN UPDATE SET {update_sql}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """

            if not dry_run:
                client_dest.query(merge_sql).result()
                client_dest.delete_table(staging_id, not_found_ok=True)
                
                # ✅ Data Quality Check (Passes BOTH columns now)
                quality_checker.run_check(dest_id, pk_col, created_col_name, updated_col_name)

            sync_results.append({"table": t_name, "status": "SUCCESS", "message": "Synced"})

        except Exception as e:
            logger.error(f"Failed {t_name}: {e}")
            sync_results.append({"table": t_name, "status": "ERROR", "message": str(e)})

    # Final Report
    success_count = len([r for r in sync_results if r['status'] == 'SUCCESS'])
    error_count = len([r for r in sync_results if r['status'] == 'ERROR'])
    
    if not dry_run:
        logger.info("Fetching Slack webhook from Secret Manager: project=%s, secret=%s", DEST_PROJECT, SECRET_NAME)
        webhook = get_secret(SECRET_NAME, DEST_PROJECT, logger=logger)
        if not webhook:
            logger.warning("Slack webhook is missing: secret '%s' not found or not accessible. No Slack report will be sent.", SECRET_NAME)
        dq_report = quality_checker.get_results()
        slack_reporter = SlackReporter(webhook, logger=logger)
        slack_reporter.send_report(sync_results, dq_report)
    else:
        logger.info("Dry run: skipping Slack report.")

    return f"Done. Success: {success_count}, Errors: {error_count}", 200

# ==========================================
# 🔌 LOCAL TESTING WRAPPER
# ==========================================
if __name__ == "__main__":
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("✅ Loaded environment variables from .env")
    except ImportError:
        pass

    class MockRequest:
        def __init__(self, args=None):
            self.args = args or {}
            
    mock_request = MockRequest(args={'dry_run': 'false'})
    print("🚀 Starting Local Execution...")
    msg, code = replicate_skio_data(mock_request)
    print(f"\n🏁 {msg} ({code})")