import functions_framework
import json
from google.cloud import bigquery
from google.cloud.bigquery_storage import BigQueryReadClient
from flask import Response
import logging
import asyncio
import datetime
import httpx
import slack_alert
from collections import defaultdict # Added for grouping

# --- Client Initialization ---
bq_client = None
bq_storage_client = None

# --- ASYNC BQ & LOGGING FUNCTIONS ---

async def get_anomaly_enrichment(project_id, target_date):
    """
    Asynchronously calls the assign_anomaly_id table function.
    """
    global bq_client
    if not bq_client:
        bq_client = bigquery.Client()

    query = f"""
        SELECT
            anomaly_id,
            anomaly_already_alerted,
            most_likely_service_description AS service_description,
            top_service_increase_list
        FROM `<project_id>.<dataset>.assign_anomaly_id`('{project_id}', DATE('{target_date}'))
        LIMIT 1
    """
    try:
        query_job = bq_client.query(query)
        rows = await asyncio.to_thread(query_job.result)

        global bq_storage_client
        if not bq_storage_client:
            bq_storage_client = BigQueryReadClient()
            
        dataframe = rows.to_dataframe(bqstorage_client=bq_storage_client)
        return dataframe.to_dict('records')[0] if not dataframe.empty else None
    except Exception as e:
        logging.error(f"Error in get_anomaly_enrichment for {project_id}: {e}", exc_info=True)
        raise

async def get_ai_recommendation(
    project_id,
    target_date,
    service_description,
    project_name,
    distance_from_threshold,
    top_service_list,
):
    """
    Asynchronously calls the generate_ai_recommendation table function.
    """
    global bq_client
    if not bq_client:
        bq_client = bigquery.Client()

    top_service_list_sql = f"'{top_service_list}'" if top_service_list else "''"

    query = f"""
        SELECT
          ai_recommendation
        FROM
          `<project_id>.<dataset>.generate_ai_recommendation` (
            '{project_id}',
            DATE('{target_date}'),
            '{service_description}',
            '{project_name}',
            CAST({distance_from_threshold} AS NUMERIC),
            {top_service_list_sql}
          )
        LIMIT 1
    """
    try:
        query_job = bq_client.query(query)
        rows = await asyncio.to_thread(query_job.result)

        global bq_storage_client
        if not bq_storage_client:
            bq_storage_client = BigQueryReadClient()

        dataframe = rows.to_dataframe(bqstorage_client=bq_storage_client)
        return dataframe.to_dict('records')[0]['ai_recommendation'] if not dataframe.empty else "No recommendation generated."
    except Exception as e:
        logging.error(f"Error in get_ai_recommendation for {project_id}: {e}", exc_info=True)
        raise

async def log_anomaly_bq_async(
    project_id,
    anomaly_date,
    owner,
    slack_recipient_list,
    slack_alert_success,
    alert_suppressed,
    upper_bound,
    total_net_cost,
    absolute_distance_from_threshold,
    absolute_percent_from_threshold,
    anomaly_id,
    most_likely_service,
    ai_recommendation,
):
    """
    Inserts a record into the cost_anomaly_alert_log table.
    Uses asyncio.to_thread to run the synchronous insert.
    """
    global bq_client
    if not bq_client:
        bq_client = bigquery.Client()
        
    table_id = "<project_id>.<dataset>.cost_anomaly_alert_log"
    alert_log_timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    row_to_insert = {
        "alert_log_timestamp": alert_log_timestamp,
        "project_id": project_id,
        "usage_start_date": anomaly_date,
        "owner": ",".join(owner) if isinstance(owner, list) else owner,
        "slack_recipient_list": ",".join(slack_recipient_list) if isinstance(slack_recipient_list, list) else slack_recipient_list,
        "email_alert_success": False, # Placeholder
        "slack_alert_success": slack_alert_success,
        "alert_suppressed": alert_suppressed,
        "expected_spend": round(upper_bound, 4) if upper_bound is not None else None,
        "actual_cost": round(total_net_cost, 4) if total_net_cost is not None else None,
        "absolute_distance_from_threshold": round(absolute_distance_from_threshold, 4) if absolute_distance_from_threshold is not None else None,
        "absolute_percent_from_threshold": round(absolute_percent_from_threshold, 4) if absolute_percent_from_threshold is not None else None,
        "anomaly_id": anomaly_id,
        "most_likely_service": most_likely_service,
        "ai_recommendation": ai_recommendation
    }

    try:
        # Wrap the synchronous call in to_thread
        errors = await asyncio.to_thread(
            bq_client.insert_rows_json, table_id, [row_to_insert]
        )
        if not errors:
            logging.info(f"Successfully logged anomaly {anomaly_id} for project {project_id}.")
            return True
        else:
            logging.error(f"Failed to log anomaly {anomaly_id} for project {project_id}: {errors}")
            return False
    except Exception as e:
        logging.error(f"Exception during log_anomaly_bq for {anomaly_id}: {e}", exc_info=True)
        raise

# --- CORE PROCESSING LOGIC ---

async def process_anomaly(anomaly_details, shared_http_client):
    """
    This function processes a SINGLE anomaly.
    """
    project_id = anomaly_details.get('project_id', 'unknown')
    anomaly_id = "N/A" # Default in case enrichment fails
    
    try:
        target_date = anomaly_details['anomaly_date']

        # --- Step 1: Get enrichment data FIRST ---
        enrichment_data = await get_anomaly_enrichment(project_id, target_date)
        if not enrichment_data:
            return {"error": f"No enrichment data found for {project_id} on {target_date}"}

        anomaly_id = enrichment_data.get('anomaly_id')
        already_alerted = enrichment_data.get('anomaly_already_alerted') == 'Yes'
        most_likely_service = enrichment_data.get('service_description')
        top_service_list = enrichment_data.get('top_service_increase_list')

        # --- Step 2: Check for suppression BEFORE doing expensive work ---
        if already_alerted:
            logging.info(f"Suppressing alert for {anomaly_id}, already alerted.")
            await log_anomaly_bq_async(
                project_id=project_id,
                anomaly_date=target_date,
                owner=anomaly_details.get('owner', 'default@example.com'),
                slack_recipient_list=anomaly_details.get('slack_recipient_list', '#default-channel'),
                slack_alert_success=False, # Not sent
                alert_suppressed=True,     # MARKED AS SUPPRESSED
                upper_bound=anomaly_details.get('forecest_upper_bound'),
                total_net_cost=anomaly_details.get('total_net_cost'),
                absolute_distance_from_threshold=anomaly_details.get('absolute_distance_from_threshold'),
                absolute_percent_from_threshold=anomaly_details.get('absolute_percent_from_threshold'),
                anomaly_id=anomaly_id,
                most_likely_service=most_likely_service,
                ai_recommendation="N/A (Suppressed)" # Save money
            )
            # Return a success response
            return {
                "anomaly_id": anomaly_id,
                "project_id": project_id,
                "slack_alert_success": False,
                "alert_suppressed": True,
                "ai_recommendation": "N/A (Suppressed)"
            }

        # --- Step 3: (Only if not suppressed) Get AI Recommendation ---
        ai_recommendation = await get_ai_recommendation(
            project_id=project_id,
            target_date=target_date,
            service_description=most_likely_service,
            project_name=anomaly_details.get('project_name', 'N/A'),
            distance_from_threshold=anomaly_details.get('absolute_distance_from_threshold', 0),
            top_service_list=top_service_list
        )

        # --- Step 4: (Only if not suppressed) Send Slack Alert ---
        slack_alert_success = await slack_alert.send_slack_alert(
            http_client=shared_http_client,
            slack_recipient_list=anomaly_details.get('slack_recipient_list', '#default-channel'),
            owner=anomaly_details.get('owner', 'default@example.com'),
            anomaly_id=anomaly_id,
            project_name=anomaly_details.get('project_name', 'N/A'),
            project_id=project_id,
            most_likely_service=most_likely_service,
            anomaly_date=target_date,
            total_net_cost=anomaly_details.get('total_net_cost', 0),
            absolute_distance_from_threshold=anomaly_details.get('absolute_distance_from_threshold', 0),
            absolute_percent_from_threshold=anomaly_details.get('absolute_percent_from_threshold', 0),
            ai_recommendation=ai_recommendation
        )

        # --- Step 5: (Only if not suppressed) Log to BigQuery ---
        await log_anomaly_bq_async(
            project_id=project_id,
            anomaly_date=target_date,
            owner=anomaly_details.get('owner', 'default@example.com'),
            slack_recipient_list=anomaly_details.get('slack_recipient_list', '#default-channel'),
            slack_alert_success=slack_alert_success,
            alert_suppressed=False, # Not suppressed
            upper_bound=anomaly_details.get('forecest_upper_bound'),
            total_net_cost=anomaly_details.get('total_net_cost'),
            absolute_distance_from_threshold=anomaly_details.get('absolute_distance_from_threshold'),
            absolute_percent_from_threshold=anomaly_details.get('absolute_percent_from_threshold'),
            anomaly_id=anomaly_id,
            most_likely_service=most_likely_service,
            ai_recommendation=ai_recommendation
        )

        # --- Step 6: Return successful result ---
        return {
            "anomaly_id": anomaly_id,
            "project_id": project_id,
            "slack_alert_success": slack_alert_success,
            "alert_suppressed": False,
            "ai_recommendation": ai_recommendation
        }

    except Exception as e:
        error_message = f"Failed to process {project_id} (Anomaly ID: {anomaly_id}): {e}"
        logging.error(error_message, exc_info=True)
        return {"error": error_message}

# --- [NEW] GROUPED PROCESSING ---

async def process_project_group(project_anomalies, shared_http_client):
    """
    Processes a list of anomalies for a SINGLE project in strict
    chronological order (Oldest -> Newest).
    """
    # Sort by date to ensure strict chronological order
    # This relies on standard YYYY-MM-DD string sorting
    sorted_anomalies = sorted(project_anomalies, key=lambda x: x.get('anomaly_date', ''))
    
    results = []
    for anomaly in sorted_anomalies:
        # [IMPORTANT] We use 'await' here to enforce serial execution within the project.
        # This ensures Day 1 is logged/established before Day 2 starts.
        result = await process_anomaly(anomaly, shared_http_client)
        results.append(result)
        
    return results

# --- ASYNC HANDLER ---

async def async_main_handler(request_json):
    """
    This new async function contains all the logic that was
    previously in the main function.
    It now manages the httpx client lifecycle.
    """
    global bq_client, bq_storage_client
    
    if not bq_client:
        bq_client = bigquery.Client()
    if not bq_storage_client:
        bq_storage_client = BigQueryReadClient()

    logging.info("Async handler invoked.")
    
    # [FIX] Set connection timeout to 30s to avoid the httpx.ConnectTimeout error
    async with httpx.AsyncClient(timeout=30.0) as shared_http_client:
        try:
            if not request_json or 'calls' not in request_json:
                return Response(json.dumps({"errorMessage": "Invalid request format."}), status=400, mimetype='application/json')

            calls = request_json['calls']
            logging.info(f"Received request with {len(calls)} calls.")

            # 1. Group by Project ID
            anomalies_by_project = defaultdict(list)
            for call in calls:
                try:
                    anomaly_details = json.loads(call[0])
                    # Note: We rely on the BQ query to pass 'project_id' and 'anomaly_date'
                    pid = anomaly_details.get('project_id', 'unknown')
                    anomalies_by_project[pid].append(anomaly_details)
                except json.JSONDecodeError:
                    logging.error(f"Failed to decode JSON for call: {call}")
                    continue

            # 2. Create tasks for each PROJECT GROUP (Parallel)
            tasks = []
            for project_id, anomalies in anomalies_by_project.items():
                tasks.append(process_project_group(anomalies, shared_http_client))

            # 3. Run all project groups concurrently
            results_lists = await asyncio.gather(*tasks)
            
            # Flatten the results (list of lists -> single list)
            flat_replies = [item for sublist in results_lists for item in sublist]

            logging.info(f"Processed {len(flat_replies)} anomalies. Returning results.")
            return Response(json.dumps({"replies": flat_replies}), status=200, mimetype='application/json')

        except Exception as e:
            logging.error(f"An unexpected error occurred in async_main_handler: {e}", exc_info=True)
            return Response(json.dumps({"errorMessage": f"An unexpected error occurred: {e}"}), status=400, mimetype='application/json')


# --- SYNCHRONOUS MAIN FUNCTION ---
@functions_framework.http
def main(request):
    """
    Main entry point for the Cloud Run function.
    This synchronous function acts as the bridge to the async logic.
    """
    logging.info("Synchronous function invoked.")
    request_json = request.get_json(silent=True)
    
    return asyncio.run(async_main_handler(request_json))
