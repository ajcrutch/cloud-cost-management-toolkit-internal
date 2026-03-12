import os
import httpx  # Use httpx for async requests
import logging
import json
import asyncio # For retries
from httpx import ConnectError, TimeoutException # For specific error handling

# Function to format a number as a currency string
def format_currency(value):
    try:
        # Use comma as thousands separator and format to two decimal places
        return f"${value:,.2f}"
    except (TypeError, ValueError):
        return "$0.00"

# Function to format a number as a percentage string
def format_percent(value):
    try:
        # Convert absolute decimal (e.g., 0.67) to percent (e.g., 67.0%)
        return f"{value:.1%}"
    except (TypeError, ValueError):
        return "0.0%"

async def send_slack_alert(
    http_client: httpx.AsyncClient, # Pass in the shared async client
    slack_recipient_list: str,
    owner: str,
    anomaly_id: str,
    project_name: str,
    project_id: str,
    most_likely_service: str,
    anomaly_date: str,
    total_net_cost: float,
    absolute_distance_from_threshold: float,
    absolute_percent_from_threshold: float,
    ai_recommendation: str
):
    """
    Sends a formatted alert to a Slack webhook URL using httpx with retries.
    """
    webhook_url = os.environ.get("aic-costbot-slack-channel-webhook")
    if not webhook_url:
        logging.error("Slack webhook URL is not set. Cannot send alert.")
        return False

    # Sanitize the AI recommendation: Slack mrkdwn uses \n, not <br>
    if ai_recommendation:
        # Replace the HTML <br> tags the LLM was asked to use with Slack's newline character.
        sanitized_recommendation = ai_recommendation.replace("<br>", "\n")
    else:
        sanitized_recommendation = "No recommendation was generated."

    # Format the numbers for display
    total_net_cost_str = format_currency(total_net_cost)
    distance_str = format_currency(absolute_distance_from_threshold)
    percent_str = format_percent(absolute_percent_from_threshold)

    # Construct the Slack message payload using blocks
    message_payload = {
        "text": f"Cost Anomaly Detected in {project_name}", # Fallback for notifications
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f":warning: Cost Anomaly Detected: {project_name}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Project Name:*\n`{project_name}`"},
                    {"type": "mrkdwn", "text": f"*Project ID:*\n`{project_id}`"},
                    {"type": "mrkdwn", "text": f"*Anomaly Date:*\n{anomaly_date}"},
                    {"type": "mrkdwn", "text": f"*Owner:*\n`{owner}`"},
                    {"type": "mrkdwn", "text": f"*Total Net Cost:*\n{total_net_cost_str}"},
                    {"type": "mrkdwn", "text": f"*Increase Over Expected:*\n{distance_str} ({percent_str})"},
                    {"type": "mrkdwn", "text": f"*Most Likely Service:*\n{most_likely_service}"},
                    {"type": "mrkdwn", "text": f"*Anomaly ID:*\n`{anomaly_id}`"}
                ]
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*AI-Generated Recommendation:*\n" + sanitized_recommendation
                }
            }
        ]
    }

    # --- Retry Logic (for connection failures) ---
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            # Send the request asynchronously using the passed client
            response = await http_client.post(webhook_url, json=message_payload)

            # Check if the request was successful
            if response.status_code == 200:
                logging.info(f"Successfully sent Slack alert for anomaly {anomaly_id}.")
                return True
            else:
                # Log the error response from Slack
                logging.error(f"Slack API request failed for {anomaly_id} (Attempt {attempt+1}/{MAX_RETRIES}): {response.status_code} - {response.text}")
                # Don't retry on non-transient errors (like 400 Bad Request)
                if 400 <= response.status_code < 500:
                    break
        
        # Catch transient network/connection errors
        except (ConnectError, TimeoutException) as e:
            logging.warning(f"Transient connection error for {anomaly_id} (Attempt {attempt+1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                # Exponential backoff: 2s, 4s
                delay = 2 ** (attempt + 1)
                await asyncio.sleep(delay)
            else:
                # Log final failure
                logging.error(f"Failed to send Slack alert for {anomaly_id} after {MAX_RETRIES} attempts due to connection timeout.")
                return False
        except Exception as e:
            # Catch other unexpected errors
            logging.error(f"Unexpected error during Slack send for {anomaly_id}: {e}", exc_info=True)
            return False

    return False
