from airflow.hooks.base import BaseHook
import requests

def notify_slack():
    conn = BaseHook.get_connection('slack_webhook')
    webhook = conn.host
    requests.post(webhook, json={"text": "Daily ETL pipeline completed successfully!"})
