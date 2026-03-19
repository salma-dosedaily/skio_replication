import ssl
import json
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.util.ssl_ import create_urllib3_context
from tabulate import tabulate


class _SSLAdapter(HTTPAdapter):
    """Handles SSL EOF errors seen in Cloud Functions with Python 3.12."""
    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        ctx.load_default_certs()
        ctx.options |= ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3
        kwargs["ssl_context"] = ctx
        super().init_poolmanager(*args, **kwargs)

class SlackReporter:
    def __init__(self, webhook_url, logger=None):
        self.webhook_url = webhook_url
        self.logger = logger or logging.getLogger(__name__)

    def send_report(self, sync_results, dq_results):
        """
        Formats and sends a combined Sync + Data Quality report to Slack.
        """
        if not self.webhook_url:
            self.logger.warning("No Slack Webhook URL provided. Skipping report.")
            return

        # 1. Filter Sync Errors
        sync_fails = [r for r in sync_results if r['status'] == 'ERROR']
        status_emoji = "🚨" if sync_fails else "✅"

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{status_emoji} Skio Pipeline Report",
                    "emoji": True
                }
            },
            {"type": "divider"}
        ]

        # 2. Add Sync Errors (if any)
        if sync_fails:
            error_text = "\n".join([f"• *{f['table']}*: {f['message']}" for f in sync_fails[:10]])
            if len(sync_fails) > 10: error_text += f"\n...and {len(sync_fails)-10} more."
            
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*❌ Sync Errors ({len(sync_fails)})*\n{error_text}"}
            })

        # 3. Add Tabular Data Quality Report
        if dq_results:
            # Create a pretty ASCII table
            table_str = tabulate(dq_results, headers="keys", tablefmt="simple")
            
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*📊 Data Quality Summary*"}
            })
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"```\n{table_str}\n```"}
            })

        # 4. Send to Slack
        try:
            self.logger.info("Sending Slack report (blocks=%d)", len(blocks))
            retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
            session = requests.Session()
            session.mount("https://", _SSLAdapter(max_retries=retry))
            response = session.post(
                self.webhook_url,
                data=json.dumps({"blocks": blocks}),
                headers={"Content-Type": "application/json"},
                timeout=15,
            )
            if response.status_code != 200:
                self.logger.error("Slack webhook returned %s: %s", response.status_code, response.text)
            else:
                self.logger.info("Slack report sent successfully.")
        except Exception as e:
            self.logger.exception("Error sending Slack alert: %s", e)