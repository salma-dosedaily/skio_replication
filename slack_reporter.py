import requests
import json
from tabulate import tabulate

class SlackReporter:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    def send_report(self, sync_results, dq_results):
        """
        Formats and sends a combined Sync + Data Quality report to Slack.
        """
        if not self.webhook_url:
            print("⚠️ No Slack Webhook URL provided. Skipping report.")
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
            response = requests.post(
                self.webhook_url, 
                data=json.dumps({"blocks": blocks}),
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            if response.status_code != 200:
                print(f"❌ Failed to send Slack alert: {response.text}")
            else:
                print("✅ Slack report sent successfully.")
        except Exception as e:
            print(f"❌ Error sending Slack alert: {e}")