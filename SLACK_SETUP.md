# Slack Report Setup (Cloud Function)

If the scheduler runs successfully but **no Slack message is sent**, check the following.

## 1. Secret Manager

The webhook URL is read from **Secret Manager**. The code uses:

- **Project:** `DEST_PROJECT` (env: `DEST_PROJECT`, default: `dosedaily-raw`)
- **Secret name:** `SECRET_NAME` (env: `SECRET_NAME`, default: `SLACK_WEBHOOK_URL`)

**Do this:**

1. In **Google Cloud Console** → **Secret Manager**, confirm that a secret named **`SLACK_WEBHOOK_URL`** exists in the **same project** where the Cloud Function runs (or in `DEST_PROJECT` if you set it).
2. The secret’s value must be the **full Slack Incoming Webhook URL** (starts with `https://hooks.slack.com/services/...`).
3. Grant the **Cloud Function’s service account** access to that secret:
   - Open the secret → **Permissions** → Add principal.
   - Principal: the function’s service account (e.g. `PROJECT_ID@appspot.gserviceaccount.com` or your custom SA).
   - Role: **Secret Manager Secret Accessor**.

## 2. Cloud Function logs

After the next scheduled run:

1. In **Cloud Console** → **Cloud Functions** → your function → **Logs**.
2. Look for:
   - `"Fetching Slack webhook from Secret Manager: project=..., secret=..."`
   - `"Secret 'SLACK_WEBHOOK_URL' fetched successfully"` → secret is OK.
   - `"Could not fetch secret ..."` or `"Slack webhook is missing"` → secret missing or no permission.
   - `"Sending Slack report"` / `"Slack report sent successfully"` → Slack call ran.
   - `"Slack webhook returned 4xx/5xx"` or `"Error sending Slack alert"` → webhook URL or network issue.

## 3. Environment variables (optional)

If your secret is in another project or has a different name, set on the Cloud Function:

- `SECRET_NAME` – Secret Manager secret name (default: `SLACK_WEBHOOK_URL`).
- `DEST_PROJECT` – GCP project used for BigQuery and for **fetching the secret** (default: `dosedaily-raw`).

Redeploy the function after changing env vars.

## 4. Quick checklist

- [ ] Secret `SLACK_WEBHOOK_URL` exists in the correct GCP project.
- [ ] Secret value is the full Slack webhook URL.
- [ ] Function’s service account has **Secret Manager Secret Accessor** on that secret.
- [ ] No “dry_run” in the scheduler URL (or report block is skipped in dry run).
- [ ] Check function logs after a run to see which of the messages above appears.
