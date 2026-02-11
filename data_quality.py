from google.cloud import bigquery

class QualityChecker:
    def __init__(self, bq_client):
        self.client = bq_client
        self.results = []

    def run_check(self, table_id, pk_col, created_col=None, updated_col=None):
        """
        Runs data quality checks. 
        Freshness Logic: Calculates hours since the LATEST of (created_col, updated_col).
        """
        table_name = table_id.split('.')[-1]
        
        # 1. Duplicate Check Logic
        if pk_col:
            dup_logic = f"COUNT(*) - COUNT(DISTINCT `{pk_col}`)"
        else:
            dup_logic = "0"

        # 2. Freshness Check Logic
        # We look for the maximum timestamp. If both Created and Updated exist, 
        # we take the GREATEST of the two max values.
        fresh_logic = "NULL"
        
        # Helper to safely cast string timestamps to TIMESTAMP
        def safe_ts(col): return f"SAFE_CAST(`{col}` AS TIMESTAMP)"

        if created_col and updated_col:
            # Handle cases where one column might be null for some rows
            ts_expr = f"GREATEST(COALESCE(MAX({safe_ts(created_col)}), '1970-01-01'), COALESCE(MAX({safe_ts(updated_col)}), '1970-01-01'))"
            fresh_logic = f"TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), {ts_expr}, HOUR)"
        elif created_col:
            fresh_logic = f"TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX({safe_ts(created_col)}), HOUR)"
        elif updated_col:
            fresh_logic = f"TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX({safe_ts(updated_col)}), HOUR)"

        query = f"""
            SELECT 
                COUNT(*) as total_rows,
                {dup_logic} as duplicate_count,
                {fresh_logic} as freshness_hours
            FROM `{table_id}`
        """

        try:
            job = self.client.query(query)
            row = list(job.result())[0]

            total_rows = row.total_rows
            duplicates = row.duplicate_count
            freshness = row.freshness_hours

            # Determine Status
            status = "✅ PASS"
            if duplicates > 0:
                status = "🔴 DUPES"
            elif total_rows == 0:
                status = "⚠️ EMPTY"
                freshness = None  # <--- ADD THIS LINE (Hides the 491k hours)
            
            # Optional: Warning for stale data (e.g., > 48 hours old)
            # You can remove this if 50+ hours is normal for your cohorts
            # if freshness is not None and freshness > 48:
            #      status = "⚠️ STALE"

            self.results.append({
                "Table": table_name,
                "Rows": f"{total_rows:,}",
                "Dupes": duplicates,
                "Fresh(Hr)": freshness if freshness is not None else "-",
                "Status": status
            })

        except Exception as e:
            # Shorten error message for cleanliness in report
            err_msg = str(e).splitlines()[0][:50] + "..." if len(str(e)) > 50 else str(e)
            print(f"❌ DQ Check failed for {table_name}: {err_msg}")
            self.results.append({
                "Table": table_name,
                "Rows": "ERR",
                "Dupes": "-",
                "Fresh(Hr)": "-",
                "Status": "❌ ERR"
            })

    def get_results(self):
        return self.results