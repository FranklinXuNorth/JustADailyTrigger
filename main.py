import os
import time
import requests

# Configuration
POSTHOG_BASE_URL = os.environ.get("POSTHOG_BASE_URL", "https://us.i.posthog.com").rstrip("/")
POSTHOG_PROJECT_ID = os.environ.get("POSTHOG_PROJECT_ID")
POSTHOG_PROJECT_API_KEY = os.environ.get("POSTHOG_PROJECT_API_KEY")
POSTHOG_PROJECT_TOKEN = os.environ.get("POSTHOG_PROJECT_TOKEN")

# Fixed event name
POSTHOG_EVENT_NAME = "[Report] Analysis Completed"

# Optional settings
POSTHOG_LOOKBACK_DAYS = int(os.environ.get("POSTHOG_LOOKBACK_DAYS", "3650"))
WRITEBACK_SLEEP_SECONDS = float(os.environ.get("WRITEBACK_SLEEP_SECONDS", "0.05"))
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"


def validate_env():
    """Validate required environment variables."""
    missing = []
    if not POSTHOG_PROJECT_ID:
        missing.append("POSTHOG_PROJECT_ID")
    if not POSTHOG_PROJECT_API_KEY:
        missing.append("POSTHOG_PROJECT_API_KEY")
    if not POSTHOG_PROJECT_TOKEN:
        missing.append("POSTHOG_PROJECT_TOKEN")

    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")


def query_posthog_completion_counts():
    """
    Query PostHog for per-user total completion counts.

    This uses the Query API with HogQL and groups by distinct_id.
    """
    url = f"{POSTHOG_BASE_URL}/api/projects/{POSTHOG_PROJECT_ID}/query/"

    headers = {
        "Authorization": f"Bearer {POSTHOG_PROJECT_API_KEY}",
        "Content-Type": "application/json",
    }

    hogql = f"""
    SELECT
        distinct_id,
        count() AS completion_total
    FROM events
    WHERE event = '{POSTHOG_EVENT_NAME}'
      AND timestamp >= now() - INTERVAL {POSTHOG_LOOKBACK_DAYS} DAY
      AND distinct_id IS NOT NULL
    GROUP BY distinct_id
    ORDER BY completion_total DESC, distinct_id ASC
    """

    payload = {
        "query": {
            "kind": "HogQLQuery",
            "query": hogql
        }
    }

    response = requests.post(url, headers=headers, json=payload, timeout=120)
    response.raise_for_status()
    data = response.json()

    # Query API commonly returns tabular data as columns + results
    columns = data.get("columns")
    results = data.get("results")

    if columns is None or results is None:
        # Some responses may nest the result
        nested = data.get("result", {})
        columns = nested.get("columns")
        results = nested.get("results")

    if not columns or results is None:
        raise RuntimeError(f"Unexpected Query API response shape: {data}")

    rows = []
    for row in results:
        if isinstance(row, list):
            row_dict = {columns[i]: row[i] for i in range(len(columns))}
        elif isinstance(row, dict):
            row_dict = row
        else:
            continue

        distinct_id = row_dict.get("distinct_id")
        completion_total = row_dict.get("completion_total")

        if distinct_id is None or completion_total is None:
            continue

        rows.append({
            "distinct_id": str(distinct_id),
            "completion_total": int(completion_total),
        })

    return rows


def compute_percentiles(user_rows):
    """
    Compute percentile rank.

    Logic:
    - Sort ascending by completion_total
    - Map rank position to 0..100 percentile
    """
    if not user_rows:
        return []

    # Sort ascending for percentile assignment
    sorted_rows = sorted(
        user_rows,
        key=lambda x: (x["completion_total"], x["distinct_id"])
    )

    n = len(sorted_rows)

    if n == 1:
        single = sorted_rows[0].copy()
        single["percentile"] = 100.0
        return [single]

    computed = []
    for idx, row in enumerate(sorted_rows):
        percentile = (idx / (n - 1)) * 100.0
        computed.append({
            "distinct_id": row["distinct_id"],
            "percentile": round(percentile, 2),
        })

    # Sort back descending for preview/logging
    computed.sort(key=lambda x: (-x["percentile"], x["distinct_id"]))
    return computed


def write_person_property(distinct_id, percentile):
    """
    Write person property back to PostHog using a $set event.
    Only writes: ai_completed_percentile
    """
    url = f"{POSTHOG_BASE_URL}/i/v0/e/"

    payload = {
        "api_key": POSTHOG_PROJECT_TOKEN,
        "event": "$set",
        "distinct_id": distinct_id,
        "properties": {
            "$set": {
                "ai_completed_percentile": percentile
            }
        }
    }

    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()


def write_back_properties(computed_rows):
    """Write all computed rows back to PostHog."""
    total = len(computed_rows)
    success = 0
    failures = 0

    for idx, row in enumerate(computed_rows, start=1):
        try:
            if DRY_RUN:
                print(
                    f"[DRY RUN] distinct_id={row['distinct_id']}, "
                    f"percentile={row['percentile']}"
                )
            else:
                write_person_property(
                    distinct_id=row["distinct_id"],
                    percentile=row["percentile"],
                )

            success += 1

        except Exception as e:
            failures += 1
            print(
                f" Failed to update distinct_id={row['distinct_id']}: {e}"
            )

        if WRITEBACK_SLEEP_SECONDS > 0:
            time.sleep(WRITEBACK_SLEEP_SECONDS)

        if idx % 100 == 0 or idx == total:
            print(f" Progress: {idx}/{total} processed")

    return success, failures


def main():
    """
    Main job entry:
    1. Fetch user completion counts from PostHog
    2. Compute percentile ranks
    3. Write ai_completed_percentile property back to PostHog
    """
    print(" Start refreshing PostHog ai_completed_percentile...")

    validate_env()

    # Step 1: Fetch raw aggregated data from PostHog
    user_rows = query_posthog_completion_counts()

    if not user_rows:
        print(" No valid completion data found. Job finished.")
        return

    print(f" Users fetched from PostHog: {len(user_rows)}")

    # Step 2: Compute percentile rank
    computed_rows = compute_percentiles(user_rows)

    heavy_user_count = sum(1 for row in computed_rows if row["percentile"] >= 75.0)
    print(f" Heavy users (>=75th percentile): {heavy_user_count}/{len(computed_rows)}")

    # Preview top users
    print("\n Preview of top users:")
    for row in computed_rows[:10]:
        print(
            f'- user={row["distinct_id"]}, '
            f'percentile={row["percentile"]}'
        )

    # Step 3: Write person property back to PostHog
    success_count, failure_count = write_back_properties(computed_rows)

    print("\n Job finished.")
    print(f" Successful updates: {success_count}")
    print(f" Failed updates: {failure_count}")
    print(f" Heavy users (>=75th): {heavy_user_count}")


if __name__ == "__main__":
    main()
