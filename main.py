import os
import re
import time
import requests

# Configuration
POSTHOG_BASE_URL = os.environ.get("POSTHOG_BASE_URL", "https://us.i.posthog.com").rstrip("/")
POSTHOG_PROJECT_ID = os.environ.get("POSTHOG_PROJECT_ID")
POSTHOG_PROJECT_API_KEY = os.environ.get("POSTHOG_PROJECT_API_KEY")
POSTHOG_PROJECT_TOKEN = os.environ.get("POSTHOG_PROJECT_TOKEN")

# Event names to track - add your events here
POSTHOG_EVENT_NAMES = [
    "[Report] Analysis Completed",
    "Application Opened",
    "[Capture] Video Recording Completed",
    "[Feedback] Praised Card",
    # Add more events here, e.g.:
    # "[Dashboard] Viewed",
    # "[Export] Completed",
]

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


def event_name_to_property_name(event_name: str) -> str:
    """
    Convert event name to property name slug.
    Example: "[Report] Analysis Completed" -> "report-analysis-completed-percentile"
    """
    # Remove brackets, lowercase, replace spaces/special chars with hyphens
    slug = event_name.lower()
    slug = re.sub(r'[\[\]]', '', slug)  # Remove brackets
    slug = re.sub(r'[^\w\s-]', '', slug)  # Remove special chars except spaces and hyphens
    slug = re.sub(r'[\s_]+', '-', slug)  # Replace spaces/underscores with hyphens
    slug = slug.strip('-')
    return f"{slug}-percentile"


def query_posthog_event_counts(event_name: str):
    """
    Query PostHog for per-user total counts for a specific event.
    """
    url = f"{POSTHOG_BASE_URL}/api/projects/{POSTHOG_PROJECT_ID}/query/"

    headers = {
        "Authorization": f"Bearer {POSTHOG_PROJECT_API_KEY}",
        "Content-Type": "application/json",
    }

    # Escape single quotes in event name for HogQL
    escaped_event = event_name.replace("'", "''")

    hogql = f"""
    SELECT
        distinct_id,
        count() AS event_total
    FROM events
    WHERE event = '{escaped_event}'
      AND timestamp >= now() - INTERVAL {POSTHOG_LOOKBACK_DAYS} DAY
      AND distinct_id IS NOT NULL
    GROUP BY distinct_id
    ORDER BY event_total DESC, distinct_id ASC
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

    columns = data.get("columns")
    results = data.get("results")

    if columns is None or results is None:
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
        event_total = row_dict.get("event_total")

        if distinct_id is None or event_total is None:
            continue

        rows.append({
            "distinct_id": str(distinct_id),
            "event_total": int(event_total),
        })

    return rows


def compute_percentiles(user_rows):
    """
    Compute percentile rank.
    """
    if not user_rows:
        return []

    sorted_rows = sorted(
        user_rows,
        key=lambda x: (x["event_total"], x["distinct_id"])
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

    computed.sort(key=lambda x: (-x["percentile"], x["distinct_id"]))
    return computed


def write_person_property(distinct_id, property_name, percentile):
    """
    Write a single person property back to PostHog using a $set event.
    """
    url = f"{POSTHOG_BASE_URL}/i/v0/e/"

    payload = {
        "api_key": POSTHOG_PROJECT_TOKEN,
        "event": "$set",
        "distinct_id": distinct_id,
        "properties": {
            "$set": {
                property_name: percentile
            }
        }
    }

    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()


def write_back_properties(event_name, property_name, computed_rows):
    """Write all computed rows back to PostHog."""
    total = len(computed_rows)
    success = 0
    failures = 0

    for idx, row in enumerate(computed_rows, start=1):
        try:
            if DRY_RUN:
                print(
                    f"[DRY RUN] {event_name} | distinct_id={row['distinct_id']}, "
                    f"percentile={row['percentile']}"
                )
            else:
                write_person_property(
                    distinct_id=row["distinct_id"],
                    property_name=property_name,
                    percentile=row["percentile"],
                )

            success += 1

        except Exception as e:
            failures += 1
            print(
                f" Failed to update {event_name} | distinct_id={row['distinct_id']}: {e}"
            )

        if WRITEBACK_SLEEP_SECONDS > 0:
            time.sleep(WRITEBACK_SLEEP_SECONDS)

        if idx % 100 == 0 or idx == total:
            print(f"   Progress: {idx}/{total} processed")

    return success, failures


def main():
    """
    Main job entry:
    1. For each event name, fetch user counts from PostHog
    2. Compute percentile ranks
    3. Write dynamic property names back to PostHog
    """
    print(" Start refreshing PostHog percentile properties...")

    validate_env()

    if not POSTHOG_EVENT_NAMES:
        print(" No event names configured. Add events to POSTHOG_EVENT_NAMES.")
        return

    total_success = 0
    total_failure = 0

    for event_name in POSTHOG_EVENT_NAMES:
        property_name = event_name_to_property_name(event_name)
        print(f"\n{'='*60}")
        print(f" Processing event: {event_name}")
        print(f" Property name: {property_name}")
        print(f"{'='*60}")

        # Step 1: Fetch raw aggregated data from PostHog
        user_rows = query_posthog_event_counts(event_name)

        if not user_rows:
            raise RuntimeError(f"Event '{event_name}' not found or has no data. Please check the event name in PostHog.")

        print(f"   Users fetched: {len(user_rows)}")

        # Step 2: Compute percentile rank
        computed_rows = compute_percentiles(user_rows)

        heavy_user_count = sum(1 for row in computed_rows if row["percentile"] >= 75.0)
        print(f"   Heavy users (>=75th): {heavy_user_count}/{len(computed_rows)}")

        # Preview top 25% users
        top_25_percent_count = max(1, len(computed_rows) // 4)
        print(f"\n   Preview of top 25% users (showing {top_25_percent_count}):")
        for row in computed_rows[:top_25_percent_count]:
            print(
                f'   - user={row["distinct_id"]}, '
                f'percentile={row["percentile"]}'
            )

        # Step 3: Write person property back to PostHog
        print(f"\n   Preparing to write property: {property_name} ({len(computed_rows)} users)")
        if DRY_RUN:
            print("   [DRY RUN] Skipping actual writes")
        success_count, failure_count = write_back_properties(
            event_name, property_name, computed_rows
        )

        total_success += success_count
        total_failure += failure_count

        print(f"\n   Finished {event_name}")
        print(f"   Successful: {success_count}, Failed: {failure_count}")

    print(f"\n{'='*60}")
    print(" All jobs finished.")
    print(f" Total successful updates: {total_success}")
    print(f" Total failed updates: {total_failure}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
