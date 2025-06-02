from dagster import ScheduleDefinition
from jobs import pipeline

def daily_config():
    return {
        "ops": {
            "load_yf": {"config": {"table_name": "yf_data"}},
            "load_fred": {"config": {"table_name": "fred_data"}},
        }
    }

daily_schedule = ScheduleDefinition(
    job=pipeline,
    cron_schedule="30 21 * * *",  # Runs every day at 3 AM
    execution_timezone="US/Eastern",  # Eastern time zone
    run_config=daily_config()  # ← THIS is what was missing
)