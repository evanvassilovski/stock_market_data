from dagster import Definitions
from jobs import pipeline
from pipeline_sched import daily_schedule

defs = Definitions(
    jobs=[pipeline],
    schedules=[daily_schedule],
)