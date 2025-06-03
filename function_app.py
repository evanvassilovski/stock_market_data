import azure.functions as func
import datetime
import json
import logging
from jobs import pipeline

app = func.FunctionApp()

@app.timer_trigger(schedule="0 30 1 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=True) 
def run_pipeline(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

    pipeline.execute_in_process()