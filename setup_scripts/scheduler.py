from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import subprocess
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the main task with a timeframe argument
def run_script(timeframe):
    try:
        # Get the directory of the main Python script
        script_path = os.path.abspath('main.py')

        # Define the command to run the main.py script with the given timeframe argument
        command = f"python3 {script_path} {timeframe}"

        # Run the command using subprocess
        logging.info(f"Running script: {command}")
        result = subprocess.run(command, shell=True, capture_output=True, text=True)

        if result.returncode == 0:
            logging.info(f"Script executed successfully for timeframe '{timeframe}'.")
        else:
            logging.error(f"Script execution failed for timeframe '{timeframe}': {result.stderr}")

    except Exception as e:
        logging.error(f"Error running the script for timeframe '{timeframe}': {e}")

# Set up the APScheduler scheduler
scheduler = BlockingScheduler()

# Define a cron job for the '4h' timeframe (runs every 4 hours at 4:05, 8:05, 12:05, 16:05, 20:05, and 00:05 UTC)
scheduler.add_job(run_script, CronTrigger(hour='4,8,12,16,20,0', minute=5, timezone='UTC'), args=['4h'])

# Add cron job for the 'd' timeframe (daily job, runs just after the day is completed at 00:15= UTC)
scheduler.add_job(run_script, CronTrigger(hour=0, minute=15, timezone='UTC'), args=['d'])

# Add cron job for the 'w' timeframe (weekly job, runs just after the week is completed at 00:25 UTC on Monday)
scheduler.add_job(run_script, CronTrigger(day_of_week='mon', hour=0, minute=25, timezone='UTC'), args=['w'])

# Start the scheduler
if __name__ == "__main__":
    logging.info("Starting the scheduler...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scheduler stopped.")
