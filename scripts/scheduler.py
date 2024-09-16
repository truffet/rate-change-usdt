from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import subprocess
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the main task (simulates the execution of the Python script)
def run_script():
    try:
        # Get the directory of the main Python script
        script_path = os.path.abspath('main.py')

        # Define the command to run the main.py script
        command = f"python3 {script_path}"

        # Run the command using subprocess
        logging.info(f"Running script: {command}")
        result = subprocess.run(command, shell=True, capture_output=True, text=True)

        if result.returncode == 0:
            logging.info("Script executed successfully.")
        else:
            logging.error(f"Script execution failed: {result.stderr}")

    except Exception as e:
        logging.error(f"Error running the script: {e}")

# Set up the APScheduler scheduler
scheduler = BlockingScheduler()

# Define a cron job that runs every day at 4:05, 8:05, 12:05, 16:05, 20:05, and 00:05 UTC
scheduler.add_job(run_script, CronTrigger(hour='4,8,12,16,20,0', minute=5, timezone='UTC'))

# Start the scheduler
if __name__ == "__main__":
    logging.info("Starting the scheduler...")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scheduler stopped.")
