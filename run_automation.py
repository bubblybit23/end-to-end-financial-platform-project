import yaml
import subprocess
import time
import logging
from datetime import datetime
import os
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
import sys  # Import the sys module

load_dotenv()

# --- Logging Configuration ---
LOG_FILE = os.getenv("LOG_FILE", "automation.log")
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Email Configuration (Placeholders in .env) ---
EMAIL_CONFIG = {
    "smtp_server": os.getenv("SMTP_SERVER"),
    "smtp_port": os.getenv("SMTP_PORT", 587),
    "smtp_username": os.getenv("SMTP_USERNAME"),
    "smtp_password": os.getenv("SMTP_PASSWORD"),
    "sender_email": os.getenv("SENDER_EMAIL"),
    "receiver_email": os.getenv("RECEIVER_EMAIL"),
}

# --- Load Configuration from YAML ---
try:
    with open("automation_config.yaml", "r") as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
    logging.error("Error: automation_config.yaml not found.")
    exit(1)
except yaml.YAMLError as e:
    logging.error(f"Error parsing automation_config.yaml: {e}")
    exit(1)

def send_failure_email(script_name, error_message):
    """Sends an email notification about a script failure."""
    if all(EMAIL_CONFIG.values()):
        try:
            server = smtplib.SMTP(EMAIL_CONFIG["smtp_server"], EMAIL_CONFIG["smtp_port"])
            server.starttls()
            server.login(EMAIL_CONFIG["smtp_username"], EMAIL_CONFIG["smtp_password"])

            msg = MIMEText(f"The script '{script_name}' in the financial data pipeline failed.\n\nError Message:\n{error_message}\n\nCheck the automation.log for more details.")
            msg['Subject'] = f"Automation Failure: {script_name}"
            msg['From'] = EMAIL_CONFIG["sender_email"]
            msg['To'] = EMAIL_CONFIG["receiver_email"]

            server.sendmail(EMAIL_CONFIG["sender_email"], [EMAIL_CONFIG["receiver_email"]], msg.as_string())
            server.quit()
            logging.info(f"Failure email sent for {script_name}.")
        except Exception as e:
            logging.error(f"Error sending failure email for {script_name}: {e}")
    else:
        logging.warning("Email configuration not fully set in .env. Skipping failure email.")

def run_script(script_config):
    """Runs a single script using subprocess and logs details."""
    name = script_config.get("name", "Unnamed Script")
    path = script_config.get("path")

    if not path or not os.path.exists(path):
        logging.error(f"Error: Script path not found for {name}: {path}")
        return False

    logging.info(f"--- Starting execution of: {name} ---")
    try:
        # Use sys.executable to ensure the subprocess uses the same Python interpreter
        process = subprocess.Popen([sys.executable, path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        if process.returncode == 0:
            logging.info(f"{name} executed successfully.")
            if stdout:
                logging.debug(f"{name} stdout:\n{stdout.decode()}")
            return True
        else:
            error_detail = f"Stdout:\n{stdout.decode()}\nStderr:\n{stderr.decode()}"
            logging.error(f"{name} failed with return code {process.returncode}.\nDetails:\n{error_detail}")
            send_failure_email(name, error_detail)
            return False
    except Exception as e:
        logging.error(f"Error executing {name}: {e}")
        send_failure_email(name, str(e))
        return False

if __name__ == "__main__":
    if config.get("schedule", {}).get("frequency") == "daily":
        run_time_str = config.get("schedule", {}).get("time", "00:00")
        try:
            run_time = datetime.strptime(run_time_str, "%H:%M").time()
        except ValueError:
            logging.error("Error: Invalid time format in automation_config.yaml. Use HH:MM.")
            exit(1)

        while True:
            now = datetime.now().time()
            # Check if the current time matches the scheduled run time
            if now.hour == run_time.hour and now.minute == run_time.minute:
                logging.info("--- Starting daily automation run ---")
                success = True
                for script in config.get("scripts", []):
                    if success:
                        if run_script(script):
                            delay = script.get("delay_after", 0)
                            logging.info(f"Waiting for {delay} seconds before next script.")
                            time.sleep(delay)  # Delay is in seconds now, based on your schedule
                        else:
                            success = False
                            logging.error("Automation run stopped due to a script failure.")
                    else:
                        logging.info("Skipping subsequent scripts due to previous failure.")
                        break
                logging.info("--- Daily automation run finished ---")
                # Wait for the next day
                now = datetime.now()
                tomorrow = datetime(now.year, now.month, now.day + 1, run_time.hour, run_time.minute)
                wait_seconds = (tomorrow - now).total_seconds()
                logging.info(f"Waiting for {wait_seconds / 3600:.2f} hours until the next run.")
                time.sleep(wait_seconds)
            else:
                time.sleep(60)  # Check every minute
    else:
        logging.warning("Schedule frequency not set to 'daily'. Script will exit.")