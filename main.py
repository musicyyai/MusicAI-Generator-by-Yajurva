import os
import json
import time
import logging
import sys
import requests # For making HTTP requests (e.g., Telegram, maybe others)
import subprocess # For running shell commands (like Kaggle CLI, ffmpeg/sox if needed locally)
from datetime import datetime # For timestamps
# Add these below existing imports
from utils import load_state, save_state, authenticate_gdrive, upload_to_gdrive # Import utils functions
from config import GDRIVE_BACKUP_FOLDER_ID # Import the folder ID

# --- Logging Configuration ---
LOG_FILE_PATH = "system_log.txt"
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
# Note: Using filename and lineno can be slightly slower, remove if performance is critical

logging.basicConfig(
    level=logging.INFO, # Set the minimum level of messages to record (e.g., INFO, WARNING, ERROR)
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, encoding='utf-8'), # Log to file
        logging.StreamHandler(sys.stdout) # Also log to console (stdout)
    ]
)

logging.info("Logging configured.") # First message using the configured logger

# --- Load Secrets from Environment Variables ---
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
GOOGLE_CREDS_JSON_STR = os.environ.get('GOOGLE_CREDS_JSON') # Load as string first
KAGGLE_JSON_1_STR = os.environ.get('KAGGLE_JSON_1')
KAGGLE_JSON_2_STR = os.environ.get('KAGGLE_JSON_2')
KAGGLE_JSON_3_STR = os.environ.get('KAGGLE_JSON_3')
KAGGLE_JSON_4_STR = os.environ.get('KAGGLE_JSON_4')

# --- Validate Secrets ---
if not TELEGRAM_BOT_TOKEN:
    logging.critical("CRITICAL ERROR: Telegram Bot Token not found in Replit Secrets.")
    sys.exit(1) # Exit script with an error code

if not TELEGRAM_CHAT_ID:
    logging.critical("CRITICAL ERROR: Telegram Chat ID not found in Replit Secrets.")
    sys.exit(1)

if not GOOGLE_CREDS_JSON_STR:
    logging.critical("CRITICAL ERROR: Google Drive Credentials JSON not found in Replit Secrets.")
    sys.exit(1)

# Store Kaggle credentials in a list for easier access by index
KAGGLE_CREDENTIALS_LIST = [
    KAGGLE_JSON_1_STR,
    KAGGLE_JSON_2_STR,
    KAGGLE_JSON_3_STR,
    KAGGLE_JSON_4_STR
]

# Check if at least one Kaggle credential was loaded
if not any(KAGGLE_CREDENTIALS_LIST):
     logging.critical("CRITICAL ERROR: No Kaggle API credentials found in Replit Secrets (KAGGLE_JSON_1 to KAGGLE_JSON_4).")
     sys.exit(1)

# Optional: Check if ALL expected Kaggle credentials were loaded (stricter)
if None in KAGGLE_CREDENTIALS_LIST:
    logging.warning("One or more Kaggle API credentials (KAGGLE_JSON_1 to KAGGLE_JSON_4) are missing. Rotation might fail.")
    # We don't exit here, maybe the user only configured some accounts

logging.info("Secrets loaded successfully.")
# Confirmation message

# --- Parse JSON Credentials (Important!) ---
# Make GOOGLE_CREDS_INFO global or pass it if needed elsewhere, defined here for auth flow
GOOGLE_CREDS_INFO = None
try:
    GOOGLE_CREDS_INFO = json.loads(GOOGLE_CREDS_JSON_STR)
    logging.info("Google credentials JSON parsed successfully.")
except json.JSONDecodeError as e:
    logging.critical(f"CRITICAL ERROR: Failed to parse Google credentials JSON: {e}")
    sys.exit(1)

# We will parse Kaggle JSON later, just before using it, as we rotate accounts.


# --- Default State Definition ---
DEFAULT_STATE = {
    "status": "stopped", # running, stopped, stopping, error, stopped_exhausted
    "active_kaggle_account_index": 0, # Index (0-3) for KAGGLE_CREDENTIALS_LIST
    "active_drive_account_index": 0, # Index for Google Drive (currently only 0)
    "current_step": "idle", # e.g., idle, selecting_style, generating, processing, uploading
    "current_instrument": None, # Name of the instrument being generated
    "last_kaggle_run_id": None, # ID of the last triggered Kaggle kernel run
    "retry_count": 0, # Counter for retrying the current step
    "total_tracks_generated": 0, # Counter for successful generations
    "style_profile_id": "default", # Identifier for the current style profile
    "fallback_active": False, # True if running on Gitpod fallback
    "kaggle_usage": [ # List to track usage per account
        {"account_index": 0, "gpu_hours_used_this_week": 0.0, "last_reset_time": None},
        {"account_index": 1, "gpu_hours_used_this_week": 0.0, "last_reset_time": None},
        {"account_index": 2, "gpu_hours_used_this_week": 0.0, "last_reset_time": None},
        {"account_index": 3, "gpu_hours_used_this_week": 0.0, "last_reset_time": None},
    ],
    "last_error": None, # Store details of the last significant error
    "_checksum": None # Placeholder for checksum if implemented
}

STATE_FILE_PATH = "state.txt" # Define the filename


# --- Main Application Logic ---

# Modified function signature to accept state and service
def run_main_cycle(current_state, gdrive_service):
    """Performs one cycle of the main application logic."""
    logging.info(f"--- Cycle Start: {datetime.now()} ---")

    # Use the passed state dictionary
    status = current_state.get("status", "error") # Safely get status

    # --- Status Checks ---
    if status == "stopped":
        logging.info("System status is 'stopped'. Cycle skipped.")
        # No sleep needed here, main loop handles sleep
        return current_state # Return unmodified state

    if status == "stopping":
        logging.info("System status is 'stopping'. Finishing cycle and stopping.")
        current_state["status"] = "stopped" # Update status
        save_state(current_state, STATE_FILE_PATH) # Save final stopped state
        logging.info("System status set to 'stopped'.")
        # Let the main loop handle exit or sleep based on final status
        return current_state

    if status == "stopped_exhausted":
        logging.info("System status is 'stopped_exhausted'. Waiting for manual restart. Cycle skipped.")
        # TODO: Add check for quota reset time?
        return current_state # Return unmodified state

    # If status is 'running' or potentially 'error', proceed with tasks

    # --- Backup Logic ---
    # Example: Backup state and log file periodically (e.g., every hour or after N cycles)
    # We'll add a simple counter for now, backup every cycle for testing
    backup_now = True # Placeholder - add proper timing logic later (e.g., check time/cycle count)

    if backup_now and gdrive_service:
        logging.info("Attempting periodic backup...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Backup state file
        state_backup_filename = f"state_{timestamp}.json" # Use .json extension
        # Save current state locally BEFORE uploading backup
        if save_state(current_state, STATE_FILE_PATH):
             logging.info(f"Attempting to upload state backup: {state_backup_filename}")
             upload_to_gdrive(gdrive_service, STATE_FILE_PATH, GDRIVE_BACKUP_FOLDER_ID, state_backup_filename)
             # TODO: Add check for upload success/failure
        else:
             logging.error("Failed to save local state before backup. Skipping state backup.")

        # Backup log file
        log_backup_filename = f"system_log_{timestamp}.txt"
        logging.info(f"Attempting to upload log backup: {log_backup_filename}")
        # Make sure the log handler has flushed recent messages before copying/uploading
        # For basic FileHandler, closing/reopening or flushing might be needed for robustness
        # For simplicity now, we just upload the current file content
        # Ensure log file exists before uploading
        if os.path.exists(LOG_FILE_PATH):
            upload_to_gdrive(gdrive_service, LOG_FILE_PATH, GDRIVE_BACKUP_FOLDER_ID, log_backup_filename)
            # TODO: Add check for upload success/failure
        else:
            logging.warning(f"Log file {LOG_FILE_PATH} not found. Skipping log backup.")

    # --- Main Task Execution ---
    logging.info("Checking for tasks...")
    # TODO: Implement actual task logic here:
    # 1. Load state properly (already passed as current_state)
    # 2. Select instrument/style
    # 3. Trigger Kaggle generation
    # 4. Wait and download results
    # 5. Perform uniqueness check (optional)
    # 6. Process audio (on Kaggle)
    # 7. Upload to Drive
    # 8. Update state/style profile (modify current_state dictionary)
    # 9. Perform cleanups (Drive, logs)
    # 10. Handle errors and retries throughout
    # 11. Save state at the end of the cycle or after significant changes (using save_state)

    logging.info("Simulating task execution...")
    time.sleep(10) # Placeholder delay
    # Example of modifying state:
    # current_state["total_tracks_generated"] += 1

    logging.info(f"--- Cycle End: {datetime.now()} ---")

    # Save state at the end of a successful cycle
    # TODO: Move this save call to appropriate places after actual tasks are done
    save_state(current_state, STATE_FILE_PATH)

    # Return the potentially modified state
    return current_state


def main():
    """Main function to run the application loop."""
    logging.info("Starting AI Music Orchestrator...")

    # --- Authenticate Google Drive on Startup ---
    gdrive_service = authenticate_gdrive()
    if not gdrive_service:
        # Decide how critical backups are. Exit for now.
        logging.critical("Failed to authenticate Google Drive. Backups will not work. Exiting.")
        sys.exit(1)
    else:
        logging.info("Google Drive authenticated successfully.")
    # -----------------------------------------

    # TODO: Send startup message via Telegram

    # Load initial state AFTER potential auth flow interaction
    state = load_state(STATE_FILE_PATH)
    logging.info(f"Initial state loaded: Status='{state.get('status', 'N/A')}'")


    while True:
        try:
            # Pass state and service to the cycle function, get updated state back
            state = run_main_cycle(state, gdrive_service)

            # Check if the cycle function set the status to 'stopped'
            if state.get("status") == "stopped":
                 logging.info("System stopped gracefully by run_main_cycle. Exiting main loop.")
                 break # Exit the while loop

            # --- Sleep Interval ---
            # TODO: Make sleep duration configurable
            sleep_duration_seconds = 60 * 5 # e.g., 5 minutes
            logging.info(f"Sleeping for {sleep_duration_seconds} seconds...")
            time.sleep(sleep_duration_seconds)

        except KeyboardInterrupt:
            logging.info("\nCtrl+C detected. Exiting gracefully...")
            # Try to save final state if possible
            state["status"] = "stopped"
            save_state(state, STATE_FILE_PATH)
            # TODO: Send shutdown message via Telegram
            sys.exit(0)
        except Exception as e:
            # Catch unexpected errors in the main loop
            logging.critical(f"CRITICAL ERROR in main loop: {e}", exc_info=True) # Log traceback
            # TODO: Send critical error message via Telegram
            # Decide on recovery: continue loop, exit, etc.
            logging.info("Attempting to continue after 1 minute delay...")
            time.sleep(60) # Delay before potentially retrying the loop

    logging.info("AI Music Orchestrator main loop finished.")

# --- Script Entry Point ---
if __name__ == "__main__":
    main()