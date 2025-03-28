import os
import json
import time
import logging
import sys
import requests # For making HTTP requests (e.g., Telegram, maybe others)
import subprocess # For running shell commands (like Kaggle CLI, ffmpeg/sox if needed locally)
from datetime import datetime # For timestamps
import random

# Add these below existing imports
# <<< UPDATED IMPORT LIST >>>
from utils import (
    load_state, save_state,
    authenticate_gdrive, upload_to_gdrive,
    setup_kaggle_api, trigger_kaggle_notebook, download_kaggle_output,
    check_kaggle_status,
    get_spotify_trending_keywords # <<< ADDED SPOTIFY FUNCTION IMPORT >>>
)
# <<< UPDATED CONFIG IMPORT >>>
from config import (
    GDRIVE_BACKUP_FOLDER_ID,
    PROMPT_GENRES, PROMPT_INSTRUMENTS, PROMPT_MOODS, PROMPT_TEMPLATES # Import prompt lists
)

# --- Logging Configuration ---
LOG_FILE_PATH = "system_log.txt"
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logging.info("Logging configured.")

# --- Load Secrets from Environment Variables ---
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
GOOGLE_CREDS_JSON_STR = os.environ.get('GOOGLE_CREDS_JSON')
KAGGLE_JSON_1_STR = os.environ.get('KAGGLE_JSON_1')
KAGGLE_JSON_2_STR = os.environ.get('KAGGLE_JSON_2')
KAGGLE_JSON_3_STR = os.environ.get('KAGGLE_JSON_3')
KAGGLE_JSON_4_STR = os.environ.get('KAGGLE_JSON_4')
# <<< ADDED SPOTIFY SECRETS LOADING >>>
SPOTIPY_CLIENT_ID = os.environ.get('SPOTIPY_CLIENT_ID')
SPOTIPY_CLIENT_SECRET = os.environ.get('SPOTIPY_CLIENT_SECRET')


# --- Validate Secrets ---
if not TELEGRAM_BOT_TOKEN: logging.critical("CRITICAL ERROR: Telegram Bot Token not found..."); sys.exit(1)
if not TELEGRAM_CHAT_ID: logging.critical("CRITICAL ERROR: Telegram Chat ID not found..."); sys.exit(1)
if not GOOGLE_CREDS_JSON_STR: logging.critical("CRITICAL ERROR: Google Drive Credentials JSON not found..."); sys.exit(1)
# <<< ADDED SPOTIFY SECRET VALIDATION >>>
if not SPOTIPY_CLIENT_ID: logging.warning("Spotify Client ID not found. Spotify features disabled.")
if not SPOTIPY_CLIENT_SECRET: logging.warning("Spotify Client Secret not found. Spotify features disabled.")
# --- Kaggle Validation ---
KAGGLE_CREDENTIALS_LIST = [ KAGGLE_JSON_1_STR, KAGGLE_JSON_2_STR, KAGGLE_JSON_3_STR, KAGGLE_JSON_4_STR ]
if not any(KAGGLE_CREDENTIALS_LIST): logging.critical("CRITICAL ERROR: No Kaggle API credentials found..."); sys.exit(1)
if None in KAGGLE_CREDENTIALS_LIST: logging.warning("One or more Kaggle API credentials missing...")
logging.info("Secrets loaded successfully.")

# --- Parse JSON Credentials (Important!) ---
GOOGLE_CREDS_INFO = None
try:
    GOOGLE_CREDS_INFO = json.loads(GOOGLE_CREDS_JSON_STR)
    logging.info("Google credentials JSON parsed successfully.")
except json.JSONDecodeError as e:
    logging.critical(f"CRITICAL ERROR: Failed to parse Google credentials JSON: {e}"); sys.exit(1)

# --- Default State Definition ---
DEFAULT_STATE = {
    "status": "stopped", "active_kaggle_account_index": 0, "active_drive_account_index": 0,
    "current_step": "idle", "current_prompt": None,
    "last_kaggle_run_id": None, "last_kaggle_trigger_time": None,
    "last_downloaded_wav": None,
    "retry_count": 0, "total_tracks_generated": 0, "style_profile_id": "default",
    "fallback_active": False, "kaggle_usage": [
        {"account_index": i, "gpu_hours_used_this_week": 0.0, "last_reset_time": None} for i in range(4)
    ], "last_error": None, "_checksum": None
}
STATE_FILE_PATH = "state.txt"

# --- Kaggle Notebook Slug ---
KAGGLE_NOTEBOOK_SLUG = "musicyyai/notebook63936fc364" # Your specific slug


# <<< UPDATED PROMPT GENERATION FUNCTION with Spotify Integration >>>
def generate_riffusion_prompt(use_spotify=True):
    """
    Generates a text prompt for Riffusion, optionally using Spotify keywords.
    Falls back to random combinations if Spotify fails or is disabled.

    Args:
        use_spotify (bool): Whether to attempt fetching keywords from Spotify.

    Returns:
        str: A generated text prompt.
    """
    # Check if Spotify credentials exist before attempting to use
    spotify_available = SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET
    spotify_keywords = []

    if use_spotify and spotify_available:
        logging.info("Attempting to fetch keywords from Spotify...")
        # Note: get_spotify_trending_keywords handles its own auth via get_spotify_client
        spotify_keywords = get_spotify_trending_keywords(limit=15)
        if not spotify_keywords:
             logging.warning("Failed to get keywords from Spotify or none found. Falling back to default lists.")
    elif use_spotify and not spotify_available:
        logging.warning("Spotify use requested, but credentials not found. Falling back to default lists.")

    try:
        template = random.choice(PROMPT_TEMPLATES)

        # --- Choose Elements ---
        potential_genres = [k for k in spotify_keywords if k in PROMPT_GENRES]
        if potential_genres: genre = random.choice(potential_genres); logging.debug(f"Using Spotify-sourced genre: {genre}")
        elif PROMPT_GENRES: genre = random.choice(PROMPT_GENRES); logging.debug("Using random genre from config.")
        else: genre = "music"

        if PROMPT_INSTRUMENTS: instrument = random.choice(PROMPT_INSTRUMENTS); logging.debug("Using random instrument from config.")
        else: instrument = "sound"

        potential_moods = [k for k in spotify_keywords if k in PROMPT_MOODS]
        if potential_moods: mood = random.choice(potential_moods); logging.debug(f"Using Spotify-sourced mood: {mood}")
        elif PROMPT_MOODS: mood = random.choice(PROMPT_MOODS); logging.debug("Using random mood from config.")
        else: mood = "neutral"

        # --- Format Prompt ---
        prompt = template.format(genre=genre, instrument=instrument, mood=mood)

        logging.info(f"Generated prompt: '{prompt}' using template: '{template}'")
        return prompt

    except IndexError:
        logging.error("Prompt generation failed: One or more keyword lists might be empty in config.")
        return "default synth music"
    except Exception as e:
        logging.error(f"Error during prompt generation: {e}", exc_info=True)
        return "default synth music"


# --- Main Application Logic ---
def run_main_cycle(current_state, gdrive_service):
    """Performs one cycle of the main application logic."""
    logging.info(f"--- Cycle Start: {datetime.now()} ---")
    status = current_state.get("status", "error")

    # --- Status Checks ---
    if status == "stopped": logging.info("System status is 'stopped'. Cycle skipped."); return current_state
    if status == "stopping":
        logging.info("System status is 'stopping'. Finishing cycle and stopping.")
        current_state["status"] = "stopped"; save_state(current_state, STATE_FILE_PATH)
        logging.info("System status set to 'stopped'."); return current_state
    if status == "stopped_exhausted": logging.info("System status is 'stopped_exhausted'. Cycle skipped."); return current_state

    # --- Backup Logic ---
    backup_now = True # Placeholder
    if backup_now and gdrive_service:
        # ... (Backup logic remains the same) ...
        logging.info("Attempting periodic backup...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        state_backup_filename = f"state_{timestamp}.json"
        if save_state(current_state, STATE_FILE_PATH):
             logging.info(f"Attempting to upload state backup: {state_backup_filename}")
             upload_to_gdrive(gdrive_service, STATE_FILE_PATH, GDRIVE_BACKUP_FOLDER_ID, state_backup_filename)
        else: logging.error("Failed to save local state before backup. Skipping state backup.")
        log_backup_filename = f"system_log_{timestamp}.txt"
        logging.info(f"Attempting to upload log backup: {log_backup_filename}")
        if os.path.exists(LOG_FILE_PATH): upload_to_gdrive(gdrive_service, LOG_FILE_PATH, GDRIVE_BACKUP_FOLDER_ID, log_backup_filename)
        else: logging.warning(f"Log file {LOG_FILE_PATH} not found. Skipping log backup.")

    # --- Main Task Execution (Kaggle Trigger / Monitoring) ---
    logging.info("Starting main task execution...")
    current_step = current_state.get("current_step", "idle")

    if current_step == "idle":
        logging.info("Current step is 'idle'. Preparing for Kaggle run.")
        active_kaggle_index = current_state.get("active_kaggle_account_index", 0)
        logging.info(f"Using Kaggle account index: {active_kaggle_index}")
        if not setup_kaggle_api(active_kaggle_index):
             logging.error(f"Failed to setup Kaggle API for account {active_kaggle_index}. Skipping cycle.")
             current_state["last_error"] = f"Kaggle API setup failed for account {active_kaggle_index}"
             current_state["status"] = "error"; save_state(current_state, STATE_FILE_PATH)
             return current_state

        # <<< USING UPDATED PROMPT GENERATION >>>
        # 2. Prepare Parameters for Riffusion
        current_prompt = generate_riffusion_prompt() # Calls the updated function
        if not current_prompt or current_prompt == "default synth music": # Check for fallback/failure
             logging.warning(f"Prompt generation resulted in fallback or failure: '{current_prompt}'. Using fallback.")
             # Decide if we should proceed with fallback or skip cycle
             # Let's proceed with fallback for now
             if not current_prompt: current_prompt = "default synth music" # Ensure it's not None

        current_seed = random.randint(0, 2**32 - 1)
        params_for_kaggle = {
            "prompt": current_prompt,
            "seed": current_seed,
            "num_inference_steps": 50,
            "guidance_scale": 7.0
        }
        logging.info(f"Parameters for Kaggle: {params_for_kaggle}")

        # 3. Trigger Kaggle Notebook Run
        if trigger_kaggle_notebook(KAGGLE_NOTEBOOK_SLUG, params_for_kaggle):
            logging.info("Successfully initiated Kaggle notebook run.")
            current_state["current_step"] = "kaggle_running"
            current_state["current_prompt"] = current_prompt
            current_state["last_kaggle_trigger_time"] = datetime.now().isoformat()
            current_state["retry_count"] = 0
            save_state(current_state, STATE_FILE_PATH)
        else:
            logging.error("Failed to initiate Kaggle notebook run.")
            current_state["last_error"] = "Failed to trigger Kaggle run"
            current_state["status"] = "error"; save_state(current_state, STATE_FILE_PATH)

    elif current_step == "kaggle_running":
        # ... (Kaggle running/monitoring logic remains the same) ...
        logging.info("Kaggle run is currently in progress. Checking status...")
        run_status = check_kaggle_status(KAGGLE_NOTEBOOK_SLUG)
        logging.info(f"Kaggle run status: {run_status}")
        if run_status == "complete":
            logging.info("Kaggle run completed. Proceeding to download output.")
            wav_path, img_path = download_kaggle_output(KAGGLE_NOTEBOOK_SLUG, destination_dir=".")
            if wav_path:
                logging.info(f"Output WAV downloaded to: {wav_path}")
                current_state["current_step"] = "processing_output"
                current_state["last_downloaded_wav"] = wav_path
                current_state["retry_count"] = 0
                save_state(current_state, STATE_FILE_PATH)
            else:
                logging.error("Kaggle run complete but failed to download output WAV file.")
                current_state["last_error"] = "Failed to download Kaggle output"
                current_state["status"] = "error"; current_state["current_step"] = "idle"
                save_state(current_state, STATE_FILE_PATH)
        elif run_status == "error" or run_status == "cancelled":
            logging.error(f"Kaggle run failed with status: {run_status}")
            current_state["last_error"] = f"Kaggle run failed with status: {run_status}"
            current_state["status"] = "error"; current_state["current_step"] = "idle"
            current_state["retry_count"] += 1
            save_state(current_state, STATE_FILE_PATH)
        elif run_status == "running" or run_status == "queued":
            logging.info(f"Kaggle run is still {run_status}. Waiting for next cycle.")
            pass
        else:
            logging.error("Failed to get Kaggle run status. Will retry next cycle.")
            current_state["retry_count"] += 1
            save_state(current_state, STATE_FILE_PATH)

    elif current_step == "processing_output":
         # ... (Processing output logic remains the same placeholder) ...
         logging.info("Processing downloaded Kaggle output...")
         pass # Placeholder for now

    else:
        # ... (Unknown step logic remains the same) ...
        logging.warning(f"Unknown current_step in state: '{current_step}'. Resetting to idle.")
        current_state["current_step"] = "idle"
        save_state(current_state, STATE_FILE_PATH)

    # --- Cycle End ---
    logging.info(f"--- Cycle End: {datetime.now()} ---")
    return current_state


def main():
    # ... (main function remains the same) ...
    logging.info("Starting AI Music Orchestrator...")
    gdrive_service = authenticate_gdrive()
    if not gdrive_service: logging.critical("Failed to authenticate Google Drive. Exiting."); sys.exit(1)
    else: logging.info("Google Drive authenticated successfully.")
    state = load_state(STATE_FILE_PATH)
    logging.info(f"Initial state loaded: Status='{state.get('status', 'N/A')}'")
    while True:
        try:
            state = run_main_cycle(state, gdrive_service)
            if state.get("status") == "stopped":
                 logging.info("System stopped gracefully. Exiting main loop.")
                 break
            sleep_duration_seconds = 60 * 5
            logging.info(f"Sleeping for {sleep_duration_seconds} seconds...")
            time.sleep(sleep_duration_seconds)
        except KeyboardInterrupt:
            logging.info("\nCtrl+C detected. Exiting gracefully...")
            state["status"] = "stopped"; save_state(state, STATE_FILE_PATH)
            sys.exit(0)
        except Exception as e:
            logging.critical(f"CRITICAL ERROR in main loop: {e}", exc_info=True)
            logging.info("Attempting to continue after 1 minute delay...")
            time.sleep(60)
    logging.info("AI Music Orchestrator main loop finished.")

if __name__ == "__main__":
    main()






