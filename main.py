        # main.py (Updated after RETHINK FIX)

import os
import json
import time
import logging
import sys
import requests
import subprocess
from datetime import datetime, timedelta, timezone
import random
import threading
import asyncio

        # Telegram Bot Imports
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, ApplicationBuilder, CallbackQueryHandler
from telegram.constants import ParseMode

        # Imports from utils and config
from utils import ( load_state, save_state, authenticate_gdrive, upload_to_gdrive, setup_kaggle_api, trigger_kaggle_notebook, download_kaggle_output, check_kaggle_status, get_spotify_trending_keywords, is_unique_enough, get_gdrive_files, delete_gdrive_file, load_style_profile, save_style_profile, retry_operation, send_telegram_message )
from config import (
            GDRIVE_BACKUP_FOLDER_ID,
            PROMPT_GENRES, PROMPT_INSTRUMENTS, PROMPT_MOODS, PROMPT_TEMPLATES,
            UNIQUENESS_CHECK_ENABLED, UNIQUENESS_FINGERPRINT_COUNT, UNIQUENESS_SIMILARITY_THRESHOLD,
            NUM_KAGGLE_ACCOUNTS,
            MAX_DRIVE_FILES, MAX_DRIVE_FILE_AGE_DAYS,
            STYLE_PROFILE_RESET_TRACK_COUNT,
            ESTIMATED_KAGGLE_RUN_HOURS,
            KAGGLE_WEEKLY_GPU_QUOTA, KAGGLE_USAGE_BUFFER,
            HEALTH_CHECK_INTERVAL_MINUTES, INTERVENTION_TIMEOUT_MINUTES,
            DRY_RUN, # <<< Import DRY_RUN
            STYLE_PROFILE_MAX_HISTORY, SCHEDULED_ROTATION_TRACK_COUNT # <<< ADDED Imports
        )

        # --- Logging Configuration ---
        # ... (Logging setup remains unchanged) ...
        LOG_FILE_PATH = "system_log.txt"; LOG_FORMAT = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
        formatter = logging.Formatter(LOG_FORMAT)
        file_handler = RotatingFileHandler(LOG_FILE_PATH, maxBytes=(5 * 1024 * 1024), backupCount=3, encoding='utf-8')
        file_handler.setFormatter(formatter); file_handler.setLevel(logging.INFO)
        console_handler = logging.StreamHandler(sys.stdout); console_handler.setFormatter(formatter); console_handler.setLevel(logging.INFO)
        logger = logging.getLogger(); logger.setLevel(logging.INFO)
        if logger.hasHandlers(): logger.handlers.clear()
        logger.addHandler(file_handler); logger.addHandler(console_handler)
        logging.info("Logging configured with RotatingFileHandler.")


        # --- Load Secrets ---
        # ... (Secret loading remains unchanged) ...
        TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN'); TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID'); GOOGLE_CREDS_JSON_STR = os.environ.get('GOOGLE_CREDS_JSON'); KAGGLE_JSON_1_STR = os.environ.get('KAGGLE_JSON_1'); KAGGLE_JSON_2_STR = os.environ.get('KAGGLE_JSON_2'); KAGGLE_JSON_3_STR = os.environ.get('KAGGLE_JSON_3'); KAGGLE_JSON_4_STR = os.environ.get('KAGGLE_JSON_4'); SPOTIPY_CLIENT_ID = os.environ.get('SPOTIPY_CLIENT_ID'); SPOTIPY_CLIENT_SECRET = os.environ.get('SPOTIPY_CLIENT_SECRET')
        if not TELEGRAM_BOT_TOKEN: logging.warning("TELEGRAM_BOT_TOKEN secret missing.");
        if not TELEGRAM_CHAT_ID: logging.warning("TELEGRAM_CHAT_ID secret missing.");
        if not GOOGLE_CREDS_JSON_STR: logging.critical("No GDrive JSON"); sys.exit(1)
        if not SPOTIPY_CLIENT_ID: logging.warning("No Spotify ID.")
        if not SPOTIPY_CLIENT_SECRET: logging.warning("No Spotify Secret.")
        KAGGLE_CREDENTIALS_LIST = [ KAGGLE_JSON_1_STR, KAGGLE_JSON_2_STR, KAGGLE_JSON_3_STR, KAGGLE_JSON_4_STR ]
        valid_kaggle_creds = [cred for cred in KAGGLE_CREDENTIALS_LIST if cred]
        if not valid_kaggle_creds: logging.critical("No valid Kaggle Creds"); sys.exit(1)
        if len(valid_kaggle_creds) < NUM_KAGGLE_ACCOUNTS: logging.warning(f"Found {len(valid_kaggle_creds)} Kaggle creds, expected {NUM_KAGGLE_ACCOUNTS}.")
        logging.info("Secrets loaded.")
        GOOGLE_CREDS_INFO = None
        try: GOOGLE_CREDS_INFO = json.loads(GOOGLE_CREDS_JSON_STR); logging.info("GDrive creds parsed.")
        except json.JSONDecodeError as e: logging.critical(f"Failed parse GDrive JSON: {e}", exc_info=True); sys.exit(1)

        # --- Default State Definition ---
        DEFAULT_STATE = { "status": "stopped", "active_kaggle_account_index": 0, "active_drive_account_index": 0, "current_step": "idle", "current_prompt": None, "last_kaggle_run_id": None, "last_kaggle_trigger_time": None, "last_downloaded_mp3": None, "last_downloaded_json": None, "retry_count": 0, "total_tracks_generated": 0, "style_profile_id": "default", "fallback_active": False, "kaggle_usage": [{"account_index": i, "gpu_hours_used_this_week": 0.0, "last_reset_time": None} for i in range(NUM_KAGGLE_ACCOUNTS)], "last_error": None, "_checksum": None, "recent_fingerprints": [], "last_gdrive_cleanup_time": None, "last_health_check_time": None, "intervention_pending_since": None }
        STATE_FILE_PATH = "state.txt"

        # --- Constants ---
        KAGGLE_NOTEBOOK_SLUG = "musicyyai/notebook63936fc364"; GDRIVE_CLEANUP_INTERVAL_HOURS = 24; MAIN_LOOP_SLEEP_SECONDS = 60 * 5; BACKUP_INTERVAL_MINUTES = 60

        # --- Telegram Callback Data Constants ---
        CALLBACK_RETRY_OPERATION = "retry_operation"
        CALLBACK_SKIP_STEP = "skip_step"
        CALLBACK_ROTATE_ACCOUNT = "rotate_account"
        CALLBACK_CHECK_DRIVE = "check_drive"
        CALLBACK_VIEW_STATE = "view_state"

        # --- Global variable for graceful shutdown ---
        _shutdown_requested = False

        # --- Helper Functions ---
        def rotate_kaggle_account(current_state, reason="Unknown"):
            if NUM_KAGGLE_ACCOUNTS <= 1: logging.warning("Rotation requested, but only 1 account."); return current_state
            original_index = current_state.get("active_kaggle_account_index", 0); next_index = (original_index + 1) % NUM_KAGGLE_ACCOUNTS
            current_state["active_kaggle_account_index"] = next_index; current_state["retry_count"] = 0
            logging.warning(f"Rotating Kaggle account from {original_index} to {next_index}. Reason: {reason}")
            send_telegram_message(f"WARNING: Rotating Kaggle account from {original_index} to {next_index}. Reason: {reason}", level="WARNING")
            save_state(current_state, STATE_FILE_PATH); return current_state

        # --- Prompt Generation Function ---
        def generate_riffusion_prompt(use_spotify=True, style_profile=None):
            # ... (Function remains unchanged) ...
            logging.info("Generating Riffusion prompt...")
            spotify_keywords = []; spotify_available = SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET and 'SPOTIPY_AVAILABLE' in globals() and SPOTIPY_AVAILABLE
            if use_spotify and spotify_available: logging.info("Attempting Spotify keyword fetch..."); spotify_keywords = get_spotify_trending_keywords(limit=20);
            if not spotify_keywords: logging.warning("Spotify fetch failed/empty.")
            else: logging.info(f"Fetched {len(spotify_keywords)} Spotify keywords.")
            elif use_spotify: logging.warning("Spotify requested but unavailable.")
            def weighted_choice(choices, weights):
                if not choices: return None;
                if not weights or len(choices) != len(weights): logging.warning("Weighted choice: Invalid args."); return random.choice(choices)
                positive_weights = [w for w in weights if w > 0]; total_weight = sum(positive_weights)
                if total_weight <= 0: logging.warning("Weighted choice: Zero weight."); return random.choice(choices)
                r = random.uniform(0, total_weight); upto = 0
                for i, choice in enumerate(choices):
                    current_weight = weights[i] if weights[i] > 0 else 0
                    if upto + current_weight >= r: return choice
                    upto += current_weight
                logging.warning("Weighted choice fallback."); return random.choice(choices)
            try:
                template = random.choice(PROMPT_TEMPLATES); genre, instrument, mood = None, None, None; EXPLORATION_CHANCE = 0.15
                genre_choices = PROMPT_GENRES[:];
                if not genre_choices: logging.warning("PROMPT_GENRES empty."); genre = "music"
                else:
                    use_weights = True;
                    if random.random() < EXPLORATION_CHANCE: logging.info("Exploring genre."); use_weights = False
                    genre_weights = [1.0] * len(genre_choices)
                    if use_weights and style_profile and "genre_counts" in style_profile:
                        genre_counts = style_profile.get("genre_counts", {});
                        if genre_counts: logging.debug(f"Using genre counts: {genre_counts}"); genre_weights = [(genre_counts.get(g, 0) + 1.0) for g in genre_choices];
                        for i, g in enumerate(genre_choices):
                             if g in spotify_keywords and g in genre_counts: genre_weights[i] *= 1.5
                    genre = weighted_choice(genre_choices, genre_weights) if use_weights else random.choice(genre_choices); genre = genre or random.choice(PROMPT_GENRES)
                instrument_choices = PROMPT_INSTRUMENTS[:];
                if not instrument_choices: logging.warning("PROMPT_INSTRUMENTS empty."); instrument = "sound"
                else:
                    use_weights = True;
                    if random.random() < EXPLORATION_CHANCE: logging.info("Exploring instrument."); use_weights = False
                    instrument_weights = [1.0] * len(instrument_choices)
                    if use_weights and style_profile and "instrument_counts" in style_profile:
                        instrument_counts = style_profile.get("instrument_counts", {});
                        if instrument_counts: logging.debug(f"Using instrument counts: {instrument_counts}"); instrument_weights = [(instrument_counts.get(inst, 0) + 1.0) for inst in instrument_choices];
                        for i, inst in enumerate(instrument_choices):
                             if inst in spotify_keywords and inst in instrument_counts: instrument_weights[i] *= 1.2
                    instrument = weighted_choice(instrument_choices, instrument_weights) if use_weights else random.choice(instrument_choices); instrument = instrument or random.choice(PROMPT_INSTRUMENTS)
                mood_choices = PROMPT_MOODS[:];
                if not mood_choices: logging.warning("PROMPT_MOODS empty."); mood = "neutral"
                else:
                    use_weights = True;
                    if random.random() < EXPLORATION_CHANCE: logging.info("Exploring mood."); use_weights = False
                    mood_weights = [1.0] * len(mood_choices)
                    if use_weights and style_profile:
                         recent_bpms = style_profile.get("recent_bpms", [])
                         if len(recent_bpms) >= 5:
                              try:
                                   numeric_bpms = [b for b in recent_bpms if isinstance(b, (int, float))]
                                   if numeric_bpms:
                                        avg_bpm = sum(numeric_bpms) / len(numeric_bpms); logging.debug(f"Avg BPM: {avg_bpm:.1f}")
                                        for i, m in enumerate(mood_choices):
                                             if avg_bpm > 115 and m in ["energetic", "upbeat", "driving", "happy", "intense", "fast"]: mood_weights[i] *= 1.5
                                             elif avg_bpm < 90 and m in ["chill", "relaxing", "ambient", "calm", "peaceful", "slow", "atmospheric", "dreamy"]: mood_weights[i] *= 1.5
                                   else: logging.warning("No numeric BPMs for bias.")
                              except Exception as bpm_avg_e: logging.warning(f"BPM bias error: {bpm_avg_e}")
                         mood_counts = style_profile.get("mood_counts", {})
                         if mood_counts: logging.debug(f"Using mood counts: {mood_counts}");
                         for i, m in enumerate(mood_choices): mood_weights[i] *= (mood_counts.get(m, 0) + 1.0)
                         for i, m in enumerate(mood_choices):
                              if m in spotify_keywords and m in mood_counts: mood_weights[i] *= 1.2
                    mood = weighted_choice(mood_choices, mood_weights) if use_weights else random.choice(mood_choices); mood = mood or random.choice(PROMPT_MOODS)
                genre = genre or "music"; instrument = instrument or "sound"; mood = mood or "neutral"
                prompt = template.format(genre=genre, instrument=instrument, mood=mood)
                log_msg = f"Generated prompt ({'influenced' if use_weights else 'random exploration'}): '{prompt}'"; logging.info(log_msg)
                return prompt
            except Exception as e: logging.error(f"Prompt generation error: {e}", exc_info=True); return "ambient synth music"

        # --- Google Drive Cleanup Function ---
        def perform_gdrive_cleanup(current_state, gdrive_service):
            # ... (Function remains unchanged) ...
            logging.info("Performing Google Drive cleanup...")
            try:
                max_files = MAX_DRIVE_FILES; max_age_days = MAX_DRIVE_FILE_AGE_DAYS
                logging.info(f"Cleanup limits: Max Files={max_files}, Max Age={max_age_days} days.")
                files = get_gdrive_files(gdrive_service, GDRIVE_BACKUP_FOLDER_ID)
                if not files: logging.info("No files found for cleanup."); return True
                try: files.sort(key=lambda x: datetime.fromisoformat(x['createdTime'].replace('Z', '+00:00')))
                except (KeyError, ValueError) as sort_e: logging.error(f"Error sorting Drive files: {sort_e}. Cleanup aborted."); return False
                deleted_count = 0; now = datetime.now(timezone.utc); age_limit = now - timedelta(days=max_age_days)
                files_to_delete_by_age = []; valid_files_for_count = []
                for file in files:
                     try:
                          created_time = datetime.fromisoformat(file['createdTime'].replace('Z', '+00:00'))
                          valid_files_for_count.append(file)
                          if created_time < age_limit: files_to_delete_by_age.append(file)
                     except (KeyError, ValueError): logging.warning(f"Could not parse createdTime for {file.get('name')}.")
                if files_to_delete_by_age:
                    logging.info(f"Found {len(files_to_delete_by_age)} files older than {max_age_days} days.")
                    for file in files_to_delete_by_age:
                         logging.info(f"Deleting old file: {file.get('name')} (ID: {file.get('id')})")
                         if delete_gdrive_file(gdrive_service, file.get('id')): deleted_count += 1
                         else: logging.error(f"Failed delete by age: {file.get('name')}")
                         time.sleep(0.5)
                deleted_ids_by_age = {f['id'] for f in files_to_delete_by_age}
                remaining_files = [f for f in files if f['id'] not in deleted_ids_by_age]
                remaining_files_count = len(remaining_files)
                files_to_delete_by_count = []
                if remaining_files_count > max_files:
                     num_to_delete = remaining_files_count - max_files
                     files_to_delete_by_count = remaining_files[:num_to_delete]
                     logging.info(f"Count ({remaining_files_count}) > limit ({max_files}). Deleting {len(files_to_delete_by_count)} oldest.")
                if files_to_delete_by_count:
                     for file in files_to_delete_by_count:
                          logging.info(f"Deleting excess file: {file.get('name')} (ID: {file.get('id')})")
                          if delete_gdrive_file(gdrive_service, file.get('id')): deleted_count += 1
                          else: logging.error(f"Failed delete by count: {file.get('name')}")
                          time.sleep(0.5)
                logging.info(f"GDrive cleanup finished. Deleted: {deleted_count}")
                return True
            except Exception as e: logging.critical(f"CRITICAL Error during GDrive cleanup: {e}", exc_info=True); return False

        # --- Main Orchestration Cycle (run in a separate thread) ---
        _last_backup_time = None

        def run_main_cycle(gdrive_service):
            # ... (Function remains unchanged, including intervention timeout check) ...
            global _shutdown_requested, _last_backup_time
            cycle_start_time = datetime.now(timezone.utc)
            logging.info(f"--- Cycle Start: {cycle_start_time.isoformat()} ---")
            current_state = load_state(STATE_FILE_PATH)
            try:
                now_utc = datetime.now(timezone.utc)
                if now_utc.weekday() == 0: # Monday
                    logging.debug("Checking Kaggle usage reset (Monday UTC).")
                    usage_list = current_state.get("kaggle_usage", [])
                    state_changed = False
                    if isinstance(usage_list, list):
                        for i in range(len(usage_list)):
                            account_usage = usage_list[i]
                            if isinstance(account_usage, dict):
                                last_reset_iso = account_usage.get("last_reset_time"); needs_reset = False
                                if last_reset_iso is None: needs_reset = True; logging.info(f"Kaggle account {i} initial reset.")
                                else:
                                    try:
                                        if now_utc - datetime.fromisoformat(last_reset_iso) > timedelta(days=6): needs_reset = True; logging.info(f"Kaggle account {i} resetting usage (>6 days).")
                                        else: logging.debug(f"Kaggle account {i} reset recently.")
                                    except ValueError: logging.warning(f"Bad last_reset_time for account {i}. Resetting."); needs_reset = True
                                if needs_reset:
                                    usage_list[i]["gpu_hours_used_this_week"] = 0.0; usage_list[i]["last_reset_time"] = now_utc.isoformat(); state_changed = True
                            else: logging.warning(f"Item index {i} in kaggle_usage not dict.")
                        if state_changed:
                            logging.info("Kaggle weekly usage counters reset.")
                            current_state["kaggle_usage"] = usage_list
                            if current_state.get("status") == "stopped_exhausted":
                                 logging.warning("Quota reset while status=stopped_exhausted. Setting status=stopped.")
                                 current_state["status"] = "stopped"
                                 send_telegram_message("INFO: Kaggle quota reset. Status set to 'stopped'. Manual /start required.", level="INFO")
                            save_state(current_state, STATE_FILE_PATH)
                    else: logging.error("kaggle_usage in state is not a list.")
                else: logging.debug("Not Monday (UTC), skipping weekly quota reset check.")
            except Exception as reset_e: logging.error(f"Error during Kaggle quota reset check: {reset_e}", exc_info=True)
            status = current_state.get("status", "error")
            if _shutdown_requested: logging.info("Orchestrator thread received shutdown signal. Exiting cycle."); if status != "stopped": current_state["status"] = "stopped"; save_state(current_state, STATE_FILE_PATH); return
            if status == "stopped": logging.info("Status 'stopped'. Cycle skipped."); return
            if status == "stopping": logging.info("Status 'stopping'."); current_state["status"] = "stopped"; save_state(current_state, STATE_FILE_PATH); logging.info("Status set 'stopped'."); return
            if status == "stopped_exhausted": logging.info("Status 'stopped_exhausted'. Cycle skipped."); return
            if status == "error":
                logging.error("Status is 'error'. Checking for pending intervention timeout.")
                intervention_start_iso = current_state.get("intervention_pending_since")
                if intervention_start_iso:
                    try:
                        intervention_start_dt = datetime.fromisoformat(intervention_start_iso); elapsed_time = datetime.now(timezone.utc) - intervention_start_dt
                        if elapsed_time >= timedelta(minutes=INTERVENTION_TIMEOUT_MINUTES):
                            logging.warning(f"Intervention timeout ({INTERVENTION_TIMEOUT_MINUTES} mins) reached. Attempting auto recovery.")
                            send_telegram_message("WARNING: No user action on error. Attempting automated recovery...", level="WARNING")
                            last_error = current_state.get("last_error", "").lower(); recovered = False
                            logging.info("Auto-Recovery: Attempting Task Restart...")
                            reset_step_to = "idle"
                            if "status check" in last_error: reset_step_to = "kaggle_running"
                            elif "download" in last_error: reset_step_to = "kaggle_running"
                            elif "processing" in last_error or "upload" in last_error:
                                 if current_state.get("last_downloaded_mp3") and current_state.get("last_downloaded_json"): reset_step_to = "processing_output"
                            elif "trigger" in last_error or "setup failed" in last_error: reset_step_to = "idle"
                            current_state["current_step"] = reset_step_to; current_state["retry_count"] = 0; current_state["last_error"] = "Automated Recovery: Task Restarted"; current_state["status"] = "running"; current_state["intervention_pending_since"] = None
                            save_state(current_state, STATE_FILE_PATH); send_telegram_message(f"INFO: Auto-Recovery - Task Restarted (Step set to: {reset_step_to}).", level="INFO"); recovered = True
                            if not recovered: logging.error("Auto-Recovery: No specific action taken."); current_state["intervention_pending_since"] = None; save_state(current_state, STATE_FILE_PATH); send_telegram_message("ERROR: Automated recovery failed.", level="ERROR")
                        else: logging.info(f"Intervention pending, timeout not reached ({elapsed_time.total_seconds()/60:.1f}/{INTERVENTION_TIMEOUT_MINUTES} mins). Cycle skipped.")
                    except ValueError: logging.error("Could not parse intervention_pending_since. Clearing flag."); current_state["intervention_pending_since"] = None; save_state(current_state, STATE_FILE_PATH)
                    except Exception as timeout_e: logging.error(f"Error during intervention timeout check: {timeout_e}", exc_info=True); current_state["intervention_pending_since"] = None; save_state(current_state, STATE_FILE_PATH)
                else: logging.error("Status 'error' but no intervention timestamp. Manual investigation needed.")
                return
            if status != "running": logging.error(f"Unknown status '{status}'. Setting 'stopped'."); current_state["status"] = "stopped"; save_state(current_state, STATE_FILE_PATH); return
            now_dt = datetime.now(timezone.utc)
            run_backup = False
            if _last_backup_time is None or now_dt - _last_backup_time >= timedelta(minutes=BACKUP_INTERVAL_MINUTES): run_backup = True
            if run_backup and gdrive_service:
                logging.info(f"Performing periodic backup..."); timestamp = now_dt.strftime("%Y%m%d_%H%M%S"); state_backup_filename = f"state_{timestamp}.json"
                state_saved_for_backup = save_state(current_state, STATE_FILE_PATH)
                if state_saved_for_backup: logging.info("State saved locally."); retry_operation(upload_to_gdrive, args=(gdrive_service, STATE_FILE_PATH, GDRIVE_BACKUP_FOLDER_ID, state_backup_filename), operation_name="State Backup Upload")
                else: logging.error("Failed save state locally before backup.")
                log_backup_filename = f"system_log_{timestamp}.txt"
                if os.path.exists(LOG_FILE_PATH):
                    for handler in logging.getLogger().handlers: handler.flush()
                    retry_operation(upload_to_gdrive, args=(gdrive_service, LOG_FILE_PATH, GDRIVE_BACKUP_FOLDER_ID, log_backup_filename), operation_name="Log Backup Upload")
                else: logging.warning(f"Log file {LOG_FILE_PATH} not found.")
                _last_backup_time = now_dt
            elif run_backup: logging.error("Backup interval reached, GDrive unavailable.")
            last_cleanup_iso = current_state.get("last_gdrive_cleanup_time"); run_cleanup = False
            if last_cleanup_iso:
                 try:
                      if now_dt - datetime.fromisoformat(last_cleanup_iso) >= timedelta(hours=GDRIVE_CLEANUP_INTERVAL_HOURS): run_cleanup = True
                 except ValueError: logging.warning("Bad last_gdrive_cleanup_time. Running cleanup."); run_cleanup = True
            else: run_cleanup = True
            if run_cleanup and gdrive_service:
                 logging.info(f"Running periodic GDrive cleanup..."); cleanup_success = perform_gdrive_cleanup(current_state, gdrive_service)
                 if cleanup_success: current_state["last_gdrive_cleanup_time"] = now_dt.isoformat(); save_state(current_state, STATE_FILE_PATH)
            elif run_cleanup: logging.error("Cleanup interval reached, GDrive unavailable.")
            run_health_check = False
            last_check_iso = current_state.get("last_health_check_time")
            if last_check_iso is None: run_health_check = True
            else:
                try:
                    if now_dt - datetime.fromisoformat(last_check_iso) >= timedelta(minutes=HEALTH_CHECK_INTERVAL_MINUTES): run_health_check = True
                except ValueError: logging.warning("Bad last_health_check_time. Running health check."); run_health_check = True
            if run_health_check:
                logging.info(f"Performing periodic health checks (Interval: {HEALTH_CHECK_INTERVAL_MINUTES} mins)...")
                all_checks_ok = True
                if gdrive_service:
                    logging.debug("Health Check: Checking GDrive API..."); def gdrive_about_call(): return gdrive_service.about().get(fields='storageQuota').execute()
                    gdrive_check_result = retry_operation(gdrive_about_call, max_retries=1, delay_seconds=5, operation_name="GDrive Health Check")
                    if gdrive_check_result is None: logging.error("Health Check FAILED: Google Drive API."); send_telegram_message("ERROR: Health Check FAILED for Google Drive API.", level="ERROR"); all_checks_ok = False
                    else: logging.info("Health Check OK: Google Drive API.")
                else: logging.warning("Health Check SKIPPED: GDrive service unavailable."); all_checks_ok = False
                logging.debug("Health Check: Checking Kaggle API...");
                def kaggle_list_call():
                     hc_kaggle_idx = current_state.get("active_kaggle_account_index", 0);
                     if not setup_kaggle_api(hc_kaggle_idx): logging.error(f"Health Check FAILED: Kaggle setup index {hc_kaggle_idx}"); return None
                     command = ["kaggle", "kernels", "list", "-m", "-p", "1"]; result = subprocess.run(command, capture_output=True, text=True, check=False, timeout=30)
                     if result.returncode != 0: logging.error(f"Kaggle health check cmd failed. Code: {result.returncode}. Stderr: {result.stderr.strip()}"); return None
                     return True
                kaggle_check_result = retry_operation(kaggle_list_call, max_retries=1, delay_seconds=5, operation_name="Kaggle Health Check")
                if kaggle_check_result is None: logging.error("Health Check FAILED: Kaggle API."); send_telegram_message("ERROR: Health Check FAILED for Kaggle API.", level="ERROR"); all_checks_ok = False
                else: logging.info("Health Check OK: Kaggle API.")
                logging.debug("Health Check: Checking Telegram API...")
                telegram_check_result = retry_operation( send_telegram_message, args=("Health Check Ping.",), kwargs={"level": "DEBUG"}, max_retries=1, delay_seconds=5, operation_name="Telegram Health Check" )
                if not telegram_check_result: logging.error("Health Check FAILED: Telegram API."); all_checks_ok = False
                else: logging.info("Health Check OK: Telegram API.")
                current_state["last_health_check_time"] = now_dt.isoformat(); save_state(current_state, STATE_FILE_PATH)
                if all_checks_ok: logging.info("All health checks passed.")
                else: logging.warning("One or more health checks failed.")
            logging.info("Starting main task execution...")
            current_step = current_state.get("current_step", "idle")
            active_kaggle_index = current_state.get("active_kaggle_account_index", 0)
            try:
                if current_step == "idle":
                    logging.info("State: Idle. Preparing Kaggle run.")
                    logging.info(f"Attempting to use Kaggle account index: {active_kaggle_index}")
                    if not setup_kaggle_api(active_kaggle_index): err_msg = f"Kaggle API setup failed (Index {active_kaggle_index})"; logging.error(err_msg); current_state["last_error"] = err_msg; send_telegram_message(f"ERROR: {err_msg}. Rotating.", level="ERROR"); current_state = rotate_kaggle_account(current_state, reason="API Setup Failure"); return
                    quota_check_passed = False; initial_check_index = active_kaggle_index; accounts_checked = 0
                    while accounts_checked < NUM_KAGGLE_ACCOUNTS:
                        current_active_index_in_loop = current_state.get("active_kaggle_account_index", 0); accounts_checked += 1; logging.info(f"Checking quota account {current_active_index_in_loop} (Check {accounts_checked}/{NUM_KAGGLE_ACCOUNTS})")
                        try:
                            usage_list = current_state.get("kaggle_usage", []);
                            if not (0 <= current_active_index_in_loop < len(usage_list)): logging.error(f"Quota check failed: Invalid index {current_active_index_in_loop}."); current_state["status"] = "error"; current_state["last_error"] = f"Invalid Kaggle index {current_active_index_in_loop}."; save_state(current_state, STATE_FILE_PATH); send_telegram_message(f"CRITICAL: Invalid Kaggle index {current_active_index_in_loop}.", level="CRITICAL"); return
                            current_usage = usage_list[current_active_index_in_loop].get("gpu_hours_used_this_week", 0.0); projected_usage = current_usage + ESTIMATED_KAGGLE_RUN_HOURS; quota_limit = KAGGLE_WEEKLY_GPU_QUOTA * KAGGLE_USAGE_BUFFER; logging.info(f"Account {current_active_index_in_loop}: Current={current_usage:.2f}h, Projected={projected_usage:.2f}h, Limit={quota_limit:.2f}h")
                            if projected_usage <= quota_limit: logging.info(f"Quota check passed account {current_active_index_in_loop}."); quota_check_passed = True; active_kaggle_index = current_active_index_in_loop; break
                            else: logging.warning(f"Quota limit for account {current_active_index_in_loop}. Rotating."); current_state = rotate_kaggle_account(current_state, reason="Quota Limit Reached")
                        except Exception as quota_e: logging.error(f"Error quota check account {current_active_index_in_loop}: {quota_e}", exc_info=True); send_telegram_message(f"ERROR: Exception quota check account {current_active_index_in_loop}. Rotating.", level="ERROR"); current_state = rotate_kaggle_account(current_state, reason="Quota Check Error")
                        if accounts_checked >= NUM_KAGGLE_ACCOUNTS and current_state.get("active_kaggle_account_index", 0) == initial_check_index and not quota_check_passed: logging.error("Quota check loop completed full rotation."); break
                    if not quota_check_passed: err_msg = "All Kaggle accounts exhausted quota."; logging.critical(f"CRITICAL: {err_msg} Stopping."); current_state["status"] = "stopped_exhausted"; current_state["last_error"] = err_msg; save_state(current_state, STATE_FILE_PATH); send_telegram_message(f"CRITICAL: {err_msg} Script stopped.", level="CRITICAL"); return
                    logging.info(f"Proceeding with Kaggle run using account index: {active_kaggle_index}")
                    style_profile = load_style_profile()
                    if style_profile:
                        total_tracks = current_state.get("total_tracks_generated", 0); last_reset = style_profile.get("last_reset_track_count", 0); tracks_since_reset = total_tracks - last_reset; logging.debug(f"Tracks: {total_tracks}. Last reset: {last_reset}. Since reset: {tracks_since_reset}.")
                        if tracks_since_reset >= STYLE_PROFILE_RESET_TRACK_COUNT: logging.warning(f"Track count >= {STYLE_PROFILE_RESET_TRACK_COUNT}. Resetting style profile."); style_profile["genre_counts"] = {}; style_profile["instrument_counts"] = {}; style_profile["mood_counts"] = {}; style_profile["prompt_keyword_counts"] = {}; style_profile["recent_bpms"] = []; style_profile["recent_keys"] = []; style_profile["last_reset_track_count"] = total_tracks; style_profile["last_updated"] = datetime.now(timezone.utc).isoformat();
                        if save_style_profile(style_profile): logging.info("Saved reset style profile.")
                        else: logging.error("Failed save reset style profile.")
                        style_profile = load_style_profile()
                    current_prompt = generate_riffusion_prompt(style_profile=style_profile)
                    if not current_prompt or current_prompt == "ambient synth music": logging.warning(f"Using fallback prompt: '{current_prompt}'")
                    current_seed = random.randint(0, 2**32 - 1); params_for_kaggle = {"prompt": current_prompt, "seed": current_seed, "num_inference_steps": 50, "guidance_scale": 7.0}; logging.info(f"Parameters for Kaggle: {params_for_kaggle}")
                    trigger_success = retry_operation( trigger_kaggle_notebook, args=(KAGGLE_NOTEBOOK_SLUG, params_for_kaggle), max_retries=2, delay_seconds=10, operation_name="Trigger Kaggle Notebook" )
                    if trigger_success: logging.info("Successfully initiated Kaggle run."); current_state["current_step"] = "kaggle_running"; current_state["current_prompt"] = current_prompt; current_state["last_kaggle_trigger_time"] = datetime.now(timezone.utc).isoformat(); current_state["retry_count"] = 0; current_state["last_error"] = None; save_state(current_state, STATE_FILE_PATH)
                    else:
                        err_msg = "Failed to trigger Kaggle run (retries exhausted)"; logging.error("Failed initiate Kaggle run after multiple retries."); current_state["last_error"] = err_msg
                        keyboard = [[InlineKeyboardButton("🔄 Rotate Account", callback_data=CALLBACK_ROTATE_ACCOUNT)]]; reply_markup = InlineKeyboardMarkup(keyboard)
                        send_telegram_message(f"ERROR: {err_msg}. Check Kaggle status/notebook. Options:", level="ERROR", reply_markup=reply_markup)
                        current_state["status"] = "error"; current_state["intervention_pending_since"] = datetime.now(timezone.utc).isoformat(); save_state(current_state, STATE_FILE_PATH); return

                elif current_step == "kaggle_running":
                    logging.info("State: Kaggle Running. Checking status...")
                    run_status = retry_operation( check_kaggle_status, args=(KAGGLE_NOTEBOOK_SLUG,), max_retries=4, delay_seconds=15, operation_name="Check Kaggle Status" )
                    if run_status == "complete":
                        logging.info("Kaggle run complete. Updating usage and downloading output.")
                        try:
                            usage_list = current_state.get("kaggle_usage", [])
                            if len(usage_list) < NUM_KAGGLE_ACCOUNTS: logging.warning("Kaggle usage list mismatch. Rebuilding."); usage_list = [{"account_index": i, "gpu_hours_used_this_week": 0.0, "last_reset_time": None} for i in range(NUM_KAGGLE_ACCOUNTS)]
                            if 0 <= active_kaggle_index < len(usage_list): run_duration_hours = ESTIMATED_KAGGLE_RUN_HOURS; usage_list[active_kaggle_index]["gpu_hours_used_this_week"] = usage_list[active_kaggle_index].get("gpu_hours_used_this_week", 0.0) + run_duration_hours; current_state["kaggle_usage"] = usage_list; logging.info(f"Updated Kaggle usage account {active_kaggle_index}: {usage_list[active_kaggle_index]['gpu_hours_used_this_week']:.2f}h estimated."); save_state(current_state, STATE_FILE_PATH)
                            else: logging.error(f"Could not update Kaggle usage: index {active_kaggle_index} out of bounds ({len(usage_list)}).")
                        except Exception as usage_e: logging.error(f"Error updating Kaggle usage: {usage_e}", exc_info=True)
                        download_result = retry_operation( download_kaggle_output, args=(KAGGLE_NOTEBOOK_SLUG,), kwargs={"destination_dir": "."}, max_retries=2, delay_seconds=20, operation_name="Download Kaggle Output" )
                        if download_result and download_result[0] and download_result[1]: mp3_path, json_path, img_path = download_result; logging.info(f"Downloaded MP3: {mp3_path}, JSON: {json_path}"); current_state["current_step"] = "processing_output"; current_state["last_downloaded_mp3"] = mp3_path; current_state["last_downloaded_json"] = json_path; current_state["retry_count"] = 0; save_state(current_state, STATE_FILE_PATH)
                        else:
                            err_msg = "Failed download Kaggle output (retries exhausted)"; logging.error("Download failed after multiple retries."); current_state["last_error"] = err_msg; current_state["current_step"] = "idle"
                            keyboard = [[InlineKeyboardButton("🔄 Rotate Account", callback_data=CALLBACK_ROTATE_ACCOUNT)], [InlineKeyboardButton("🔁 Retry Full Cycle", callback_data=CALLBACK_RETRY_OPERATION)]]; reply_markup = InlineKeyboardMarkup(keyboard)
                            send_telegram_message(f"ERROR: {err_msg}. Check Kaggle notebook output. Options:", level="ERROR", reply_markup=reply_markup)
                            current_state["status"] = "error"; current_state["intervention_pending_since"] = datetime.now(timezone.utc).isoformat(); save_state(current_state, STATE_FILE_PATH); return
                    elif run_status in ["error", "cancelled"]: logging.error(f"Kaggle run failed: {run_status}"); current_state["last_error"] = f"Kaggle run failed: {run_status}"; current_state["current_step"] = "idle"; save_state(current_state, STATE_FILE_PATH); send_telegram_message(f"WARNING: Kaggle run {KAGGLE_NOTEBOOK_SLUG} finished with status: {run_status}", level="WARNING")
                    elif run_status in ["running", "queued"]: logging.info(f"Kaggle run still {run_status}.")
                    else:
                        err_msg = "Failed Kaggle status check (retries exhausted)"; logging.error("Failed get Kaggle status after multiple retries."); current_state["last_error"] = err_msg
                        keyboard = [[InlineKeyboardButton("🔄 Rotate Account", callback_data=CALLBACK_ROTATE_ACCOUNT)], [InlineKeyboardButton("🔁 Retry Status Check", callback_data=CALLBACK_RETRY_OPERATION)]]; reply_markup = InlineKeyboardMarkup(keyboard)
                        send_telegram_message(f"ERROR: {err_msg}. Check Kaggle status/API. Options:", level="ERROR", reply_markup=reply_markup)
                        current_state["status"] = "error"; current_state["intervention_pending_since"] = datetime.now(timezone.utc).isoformat(); save_state(current_state, STATE_FILE_PATH); return

                elif current_step == "processing_output":
                    logging.info("State: Processing Output.")
                    downloaded_mp3 = current_state.get("last_downloaded_mp3"); downloaded_json = current_state.get("last_downloaded_json")
                    proceed_with_upload = False; upload_success = False; gdrive_filename = None
                    if downloaded_mp3 and downloaded_json and os.path.exists(downloaded_mp3) and os.path.exists(downloaded_json):
                        logging.info(f"Processing MP3: {downloaded_mp3}, JSON: {downloaded_json}")
                        analysis_data = None; new_fingerprint = None
                        try:
                            with open(downloaded_json, 'r', encoding='utf-8') as f: analysis_data = json.load(f); logging.info("Loaded analysis data.")
                            if UNIQUENESS_CHECK_ENABLED:
                                logging.info("Performing uniqueness check...")
                                new_fingerprint = analysis_data.get('fingerprint'); fingerprint_error = analysis_data.get('fingerprint_error'); recent_fingerprints = current_state.get("recent_fingerprints", [])
                                if new_fingerprint and not fingerprint_error:
                                    if is_unique_enough(new_fingerprint, recent_fingerprints, UNIQUENESS_SIMILARITY_THRESHOLD): proceed_with_upload = True; recent_fingerprints.append(new_fingerprint); current_state["recent_fingerprints"] = recent_fingerprints[-UNIQUENESS_FINGERPRINT_COUNT:]; logging.info("Uniqueness check passed.")
                                    else: logging.warning(f"Uniqueness check failed."); proceed_with_upload = False; current_state["last_error"] = "Discarded: Track too similar"
                                elif fingerprint_error: logging.error(f"Cannot check uniqueness: {fingerprint_error}"); proceed_with_upload = False; current_state["last_error"] = f"Fingerprint error: {fingerprint_error}"
                                else: logging.warning("No fingerprint. Skipping check."); proceed_with_upload = True
                            else: logging.info("Uniqueness check disabled."); proceed_with_upload = True
                            if analysis_data:
                                bpm = analysis_data.get("estimated_bpm"); key = analysis_data.get("estimated_key"); duration = analysis_data.get("duration"); logging.info(f"Metadata - BPM:{bpm}, Key:{key}, Duration:{duration if duration else 'N/A'}s")
                                mp3_check_ok = analysis_data.get("mp3_check_ok", False); processing_error = analysis_data.get("processing_error");
                                if not mp3_check_ok or processing_error: logging.warning(f"Kaggle MP3 issue: OK={mp3_check_ok}, Error='{processing_error}'.")
                            if proceed_with_upload:
                                logging.info("Proceeding to upload track to GDrive...")
                                try:
                                    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"); prompt_theme = current_state.get("current_prompt", "unknown_prompt"); safe_prompt_theme = "".join(c if c.isalnum() else "_" for c in prompt_theme.split(',')[0])[:30].strip('_'); bpm_str = str(analysis_data.get("estimated_bpm", "UNK")); key_str = str(analysis_data.get("estimated_key", "UNK")).replace("#","s"); gdrive_filename = f"track_{timestamp_str}_{safe_prompt_theme}_bpm{bpm_str}_key{key_str}.mp3"; logging.info(f"GDrive filename: {gdrive_filename}")
                                    if gdrive_service and downloaded_mp3:
                                        file_id = retry_operation( upload_to_gdrive, args=(gdrive_service, downloaded_mp3, GDRIVE_BACKUP_FOLDER_ID, gdrive_filename), max_retries=2, delay_seconds=10, operation_name="Upload to Google Drive" )
                                        if file_id: logging.info(f"Uploaded MP3. ID: {file_id}"); current_state["total_tracks_generated"] += 1; upload_success = True; send_telegram_message(f"Successfully generated and uploaded track: {gdrive_filename}", level="INFO")
                                        else:
                                             err_msg = "GDrive upload failed (retries exhausted)"; logging.error("GDrive upload failed after retries."); current_state["last_error"] = err_msg
                                             keyboard = [[InlineKeyboardButton("➡️ Continue (Skip Upload)", callback_data=CALLBACK_SKIP_STEP)], [InlineKeyboardButton("🌐 Check Drive Connection", callback_data=CALLBACK_CHECK_DRIVE)]]; reply_markup = InlineKeyboardMarkup(keyboard)
                                             send_telegram_message(f"ERROR: {err_msg}. Check Drive permissions/quota. Options:", level="ERROR", reply_markup=reply_markup)
                                             current_state["status"] = "error"; current_state["intervention_pending_since"] = datetime.now(timezone.utc).isoformat();
                                    else: err_msg = "Cannot upload to GDrive - Service/MP3 missing."; logging.error(err_msg); current_state["last_error"] = err_msg; send_telegram_message(f"ERROR: {err_msg}", level="ERROR")
                                except Exception as upload_err: logging.error(f"Error during upload setup/call: {upload_err}", exc_info=True); current_state["last_error"] = "GDrive Filename/Upload Error"
                            else: logging.info("Skipping GDrive upload.")
                            if proceed_with_upload and upload_success and analysis_data:
                                logging.info("Updating style profile...")
                                try:
                                    style_profile = load_style_profile(); profile_updated = False;
                                    current_bpm = analysis_data.get("estimated_bpm")
                                    if current_bpm and isinstance(current_bpm, (int, float)): recent_bpms = style_profile.get("recent_bpms", []); recent_bpms.append(round(current_bpm)); style_profile["recent_bpms"] = recent_bpms[-STYLE_PROFILE_MAX_HISTORY:]; profile_updated = True; logging.debug(f"Added BPM {round(current_bpm)}.") # Use constant
                                    current_key = analysis_data.get("estimated_key")
                                    if current_key and isinstance(current_key, str): recent_keys = style_profile.get("recent_keys", []); recent_keys.append(current_key); style_profile["recent_keys"] = recent_keys[-STYLE_PROFILE_MAX_HISTORY:]; profile_updated = True; logging.debug(f"Added Key {current_key}.") # Use constant
                                    if profile_updated: style_profile["last_updated"] = datetime.now(timezone.utc).isoformat();
                                    if save_style_profile(style_profile): logging.info("Saved updated style profile.")
                                    else: logging.error("Failed save updated style profile.")
                                    else: logging.info("No new data to update style profile.")
                                except Exception as style_e: logging.error(f"Error updating style profile: {style_e}", exc_info=True)
                            # Use constant for scheduled rotation check
                            if upload_success and current_state["total_tracks_generated"] > 0 and current_state["total_tracks_generated"] % (SCHEDULED_ROTATION_TRACK_COUNT * NUM_KAGGLE_ACCOUNTS) == 0: logging.info(f"Reached {current_state['total_tracks_generated']} tracks. Scheduled rotation."); current_state = rotate_kaggle_account(current_state, reason=f"Scheduled rotation")
                            logging.info("Cleaning up downloaded files...")
                            files_to_remove = [downloaded_mp3, downloaded_json]
                            for f_path in files_to_remove:
                                if f_path and os.path.exists(f_path): try: os.remove(f_path); logging.info(f"Removed: {f_path}"); except OSError as rm_e: logging.warning(f"Error removing {f_path}: {rm_e}", exc_info=True)
                                elif f_path: logging.warning(f"File {f_path} not found for cleanup.")
                            if current_state.get("status") != "error":
                                current_state["current_step"] = "idle"; current_state["last_downloaded_mp3"] = None; current_state["last_downloaded_json"] = None; current_state["current_prompt"] = None
                                if (proceed_with_upload and upload_success) or not proceed_with_upload:
                                     if current_state.get("last_error") not in ["Discarded: Track too similar", "GDrive upload failed (retries exhausted)"]: current_state["last_error"] = None
                                save_state(current_state, STATE_FILE_PATH); logging.info("Processing complete. State reset to idle.")
                            else:
                                 save_state(current_state, STATE_FILE_PATH); logging.warning("Processing finished, but state is in error due to upload failure.")
                        except json.JSONDecodeError as json_e: logging.error(f"Failed decode results JSON '{downloaded_json}': {json_e}", exc_info=True); current_state["current_step"] = "idle"; current_state["last_error"] = "Failed decode results JSON"; if downloaded_json and os.path.exists(downloaded_json): os.remove(downloaded_json); if downloaded_mp3 and os.path.exists(downloaded_mp3): os.remove(downloaded_mp3); save_state(current_state, STATE_FILE_PATH)
                        except Exception as proc_e: logging.critical(f"CRITICAL error during output processing: {proc_e}", exc_info=True); current_state["current_step"] = "idle"; current_state["last_error"] = f"Processing error: {proc_e}"; try: if downloaded_mp3 and os.path.exists(downloaded_mp3): os.remove(downloaded_mp3); if downloaded_json and os.path.exists(downloaded_json): os.remove(downloaded_json); except OSError as rm_e: logging.warning(f"Error cleaning files after processing error: {rm_e}", exc_info=True); save_state(current_state, STATE_FILE_PATH)
                    else: logging.error("Downloaded files missing."); current_state["current_step"] = "idle"; current_state["last_error"] = "Downloaded files missing"; current_state["last_downloaded_mp3"] = None; current_state["last_downloaded_json"] = None; save_state(current_state, STATE_FILE_PATH)

                else: # Unknown step
                    logging.warning(f"Unknown step: '{current_step}'. Resetting."); current_state["current_step"] = "idle"; current_state["last_error"] = f"Unknown step: {current_step}"; save_state(current_state, STATE_FILE_PATH)

            except Exception as cycle_e:
                 err_msg = f"Unhandled Cycle Error: {cycle_e}"
                 logging.critical(f"CRITICAL UNHANDLED ERROR during cycle step '{current_step}': {cycle_e}", exc_info=True)
                 send_telegram_message(f"CRITICAL: {err_msg}. Check logs. Consider Gitpod fallback if persists.", level="CRITICAL")
                 try: current_state = load_state(STATE_FILE_PATH); current_state["last_error"] = err_msg; current_state["status"] = "error"; current_state["current_step"] = "idle"; save_state(current_state, STATE_FILE_PATH); logging.info("Set status=error, step=idle due to cycle error.")
                 except Exception as save_e: logging.error(f"Failed save error state after cycle error: {save_e}", exc_info=True); send_telegram_message("CRITICAL: Failed to save state after unhandled cycle error!", level="CRITICAL")

            cycle_end_time = datetime.now(timezone.utc)
            logging.info(f"--- Cycle End: {cycle_end_time.isoformat()} (Duration: {cycle_end_time - cycle_start_time}) ---")


        # --- Telegram Bot Setup ---

        async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            # ... (Function remains unchanged) ...
            user_id = update.effective_user.id; logging.info(f"Received /start command from user {user_id}"); reply_message = ""
            try:
                current_state = load_state(STATE_FILE_PATH); status = current_state.get("status", "unknown")
                if status == "running": reply_message = "Orchestrator is already running."; logging.info("Start command received but already running.")
                elif status == "stopping": reply_message = "Orchestrator is stopping. Please wait and try /start again."; logging.info("Start command received but stopping.")
                elif status in ["stopped", "stopped_exhausted", "error"]:
                    logging.info(f"Current status '{status}'. Setting status to 'running'."); current_state["status"] = "running"; current_state["last_error"] = None
                    if save_state(current_state, STATE_FILE_PATH): reply_message = "Orchestrator start initiated."; logging.info("Status set to 'running' by /start.")
                    else: reply_message = "ERROR: Failed to save state file. Check logs."; logging.error("Failed save state for /start.")
                else: reply_message = f"Cannot start from current status: '{status}'."; logging.warning(f"Start command in unexpected state: {status}")
            except Exception as e: logging.error(f"Error processing /start: {e}", exc_info=True); reply_message = "Internal error processing /start."
            if update.message: await update.message.reply_text(reply_message)
            elif update.callback_query: await update.callback_query.edit_message_text(reply_message)

        async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            # ... (Function remains unchanged) ...
            user_id = update.effective_user.id; logging.info(f"Received /status command from user {user_id}"); reply_message = "Failed to retrieve status."
            try:
                current_state = load_state(STATE_FILE_PATH)
                status = current_state.get("status", "Unknown"); step = current_state.get("current_step", "Unknown"); prompt = current_state.get("current_prompt", "N/A"); total_tracks = current_state.get("total_tracks_generated", 0); active_kaggle = current_state.get("active_kaggle_account_index", "N/A"); fallback = current_state.get("fallback_active", False); last_error = current_state.get("last_error", "None"); last_trigger_time_iso = current_state.get("last_kaggle_trigger_time"); last_trigger_time_str = "N/A"
                if last_trigger_time_iso: try: last_trigger_dt = datetime.fromisoformat(last_trigger_time_iso).astimezone(timezone.utc); last_trigger_time_str = last_trigger_dt.strftime('%Y-%m-%d %H:%M:%S UTC'); except ValueError: last_trigger_time_str = "Invalid timestamp"
                def escape_md(text):
                     if text is None: return 'N/A'; text = str(text); escape_chars = r'_*[]()~`>#+-=|{}.!'; return ''.join(f'\\{char}' if char in escape_chars else char for char in text)
                reply_message = ( f"*Orchestrator Status*\n" f"----------------------\n" f"*Status:* `{escape_md(status)}`\n" f"*Current Step:* `{escape_md(step)}`\n" f"*Total Tracks Generated:* `{escape_md(total_tracks)}`\n" f"*Active Kaggle Account:* `{escape_md(active_kaggle)}`\n" f"*Fallback Mode Active:* `{escape_md(fallback)}`\n" f"*Current Prompt:* `{escape_md(prompt)}`\n" f"*Last Kaggle Trigger:* `{escape_md(last_trigger_time_str)}`\n" f"*Last Error:* `{escape_md(last_error)}`" )
                logging.info(f"Reporting status: {status}, Step: {step}, Tracks: {total_tracks}")
            except Exception as e: logging.error(f"Error processing /status command: {e}", exc_info=True); reply_message = "Internal error retrieving status."
            if update.message: await update.message.reply_text(reply_message, parse_mode=ParseMode.MARKDOWN_V2)
            elif update.callback_query:
                 try: await update.callback_query.edit_message_text(reply_message, parse_mode=ParseMode.MARKDOWN_V2)
                 except Exception as e: logging.warning(f"Failed edit message for /status callback: {e}");
                 if update.effective_chat: await context.bot.send_message(chat_id=update.effective_chat.id, text=reply_message, parse_mode=ParseMode.MARKDOWN_V2)

        async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            # ... (Function remains unchanged) ...
            user_id = update.effective_user.id; logging.info(f"Received /stop command from user {user_id}"); reply_message = ""
            try:
                current_state = load_state(STATE_FILE_PATH); status = current_state.get("status", "unknown")
                if status == "running":
                    logging.info("Current status 'running'. Setting status to 'stopping'.")
                    current_state["status"] = "stopping"
                    if save_state(current_state, STATE_FILE_PATH): reply_message = "Orchestrator stop initiated. Will stop after current cycle."; logging.info("Status set to 'stopping' by /stop.")
                    else: reply_message = "ERROR: Failed to save state file."; logging.error("Failed save state for /stop.")
                elif status == "stopping": reply_message = "Orchestrator is already stopping."
                elif status == "stopped": reply_message = "Orchestrator is already stopped."
                elif status == "stopped_exhausted": reply_message = "Orchestrator stopped (quota). Use /start after reset."
                elif status == "error": reply_message = "Orchestrator in error state. Forcing status to 'stopped'."; current_state["status"] = "stopped"; save_state(current_state, STATE_FILE_PATH); logging.info("Status forced to 'stopped' from 'error' by /stop.")
                else: reply_message = f"Unknown status '{status}'. Forcing 'stopped'."; logging.warning(f"Stop command in unexpected state: {status}. Forcing 'stopped'."); current_state["status"] = "stopped"; save_state(current_state, STATE_FILE_PATH)
            except Exception as e: logging.error(f"Error processing /stop command: {e}", exc_info=True); reply_message = "Internal error processing /stop command."
            if update.message: await update.message.reply_text(reply_message)
            elif update.callback_query: await update.callback_query.edit_message_text(reply_message)

        async def usage_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            # ... (Function remains unchanged) ...
            user_id = update.effective_user.id; logging.info(f"Received /usage command from user {user_id}"); reply_message = "Failed to retrieve usage data."
            def escape_md(text):
                 if text is None: return 'N/A'; text = str(text); escape_chars = r'_*[]()~`>#+-=|{}.!'; return ''.join(f'\\{char}' if char in escape_chars else char for char in text)
            try:
                current_state = load_state(STATE_FILE_PATH); usage_list = current_state.get("kaggle_usage", []); active_index = current_state.get("active_kaggle_account_index", -1)
                if not isinstance(usage_list, list) or not usage_list: reply_message = "No Kaggle usage data found in state."
                else:
                    lines = ["*Kaggle GPU Usage Estimate*"]; lines.append("-------------------------")
                    quota_limit = KAGGLE_WEEKLY_GPU_QUOTA; buffer_limit = quota_limit * KAGGLE_USAGE_BUFFER
                    lines.append(f"Weekly Quota: `{escape_md(quota_limit)}` hours"); lines.append(f"Usage Buffer Limit: `{escape_md(f'{buffer_limit:.2f}')}` hours (`{escape_md(KAGGLE_USAGE_BUFFER * 100)}`%)"); lines.append("")
                    for i in range(len(usage_list)):
                        account_usage = usage_list[i]
                        if not isinstance(account_usage, dict): lines.append(f"*Account {i}:* `Invalid data`"); continue
                        used_hours = account_usage.get("gpu_hours_used_this_week", 0.0); remaining_hours = max(0.0, quota_limit - used_hours); remaining_buffered = max(0.0, buffer_limit - used_hours); last_reset_iso = account_usage.get("last_reset_time"); reset_time_str = "Never"; next_reset_str = "Unknown"
                        if last_reset_iso: try: last_reset_dt = datetime.fromisoformat(last_reset_iso).astimezone(timezone.utc); reset_time_str = last_reset_dt.strftime('%Y-%m-%d %H:%M UTC'); next_reset_dt = last_reset_dt + timedelta(days=7); next_reset_str = next_reset_dt.strftime('%Y-%m-%d %H:%M UTC'); except ValueError: reset_time_str = "Invalid timestamp"
                        active_marker = " ✅" if i == active_index else ""
                        lines.append(f"*Account {i}{active_marker}:*")
                        lines.append(f"  \\- Used: `{escape_md(f'{used_hours:.2f}')}` hours")
                        lines.append(f"  \\- Remaining \\(Buffered\\): `{escape_md(f'{remaining_buffered:.2f}')}` hours")
                        lines.append(f"  \\- Last Reset: `{escape_md(reset_time_str)}`")
                    reply_message = "\n".join(lines)
                logging.info("Reporting Kaggle usage.")
            except Exception as e: logging.error(f"Error processing /usage command: {e}", exc_info=True); reply_message = "Internal error retrieving usage data."
            if update.message: await update.message.reply_text(reply_message, parse_mode=ParseMode.MARKDOWN_V2)
            elif update.callback_query:
                 try: await update.callback_query.edit_message_text(reply_message, parse_mode=ParseMode.MARKDOWN_V2)
                 except Exception as e: logging.warning(f"Failed edit message for /usage callback: {e}");
                 if update.effective_chat: await context.bot.send_message(chat_id=update.effective_chat.id, text=reply_message, parse_mode=ParseMode.MARKDOWN_V2)

        async def logs_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            # ... (Function remains unchanged) ...
            user_id = update.effective_user.id; logging.info(f"Received /logs command from user {user_id}"); reply_message = "Failed to retrieve logs."; lines_to_fetch = 30
            def escape_md_code(text):
                 if not text: return ''; return text.replace('\\', '\\\\').replace('`', '\\`')
            try:
                if not os.path.exists(LOG_FILE_PATH): reply_message = f"Log file ({LOG_FILE_PATH}) not found."
                else:
                    try:
                        with open(LOG_FILE_PATH, 'r', encoding='utf-8') as f: lines = f.readlines()
                        recent_lines = lines[-lines_to_fetch:]
                        if not recent_lines: reply_message = "Log file is empty."
                        else:
                            log_content = "".join(recent_lines); escaped_log_content = escape_md_code(log_content)
                            header = f"*Last {len(recent_lines)} log entries:*\n"; formatted_log = f"```\n{escaped_log_content}\n```"; full_message = header + formatted_log
                            if len(full_message) > 4096:
                                 if len(formatted_log) <= 4096: reply_message = formatted_log
                                 else:
                                      truncate_at = 4096 - 10; truncated_log = escape_md_code("".join(lines[-(lines_to_fetch//2):]));
                                      if len(truncated_log) > truncate_at: truncated_log = escape_md_code(log_content)[:truncate_at] + "\n\\.\\.\\. \\(truncated\\)"
                                      reply_message = f"*Last log entries (truncated):*\n```\n{truncated_log}\n```"; logging.warning("Log content for /logs truncated.")
                            else: reply_message = full_message
                            logging.info(f"Sending last {len(recent_lines)} log lines.")
                    except Exception as read_e: logging.error(f"Error reading log file {LOG_FILE_PATH}: {read_e}", exc_info=True); reply_message = f"Error reading log file: {read_e}"
            except Exception as e: logging.error(f"Error processing /logs command: {e}", exc_info=True); reply_message = "Internal error processing /logs command."
            if update.message: await update.message.reply_text(reply_message, parse_mode=ParseMode.MARKDOWN_V2)
            elif update.callback_query:
                 try: await update.callback_query.edit_message_text(reply_message, parse_mode=ParseMode.MARKDOWN_V2)
                 except Exception as e: logging.warning(f"Failed edit message for /logs callback: {e}");
                 if update.effective_chat: await context.bot.send_message(chat_id=update.effective_chat.id, text=reply_message, parse_mode=ParseMode.MARKDOWN_V2)

        async def errors_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            # ... (Function remains unchanged) ...
            user_id = update.effective_user.id; logging.info(f"Received /errors command from user {user_id}"); reply_message = "Failed to retrieve errors from log."; max_errors_to_fetch = 20
            def escape_md_code(text):
                 if not text: return ''; return text.replace('\\', '\\\\').replace('`', '\\`')
            try:
                if not os.path.exists(LOG_FILE_PATH): reply_message = f"Log file ({LOG_FILE_PATH}) not found."
                else:
                    error_lines = []
                    try:
                        with open(LOG_FILE_PATH, 'r', encoding='utf-8') as f:
                            all_lines = f.readlines()
                            for line in reversed(all_lines):
                                if "ERROR" in line or "CRITICAL" in line: error_lines.append(line);
                                if len(error_lines) >= max_errors_to_fetch: break
                            error_lines.reverse()
                        if not error_lines: reply_message = "No ERROR or CRITICAL messages found in recent logs."
                        else:
                            log_content = "".join(error_lines); escaped_log_content = escape_md_code(log_content)
                            header = f"*Last {len(error_lines)} ERROR/CRITICAL log entries:*\n"; formatted_log = f"```\n{escaped_log_content}\n```"; full_message = header + formatted_log
                            if len(full_message) > 4096:
                                 if len(formatted_log) <= 4096: reply_message = formatted_log
                                 else:
                                      truncate_at = 4096 - 10; truncated_log = escape_md_code("".join(error_lines[-(max_errors_to_fetch//2):]));
                                      if len(truncated_log) > truncate_at: truncated_log = escape_md_code(log_content)[:truncate_at] + "\n\\.\\.\\. \\(truncated\\)"
                                      reply_message = f"*Last ERROR/CRITICAL entries (truncated):*\n```\n{truncated_log}\n```"; logging.warning("Error log content for /errors truncated.")
                            else: reply_message = full_message
                            logging.info(f"Sending last {len(error_lines)} error/critical log lines.")
                    except Exception as read_e: logging.error(f"Error reading log file {LOG_FILE_PATH} for errors: {read_e}", exc_info=True); reply_message = f"Error reading log file: {read_e}"
            except Exception as e: logging.error(f"Error processing /errors command: {e}", exc_info=True); reply_message = "Internal error processing /errors command."
            if update.message: await update.message.reply_text(reply_message, parse_mode=ParseMode.MARKDOWN_V2)
            elif update.callback_query:
                 try: await update.callback_query.edit_message_text(reply_message, parse_mode=ParseMode.MARKDOWN_V2)
                 except Exception as e: logging.warning(f"Failed edit message for /errors callback: {e}");
                 if update.effective_chat: await context.bot.send_message(chat_id=update.effective_chat.id, text=reply_message, parse_mode=ParseMode.MARKDOWN_V2)

        async def restart_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            # ... (Function remains unchanged) ...
            user_id = update.effective_user.id; logging.info(f"Received /restart command from user {user_id}"); reply_message = ""
            try:
                current_state = load_state(STATE_FILE_PATH); status = current_state.get("status", "unknown"); stop_initiated = False
                if status == "running":
                    logging.info("Restart: Setting status to 'stopping'."); current_state["status"] = "stopping"
                    if save_state(current_state, STATE_FILE_PATH): stop_initiated = True; logging.info("Restart: Status set to 'stopping'."); await update.message.reply_text("Restart initiated: Stopping current process...") ; await asyncio.sleep(5)
                    else: reply_message = "ERROR: Failed save state for stop phase."; logging.error("Restart: Failed save 'stopping' state."); await update.message.reply_text(reply_message); return
                elif status in ["stopping", "stopped", "stopped_exhausted", "error"]: logging.info(f"Restart: Status already '{status}'. Proceeding to start."); stop_initiated = True; await update.message.reply_text(f"Restart: Orchestrator already {status}. Attempting start...")
                else: reply_message = f"Restart: Unknown status '{status}'. Forcing 'stopped'."; logging.warning(f"Restart in unexpected state: {status}. Forcing 'stopped'."); current_state["status"] = "stopped"; save_state(current_state, STATE_FILE_PATH); stop_initiated = True
                if stop_initiated:
                    current_state = load_state(STATE_FILE_PATH); status = current_state.get("status", "unknown")
                    if status in ["stopped", "stopped_exhausted", "error"]:
                        logging.info(f"Restart: Current status '{status}'. Setting status to 'running'."); current_state["status"] = "running"; current_state["last_error"] = None
                        if save_state(current_state, STATE_FILE_PATH): reply_message = "Restart complete: Orchestrator status set to 'running'."; logging.info("Restart: Status set to 'running'.")
                        else: reply_message = "ERROR: Failed save state for start phase."; logging.error("Restart: Failed save 'running' state.")
                    elif status == "running": reply_message = "Restart: Orchestrator already running (stop might not have completed?)."; logging.warning("Restart: Status 'running' during start phase.")
                    elif status == "stopping": reply_message = "Restart: Orchestrator still stopping. Try /start manually."; logging.warning("Restart: Status 'stopping' during start phase.")
                    else: reply_message = f"Restart Error: Unexpected status '{status}' after stop attempt."; logging.error(f"Restart: Unexpected status '{status}' during start phase.")
            except Exception as e: logging.error(f"Error processing /restart command: {e}", exc_info=True); reply_message = "Internal error processing /restart command."
            if update.message: await update.message.reply_text(reply_message)
            elif update.callback_query: await update.callback_query.edit_message_text(reply_message)

        async def restart_task_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            # ... (Function remains unchanged) ...
            user_id = update.effective_user.id; logging.info(f"Received /restart_task command from user {user_id}"); reply_message = ""
            try:
                current_state = load_state(STATE_FILE_PATH); status = current_state.get("status", "unknown"); current_step = current_state.get("current_step", "idle"); last_error = current_state.get("last_error")
                if status == "running": reply_message = "Orchestrator running. Restarting task might interrupt."; logging.warning("Restart_task called while running.")
                elif status != "error": reply_message = f"Orchestrator status '{status}'. Restarting task might not be effective."; logging.warning(f"Restart_task called while status is '{status}'.")
                reset_step_to = "idle"; error_lower = last_error.lower() if last_error else ""
                if last_error:
                     if "status check" in error_lower or "kaggle run is still" in current_step: reset_step_to = "kaggle_running"
                     elif "download" in error_lower: reset_step_to = "kaggle_running"
                     elif "processing" in error_lower or "upload" in error_lower or "uniqueness" in error_lower or "fingerprint" in error_lower:
                          if current_state.get("last_downloaded_mp3") and current_state.get("last_downloaded_json"): reset_step_to = "processing_output"
                          else: logging.warning("Cannot restart processing: files missing."); reset_step_to = "idle"
                     elif "trigger" in error_lower or "setup failed" in error_lower: reset_step_to = "idle"
                     else: reset_step_to = "idle"
                elif current_step != "idle": reply_message += f"\nNo specific error, resetting to '{reset_step_to}'."; reset_step_to = "idle"
                else: reply_message += "\nAlready idle."; reset_step_to = "idle"
                logging.info(f"Restarting task: Resetting step to '{reset_step_to}', clearing error/retry.")
                current_state["current_step"] = reset_step_to; current_state["retry_count"] = 0; current_state["last_error"] = None
                if status == "error": current_state["status"] = "running"; logging.info("Setting status to 'running' from 'error'.")
                if save_state(current_state, STATE_FILE_PATH): reply_message += f"\nTask restart initiated. State reset to step '{reset_step_to}'."; logging.info(f"State reset to step '{reset_step_to}' by /restart_task.")
                else: reply_message = "ERROR: Failed to save state file. Check logs."; logging.error("Failed save state for /restart_task.")
            except Exception as e: logging.error(f"Error processing /restart_task command: {e}", exc_info=True); reply_message = "Internal error processing /restart_task command."
            if update.message: await update.message.reply_text(reply_message)
            elif update.callback_query: await update.callback_query.edit_message_text(reply_message)

        async def exit_fallback_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            logging.info(f"Received /exit_fallback command from user {update.effective_user.id}")
            await update.message.reply_text('Exit Fallback command received. Implementation pending (Task 6.7.5).')

        async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None: ## <<< MODIFIED >>> ##
            """Handles button presses (CallbackQuery)."""
            query = update.callback_query
            await query.answer() # Answer the callback query first
            callback_data = query.data
            user_id = update.effective_user.id
            logging.info(f"Received button press data: {callback_data} from user {user_id}")

            # --- Handle Callback Data ---
            try:
                current_state = load_state(STATE_FILE_PATH)
                action_taken = False
                new_reply_text = f"Processing action: {callback_data}..." # Initial reply
                state_modified = False # Flag to track if state needs saving

                if callback_data == CALLBACK_RETRY_OPERATION:
                    logging.info("Button: Handling retry operation...")
                    current_state["retry_count"] = 0; current_state["last_error"] = None; current_state["intervention_pending_since"] = None
                    if current_state["status"] == "error": current_state["status"] = "running"
                    action_taken = True; state_modified = True
                    new_reply_text = "Retry initiated. Cleared last error and retry count."

                elif callback_data == CALLBACK_SKIP_STEP:
                    logging.info("Button: Handling skip step...")
                    current_state["current_step"] = "idle"; current_state["retry_count"] = 0; current_state["last_error"] = "Step skipped by user."; current_state["intervention_pending_since"] = None
                    if current_state["status"] == "error": current_state["status"] = "running"
                    action_taken = True; state_modified = True
                    new_reply_text = "Skip initiated. Current step set to idle."
                    try:
                        dl_mp3 = current_state.get("last_downloaded_mp3"); dl_json = current_state.get("last_downloaded_json")
                        if dl_mp3 and os.path.exists(dl_mp3): os.remove(dl_mp3); logging.info(f"Cleaned up {dl_mp3} on skip.")
                        if dl_json and os.path.exists(dl_json): os.remove(dl_json); logging.info(f"Cleaned up {dl_json} on skip.")
                        current_state["last_downloaded_mp3"] = None; current_state["last_downloaded_json"] = None
                    except OSError as e: logging.warning(f"Error cleaning up files during skip: {e}")

                elif callback_data == CALLBACK_ROTATE_ACCOUNT:
                    logging.info("Button: Handling rotate account...")
                    original_index = current_state.get("active_kaggle_account_index", "N/A")
                    current_state = rotate_kaggle_account(current_state, reason="Manual Rotation via Button")
                    if current_state["status"] == "error": current_state["status"] = "running"
                    current_state["intervention_pending_since"] = None # Clear intervention flag
                    action_taken = True # State saved within rotate function
                    new_reply_text = f"Account rotation initiated. Switched from {original_index} to {current_state.get('active_kaggle_account_index')}."

                elif callback_data == CALLBACK_CHECK_DRIVE:
                     logging.info("Button: Handling Check Drive...")
                     action_taken = True; state_modified = False
                     new_reply_text = "Attempting to check Google Drive connection..."
                     await query.edit_message_text(text=new_reply_text)
                     gdrive_service_check = retry_operation(authenticate_gdrive, max_retries=1, delay_seconds=2, operation_name="Manual GDrive Check")
                     if gdrive_service_check:
                          def gdrive_about_call(): return gdrive_service_check.about().get(fields='user(displayName)').execute()
                          about_info = retry_operation(gdrive_about_call, max_retries=0, operation_name="Manual GDrive About Call")
                          if about_info:
                               user_name = about_info.get("user", {}).get("displayName", "Unknown User")
                               new_reply_text = f"Google Drive check successful! Connected as: {user_name}"
                               logging.info("Manual Google Drive check successful.")
                          else: new_reply_text = "Google Drive check failed: Could not retrieve account info."; logging.error("Manual GDrive check failed (about call).")
                     else: new_reply_text = "Google Drive check failed: Authentication failed."; logging.error("Manual GDrive check failed (auth).")

                elif callback_data == CALLBACK_VIEW_STATE:
                     logging.info("Button: Handling View State...")
                     action_taken = True; state_modified = False
                     try:
                          state_to_view = current_state.copy(); state_to_view.pop("recent_fingerprints", None)
                          state_str = json.dumps(state_to_view, indent=2)
                          def escape_md_code(text): return text.replace('\\', '\\\\').replace('`', '\\`')
                          escaped_state = escape_md_code(state_str)
                          if len(escaped_state) > 4000: escaped_state = escaped_state[:4000] + "\n... (truncated)"
                          new_reply_text_state = f"*Current State:*\n```json\n{escaped_state}\n```"
                          await context.bot.send_message(chat_id=query.message.chat_id, text=new_reply_text_state, parse_mode=ParseMode.MARKDOWN_V2)
                          new_reply_text = "Current state sent as a new message."
                     except Exception as view_e: logging.error(f"Error formatting/sending state: {view_e}", exc_info=True); new_reply_text = "Error retrieving/formatting state."

                else:
                    logging.warning(f"Unknown callback data received: {callback_data}")
                    new_reply_text = f"Unknown action: {callback_data}"

                # Save state if modified by the action (and not already saved)
                if state_modified:
                    if not save_state(current_state, STATE_FILE_PATH):
                        logging.error("Failed to save state after button action!")
                        new_reply_text += "\nERROR: Failed to save state!"

                # Edit the original message unless we sent a new one
                if callback_data != CALLBACK_VIEW_STATE:
                     await query.edit_message_text(text=new_reply_text)

            except Exception as e:
                logging.error(f"Error processing button callback {callback_data}: {e}", exc_info=True)
                try: await query.edit_message_text(text=f"Error processing action: {callback_data}. Check logs.")
                except Exception as edit_e: logging.error(f"Failed to edit message after button processing error: {edit_e}")


            # --- Orchestrator Loop Function ---

            def run_orchestrator_loop():
                # ... (Function remains unchanged) ...
                logging.info("Starting AI Music Orchestrator main loop thread...")
                gdrive_service = None
                try:
                    gdrive_service = retry_operation( authenticate_gdrive, max_retries=3, delay_seconds=10, allowed_exceptions=(RefreshError, requests.exceptions.RequestException, socket.timeout, TimeoutError, HttpError), operation_name="Initial Google Drive Authentication" )
                    if not gdrive_service: logging.critical("GDrive Auth failed in orchestrator thread. Thread exiting."); send_telegram_message("CRITICAL: GDrive Auth failed in main loop thread.", level="CRITICAL"); return
                    else: logging.info("GDrive authenticated successfully in orchestrator thread.")
                except Exception as auth_e: logging.critical(f"Critical error during GDrive auth in thread: {auth_e}", exc_info=True); send_telegram_message(f"CRITICAL: Unhandled error during GDrive Auth in thread: {auth_e}", level="CRITICAL"); return
                state = load_state(STATE_FILE_PATH)
                logging.info(f"Orchestrator thread starting loop. Initial Status='{state.get('status', 'N/A')}'")
                while not _shutdown_requested:
                    try:
                        current_state_check = load_state(STATE_FILE_PATH); status = current_state_check.get("status")
                        if status == "stopped" or status == "stopping":
                            logging.info(f"Orchestrator thread detected status '{status}'. Exiting loop.")
                            if status == "stopping": current_state_check["status"] = "stopped"; save_state(current_state_check, STATE_FILE_PATH)
                            break
                        if status == "running": run_main_cycle(gdrive_service)
                        elif status == "stopped_exhausted": logging.info("Orchestrator thread: Status is stopped_exhausted. Sleeping.")
                        elif status == "error": logging.error("Orchestrator thread: Status is error. Sleeping.") # Timeout check now happens in run_main_cycle
                        else: logging.warning(f"Orchestrator thread: Unknown status '{status}'. Sleeping.")
                        sleep_time = MAIN_LOOP_SLEEP_SECONDS if status == "running" else 60
                        logging.debug(f"Orchestrator thread sleeping for {sleep_time} seconds...")
                        for _ in range(int(sleep_time)):
                             if _shutdown_requested: logging.info("Orchestrator thread received shutdown during sleep. Exiting loop."); break
                             time.sleep(1)
                        if _shutdown_requested: break
                    except Exception as e:
                        logging.critical(f"CRITICAL UNHANDLED ERROR in orchestrator loop thread: {e}", exc_info=True)
                        send_telegram_message(f"CRITICAL: Unhandled error in orchestrator thread: {e}", level="CRITICAL")
                        try: state = load_state(STATE_FILE_PATH); state["status"] = "error"; state["last_error"] = f"Orchestrator Thread Error: {e}"; save_state(state, STATE_FILE_PATH); logging.info("Set status to 'error' due to thread error.")
                        except Exception as save_e: logging.error(f"Failed save error state after thread error: {save_e}", exc_info=True); send_telegram_message("CRITICAL: Failed to save state after orchestrator thread crash!", level="CRITICAL")
                        logging.info("Orchestrator thread attempting continue loop after 1 min delay...")
                        time.sleep(60)
                logging.info("AI Music Orchestrator main loop thread finished.")


            # --- Main Function (Entry Point & Telegram Bot Runner) ---

            def main() -> None:
                global _shutdown_requested
                logging.info("Starting AI Music Orchestrator main process...")
                send_telegram_message("Orchestrator script starting up.", level="INFO")
                try:
                    state = load_state(STATE_FILE_PATH)
                    logging.info(f"Initial state: Status='{state.get('status', 'N/A')}', Step='{state.get('current_step', 'N/A')}'")
                    initial_status = state.get("status")
                    if initial_status == "stopping": logging.warning(f"Initial status 'stopping'. Setting 'stopped'."); state["status"] = "stopped"; save_state(state, STATE_FILE_PATH)
                    elif initial_status not in ["running", "stopped", "stopped_exhausted", "error"]: logging.warning(f"Initial status '{initial_status}' invalid. Setting 'stopped'."); state["status"] = "stopped"; save_state(state, STATE_FILE_PATH)
                except Exception as state_init_e: logging.critical(f"Failed load/init state: {state_init_e}", exc_info=True); send_telegram_message("CRITICAL: Failed load/init state!", level="CRITICAL"); sys.exit(1)
                try:
                    gitpod_workspace_id = os.environ.get('GITPOD_WORKSPACE_ID')
                    if gitpod_workspace_id:
                        logging.warning("Gitpod environment detected.");
                        if not state.get("fallback_active"): logging.info("Setting fallback_active=True."); state["fallback_active"] = True; save_state(state, STATE_FILE_PATH); send_telegram_message("INFO: Script started in Gitpod fallback.", level="INFO")
                        else: logging.info("fallback_active already True.")
                    else:
                        if state.get("fallback_active"): logging.warning("Not in Gitpod, but fallback_active=True. Setting False."); state["fallback_active"] = False; save_state(state, STATE_FILE_PATH)
                except Exception as gitpod_check_e: logging.error(f"Error during Gitpod check: {gitpod_check_e}", exc_info=True)
                logging.info("Creating and starting orchestrator loop thread...")
                orchestrator_thread = threading.Thread(target=run_orchestrator_loop, daemon=True)
                orchestrator_thread.start()
                logging.info("Orchestrator thread started.")
                token = os.environ.get('TELEGRAM_BOT_TOKEN')
                if not token: logging.critical("TELEGRAM_BOT_TOKEN missing. Cannot start bot. Exiting."); sys.exit(1)
                application = None
                try:
                    logging.info("Setting up Telegram bot application...")
                    application = ApplicationBuilder().token(token).build()
                    # Register command handlers
                    application.add_handler(CommandHandler("start", start_command))
                    application.add_handler(CommandHandler("status", status_command))
                    application.add_handler(CommandHandler("stop", stop_command))
                    application.add_handler(CommandHandler("usage", usage_command))
                    application.add_handler(CommandHandler("logs", logs_command))
                    application.add_handler(CommandHandler("errors", errors_command))
                    application.add_handler(CommandHandler("restart", restart_command))
                    application.add_handler(CommandHandler("restart_task", restart_task_command))
                    application.add_handler(CommandHandler("exit_fallback", exit_fallback_command))
                    # Register button handler
                    application.add_handler(CallbackQueryHandler(button_handler))
                    logging.info("Starting Telegram bot polling...")
                    application.run_polling(allowed_updates=Update.ALL_TYPES)
                except telegram.error.InvalidToken: logging.critical("Invalid Telegram Bot Token. Exiting."); _shutdown_requested = True
                except Exception as bot_e: logging.critical(f"Unhandled error in Telegram bot setup/polling: {bot_e}", exc_info=True); send_telegram_message(f"CRITICAL: Unhandled error running Telegram bot: {bot_e}", level="CRITICAL"); _shutdown_requested = True
                logging.info("Telegram bot polling stopped or failed.")
                _shutdown_requested = True
                logging.info("Waiting for orchestrator thread to finish...")
                orchestrator_thread.join(timeout=MAIN_LOOP_SLEEP_SECONDS + 30)
                if orchestrator_thread.is_alive(): logging.warning("Orchestrator thread did not exit cleanly.")
                logging.info("AI Music Orchestrator main process finished.")
                send_telegram_message("Orchestrator script stopped.", level="INFO")

            # --- Script Entry Point ---
            if __name__ == "__main__":
                try:
                    main()
                except KeyboardInterrupt: logging.info("KeyboardInterrupt caught at top level. Exiting."); sys.exit(0)
                except Exception as top_level_e:
                     logging.critical(f"Top-level unhandled exception: {top_level_e}", exc_info=True)
                     try: send_telegram_message(f"CRITICAL: Top-level unhandled exception: {top_level_e}", level="CRITICAL")
                     except Exception: logging.error("Failed send final critical error message via Telegram.")
                     sys.exit(1)

