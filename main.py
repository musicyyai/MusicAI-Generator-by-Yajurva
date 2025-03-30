                # main.py (Updated for Step 6.6.1 - Health Checks)

                import os
                import json
                import time
                import logging
                import sys
                import requests
                import subprocess
                from datetime import datetime, timedelta, timezone
                import random

                # Imports from utils and config
                from utils import ( load_state, save_state, authenticate_gdrive, upload_to_gdrive, setup_kaggle_api, trigger_kaggle_notebook, download_kaggle_output, check_kaggle_status, get_spotify_trending_keywords, is_unique_enough, get_gdrive_files, delete_gdrive_file, load_style_profile, save_style_profile, retry_operation, send_telegram_message )
                from config import ( GDRIVE_BACKUP_FOLDER_ID, PROMPT_GENRES, PROMPT_INSTRUMENTS, PROMPT_MOODS, PROMPT_TEMPLATES, UNIQUENESS_CHECK_ENABLED, UNIQUENESS_FINGERPRINT_COUNT, UNIQUENESS_SIMILARITY_THRESHOLD, NUM_KAGGLE_ACCOUNTS, MAX_DRIVE_FILES, MAX_DRIVE_FILE_AGE_DAYS, STYLE_PROFILE_RESET_TRACK_COUNT, ESTIMATED_KAGGLE_RUN_HOURS, KAGGLE_WEEKLY_GPU_QUOTA, KAGGLE_USAGE_BUFFER, HEALTH_CHECK_INTERVAL_MINUTES # <<< ADDED
                )

                # --- Logging Configuration ---
                LOG_FILE_PATH = "system_log.txt"; LOG_FORMAT = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
                logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, handlers=[ logging.FileHandler(LOG_FILE_PATH, encoding='utf-8'), logging.StreamHandler(sys.stdout) ])
                logging.info("Logging configured.")

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

                # --- Default State Definition --- ## <<< MODIFIED >>> ##
                DEFAULT_STATE = { "status": "stopped", "active_kaggle_account_index": 0, "active_drive_account_index": 0, "current_step": "idle", "current_prompt": None, "last_kaggle_run_id": None, "last_kaggle_trigger_time": None, "last_downloaded_mp3": None, "last_downloaded_json": None, "retry_count": 0, "total_tracks_generated": 0, "style_profile_id": "default", "fallback_active": False, "kaggle_usage": [{"account_index": i, "gpu_hours_used_this_week": 0.0, "last_reset_time": None} for i in range(NUM_KAGGLE_ACCOUNTS)], "last_error": None, "_checksum": None, "recent_fingerprints": [], "last_gdrive_cleanup_time": None, "last_health_check_time": None # <<< ADDED
                }
                STATE_FILE_PATH = "state.txt"

                # --- Constants ---
                KAGGLE_NOTEBOOK_SLUG = "musicyyai/notebook63936fc364"; GDRIVE_CLEANUP_INTERVAL_HOURS = 24; MAIN_LOOP_SLEEP_SECONDS = 60 * 5; BACKUP_INTERVAL_MINUTES = 60

                # --- Global variable for graceful shutdown ---
                _shutdown_requested = False

                # --- Helper Functions ---
                def rotate_kaggle_account(current_state, reason="Unknown"):
                    # ... (Function remains unchanged) ...
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
                    # ... (Function content remains unchanged) ...
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

                # --- Main Application Logic ---
                _last_backup_time = None

                def run_main_cycle(gdrive_service):
                    global _shutdown_requested, _last_backup_time
                    cycle_start_time = datetime.now(timezone.utc)
                    logging.info(f"--- Cycle Start: {cycle_start_time.isoformat()} ---")
                    current_state = load_state(STATE_FILE_PATH)

                    # --- Check for Weekly Kaggle Quota Reset ---
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
                    # --- End Weekly Reset Check ---

                    status = current_state.get("status", "error") # Reload status

                    # --- Shutdown/Status Checks ---
                    if _shutdown_requested: logging.info("Shutdown requested."); current_state["status"] = "stopped"; save_state(current_state, STATE_FILE_PATH); logging.info("Status set 'stopped'."); return
                    if status == "stopped": logging.info("Status 'stopped'. Cycle skipped."); return
                    if status == "stopping": logging.info("Status 'stopping'."); current_state["status"] = "stopped"; save_state(current_state, STATE_FILE_PATH); logging.info("Status set 'stopped'."); return
                    if status == "stopped_exhausted": logging.info("Status 'stopped_exhausted'. Cycle skipped."); return
                    if status == "error": logging.error("Status 'error'. Investigate logs. Cycle skipped."); return
                    if status != "running": logging.error(f"Unknown status '{status}'. Setting 'stopped'."); current_state["status"] = "stopped"; save_state(current_state, STATE_FILE_PATH); return

                    # --- Periodic Backup ---
                    now_dt = datetime.now(timezone.utc) # Use now_dt defined earlier
                    run_backup = False
                    if _last_backup_time is None or now_dt - _last_backup_time >= timedelta(minutes=BACKUP_INTERVAL_MINUTES): run_backup = True
                    if run_backup and gdrive_service:
                        logging.info(f"Performing periodic backup..."); timestamp = now_dt.strftime("%Y%m%d_%H%M%S"); state_backup_filename = f"state_{timestamp}.json"
                        if save_state(current_state, STATE_FILE_PATH): logging.info("State saved locally."); upload_to_gdrive(gdrive_service, STATE_FILE_PATH, GDRIVE_BACKUP_FOLDER_ID, state_backup_filename)
                        else: logging.error("Failed save state locally before backup.")
                        log_backup_filename = f"system_log_{timestamp}.txt"
                        if os.path.exists(LOG_FILE_PATH):
                            for handler in logging.getLogger().handlers: handler.flush()
                            upload_to_gdrive(gdrive_service, LOG_FILE_PATH, GDRIVE_BACKUP_FOLDER_ID, log_backup_filename)
                        else: logging.warning(f"Log file {LOG_FILE_PATH} not found.")
                        _last_backup_time = now_dt
                    elif run_backup: logging.error("Backup interval reached, GDrive unavailable.")

                    # --- Periodic Google Drive Cleanup Trigger ---
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

                    # --- Periodic Health Checks --- <<< ADDED BLOCK >>>
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
                        # 1. GDrive Check
                        if gdrive_service:
                            logging.debug("Health Check: Checking GDrive API..."); def gdrive_about_call(): return gdrive_service.about().get(fields='storageQuota').execute()
                            gdrive_check_result = retry_operation(gdrive_about_call, max_retries=1, delay_seconds=5, operation_name="GDrive Health Check")
                            if gdrive_check_result is None: logging.error("Health Check FAILED: Google Drive API."); send_telegram_message("ERROR: Health Check FAILED for Google Drive API.", level="ERROR"); all_checks_ok = False
                            else: logging.info("Health Check OK: Google Drive API.")
                        else: logging.warning("Health Check SKIPPED: GDrive service unavailable."); all_checks_ok = False
                        # 2. Kaggle Check
                        logging.debug("Health Check: Checking Kaggle API...");
                        def kaggle_list_call():
                             hc_kaggle_idx = current_state.get("active_kaggle_account_index", 0) # Use current index for check
                             if not setup_kaggle_api(hc_kaggle_idx): logging.error(f"Health Check FAILED: Kaggle setup index {hc_kaggle_idx}"); return None
                             command = ["kaggle", "kernels", "list", "-m", "-p", "1"]; result = subprocess.run(command, capture_output=True, text=True, check=False, timeout=30)
                             if result.returncode != 0: logging.error(f"Kaggle health check cmd failed. Code: {result.returncode}. Stderr: {result.stderr.strip()}"); return None
                             return True
                        kaggle_check_result = retry_operation(kaggle_list_call, max_retries=1, delay_seconds=5, operation_name="Kaggle Health Check")
                        if kaggle_check_result is None: logging.error("Health Check FAILED: Kaggle API."); send_telegram_message("ERROR: Health Check FAILED for Kaggle API.", level="ERROR"); all_checks_ok = False
                        else: logging.info("Health Check OK: Kaggle API.")
                        # 3. Telegram Check
                        logging.debug("Health Check: Checking Telegram API...")
                        telegram_check_result = retry_operation( send_telegram_message, args=("Health Check Ping.",), kwargs={"level": "DEBUG"}, max_retries=1, delay_seconds=5, operation_name="Telegram Health Check" )
                        if not telegram_check_result: logging.error("Health Check FAILED: Telegram API."); all_checks_ok = False # Cannot send TG msg about TG failure
                        else: logging.info("Health Check OK: Telegram API.")
                        # Update last check time
                        current_state["last_health_check_time"] = now_dt.isoformat(); save_state(current_state, STATE_FILE_PATH)
                        if all_checks_ok: logging.info("All health checks passed.")
                        else: logging.warning("One or more health checks failed.")
                    # --- End Health Checks ---

                    # --- Main Task Execution ---
                    logging.info("Starting main task execution...")
                    current_step = current_state.get("current_step", "idle")
                    active_kaggle_index = current_state.get("active_kaggle_account_index", 0) # Get index again after potential health check rotation? No, health check doesn't rotate.

                    try:
                        if current_step == "idle":
                            # ... (idle step logic including quota check remains unchanged) ...
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
                            else: err_msg = "Failed to trigger Kaggle run (retries exhausted)"; logging.error("Failed initiate Kaggle run after retries."); current_state["last_error"] = err_msg; send_telegram_message(f"ERROR: {err_msg}. Rotating.", level="ERROR"); current_state = rotate_kaggle_account(current_state, reason="Trigger Failure"); return

                        elif current_step == "kaggle_running":
                            # ... (kaggle_running logic remains unchanged) ...
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
                                else: err_msg = "Failed download Kaggle output (retries exhausted)"; logging.error("Download failed after retries."); current_state["last_error"] = err_msg; current_state["current_step"] = "idle"; send_telegram_message(f"ERROR: {err_msg}. Rotating.", level="ERROR"); current_state = rotate_kaggle_account(current_state, reason="Download Failure"); return
                            elif run_status in ["error", "cancelled"]: logging.error(f"Kaggle run failed: {run_status}"); current_state["last_error"] = f"Kaggle run failed: {run_status}"; current_state["current_step"] = "idle"; save_state(current_state, STATE_FILE_PATH); send_telegram_message(f"WARNING: Kaggle run {KAGGLE_NOTEBOOK_SLUG} finished with status: {run_status}", level="WARNING")
                            elif run_status in ["running", "queued"]: logging.info(f"Kaggle run still {run_status}.")
                            else: err_msg = "Failed Kaggle status check (retries exhausted)"; logging.error("Failed get Kaggle status after retries."); current_state["last_error"] = err_msg; send_telegram_message(f"ERROR: {err_msg}. Rotating.", level="ERROR"); current_state = rotate_kaggle_account(current_state, reason="Status Check Failure"); return

                        elif current_step == "processing_output":
                            # ... (processing_output logic remains unchanged) ...
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
                                                else: err_msg = "GDrive upload failed (retries exhausted)"; logging.error("GDrive upload failed after retries."); current_state["last_error"] = err_msg; send_telegram_message(f"ERROR: {err_msg}.", level="ERROR")
                                            else: err_msg = "Cannot upload to GDrive - Service/MP3 missing."; logging.error(err_msg); current_state["last_error"] = err_msg; send_telegram_message(f"ERROR: {err_msg}", level="ERROR")
                                        except Exception as upload_err: logging.error(f"Error during upload setup/call: {upload_err}", exc_info=True); current_state["last_error"] = "GDrive Filename/Upload Error"
                                    else: logging.info("Skipping GDrive upload.")
                                    if proceed_with_upload and upload_success and analysis_data:
                                        logging.info("Updating style profile...")
                                        try:
                                            style_profile = load_style_profile(); profile_updated = False; MAX_HISTORY = 20
                                            current_bpm = analysis_data.get("estimated_bpm")
                                            if current_bpm and isinstance(current_bpm, (int, float)): recent_bpms = style_profile.get("recent_bpms", []); recent_bpms.append(round(current_bpm)); style_profile["recent_bpms"] = recent_bpms[-MAX_HISTORY:]; profile_updated = True; logging.debug(f"Added BPM {round(current_bpm)}.")
                                            current_key = analysis_data.get("estimated_key")
                                            if current_key and isinstance(current_key, str): recent_keys = style_profile.get("recent_keys", []); recent_keys.append(current_key); style_profile["recent_keys"] = recent_keys[-MAX_HISTORY:]; profile_updated = True; logging.debug(f"Added Key {current_key}.")
                                            if profile_updated: style_profile["last_updated"] = datetime.now(timezone.utc).isoformat();
                                            if save_style_profile(style_profile): logging.info("Saved updated style profile.")
                                            else: logging.error("Failed save updated style profile.")
                                            else: logging.info("No new data to update style profile.")
                                        except Exception as style_e: logging.error(f"Error updating style profile: {style_e}", exc_info=True)
                                    SUCCESSFUL_RUNS_BEFORE_ROTATION = 25
                                    if upload_success and current_state["total_tracks_generated"] > 0 and current_state["total_tracks_generated"] % (SUCCESSFUL_RUNS_BEFORE_ROTATION * NUM_KAGGLE_ACCOUNTS) == 0: logging.info(f"Reached {current_state['total_tracks_generated']} tracks. Scheduled rotation."); current_state = rotate_kaggle_account(current_state, reason=f"Scheduled rotation")
                                    logging.info("Cleaning up downloaded files...")
                                    files_to_remove = [downloaded_mp3, downloaded_json]
                                    for f_path in files_to_remove:
                                        if f_path and os.path.exists(f_path): try: os.remove(f_path); logging.info(f"Removed: {f_path}"); except OSError as rm_e: logging.warning(f"Error removing {f_path}: {rm_e}", exc_info=True)
                                        elif f_path: logging.warning(f"File {f_path} not found for cleanup.")
                                    current_state["current_step"] = "idle"; current_state["last_downloaded_mp3"] = None; current_state["last_downloaded_json"] = None; current_state["current_prompt"] = None
                                    if (proceed_with_upload and upload_success) or not proceed_with_upload:
                                         if current_state.get("last_error") not in ["Discarded: Track too similar", "GDrive upload failed (retries exhausted)"]: current_state["last_error"] = None
                                    save_state(current_state, STATE_FILE_PATH); logging.info("Processing complete. State reset to idle.")
                                except json.JSONDecodeError as json_e: logging.error(f"Failed decode results JSON '{downloaded_json}': {json_e}", exc_info=True); current_state["current_step"] = "idle"; current_state["last_error"] = "Failed decode results JSON"; if downloaded_json and os.path.exists(downloaded_json): os.remove(downloaded_json); if downloaded_mp3 and os.path.exists(downloaded_mp3): os.remove(downloaded_mp3); save_state(current_state, STATE_FILE_PATH)
                                except Exception as proc_e: logging.critical(f"CRITICAL error during output processing: {proc_e}", exc_info=True); current_state["current_step"] = "idle"; current_state["last_error"] = f"Processing error: {proc_e}"; try: if downloaded_mp3 and os.path.exists(downloaded_mp3): os.remove(downloaded_mp3); if downloaded_json and os.path.exists(downloaded_json): os.remove(downloaded_json); except OSError as rm_e: logging.warning(f"Error cleaning files after processing error: {rm_e}", exc_info=True); save_state(current_state, STATE_FILE_PATH)
                            else: logging.error("Downloaded files missing."); current_state["current_step"] = "idle"; current_state["last_error"] = "Downloaded files missing"; current_state["last_downloaded_mp3"] = None; current_state["last_downloaded_json"] = None; save_state(current_state, STATE_FILE_PATH)

                        else: # Unknown step
                            logging.warning(f"Unknown step: '{current_step}'. Resetting."); current_state["current_step"] = "idle"; current_state["last_error"] = f"Unknown step: {current_step}"; save_state(current_state, STATE_FILE_PATH)

                    except Exception as cycle_e:
                         err_msg = f"Unhandled Cycle Error: {cycle_e}"
                         logging.critical(f"CRITICAL UNHANDLED ERROR during cycle step '{current_step}': {cycle_e}", exc_info=True)
                         send_telegram_message(f"CRITICAL: {err_msg}", level="CRITICAL")
                         try: current_state = load_state(STATE_FILE_PATH); current_state["last_error"] = err_msg; current_state["status"] = "error"; current_state["current_step"] = "idle"; save_state(current_state, STATE_FILE_PATH); logging.info("Set status=error, step=idle due to cycle error.")
                         except Exception as save_e: logging.error(f"Failed save error state after cycle error: {save_e}", exc_info=True); send_telegram_message("CRITICAL: Failed to save state after unhandled cycle error!", level="CRITICAL")

                    cycle_end_time = datetime.now(timezone.utc)
                    logging.info(f"--- Cycle End: {cycle_end_time.isoformat()} (Duration: {cycle_end_time - cycle_start_time}) ---")


                # --- Simplified main function ---
                def main():
                    logging.info("Starting AI Music Orchestrator...")
                    send_telegram_message("Orchestrator script starting up.", level="INFO")
                    gdrive_service = None
                    try:
                        gdrive_service = retry_operation( authenticate_gdrive, max_retries=3, delay_seconds=10, allowed_exceptions=(RefreshError, requests.exceptions.RequestException, socket.timeout, TimeoutError, HttpError), operation_name="Initial Google Drive Authentication" )
                        if not gdrive_service: logging.critical("Failed GDrive Auth after retries. Exiting."); send_telegram_message("CRITICAL: Failed GDrive Auth. Script cannot start.", level="CRITICAL"); sys.exit(1)
                        else: logging.info("Google Drive authenticated successfully.")
                    except Exception as auth_e: logging.critical(f"Critical error during initial GDrive auth setup: {auth_e}", exc_info=True); send_telegram_message(f"CRITICAL: Unhandled error during initial GDrive Auth: {auth_e}", level="CRITICAL"); sys.exit(1)

                    state = load_state(STATE_FILE_PATH)
                    logging.info(f"Initial state: Status='{state.get('status', 'N/A')}', Step='{state.get('current_step', 'N/A')}'")
                    initial_status = state.get("status")
                    if initial_status not in ["running", "stopped", "stopped_exhausted", "error"]: logging.warning(f"Initial status '{initial_status}' invalid. Setting 'stopped'."); state["status"] = "stopped"; save_state(state, STATE_FILE_PATH)

                    logging.info(f"Entering main loop. Current status: '{state.get('status')}'")
                    while True:
                        try:
                            current_state_check = load_state(STATE_FILE_PATH)
                            if current_state_check.get("status") == "stopped": logging.info("Status 'stopped'. Exiting main loop."); break
                            run_main_cycle(gdrive_service)
                            current_state_check = load_state(STATE_FILE_PATH) # Check again after cycle
                            if current_state_check.get("status") == "stopped": logging.info("Status set 'stopped' during cycle. Exiting."); break
                            logging.info(f"Cycle finished. Sleeping for {MAIN_LOOP_SLEEP_SECONDS} seconds..."); time.sleep(MAIN_LOOP_SLEEP_SECONDS)
                        except KeyboardInterrupt:
                            logging.info("\nCtrl+C detected. Attempting graceful shutdown..."); _shutdown_requested = True
                            send_telegram_message("Shutdown requested (Ctrl+C). Attempting graceful stop.", level="WARNING")
                            try: logging.info("Running final cycle..."); run_main_cycle(gdrive_service)
                            except Exception as final_cycle_e: logging.error(f"Error during final cycle: {final_cycle_e}", exc_info=True)
                            try: state = load_state(STATE_FILE_PATH); state["status"] = "stopped"; save_state(state, STATE_FILE_PATH); logging.info("Final status set 'stopped'.")
                            except Exception as save_e: logging.error(f"Failed save final state on Ctrl+C: {save_e}", exc_info=True)
                            sys.exit(0)
                        except Exception as e:
                            logging.critical(f"CRITICAL UNHANDLED ERROR in main loop: {e}", exc_info=True)
                            send_telegram_message(f"CRITICAL: Unhandled error in main loop: {e}", level="CRITICAL")
                            try: state = load_state(STATE_FILE_PATH); state["status"] = "error"; state["last_error"] = f"Main Loop Error: {e}"; save_state(state, STATE_FILE_PATH); logging.info("Set status=error due to main loop error.")
                            except Exception as save_e: logging.error(f"Failed save error state after main loop error: {save_e}", exc_info=True); send_telegram_message("CRITICAL: Failed to save state after unhandled main loop error!", level="CRITICAL")
                            logging.info("Attempting continue loop after 1 min delay..."); time.sleep(60)
                    logging.info("AI Music Orchestrator main loop finished.")
                    send_telegram_message("Orchestrator script stopped normally.", level="INFO")

                # --- Script Entry Point ---
                if __name__ == "__main__":
                    main()