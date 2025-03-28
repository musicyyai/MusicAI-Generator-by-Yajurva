        import json
        import os
        import logging  # Import logging module
import spotipy
from spotipy.oauth2
import SpotifyClientCredentials
# Ensure 'os' and 'logging' are already imported
        import hashlib
        import subprocess # <<< ADDED THIS IMPORT >>>
        # Add these to existing imports
        import pickle  # To save/load the token object
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build
        from googleapiclient.errors import HttpError
        from googleapiclient.http import MediaFileUpload

        # We need access to DEFAULT_STATE, let's import it from main
        try:
            from main import DEFAULT_STATE
        except ImportError:
            logging.warning(
                "Could not import DEFAULT_STATE from main. Using fallback definition.")
            DEFAULT_STATE = {
                "status": "stopped", "active_kaggle_account_index": 0, "active_drive_account_index": 0,
                "current_step": "idle", "current_instrument": None, "last_kaggle_run_id": None,
                "retry_count": 0, "total_tracks_generated": 0, "style_profile_id": "default",
                "fallback_active": False, "kaggle_usage": [
                    {"account_index": i, "gpu_hours_used_this_week": 0.0, "last_reset_time": None} for i in range(4)
                ], "last_error": None, "_checksum": None
            }

        # --- State Management Functions ---
        def load_state(filepath):
            # ... (load_state code as before - no changes needed here) ...
            logging.info(f"Attempting to load state from {filepath}...")
            try:
                if not os.path.exists(filepath):
                    logging.warning(f"State file '{filepath}' not found. Returning default state.")
                    return DEFAULT_STATE.copy()
                with open(filepath, 'r', encoding='utf-8') as f: state_str = f.read()
                if not state_str.strip():
                     logging.warning(f"State file '{filepath}' is empty. Returning default state.")
                     return DEFAULT_STATE.copy()
                loaded_state = json.loads(state_str)
                stored_checksum = loaded_state.get('_checksum')
                if stored_checksum:
                    state_copy_for_checksum = loaded_state.copy()
                    state_copy_for_checksum.pop('_checksum', None)
                    try:
                        checksum_str = json.dumps(state_copy_for_checksum, separators=(',', ':'), sort_keys=True).encode('utf-8')
                        calculated_checksum = hashlib.sha256(checksum_str).hexdigest()
                        if calculated_checksum == stored_checksum:
                            logging.info("State checksum verified successfully.")
                        else:
                            logging.error(f"STATE CHECKSUM MISMATCH! Expected: {stored_checksum}, Calculated: {calculated_checksum}. State file might be corrupted. Returning default state.")
                            return DEFAULT_STATE.copy()
                    except Exception as checksum_e:
                        logging.error(f"Failed to verify state checksum: {checksum_e}. Proceeding cautiously.")
                else:
                    logging.warning("No checksum found in state file. Skipping verification.")
                state_updated = False
                for key, default_value in DEFAULT_STATE.items():
                    if key not in loaded_state:
                        logging.warning(f"Key '{key}' missing in loaded state. Adding default value: {default_value}")
                        loaded_state[key] = default_value
                        state_updated = True
                if state_updated: logging.info("Loaded state was updated with missing default keys.")
                return loaded_state
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode JSON from state file '{filepath}': {e}. Returning default state.")
                return DEFAULT_STATE.copy()
            except IOError as e:
                logging.error(f"Failed to read state file '{filepath}': {e}. Returning default state.")
                return DEFAULT_STATE.copy()
            except Exception as e:
                logging.critical(f"An unexpected error occurred during state loading from '{filepath}': {e}. Returning default state.", exc_info=True)
                return DEFAULT_STATE.copy()

        def save_state(state_data, filepath):
            # ... (save_state code as before - no changes needed here) ...
            temp_filepath = filepath + ".tmp"
            try:
                state_copy_for_checksum = state_data.copy()
                state_copy_for_checksum.pop('_checksum', None)
                try:
                    checksum_str = json.dumps(state_copy_for_checksum, separators=(',', ':'), sort_keys=True).encode('utf-8')
                    calculated_checksum = hashlib.sha256(checksum_str).hexdigest()
                    state_data['_checksum'] = calculated_checksum
                    logging.info(f"Calculated state checksum: {calculated_checksum}")
                except Exception as checksum_e:
                    logging.error(f"Failed to calculate state checksum: {checksum_e}. Proceeding without checksum.")
                    state_data['_checksum'] = None
                state_str = json.dumps(state_data, indent=4)
                with open(temp_filepath, 'w', encoding='utf-8') as f: f.write(state_str)
                os.replace(temp_filepath, filepath)
                logging.info(f"State successfully saved to {filepath}.")
                return True
            except TypeError as e:
                logging.error(f"Failed to serialize state data to JSON for '{filepath}': {e}. State NOT saved.")
                return False
            except IOError as e:
                logging.error(f"Failed to write state to temporary file '{temp_filepath}' or rename to '{filepath}': {e}. State NOT saved.")
                if os.path.exists(temp_filepath):
                    try: os.remove(temp_filepath)
                    except OSError as rm_e: logging.error(f"Additionally failed to remove temporary state file '{temp_filepath}': {rm_e}")
                return False
            except Exception as e:
                logging.critical(f"An unexpected error occurred during state saving to '{filepath}': {e}. State NOT saved.", exc_info=True)
                if os.path.exists(temp_filepath):
                     try: os.remove(temp_filepath)
                     except OSError as rm_e: logging.error(f"Additionally failed to remove temporary state file '{temp_filepath}': {rm_e}")
                return False

        # --- Google Drive Authentication ---
        SCOPES = ['https://www.googleapis.com/auth/drive.file']
        TOKEN_PICKLE_PATH = 'token.pickle'
        def authenticate_gdrive():
            # ... (authenticate_gdrive code as before - no changes needed here) ...
            creds = None
            if os.path.exists(TOKEN_PICKLE_PATH):
                try:
                    with open(TOKEN_PICKLE_PATH, 'rb') as token_file: creds = pickle.load(token_file)
                    logging.info("Loaded Google Drive token from file.")
                except Exception as e:
                     logging.warning(f"Failed to load token from {TOKEN_PICKLE_PATH}: {e}. Will re-authenticate.")
                     creds = None
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    try:
                        logging.info("Google Drive token expired. Refreshing...")
                        creds.refresh(Request())
                        logging.info("Token refreshed successfully.")
                    except Exception as e:
                        logging.error(f"Failed to refresh Google Drive token: {e}. Need to re-authenticate.")
                        creds = None
                else:
                    try:
                        from main import GOOGLE_CREDS_JSON_STR, GOOGLE_CREDS_INFO
                        if not GOOGLE_CREDS_JSON_STR:
                             logging.critical("Google credentials JSON string not available for authentication.")
                             return None
                        flow = InstalledAppFlow.from_client_info(GOOGLE_CREDS_INFO, SCOPES)
                        logging.info("Attempting Google Drive authorization flow...")
                        logging.warning("Please follow the instructions printed below (copy URL, grant access, paste code).")
                        creds = flow.run_console()
                        logging.info("Authorization flow completed.")
                    except ImportError:
                         logging.critical("Could not import Google credentials from main.py for authentication.")
                         return None
                    except Exception as e:
                        logging.critical(f"Failed to run Google Drive authorization flow: {e}", exc_info=True)
                        return None
                if creds:
                    try:
                        with open(TOKEN_PICKLE_PATH, 'wb') as token_file: pickle.dump(creds, token_file)
                        logging.info(f"Google Drive token saved to {TOKEN_PICKLE_PATH}.")
                    except Exception as e: logging.error(f"Failed to save Google Drive token to {TOKEN_PICKLE_PATH}: {e}")
                else: logging.error("Authentication resulted in None credentials. Cannot save token.")
            if not creds:
                 logging.error("Cannot build Google Drive service: No valid credentials.")
                 return None
            try:
                service = build('drive', 'v3', credentials=creds)
                logging.info("Google Drive API service built successfully.")
                return service
            except HttpError as error:
                logging.error(f"An error occurred building the Google Drive service: {error}")
                return None
            except Exception as e:
                logging.critical(f"An unexpected error occurred building the Google Drive service: {e}", exc_info=True)
                return None

        # --- Google Drive File Operations ---
        def upload_to_gdrive(service, local_filepath, gdrive_folder_id, gdrive_filename):
            # ... (upload_to_gdrive code as before - no changes needed here) ...
            if not service: logging.error("Google Drive service object is invalid. Cannot upload."); return None
            if not os.path.exists(local_filepath): logging.error(f"Local file '{local_filepath}' not found. Cannot upload."); return None
            try:
                logging.info(f"Attempting to upload '{local_filepath}' to Drive folder '{gdrive_folder_id}' as '{gdrive_filename}'...")
                file_metadata = {'name': gdrive_filename, 'parents': [gdrive_folder_id]}
                media = MediaFileUpload(local_filepath, resumable=True)
                file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                uploaded_file_id = file.get('id')
                logging.info(f"File '{gdrive_filename}' uploaded successfully to Google Drive. File ID: {uploaded_file_id}")
                return uploaded_file_id
            except HttpError as error:
                logging.error(f"An HTTP error occurred during Google Drive upload: {error}"); return None
            except Exception as e:
                logging.critical(f"An unexpected error occurred during Google Drive upload: {e}", exc_info=True); return None

        # --- Kaggle API Setup ---
        KAGGLE_CONFIG_DIR = os.path.expanduser("~/.kaggle")
        KAGGLE_JSON_PATH = os.path.join(KAGGLE_CONFIG_DIR, "kaggle.json")
        def setup_kaggle_api(account_index):
            # ... (setup_kaggle_api code as before - no changes needed here) ...
            logging.info(f"Setting up Kaggle API for account index: {account_index}")
            try: from main import KAGGLE_CREDENTIALS_LIST
            except ImportError: logging.critical("Could not import KAGGLE_CREDENTIALS_LIST from main.py for Kaggle setup."); return False
            if not 0 <= account_index < len(KAGGLE_CREDENTIALS_LIST): logging.error(f"Invalid Kaggle account index: {account_index}. Must be between 0 and {len(KAGGLE_CREDENTIALS_LIST)-1}."); return False
            kaggle_json_str = KAGGLE_CREDENTIALS_LIST[account_index]
            if not kaggle_json_str: logging.error(f"Kaggle credentials for account index {account_index} are missing or empty."); return False
            try:
                os.makedirs(KAGGLE_CONFIG_DIR, exist_ok=True)
                logging.debug(f"Ensured Kaggle config directory exists: {KAGGLE_CONFIG_DIR}")
                with open(KAGGLE_JSON_PATH, 'w') as f: f.write(kaggle_json_str)
                logging.debug(f"Wrote credentials to {KAGGLE_JSON_PATH}")
                os.chmod(KAGGLE_JSON_PATH, 0o600)
                logging.debug(f"Set permissions for {KAGGLE_JSON_PATH} to 600.")
                logging.info(f"Kaggle API setup successful for account index {account_index}.")
                return True
            except IOError as e: logging.error(f"Failed to write Kaggle credentials to {KAGGLE_JSON_PATH}: {e}"); return False
            except OSError as e: logging.error(f"Failed to create directory or set permissions for Kaggle API: {e}"); return False
            except Exception as e: logging.critical(f"An unexpected error occurred during Kaggle API setup: {e}", exc_info=True); return False

        # --- Kaggle Notebook Execution ---
        PARAMS_JSON_FILENAME = "params.json"
        PARAMS_DATASET_SLUG = "notebook-params-temp"
        def trigger_kaggle_notebook(notebook_slug, params_dict):
            # ... (trigger_kaggle_notebook code as before - no changes needed here) ...
            logging.info(f"Attempting to trigger Kaggle notebook: {notebook_slug}")
            try:
                params_json_str = json.dumps(params_dict)
                with open(PARAMS_JSON_FILENAME, 'w') as f: f.write(params_json_str)
                logging.info(f"Created local {PARAMS_JSON_FILENAME} with params: {params_dict}")
            except (TypeError, IOError) as e: logging.error(f"Failed to create local {PARAMS_JSON_FILENAME}: {e}"); return False
            metadata_content = {"title": "Notebook Params Temp", "id": f"{notebook_slug.split('/')[0]}/{PARAMS_DATASET_SLUG}", "licenses": [{"name": "CC0-1.0"}]}
            metadata_filename = "dataset-metadata.json"
            try:
                with open(metadata_filename, 'w') as f: json.dump(metadata_content, f, indent=4)
                logging.info(f"Created local {metadata_filename}")
                if not os.path.exists(PARAMS_JSON_FILENAME) or not os.path.exists(metadata_filename):
                     logging.error("params.json or dataset-metadata.json missing before Kaggle API call."); return False
                logging.info(f"Uploading {PARAMS_JSON_FILENAME} as dataset: {PARAMS_DATASET_SLUG}...")
                command = ["kaggle", "datasets", "create", "-p", ".", "-m", "Update notebook params", "--dir-mode", "skip"]
                result = subprocess.run(command, capture_output=True, text=True, check=False)
                if result.stdout: logging.info(f"Kaggle datasets create stdout:\n{result.stdout}")
                if result.stderr: logging.error(f"Kaggle datasets create stderr:\n{result.stderr}")
                if result.returncode != 0:
                    logging.error(f"Kaggle datasets create command failed with return code {result.returncode}.")
                    return False # Keep local files for debugging if create fails
                else: logging.info("Kaggle dataset for parameters updated successfully.")
            except Exception as e:
                logging.critical(f"An unexpected error occurred during Kaggle dataset creation: {e}", exc_info=True)
                return False # Keep local files for debugging if create fails
            finally:
                 if os.path.exists(PARAMS_JSON_FILENAME):
                     try: os.remove(PARAMS_JSON_FILENAME)
                     except OSError: logging.warning(f"Could not remove {PARAMS_JSON_FILENAME}")
                 if os.path.exists(metadata_filename):
                     try: os.remove(metadata_filename)
                     except OSError: logging.warning(f"Could not remove {metadata_filename}")
            try:
                logging.info(f"Triggering Kaggle kernel push for notebook: {notebook_slug}")
                params_dataset_full_slug = f"{notebook_slug.split('/')[0]}/{PARAMS_DATASET_SLUG}"
                dummy_dir = "kaggle_push_dummy"
                os.makedirs(dummy_dir, exist_ok=True)
                kernel_metadata = {"id": notebook_slug, "language": "python", "kernel_type": "notebook", "is_private": "true", "enable_gpu": "true", "enable_internet": "true", "dataset_sources": [params_dataset_full_slug], "competition_sources": [], "kernel_sources": []}
                kernel_metadata_path = os.path.join(dummy_dir, "kernel-metadata.json")
                with open(kernel_metadata_path, 'w') as f: json.dump(kernel_metadata, f)
                logging.info(f"Pushing kernel {notebook_slug} with dataset {params_dataset_full_slug}...")
                command_push = ["kaggle", "kernels", "push", "-p", dummy_dir]
                result_push = subprocess.run(command_push, capture_output=True, text=True, check=False)
                try: os.remove(kernel_metadata_path); os.rmdir(dummy_dir)
                except OSError: logging.warning("Could not clean up dummy push directory.")
                if result_push.stdout: logging.info(f"Kaggle kernels push stdout:\n{result_push.stdout}")
                if result_push.stderr: logging.error(f"Kaggle kernels push stderr:\n{result_push.stderr}")
                if result_push.returncode != 0:
                    logging.error(f"Kaggle kernels push command failed with return code {result_push.returncode}.")
                    return False
                else:
                    if "successfully" in result_push.stdout.lower():
                         logging.info("Kaggle kernel push initiated successfully.")
                         return True
                    else:
                         logging.error("Kaggle kernel push command ran but might not have succeeded (no success message found).")
                         return False
            except Exception as e:
                logging.critical(f"An unexpected error occurred during Kaggle kernel push: {e}", exc_info=True)
                return False



# Still in: --- Kaggle Notebook Execution ---

# Define expected output filenames from the Kaggle notebook
KAGGLE_OUTPUT_WAV = "output.wav"
KAGGLE_OUTPUT_IMG = "output_spectrogram.png" # Optional image download

def download_kaggle_output(notebook_slug, destination_dir=".", download_image=False):
    """
    Downloads output files (output.wav, optionally output_spectrogram.png)
    from the latest completed run of a Kaggle notebook.

    Args:
        notebook_slug (str): The slug of the Kaggle notebook (e.g., "username/notebook-name").
        destination_dir (str): The local directory in Replit to save downloaded files. Defaults to current dir.
        download_image (bool): Whether to also download the spectrogram image. Defaults to False.

    Returns:
        tuple: (path_to_wav, path_to_img) where paths are None if download failed or skipped.
               Returns (None, None) if the core command fails.
    """
    logging.info(f"Attempting to download output from latest run of: {notebook_slug}")

    # Ensure destination directory exists
    try:
        os.makedirs(destination_dir, exist_ok=True)
    except OSError as e:
        logging.error(f"Failed to create destination directory '{destination_dir}': {e}")
        return None, None

    # Use 'kaggle kernels output' command
    # Command: kaggle kernels output -k <slug> -p <path> [--force]
    # The command downloads *all* output files from the latest *completed* kernel version.
    command = ["kaggle", "kernels", "output", "-k", notebook_slug, "-p", destination_dir, "--force"]

    downloaded_wav_path = None
    downloaded_img_path = None

    try:
        logging.info(f"Running Kaggle output download command...")
        result = subprocess.run(command, capture_output=True, text=True, check=False)

        # Log output/errors
        if result.stdout: logging.info(f"Kaggle kernels output stdout:\n{result.stdout}")
        if result.stderr: logging.error(f"Kaggle kernels output stderr:\n{result.stderr}") # Often shows download progress here too

        if result.returncode != 0:
            logging.error(f"Kaggle kernels output command failed with return code {result.returncode}.")
            # Check stderr for common issues like "kernel version not found" or "no output files"
            if "404" in result.stderr or "not found" in result.stderr.lower():
                 logging.error("Kernel or kernel version not found, or no completed runs yet.")
            elif "no output files" in result.stderr.lower():
                 logging.warning("Kaggle reported no output files for the latest completed run.")
            return None, None # Indicate command failure

        # If command succeeded, check if expected files were downloaded
        logging.info("Kaggle kernels output command finished. Checking for downloaded files...")

        potential_wav_path = os.path.join(destination_dir, KAGGLE_OUTPUT_WAV)
        if os.path.exists(potential_wav_path) and os.path.getsize(potential_wav_path) > 0:
            downloaded_wav_path = potential_wav_path
            logging.info(f"Successfully downloaded and verified: {downloaded_wav_path}")
        else:
            logging.warning(f"Expected output file '{KAGGLE_OUTPUT_WAV}' not found or empty in '{destination_dir}'.")

        if download_image:
            potential_img_path = os.path.join(destination_dir, KAGGLE_OUTPUT_IMG)
            if os.path.exists(potential_img_path) and os.path.getsize(potential_img_path) > 0:
                downloaded_img_path = potential_img_path
                logging.info(f"Successfully downloaded and verified: {downloaded_img_path}")
            else:
                logging.warning(f"Optional output file '{KAGGLE_OUTPUT_IMG}' not found or empty in '{destination_dir}'.")

        # Return paths even if only one file was found, or if image wasn't requested
        return downloaded_wav_path, downloaded_img_path

    except Exception as e:
        logging.critical(f"An unexpected error occurred during Kaggle output download: {e}", exc_info=True)
        return None, None







# --- Spotify API Interaction ---

# Global variable to hold the authenticated Spotipy client
# This avoids re-authenticating on every call within a single script run
_spotify_client = None

def get_spotify_client():
    """Authenticates with Spotify using Client Credentials and returns the client object."""
    global _spotify_client
    if _spotify_client:
        # TODO: Add check if token is expired and needs refresh?
        # Client Credentials usually don't expire quickly or Spotipy handles it.
        return _spotify_client

    logging.info("Attempting to authenticate with Spotify...")
    try:
        # Load credentials from environment variables (loaded in main.py)
        # For robustness, access them directly here too
        client_id = os.environ.get('SPOTIPY_CLIENT_ID')
        client_secret = os.environ.get('SPOTIPY_CLIENT_SECRET')

        if not client_id or not client_secret:
            logging.error("Spotify Client ID or Secret not found in environment variables.")
            return None

        # Use Client Credentials flow
        client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
        sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

        # Test authentication with a simple call
        sp.categories(limit=1) # Try fetching one category
        logging.info("Spotify client authenticated successfully.")
        _spotify_client = sp # Store the client globally
        return _spotify_client

    except spotipy.SpotifyException as e:
        logging.error(f"Spotify authentication/API error: {e}")
        return None
    except Exception as e:
        logging.critical(f"An unexpected error occurred during Spotify authentication: {e}", exc_info=True)
        return None


def get_spotify_trending_keywords(limit=10):
    """
    Fetches keywords (genres, potentially mood indicators) from Spotify.
    Placeholder: Fetches genres from several popular categories.

    Args:
        limit (int): Max number of keywords to return.

    Returns:
        list: A list of keyword strings, or an empty list on failure.
    """
    sp = get_spotify_client()
    if not sp:
        logging.error("Cannot fetch Spotify keywords: Client not authenticated.")
        return []

    keywords = set() # Use a set to avoid duplicates

    try:
        # Strategy 1: Get genres from featured playlists or categories
        logging.info("Fetching Spotify categories to get genre keywords...")
        categories = sp.categories(country='US', limit=max(limit // 2, 5))['categories']['items'] # Get a few categories
        for category in categories:
             # Use category name as a keyword/genre
             keywords.add(category['name'].lower())
             # Get playlists for that category
             try:
                  playlists = sp.category_playlists(category_id=category['id'], limit=2)['playlists']['items']
                  for playlist in playlists:
                       # Extract potential genres/moods from playlist name/description (very rough)
                       name_words = playlist['name'].lower().split()
                       # Add common genre words if found? This is very heuristic.
                       # Example: Add 'pop', 'rock', 'chill', etc. if in name
                       pass # Keep it simple for now, just use category name

             except spotipy.SpotifyException as pl_e:
                  logging.warning(f"Could not fetch playlists for category {category['id']}: {pl_e}")


        # Strategy 2: Get genres from recommendations based on seed genres (requires seed genres)
        # seed_genres = ['pop', 'rock', 'electronic'] # Example
        # try:
        #      recs = sp.recommendations(seed_genres=seed_genres, limit=limit)
        #      for track in recs['tracks']:
        #           # Get genres associated with the artists of recommended tracks
        #           for artist in track['artists']:
        #                try:
        #                     artist_info = sp.artist(artist['id'])
        #                     keywords.update(artist_info['genres'])
        #                except spotipy.SpotifyException as art_e:
        #                     logging.warning(f"Could not fetch genres for artist {artist['id']}: {art_e}")
        # except spotipy.SpotifyException as rec_e:
        #      logging.warning(f"Could not get recommendations: {rec_e}")


        # Strategy 3: Get genres from "New Releases" (often diverse)
        try:
             new_releases = sp.new_releases(limit=limit)['albums']['items']
             for album in new_releases:
                  for artist in album['artists']:
                       try:
                            artist_info = sp.artist(artist['id'])
                            keywords.update(g.lower() for g in artist_info.get('genres', [])) # Add artist genres
                       except spotipy.SpotifyException as art_e:
                            logging.warning(f"Could not fetch genres for artist {artist['id']}: {art_e}")
        except spotipy.SpotifyException as nr_e:
             logging.warning(f"Could not fetch new releases: {nr_e}")


        # Limit the number of keywords
        final_keywords = list(keywords)[:limit]
        logging.info(f"Fetched {len(final_keywords)} Spotify keywords: {final_keywords}")
        return final_keywords

    except spotipy.SpotifyException as e:
        logging.error(f"Spotify API error while fetching keywords: {e}")
        return []
    except Exception as e:
        logging.critical(f"An unexpected error occurred fetching Spotify keywords: {e}", exc_info=True)
        return []








