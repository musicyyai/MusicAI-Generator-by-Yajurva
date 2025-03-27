import json
import os
import logging  # Import logging module
import hashlib
# Add these to existing imports
import pickle  # To save/load the token object
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from googleapiclient.http import MediaFileUpload
# Also ensure these are present from previous steps
# import json, os, logging, hashlib
# We need access to DEFAULT_STATE, let's import it from main
# This creates a potential circular import if utils also imports main heavily.
# A better structure might be to move DEFAULT_STATE to config.py later.
# For now, this works:
try:
    from main import DEFAULT_STATE
except ImportError:
    # Fallback if run standalone or structure changes - define it here too
    # (This is slightly redundant but makes utils more self-contained for now)
    logging.warning(
        "Could not import DEFAULT_STATE from main. Using fallback definition.")
    DEFAULT_STATE = {
        "status":
        "stopped",
        "active_kaggle_account_index":
        0,
        "active_drive_account_index":
        0,
        "current_step":
        "idle",
        "current_instrument":
        None,
        "last_kaggle_run_id":
        None,
        "retry_count":
        0,
        "total_tracks_generated":
        0,
        "style_profile_id":
        "default",
        "fallback_active":
        False,
        "kaggle_usage": [{
            "account_index": i,
            "gpu_hours_used_this_week": 0.0,
            "last_reset_time": None
        } for i in range(4)],
        "last_error":
        None,
        "_checksum":
        None
    }


def load_state(filepath):
    """
    Safely loads the state dictionary from a JSON file.

    Args:
        filepath (str): The path to the state file.

    Returns:
        dict: The loaded state dictionary, or DEFAULT_STATE if loading fails.
    """
    logging.info(f"Attempting to load state from {filepath}...")
    try:
        # Check if file exists
        if not os.path.exists(filepath):
            logging.warning(
                f"State file '{filepath}' not found. Returning default state.")
            return DEFAULT_STATE.copy(
            )  # Return a copy to avoid modifying the original

        # Read the file content
        with open(filepath, 'r', encoding='utf-8') as f:
            state_str = f.read()

        # Handle empty file case
        if not state_str.strip():
            logging.warning(
                f"State file '{filepath}' is empty. Returning default state.")
            return DEFAULT_STATE.copy()

        # Attempt to parse JSON
        loaded_state = json.loads(state_str)
        # Removed redundant log message here, checksum verification logs success

        # --- Checksum Verification ---
        stored_checksum = loaded_state.get('_checksum')
        if stored_checksum:
            state_copy_for_checksum = loaded_state.copy()
            state_copy_for_checksum.pop('_checksum',
                                        None)  # Remove checksum field

            try:
                checksum_str = json.dumps(state_copy_for_checksum,
                                          separators=(',', ':'),
                                          sort_keys=True).encode('utf-8')
                calculated_checksum = hashlib.sha256(checksum_str).hexdigest()

                if calculated_checksum == stored_checksum:
                    logging.info("State checksum verified successfully.")
                else:
                    logging.error(
                        f"STATE CHECKSUM MISMATCH! Expected: {stored_checksum}, Calculated: {calculated_checksum}. State file might be corrupted. Returning default state."
                    )
                    return DEFAULT_STATE.copy(
                    )  # Treat mismatch as critical error

            except Exception as checksum_e:
                logging.error(
                    f"Failed to verify state checksum: {checksum_e}. Proceeding cautiously."
                )
                # Decide how to handle verification failure - maybe return default? For now, just log.
        else:
            logging.warning(
                "No checksum found in state file. Skipping verification.")
        # --- End Checksum Verification ---

        # Basic validation: Ensure essential keys exist, merge if necessary
        # This helps if we add new keys to DEFAULT_STATE later
        state_updated = False
        for key, default_value in DEFAULT_STATE.items():
            if key not in loaded_state:
                logging.warning(
                    f"Key '{key}' missing in loaded state. Adding default value: {default_value}"
                )
                loaded_state[key] = default_value
                state_updated = True  # Mark that we should probably save the updated state soon

        # You might want to add more specific validation if needed
        if state_updated:
            logging.info("Loaded state was updated with missing default keys."
                         )  # Log if merge happened

        return loaded_state

    except json.JSONDecodeError as e:
        logging.error(
            f"Failed to decode JSON from state file '{filepath}': {e}. Returning default state."
        )
        return DEFAULT_STATE.copy()
    except IOError as e:
        logging.error(
            f"Failed to read state file '{filepath}': {e}. Returning default state."
        )
        return DEFAULT_STATE.copy()
    except Exception as e:
        logging.critical(
            f"An unexpected error occurred during state loading from '{filepath}': {e}. Returning default state.",
            exc_info=True)
        return DEFAULT_STATE.copy()


def save_state(state_data, filepath):
    """
    Safely saves the state dictionary to a JSON file using atomic write.

    Args:
        state_data (dict): The state dictionary to save.
        filepath (str): The path to the target state file.

    Returns:
        bool: True if saving was successful, False otherwise.
    """
    temp_filepath = filepath + ".tmp"  # Define temporary file path
    try:
        # --- Checksum Calculation ---
        # Create a copy to calculate checksum without the checksum field itself
        state_copy_for_checksum = state_data.copy()
        state_copy_for_checksum.pop('_checksum',
                                    None)  # Remove old checksum if exists

        try:
            # Convert the copy to a compact JSON string for consistent hashing
            checksum_str = json.dumps(state_copy_for_checksum,
                                      separators=(',', ':'),
                                      sort_keys=True).encode('utf-8')
            # Calculate SHA-256 hash
            calculated_checksum = hashlib.sha256(checksum_str).hexdigest()
            # Store the new checksum in the data to be saved
            state_data['_checksum'] = calculated_checksum
            logging.info(f"Calculated state checksum: {calculated_checksum}"
                         )  # Changed from debug
        except Exception as checksum_e:
            logging.error(
                f"Failed to calculate state checksum: {checksum_e}. Proceeding without checksum."
            )
            state_data[
                '_checksum'] = None  # Ensure checksum is None if calculation fails
        # --- End Checksum Calculation ---

        # Convert state dictionary to JSON string with indentation for readability
        state_str = json.dumps(state_data, indent=4)

        # Write to temporary file
        with open(temp_filepath, 'w', encoding='utf-8') as f:
            f.write(state_str)

        # Atomically replace the original file with the temporary file
        # This operation is generally atomic on most OSes, preventing corruption
        os.replace(temp_filepath, filepath)

        logging.info(f"State successfully saved to {filepath}.")
        return True

    except TypeError as e:
        logging.error(
            f"Failed to serialize state data to JSON for '{filepath}': {e}. State NOT saved."
        )
        return False
    except IOError as e:
        logging.error(
            f"Failed to write state to temporary file '{temp_filepath}' or rename to '{filepath}': {e}. State NOT saved."
        )
        # Attempt to clean up the temporary file if it exists
        if os.path.exists(temp_filepath):
            try:
                os.remove(temp_filepath)
            except OSError as rm_e:
                logging.error(
                    f"Additionally failed to remove temporary state file '{temp_filepath}': {rm_e}"
                )
        return False
    except Exception as e:
        logging.critical(
            f"An unexpected error occurred during state saving to '{filepath}': {e}. State NOT saved.",
            exc_info=True)
        # Attempt to clean up the temporary file if it exists
        if os.path.exists(temp_filepath):
            try:
                os.remove(temp_filepath)
            except OSError as rm_e:
                logging.error(
                    f"Additionally failed to remove temporary state file '{temp_filepath}': {rm_e}"
                )
        return False




# --- Google Drive Authentication ---

# Define the scopes required for Google Drive API access
# https://developers.google.com/identity/protocols/oauth2/scopes#drive
SCOPES = ['https://www.googleapis.com/auth/drive.file']
# 'drive.file' scope allows access to files created or opened by the app.
# Use 'https://www.googleapis.com/auth/drive' for full access if needed, but start specific.

TOKEN_PICKLE_PATH = 'token.pickle' # Where to store the auth token

def authenticate_gdrive():
    """
    Authenticates with Google Drive API using OAuth 2.0 flow.
    Handles token loading, refreshing, and initial authorization.

    Returns:
        googleapiclient.discovery.Resource: An authorized Google Drive API service object, or None if auth fails.
    """
    creds = None
    # Check if token file exists
    if os.path.exists(TOKEN_PICKLE_PATH):
        try:
            with open(TOKEN_PICKLE_PATH, 'rb') as token_file:
                creds = pickle.load(token_file)
            logging.info("Loaded Google Drive token from file.")
        except (pickle.UnpicklingError, EOFError, FileNotFoundError, Exception) as e:
             logging.warning(f"Failed to load token from {TOKEN_PICKLE_PATH}: {e}. Will re-authenticate.")
             creds = None # Ensure creds is None if loading fails

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                logging.info("Google Drive token expired. Refreshing...")
                creds.refresh(Request())
                logging.info("Token refreshed successfully.")
            except Exception as e:
                logging.error(f"Failed to refresh Google Drive token: {e}. Need to re-authenticate.")
                creds = None # Force re-authentication
        else:
            # Load credentials from the environment variable string
            try:
                # We need the original JSON string loaded earlier in main.py
                # Let's re-load it here for simplicity, though passing it would be cleaner
                # IMPORTANT: Assumes main.py has loaded GOOGLE_CREDS_JSON_STR
                # This import might be better placed inside the function if main.py also imports utils
                from main import GOOGLE_CREDS_JSON_STR, GOOGLE_CREDS_INFO

                if not GOOGLE_CREDS_JSON_STR:
                     logging.critical("Google credentials JSON string not available for authentication.")
                     return None

                # Use InstalledAppFlow for Desktop app type credentials
                # It will attempt to launch a browser or print a URL
                flow = InstalledAppFlow.from_client_info(GOOGLE_CREDS_INFO, SCOPES)

                # Run the flow - This part might require manual interaction in Replit console
                logging.info("Attempting Google Drive authorization flow...")
                logging.warning("Please follow the instructions printed below (copy URL, grant access, paste code).")
                # Use run_console() which prints URL and waits for code input
                creds = flow.run_console()

                logging.info("Authorization flow completed.")

            except ImportError:
                 logging.critical("Could not import Google credentials from main.py for authentication.")
                 return None
            except Exception as e:
                logging.critical(f"Failed to run Google Drive authorization flow: {e}", exc_info=True)
                return None # Auth failed

        # Save the credentials for the next run
        # Ensure 'creds' is not None before trying to save
        if creds:
            try:
                with open(TOKEN_PICKLE_PATH, 'wb') as token_file:
                    pickle.dump(creds, token_file)
                logging.info(f"Google Drive token saved to {TOKEN_PICKLE_PATH}.")
                # IMPORTANT: Secure this token.pickle file! Add 'token.pickle' to .gitignore
            except Exception as e:
                logging.error(f"Failed to save Google Drive token to {TOKEN_PICKLE_PATH}: {e}")
        else:
            logging.error("Authentication resulted in None credentials. Cannot save token.")


    # Build the Drive API service
    # Ensure 'creds' is not None before trying to build service
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
    """
    Uploads a local file to a specific folder in Google Drive.

    Args:
        service: Authorized Google Drive API service object.
        local_filepath (str): Path to the local file to upload.
        gdrive_folder_id (str): The ID of the Google Drive folder to upload into.
        gdrive_filename (str): The desired filename for the file on Google Drive.

    Returns:
        str: The file ID of the uploaded file on Google Drive, or None if upload fails.
    """
    if not service:
        logging.error("Google Drive service object is invalid. Cannot upload.")
        return None

    if not os.path.exists(local_filepath):
        logging.error(f"Local file '{local_filepath}' not found. Cannot upload.")
        return None

    try:
        logging.info(f"Attempting to upload '{local_filepath}' to Drive folder '{gdrive_folder_id}' as '{gdrive_filename}'...")

        # Define file metadata
        file_metadata = {
            'name': gdrive_filename,
            'parents': [gdrive_folder_id] # Specify the target folder
        }

        # Define the media to upload
        # Use resumable=True for larger files, might be overkill for small logs/state
        media = MediaFileUpload(local_filepath, resumable=True)

        # Create the file using the Drive API v3 files().create method
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id' # Request only the file ID in the response
        ).execute()

        uploaded_file_id = file.get('id')
        logging.info(f"File '{gdrive_filename}' uploaded successfully to Google Drive. File ID: {uploaded_file_id}")
        return uploaded_file_id

    except HttpError as error:
        logging.error(f"An HTTP error occurred during Google Drive upload: {error}")
        # TODO: Implement more specific error handling (e.g., check error code for quota issues)
        return None
    except Exception as e:
        logging.critical(f"An unexpected error occurred during Google Drive upload: {e}", exc_info=True)
        return None





