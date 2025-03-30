# config.py (Updated for Step 6.6.1)

# --- Google Drive Configuration ---
GDRIVE_BACKUP_FOLDER_ID = "1studdk-TECgOtTwnbHasA2dKBSFstMU1" # Your Folder ID

# --- Riffusion Prompt Generation Configuration ---
PROMPT_GENRES = [ "pop", "rock", "jazz", "classical", "electronic", "hip hop", "ambient", "techno", "house", "disco", "funk", "blues", "reggae", "country", "cinematic", "orchestral", "lofi", "synthwave", "chiptune", "folk", "metal", "punk", "soul", "gospel", "latin", "world music", ]
PROMPT_INSTRUMENTS = [ "piano", "guitar", "acoustic guitar", "electric guitar", "bass guitar", "drums", "synthesizer", "synth pads", "arp synth", "lead synth", "violin", "cello", "strings section", "flute", "saxophone", "trumpet", "brass section", "organ", "electric piano", "vibraphone", "marimba", "bells", "choir", "vocals (instrumental focus)", "beat", "percussion", "tabla", "sitar", ]
PROMPT_MOODS = [ "upbeat", "chill", "relaxing", "energetic", "driving", "melancholic", "sad", "happy", "ethereal", "atmospheric", "dark", "mysterious", "epic", "intense", "calm", "peaceful", "groovy", "funky", "dreamy", "nostalgic", "romantic", "suspenseful", "minimalist", "experimental", "aggressive", "smooth", ]
PROMPT_TEMPLATES = [ "{genre} track with {instrument}", "{mood} {genre} featuring {instrument}", "A {mood} piece based on {instrument} in a {genre} style", "{instrument} solo over a {genre} beat", "Atmospheric {genre} with {mood} {instrument}", "{genre}", "{instrument}", "{mood} {genre}", "{mood} {instrument}", ]

# --- Uniqueness Check Configuration ---
UNIQUENESS_CHECK_ENABLED = True
UNIQUENESS_FINGERPRINT_COUNT = 50
UNIQUENESS_SIMILARITY_THRESHOLD = 0.90

# --- Kaggle Configuration ---
NUM_KAGGLE_ACCOUNTS = 4
ESTIMATED_KAGGLE_RUN_HOURS = 0.2 # Estimated GPU time per run in hours (ADJUST THIS VALUE!)
KAGGLE_WEEKLY_GPU_QUOTA = 30.0 # Weekly GPU quota per account (Check Kaggle!)
KAGGLE_USAGE_BUFFER = 0.90   # Stop using account at this fraction of quota

# --- Google Drive Cleanup Configuration ---
MAX_DRIVE_FILES = 50
MAX_DRIVE_FILE_AGE_DAYS = 7

# --- Style Profile Configuration ---
STYLE_PROFILE_RESET_TRACK_COUNT = 100

# --- Other Configurations --- ## <<< MODIFIED >>> ##
HEALTH_CHECK_INTERVAL_MINUTES = 30 # <<< ADDED: How often to run health checks





