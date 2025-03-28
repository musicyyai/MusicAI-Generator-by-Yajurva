# config.py

# --- Google Drive Configuration ---
GDRIVE_BACKUP_FOLDER_ID = "1studdk-TECgOtTwnbHasA2dKBSFstMU1"

# config.py

# --- Google Drive Configuration ---
GDRIVE_BACKUP_FOLDER_ID = "1studdk-TECgOtTwnbHasA2dKBSFstMU1" # Your Folder ID

# --- Riffusion Prompt Generation Configuration ---

PROMPT_GENRES = [
    "pop", "rock", "jazz", "classical", "electronic", "hip hop", "ambient",
    "techno", "house", "disco", "funk", "blues", "reggae", "country",
    "cinematic", "orchestral", "lofi", "synthwave", "chiptune", "folk",
    "metal", "punk", "soul", "gospel", "latin", "world music",
]

PROMPT_INSTRUMENTS = [
    "piano", "guitar", "acoustic guitar", "electric guitar", "bass guitar", "drums",
    "synthesizer", "synth pads", "arp synth", "lead synth", "violin", "cello",
    "strings section", "flute", "saxophone", "trumpet", "brass section",
    "organ", "electric piano", "vibraphone", "marimba", "bells", "choir",
    "vocals (instrumental focus)", "beat", "percussion", "tabla", "sitar",
]

PROMPT_MOODS = [
    "upbeat", "chill", "relaxing", "energetic", "driving", "melancholic", "sad",
    "happy", "ethereal", "atmospheric", "dark", "mysterious", "epic", "intense",
    "calm", "peaceful", "groovy", "funky", "dreamy", "nostalgic", "romantic",
    "suspenseful", "minimalist", "experimental", "aggressive", "smooth",
]

PROMPT_TEMPLATES = [
    "{genre} track with {instrument}",
    "{mood} {genre} featuring {instrument}",
    "A {mood} piece based on {instrument} in a {genre} style",
    "{instrument} solo over a {genre} beat",
    "Atmospheric {genre} with {mood} {instrument}",
    "{genre}", # Sometimes just genre works
    "{instrument}", # Sometimes just instrument works
    "{mood} {genre}",
    "{mood} {instrument}",
]

# --- Other Configurations (Add more later) ---
# Example:
# KAGGLE_WEEKLY_GPU_QUOTA = 30
# KAGGLE_USAGE_BUFFER = 0.9
# --- Other Configurations (Add more later) ---
# Example:
# KAGGLE_WEEKLY_GPU_QUOTA = 30
# KAGGLE_USAGE_BUFFER = 0.9


