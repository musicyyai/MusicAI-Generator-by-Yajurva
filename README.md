# MusicAI-Generator-by-Yajurva
AI-based music generator 
## Gitpod Fallback Environment

This repository is configured to run in Gitpod as a fallback environment if the primary Replit instance encounters critical issues.

**IMPORTANT: Setting Up Secrets in Gitpod**

When running this project in Gitpod, the secrets configured in Replit **will not** be automatically available. You **must** manually configure them as **Environment Variables** within the Gitpod workspace settings:

1.  **Open Gitpod Workspace:** Launch the repository in Gitpod.
2.  **Access Variables:** Click the Gitpod logo (usually top-left or bottom-left status bar) or use the command palette (`Ctrl+Shift+P` or `Cmd+Shift+P`) and search for "Gitpod: Open Workspace Settings". Navigate to the "Variables" section. Alternatively, go to [https://gitpod.io/variables](https://gitpod.io/variables) and scope the variables to this specific repository or your user account (`*/*`).
3.  **Add Variables:** For each secret required by the script, add a new environment variable. The **Key** must match the name used in the Replit Secrets and expected by `os.environ.get()` in the Python code. The **Value** is the corresponding secret credential.

    **Required Variables:**
    *   `TELEGRAM_BOT_TOKEN`: Your Telegram Bot Token.
    *   `TELEGRAM_CHAT_ID`: Your Telegram Chat ID.
    *   `GOOGLE_CREDS_JSON`: The *entire JSON content* from your `credentials.json` file.
    *   `KAGGLE_JSON_1`: The *entire JSON content* from your first Kaggle account's `kaggle.json`.
    *   `KAGGLE_JSON_2`: JSON content from the second Kaggle account.
    *   `KAGGLE_JSON_3`: JSON content from the third Kaggle account.
    *   `KAGGLE_JSON_4`: JSON content from the fourth Kaggle account.
    *   `SPOTIPY_CLIENT_ID`: Your Spotify App Client ID.
    *   `SPOTIPY_CLIENT_SECRET`: Your Spotify App Client Secret.

4.  **Restart Workspace (if needed):** After adding/updating variables, Gitpod usually prompts you to restart the workspace for changes to take effect. If not, manually restart it (Command Palette -> "Gitpod: Restart Workspace").

Once the environment variables are set, you can manually run the script from the Gitpod terminal: `python main.py`