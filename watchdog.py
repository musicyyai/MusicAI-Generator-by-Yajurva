import os
import time
import requests

def check_main_py():
    try:
        # Replace "your_replit_url" with the actual URL of your Replit project.
        response = requests.get("your_replit_url")
        if response.status_code == 200:
            print("main.py is running!")
            return True
        else:
            print("main.py is not responding.")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error checking main.py: {e}")
        return False

def restart_main_py():
    print("Restarting main.py...")
    os.system("python main.py") #this will run the main.py file again.

while True:
    if not check_main_py():
        restart_main_py()
    time.sleep(60) #checks every minute.