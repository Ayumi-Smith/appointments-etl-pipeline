import requests

#Very basic example of notifications sending, not requiring any additional setup.
NOTIFICATION_URL = 'https://webhook.site/03075537-a273-451c-8f97-da4952dc434f'

def send_file_failure_notification(filename, error_msg):
    msg = {
        "status": "error",
        "file": filename,
        "message": error_msg,
    }
    requests.post(NOTIFICATION_URL, json=msg)

# soft notify about no files in daily flow.
def send_no_new_file_notification():
    msg = {
        "status": "warning",
        "message": "No new files were found in the source."
    }
    requests.post(NOTIFICATION_URL, json=msg)
