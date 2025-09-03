from pathlib import Path
from scripts.notifications_handler import send_file_failure_notification
from scripts.db import get_processed_filenames
import pandas as pd

#airflow path
FOLDER = '/opt/airflow/appointments_data'

#use this for running locally from main.
#FOLDER = 'appointments_data'

def read_and_verify_headers(filename):
    """Read the file into a pd.DataFrame and verify if all the columns required for processing
    are present."""

    # if the files are big can be improved by reading in chunks
    df = pd.read_csv(Path(FOLDER) / filename)

    df.columns = [c.strip().lower() for c in df.columns]
    required_cols = {'appointment_id', 'clinic_id', 'patient_id', 'created_at'}
    missing = required_cols.difference(df.columns)

    if missing:
        msg = f'Missing required column(s): {sorted(missing)}. Present columns: {list(df.columns)}'
        send_file_failure_notification(filename, msg)
        raise ValueError(msg)

    return df


def get_unprocessed_files():
    """ Check between the filenames from the folder with files and filenames written to db."""

    # Assuming that since we are processing flow where files are loaded daily -
    # the number of files is not huge, and we can load it all in memory for comparison
    all_files = _get_csv_filenames_from_folder()
    processed_files = get_processed_filenames()
    new_files = set(all_files) - processed_files
    return new_files

def _get_csv_filenames_from_folder():
    return [f.name for f in Path(FOLDER).glob('*.csv')]