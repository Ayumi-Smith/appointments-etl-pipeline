from scripts.db import get_processed_filenames

DB_PATH = '/opt/airflow/data/appointments.db'

def get_unprocessed_files(folder):
    ''' Check between the filenames from the folder with files and filenames written to db.
    Assuming that since we are processing flow where files are loaded daily - we can load it all
    in memory for comparison '''
    all_files = get_csv_filenames_from_folder(folder)
    processed_files = get_processed_filenames()
    new_files = set(all_files) - processed_files
    return new_files

def get_csv_filenames_from_folder(folder):
    return [f.name for f in folder.glob('*.csv')]