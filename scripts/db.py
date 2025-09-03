import sqlite3
from datetime import date
import logging

# airflow path
DB_PATH = '/opt/airflow/data/appointments.db'

#use this for running locally from main.
#DB_PATH = 'data/appointments.db'

def get_processed_filenames():
    with sqlite3.connect(DB_PATH) as con:
        con.execute('''
                    CREATE TABLE IF NOT EXISTS processed_files
                    (
                        filename       TEXT PRIMARY KEY,
                        processed_date TEXT,
                        status         TEXT
                    )
                    ''')
        rows = {r[0] for r in con.execute('SELECT filename FROM processed_files')}
    return rows

def write_processed_data (data):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            data.to_sql('clinic_daily_counts', conn, if_exists='append', index=False)
            logging.info(f'Wrote processed {len(data)} rows to DB.')
    except Exception as e:
        # separate logging to highlight the fact that already transformed and cleaned data
        # just failed to be written.
        logging.error(
            f'Failed to write processed data to DB: {e}',
            exc_info=True
        )
        raise

def mark_file_as_processing(filename):
    #Add filename and date to DB to consider "seen" by the script
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            "INSERT INTO processed_files (filename, processed_date) VALUES (?, ?)",
            (filename, date.today().isoformat())
        )
    logging.info(f'Added filename {filename} to "processed_files" table.')


def mark_file_as_processed(filename):
    _set_status(filename, 'Processed')
    logging.info(f'Assigned status "Processed" to {filename}.')

def mark_file_as_failed(filename):
    _set_status(filename,'Failed')
    logging.info(f'Assigned status "Failed" to {filename}.')

def mark_file_as_empty(filename):
    _set_status(filename, 'Empty')
    logging.info(f'Assigned status "Empty" to {filename}.')

def _set_status(filename, status):
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            "UPDATE processed_files SET status = ? WHERE filename = ?",
            (status, filename)
        )