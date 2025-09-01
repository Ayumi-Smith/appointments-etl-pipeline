import sqlite3
from datetime import date
import logging

DB_PATH = '/opt/airflow/data/appointments.db'

def get_processed_filenames():
    with sqlite3.connect(DB_PATH) as con:
        con.execute('''
                    CREATE TABLE IF NOT EXISTS processed_files
                    (
                        filename       TEXT PRIMARY KEY,
                        processed_date TEXT
                    )
                    ''')
        rows = {r[0] for r in con.execute('SELECT filename FROM processed_files')}
    return rows

def write_processed_data (data):
    conn = sqlite3.connect(DB_PATH)

    try:
        data.to_sql('clinic_daily_counts', conn, if_exists='append', index=False)
        logging.info(f'Wrote processed {len(data)} rows to DB.')

    except Exception as e:
        logging.error(f'Failed to write processed data to DB: {e}', exc_info=True)
        raise
    finally:
        conn.close()

def mark_file_as_processing(filename):
    #Add filename and date to DB to consider "seen" by the script
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO processed_files (filename, processed_date)
        VALUES (?, ?)
    """, (filename, date.today().isoformat()))
    con.commit()
    con.close()


def mark_file_as_processed(filename):
    _set_status(filename, 'Processed')

def mark_file_as_failed(filename):
    _set_status(filename,'Failed')

def mark_file_as_empty(filename):
    _set_status(filename, 'Empty')

def _set_status(filename, status):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    try:
        cur.execute("ALTER TABLE processed_files ADD COLUMN status TEXT")
    except sqlite3.OperationalError:
        pass

    cur.execute('UPDATE processed_files SET status = ? WHERE filename = ?', (status, filename))
    con.commit()
    con.close()