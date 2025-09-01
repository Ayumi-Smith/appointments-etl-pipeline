import sqlite3
from datetime import date
import logging

DB_PATH = '/opt/airflow/data/appointments.db'

def write_processed_data_to_db (data, db_path: str = DB_PATH):
    conn = sqlite3.connect(db_path)

    try:
        data.to_sql('clinic_daily_counts', conn, if_exists='append', index=False)
        logging.info(f'Wrote processed {len(data)} rows to DB.')

    except Exception as e:
        logging.error(f'Failed to write processed data to DB: {e}', exc_info=True)
        raise
    finally:
        conn.close()



def add_filename_to_db(filename: str, db_path: str = DB_PATH):
    #Add filename and date to DB to consider "seen" by the script
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO processed_files (filename, processed_date)
        VALUES (?, ?)
    """, (filename, date.today().isoformat()))
    con.commit()
    con.close()


def add_status_to_files_in_db(filename: str, status: str, db_path: str = DB_PATH):
    #Updates status for files in db: "Processed" or "Failed"
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    #if the column doesn't exist
    try:
        cur.execute("ALTER TABLE processed_files ADD COLUMN status TEXT")
    except sqlite3.OperationalError:
        pass

    cur.execute('UPDATE processed_files SET status = ? WHERE filename = ?', (status, filename))
    con.commit()
    con.close()

