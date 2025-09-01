from pathlib import Path
import sqlite3

DB_PATH = '/opt/airflow/data/appointments.db'

def init_filenames_table (db_path: str = DB_PATH):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS processed_files (
            filename TEXT PRIMARY KEY,
            processed_date TEXT
        )
    ''')
    con.commit()
    con.close()    

def get_csv_filenames_from_folder(folder: Path) -> list[str]:
    return [f.name for f in folder.glob('*.csv')]

def get_processed_filenames (db_path: str = DB_PATH):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute('SELECT filename FROM processed_files')
    rows = {r[0] for r in cur.fetchall()}
    con.close()
    return rows

def get_unprocessed_files (folder: Path, db_path: str = DB_PATH):
    init_filenames_table()
    all_files = get_csv_filenames_from_folder(folder)
    processed_files = get_processed_filenames(db_path)
    new_files = set(all_files) - processed_files
    return new_files


    