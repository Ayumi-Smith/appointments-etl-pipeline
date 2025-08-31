from pathlib import Path
import sqlite3

DB_PATH = 'appointments.db'

def init_db(db_path: str = DB_PATH):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS processed_files (
            filename TEXT PRIMARY KEY,
            processed_date TEXT
        )
    ''')
    # Useful index for lookups by name, too
    cur.execute('CREATE INDEX IF NOT EXISTS idx_files_name ON processed_files(filename)')
    con.commit()
    con.close()    

def csv_filenames(folder: Path) -> list[str]:
    return [f.name for f in folder.glob('*.csv')]

def get_processed_filenames (db_path: str = DB_PATH):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute('SELECT filename FROM processed_files')
    rows = {r[0] for r in cur.fetchall()}
    con.close()
    return rows

def get_unprocessed_files (folder: Path, db_path: str = DB_PATH, recursive: bool = False):
    all_files = csv_filenames(folder)
    processed_files = get_processed_filenames(db_path)
    new_files = set(all_files) - processed_files
    return new_files


    