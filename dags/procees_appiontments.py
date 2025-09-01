

from airflow import DAG
from pathlib import Path
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from scripts import finding_unprocessed_files as ff
from scripts import read_and_transform_file as rtf
from scripts import write_to_db as wdb
import logging


@task
def find_unprocessed_files(folder):

    logging.info('Check for new files.')
    unprocessed_files = ff.get_unprocessed_files(folder)
    if not unprocessed_files:
        logging.info('No new files found.')
    else:
        logging.info(f'Found {len(unprocessed_files)} file(s): {unprocessed_files}')
        return unprocessed_files

@task.short_circuit
def has_files(files: list[str]) -> bool:
    return bool(files)

@task
def process_files(folder, unprocessed_files):
    for unprocessed_file in unprocessed_files:
                logging.info(f'Processing file: {unprocessed_file}')
                file_path = folder / unprocessed_file
                wdb.add_filename_to_db(unprocessed_file)


                df = rtf.read_csv_file(file_path)

                transformed_data = rtf.clean_and_transform(df)

                wdb.write_processed_data_to_db(transformed_data)
                wdb.add_status_to_files_in_db(unprocessed_file, 'Processed')
                logging.info(f'Processed the file {unprocessed_file}. Data pushed to the database.')



@dag(start_date=datetime(2024,1,1), schedule="@once", catchup=False)
def pipeline():
    folder = Path('/opt/airflow/appointments_data')
    unprocessed_files = find_unprocessed_files(folder)
    gate = has_files(unprocessed_files)
    proc = process_files(folder, unprocessed_files)
    gate >> proc

dag = pipeline()