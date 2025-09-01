

from airflow import DAG
from pathlib import Path
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from scripts import read_files as rf
from scripts import transform_file as tf
from scripts import write_to_db as wdb
import logging


@task
def find_unprocessed_files(folder):
    rf.init_db()
    logging.info('Check for new files.')
    unprocessed_files = rf.get_unprocessed_files(folder)
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
    for new_file in unprocessed_files:
                logging.info(f'Processing file: {new_file}')
                file_path = folder / new_file
                wdb.add_processed_filename_to_db(new_file)


                df = tf.read_file(file_path)

                transformed_data = tf.clean_and_transform(df)

                wdb.write_processed_data_to_db(transformed_data)
                wdb.add_processed_time_to_db(new_file)
                logging.info(f'Processed the file {new_file}. Data pushed to the database.')



@dag(start_date=datetime(2024,1,1), schedule="@once", catchup=False)
def pipeline():
    folder = Path('/opt/airflow/appointments_data')
    unprocessed_files = find_unprocessed_files(folder)
    gate = has_files(unprocessed_files)
    proc = process_files(folder, unprocessed_files)
    gate >> proc

dag = pipeline()