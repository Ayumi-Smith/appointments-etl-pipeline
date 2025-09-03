# deal with warning packages
from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime
from scripts import data_processing
from scripts import db
from scripts import read_file
import logging
from scripts import notifications_handler as nh

@task
def get_unprocessed_filenames():
    logging.info('Start checking for new files...')
    return read_file.get_unprocessed_files()

@task.short_circuit
def has_files(unprocessed_files):
    if not unprocessed_files:
        logging.warning('No new files found.')
        nh.send_no_new_file_notification()
        return False
    else:
        logging.info(f'Found {len(unprocessed_files)} file(s): {unprocessed_files}')
        return True

@task
def process_files(unprocessed_filenames):
    for filename in unprocessed_filenames:
        try:
            logging.info(f'Processing file: {filename}')

            db.mark_file_as_processing(filename)

            df = read_file.read_and_verify_headers(filename)
            if df.empty:
                msg = f'No content for handling in file {filename}.'
                logging.warning(msg)
                nh.send_file_failure_notification(filename, msg)
                db.mark_file_as_empty(filename)
                continue

            transformed_data = data_processing.clean_and_transform(df)

            if transformed_data.empty:
                msg = f'No valid content after cleanup & transforming data in file {filename}.'
                logging.warning(msg)
                nh.send_file_failure_notification(filename, msg)
                db.mark_file_as_empty(filename)
                continue

            db.write_processed_data(transformed_data)

            logging.info(f'Processed the file {filename}. Data pushed to the database.')
            db.mark_file_as_processed(filename)

        except Exception as e:
            msg = f'Error during processing file {filename}: {e}'
            logging.error(msg)
            db.mark_file_as_failed(filename)
            nh.send_file_failure_notification(filename, msg)



@dag(
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False
    # placeholder for global Slack notification on failure (I can't test it, therefore
    # not putting the code at this point)
)
def daily_appointments_count_aggregation():
    # for simplicity assume that files are always located under the same path.
    # can be moved into configuration.
    unprocessed_filenames = get_unprocessed_filenames()
    gate = has_files(unprocessed_filenames)
    proc = process_files(unprocessed_filenames)
    gate >> proc

dag = daily_appointments_count_aggregation()