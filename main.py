from pathlib import Path
from scripts import finding_unprocessed_files as ff
from scripts import data_processing
from scripts import db
from scripts import read_file
import logging


def get_unprocessed_filenames(source):
    logging.info('Start checking for ne files...')
    return ff.get_unprocessed_files(source)

def has_files(unprocessed_files):
    if not unprocessed_files:
        logging.info('No new files found.')
        return False
    else:
        logging.info(f'Found {len(unprocessed_files)} file(s): {unprocessed_files}')
        return True

def process_files(folder, unprocessed_filenames):
    for filename in unprocessed_filenames:
        try:
            logging.info(f'Processing file: {filename}')

            db.mark_file_as_processing(filename)

            file_path = folder / filename
            df = read_file.read_and_verify_headers(file_path)
            if df.empty:
                logging.warning(f'No content for handling in file {filename}. Skip.')
                db.mark_file_as_empty(filename)
                continue

            transformed_data = data_processing.clean_and_transform(df)
            db.write_processed_data(transformed_data)

            if df.empty:
                logging.warning(f'No valid content after cleanup & transforming data in file {filename}. Skip.')
                db.mark_file_as_empty(filename)
                continue

            logging.info(f'Processed the file {filename}. Data pushed to the database.')
            db.mark_file_as_processed(filename)
        except Exception as e:
            logging.error(f'Error during processing file {filename}: {e}')
            db.mark_file_as_failed(filename)


if __name__ == '__main__':
    source = Path('appointments_data')
    unprocessed_filenames = get_unprocessed_filenames(source)
    process_files(source, unprocessed_filenames)