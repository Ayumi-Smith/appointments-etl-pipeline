from scripts import data_processing
from scripts import db
from scripts import read_file
import logging

def get_unprocessed_filenames():
    logging.info('Start checking for ne files...')
    return read_file.get_unprocessed_files()

def has_files(unprocessed_files):
    if not unprocessed_files:
        logging.info('No new files found.')
        return False
    else:
        logging.info(f'Found {len(unprocessed_files)} file(s): {unprocessed_files}')
        return True

def process_files(unprocessed_filenames):
    for filename in unprocessed_filenames:
        try:
            logging.info(f'Processing file: {filename}')

            db.mark_file_as_processing(filename)
            df = read_file.read_and_verify_headers(filename)
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

            logging.info(f'Finished processing the file {filename}.')
            db.mark_file_as_processed(filename)
        except Exception as e:
            logging.error(f'Error during processing file {filename}: {e}')
            db.mark_file_as_failed(filename)


if __name__ == '__main__':
    db.init_db()
    unprocessed_filenames = get_unprocessed_filenames()
    any_files = has_files(unprocessed_filenames)
    if any_files:
        process_files(unprocessed_filenames)