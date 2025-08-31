from pathlib import Path
import read_files as rf
import transform_file as tf
import write_to_db as wdb
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('processing.log'),
        logging.StreamHandler()
    ]
)
if __name__ == '__main__':
    folder = Path('appointments_data')
    rf.init_db()

    logging.info('Check for new files.')
    new_files = rf.get_unprocessed_files(folder)

    if not new_files:
        logging.info('No new files found.')
    else:
        for new_file in new_files:
            logging.info(f'Processing file: {new_file}')
            file_path = folder / new_file
            wdb.add_processed_filename_to_db (new_file)

            try:
                df = tf.read_file(file_path)

                transformed_data = tf.clean_and_transform(df)
                
                wdb.write_processed_data_to_db (transformed_data)
                wdb.add_processed_time_to_db (new_file)
                logging.info(f'Processed the file {new_file}. Data pushed to the database.')
            except Exception as e:
                logging.error(f'Failed to process {new_file}: {e}', exc_info=True)

