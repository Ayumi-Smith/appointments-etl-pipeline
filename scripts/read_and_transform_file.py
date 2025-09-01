import pandas as pd
import logging
import datetime

def read_csv_file(filepath):
    try:
        df = pd.read_csv(filepath) #if the files are big can be improved by reading in chunks
    except Exception as e:
        logging.error(f'Failed to read CSV at {filepath}: {e}', exc_info=True)
        raise

    if df.empty:
        logging.warning('Data in the file has no rows, only headers.')

    df.columns = [c.strip().lower() for c in df.columns]
    #Check if all the mandatory columns are present
    required_cols = {'appointment_id', 'clinic_id', 'patient_id', 'created_at'}
    missing = required_cols.difference(df.columns)
    if missing:
        msg = f'Missing required column(s): {sorted(missing)}. Present columns: {list(df.columns)}'
        logging.error(msg)
        raise ValueError(msg)

    return df


def clean_data (df):

    #Standardize clinic_id (remove spaces, lowercase).
    if df['clinic_id'].isna().any():
        # Check if we are dropping duplicates?
        df = df.dropna(subset=['clinic_id'])
        logging.warning('Missing values were found and in "clinic_id" column. Rows dropped.')

    df['clinic_id'] = df['clinic_id'].str.strip().str.lower()

    # Parse created_at into a proper datetime format.
    # Assuming that there is always year month and day - if one of these is missing - it will be autocompleted.
    # Alternative option - to parse manually, by string transformation or regex.

    df['appointment_date'] = (
        pd.to_datetime(df['created_at'], format='mixed', utc=True)
        .dt.tz_convert(None)
        .dt.date
    )
    #Drop rows with missing appointment_id.
    df = df.dropna(subset=['appointment_id'])
    return df


def data_quality_check (df):
    if df['appointment_id'].duplicated().any():
        # Check if we are dropping duplicates?
        df = df.drop_duplicates(subset=['appointment_id'])
        logging.warning('Duplicate values were found and in "appointment_id" column. Rows dropped.')


    if df['appointment_date'].isna().any():
        df = df.dropna(subset=['appointment_date'])
        logging.warning('Invalid date was found in the "appointment_date" column. Rows dropped.')

    today = datetime.date.today()
    #Checking if the date is not too old or is not in future
    valid_date_mask = df['appointment_date'].between(datetime.date(2000, 1, 1), today)

    if not valid_date_mask.all():
        rows_to_drop = df.loc[~valid_date_mask]
        logging.warning(f'Invalid date was found in the "appointment_date" column. Rows dropped.{rows_to_drop["appointment_date"].tolist()}')
        df = df.loc[valid_date_mask]


    return df

def transform_data (df):
    df = df[['clinic_id', 'appointment_date']].copy()
    #Aggregate the data into daily counts of appointments per clinic.

    aggregated = (
        df.groupby(['clinic_id', 'appointment_date'], as_index=False)
        .size()
        .rename(columns={'size': 'appointment_count'})
    )

    return aggregated

def clean_and_transform(df):
    df = clean_data(df)
    df = data_quality_check(df)
    df = transform_data(df)
    return df
