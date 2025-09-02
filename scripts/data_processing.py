import pandas as pd
import logging
from datetime import datetime, timezone, date

def clean_and_transform_created_at(df):

    if df['created_at'].isna().any():
        df = df.dropna(subset=['created_at'])
        logging.warning('Invalid date was found in the "created_at" column. Rows dropped.')

    # Parse created_at into a proper datetime format.
    # Assuming that there is always year month and day - if one of these is missing - it will be autocompleted.
    # Alternative option - to parse manually, by string transformation or regex.

    df['appointment_date'] = (
        pd.to_datetime(df['created_at'], format='mixed', utc=True)
        .dt.tz_convert(None)
        .dt.date
    )
    today = datetime.now(timezone.utc).date()
    #Checking if the date is not too old or is not in future
    valid_date_mask = df['appointment_date'].between(date(2000, 1, 1), today)

    if not valid_date_mask.all():
        rows_to_drop = df.loc[~valid_date_mask]
        logging.warning(f'Invalid date was found in the "appointment_date" column. Rows dropped.{rows_to_drop["appointment_date"].tolist()}')
        df = df.loc[valid_date_mask]
    return df

def clean_ids (df):
    #Assuming that if there are blank clinic ids - we are dropping the rows
    if df['clinic_id'].isna().any():
        df = df.dropna(subset=['clinic_id'])
        logging.warning('Missing values were found and in "clinic_id" column. Rows dropped.')
    # Standardize clinic_id (remove spaces, lowercase).
    df['clinic_id'] = df['clinic_id'].str.strip().str.lower()

    #Drop rows with missing appointment_id.
    df = df.dropna(subset=['appointment_id'])
    #Assuming that if there are duplicated appointment ids - we are dropping the rows
    if df['appointment_id'].duplicated().any():
        df = df.drop_duplicates(subset=['appointment_id'])
        logging.warning('Duplicate values were found and in "appointment_id" column. Rows dropped.')

    return df

def aggregate_appointments_per_clinic (df):
    df = df[['clinic_id', 'appointment_date']].copy()

    aggregated = (
        df.groupby(['clinic_id', 'appointment_date'], as_index=False)
        .size()
        .rename(columns={'size': 'appointment_count'})
    )

    return aggregated

def clean_and_transform(df):
    df = clean_and_transform_created_at(df)
    df = clean_ids(df)
    df = aggregate_appointments_per_clinic(df)
    return df
