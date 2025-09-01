import pandas as pd

#TODO: add summary for the function
def read_and_verify_headers(filepath):
    # if the files are big can be improved by reading in chunks
    df = pd.read_csv(filepath)
    df.columns = [c.strip().lower() for c in df.columns]
    required_cols = {'appointment_id', 'clinic_id', 'patient_id', 'created_at'}
    missing = required_cols.difference(df.columns)
    if missing:
        msg = f'Missing required column(s): {sorted(missing)}. Present columns: {list(df.columns)}'
        raise ValueError(msg)
    return df