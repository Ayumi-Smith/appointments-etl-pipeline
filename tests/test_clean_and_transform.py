import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
from scripts.data_processing import clean_and_transform

def test_clean_and_transform_basic_transformation_counts_and_cleaning():
   #Correct data case
    df = pd.DataFrame(
        [
            [2191, 'clinic_b', 492, '2023-05-11 00:00'],
            [2540, 'clinic_a', 264, '2023/05/11 00:00:00'],
            [2365, 'clinic_d', 756, '2023-05-11 00:00'],
            [4141, 'clinic_d', 514, '2023-05-11T00:00:00'],
            [5186, 'clinic_b', 957, '2023-05-11T00:00:00'],
            [5268, 'clinic_a', 853, '2023-05-11T00:00:00'],
            [7619, 'clinic_d', 928, '2023-05-11 00:00'],
            [7530, 'clinic_a', 447, '2023-05-11T00:00:00'],
            [8576, 'clinic_d', 216, '2023-05-11T00:00:00'],
            [7316, 'clinic_a', 914, '2023-05-11T00:00:00'],
        ],
        columns=['appointment_id', 'clinic_id', 'patient_id', 'created_at']
    )

    result = clean_and_transform(df)


    expected = pd.DataFrame(
    [
        ['clinic_b', '2023-05-11', 2],
        ['clinic_a', '2023-05-11', 4],
        ['clinic_d', '2023-05-11', 4]
    ],
    columns=['clinic_id', 'appointment_date', 'appointment_count']
)
    expected['appointment_date'] = pd.to_datetime(expected['appointment_date']).dt.date

    result_sorted = result.sort_values(['clinic_id', 'appointment_date']).reset_index(drop=True)
    expected_sorted = expected.sort_values(['clinic_id', 'appointment_date']).reset_index(drop=True)


    assert_frame_equal(result_sorted, expected_sorted)

def test_clean_and_transform_dropping_missing_values_incorrect_date_formatting_and_aggregation():

    df = pd.DataFrame(
        [
            [2191, 'clinic_b', 492, '2029-05-11 00:00'], #future date
            [2540, 'clinic_a', 264, '2023/05/11 00:00:00'],
            [2365, ' clinic_d', 756, '2023-05-11 00:00'], # space
            [4141, 'clinic_d', 514, '2023-05-11T00:00:00'],
            [None, 'clinic_b ', 957, '2023-05-11T00:00:00'], #missing appointment id
            [5268, 'CLINIC_A', 853, '2023-05-11T00:00:00'], # uppercase
            [7619, 'clinic_D', 928, '2023-05-11 00:00'], #mixed case
            [7530, ' cliNic_a ', 447, '2023-05-11T00:00:00'], #space, mixed case
            [8576, 'CLINIC_D', 216, '2023-05-11T00:00:00'], #uppercase
            [7316, 'clinic_a', 914, '2023-05-11T00:00:00'],
        ],
        columns=['appointment_id', 'clinic_id', 'patient_id', 'created_at']
    )

    result = clean_and_transform(df)


    expected = pd.DataFrame(
    [

        ['clinic_a', '2023-05-11', 4],
        ['clinic_d', '2023-05-11', 4]
    ],
    columns=['clinic_id', 'appointment_date', 'appointment_count']
)
    expected['appointment_date'] = pd.to_datetime(expected['appointment_date']).dt.date

    result_sorted = result.sort_values(['clinic_id', 'appointment_date']).reset_index(drop=True)
    expected_sorted = expected.sort_values(['clinic_id', 'appointment_date']).reset_index(drop=True)

    print('result shape:', result.shape)
    print('expected shape:', expected.shape)
    print('result columns:', list(result.columns))
    print('expected columns:', list(expected.columns))

    assert_frame_equal(result_sorted, expected_sorted)

def test_clean_and_transform_empty_df():
    #Check if the file only has column names, no rows

    df = pd.DataFrame([], columns=['appointment_id', 'clinic_id', 'patient_id', 'created_at'])

    result = clean_and_transform(df)

    assert list(result.columns) == ['clinic_id', 'appointment_date', 'appointment_count']
    assert result.empty

def test_clean_and_transform_all_missing_ids_results_empty():
    #Check if all values in 'appointment_id' are missing
    df = pd.DataFrame(
        [
            ['clinic_a', '2024-01-01', np.nan],
            ['clinic_a', '2024-01-01', np.nan],
        ],
        columns=['clinic_id', 'created_at', 'appointment_id']
    )

    result = clean_and_transform(df)

    # Expect an empty DataFrame with specific columns
    expected_columns = ['clinic_id', 'appointment_date', 'appointment_count']

    assert list(result.columns) == expected_columns
    assert result.empty

def test_clean_and_transform_ignores_additional_columns():
    # Extra columns should be ignored; output must be identical to what we'd get if only
    # the required columns were present.
    df_with_extra = pd.DataFrame(
        [
            [2191, ' clinic_b ', 492, '2023-05-11 00:00', 'xtra1'],
            [2540, 'clinic_a', 264, '2023/05/11 00:00:00', 'xtra2'],
            [2365, 'clinic_d', 756, '2023-05-11 00:00', 'xtra3'],
            [4141, 'clinic_d', 514, '2023-05-11T00:00:00', 'xtra4'],
            [5268, 'clinic_a', 853, '2023-05-11T00:00:00', 'xtra5'],
        ],
        columns=['appointment_id', 'clinic_id', 'patient_id', 'created_at', 'extra_col'],
    )

    # Expected result: same as if we dropped the extra column
    df_required_only = df_with_extra.drop(columns=['extra_col'])

    result = clean_and_transform(df_with_extra)
    expected = clean_and_transform(df_required_only)

    result_sorted = result.sort_values(['clinic_id', 'appointment_date']).reset_index(drop=True)
    expected_sorted = expected.sort_values(['clinic_id', 'appointment_date']).reset_index(drop=True)

    assert list(result_sorted.columns) == ['clinic_id', 'appointment_date', 'appointment_count']
    assert_frame_equal(result_sorted, expected_sorted, check_dtype=False)
