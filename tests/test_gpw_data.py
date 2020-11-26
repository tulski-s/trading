# 3rd part
import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest

# custom
from gpw_data import GPWData
from strategies.helpers import split_into_subsets


@pytest.fixture()
def mock_dates_df():
    dates_1 = ['2010-09-28', '2010-09-29', '2010-09-30', '2010-10-01', '2010-10-02']
    dates_2 = ['2010-09-30', '2010-10-01', '2010-10-02', '2010-10-03']
    s1_data = {
        'mock': [1 for _ in range(len(dates_1))],
    }
    s2_data = {
        'mock': [1 for _ in range(len(dates_2))],
    }
    return {
        'TEST_SIGS_1': pd.DataFrame(s1_data, index=pd.DatetimeIndex(dates_1)),
        'TEST_SIGS_2': pd.DataFrame(s2_data, index=pd.DatetimeIndex(dates_2))
    }


@pytest.fixture()
def mock_dates_dict():
    return {
        'TEST_SIGS_1': [[d, 1] for d in ['2011-01-01','2011-01-02','2011-01-03','2011-01-04', '2011-01-05']],
        'TEST_SIGS_2': [[d, 1] for d in ['2011-01-04','2011-01-05','2011-01-06', '2011-01-07', '2011-01-08']]
    }


def test_splitting_into_subsets_df(mock_dates_df):
    test_data, validation_data = split_into_subsets(mock_dates_df, 0.5)
    expected_test_sigs1 = pd.DatetimeIndex(['2010-09-28', '2010-09-29', '2010-09-30'])
    expected_validation_sigs1 = pd.DatetimeIndex(['2010-10-01', '2010-10-02'])
    expected_test_sigs2 =  pd.DatetimeIndex(['2010-09-30'])
    expected_validation_sigs2 = pd.DatetimeIndex(['2010-10-01', '2010-10-02', '2010-10-03'])
    assert((test_data['TEST_SIGS_1'].index == expected_test_sigs1).all() == True)
    assert((validation_data['TEST_SIGS_1'].index == expected_validation_sigs1).all() == True)
    assert((test_data['TEST_SIGS_2'].index == expected_test_sigs2).all() == True)
    assert((validation_data['TEST_SIGS_2'].index == expected_validation_sigs2).all() == True)


def test_splitting_into_subsets_dict(mock_dates_dict):
    test_data, validation_data = split_into_subsets(mock_dates_dict, 0.5, df=False)
    expected_test_data = {
        'TEST_SIGS_1': [[d, 1] for d in ['2011-01-01','2011-01-02','2011-01-03','2011-01-04']],
        'TEST_SIGS_2': [[d, 1] for d in ['2011-01-04']]
    }
    expected_validation_data = {
        'TEST_SIGS_1': [[d, 1] for d in ['2011-01-05']],
        'TEST_SIGS_2': [[d, 1] for d in ['2011-01-05','2011-01-06', '2011-01-07', '2011-01-08']]
    }
    assert(test_data == expected_test_data)
    assert(validation_data == expected_validation_data)


def test_detrend_df():
    gpw_data = GPWData()
    df = gpw_data.load(symbols='CCC')
    df = gpw_data.detrend(df)
    df.loc[:, 'adj_diff'] = df['adj_open'] - df['adj_open'].shift(1)
    assert pytest.approx(df['adj_diff'].mean()) == 0


def test_detrend_list():
    gpw_data = GPWData()
    data = gpw_data.load(symbols='CCC', df=False)
    data = gpw_data.detrend(data)
    close_col_idx = 9
    adj_price_changes = [
        data[i][close_col_idx] - data[i-1][close_col_idx] for i in range(1, len(data))
    ]
    mean_change = sum(adj_price_changes)/len(adj_price_changes)
    assert pytest.approx(mean_change) == 0
