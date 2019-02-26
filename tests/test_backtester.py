# 3rd party
import pandas as pd
import pytest

# custom
from position_size import MaxFirstEncountered
from backtester import Backtester

@pytest.fixture
def signals_test_sigs_1():
    """
    Creates signals with one symbol 'TEST_SIGS_1' and following data:
    date           close    entry_long  exit_long  entry_short  exit_short
    2010-09-28       100         1          0            0           0
    2010-09-29       120         0          1            0           0
    2010-09-30        90         0          1            1           0
    2010-10-01        80         0          0            0           1
    """
    dates = ['2010-09-28', '2010-09-29', '2010-09-30', '2010-10-01']
    signals_data = {
        'close': [100, 210, 90, 80],
        'entry_long': [1,0,0,0],
        'exit_long': [0,1,0,0],
        'entry_short': [0,0,1,0],
        'exit_short': [0,0,0,1],
    }
    return {'TEST_SIGS_1': pd.DataFrame(signals_data, index=pd.DatetimeIndex(dates))}


@pytest.fixture
def max_first_encountered_alpha_sizer():
    """
    Returns MaxFirstEncountered position sizer with alphabetical sorting. That position sizer will decide
    to buy maximum amount of shares of first encountered cadidate (processing cadidates in alphabetical order)
    """
    return MaxFirstEncountered(sort_type='alphabetically')


def test_func_full_backtester_TEST_SIGS_1(signals_test_sigs_1, max_first_encountered_alpha_sizer):
    backtester = Backtester(
        signals_test_sigs_1, 
        position_sizer=max_first_encountered_alpha_sizer,
    )
    print('created backtester')





