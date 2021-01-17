# 3rd party
import pandas as pd
import pytest

# custom
from position_size import (
    MaxFirstEncountered,
    FixedCapitalPerc,
)
from backtester import (
    AccountBankruptError,
    Backtester,
)


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

def signals_test_sigs_2():
    """
    Creates signals:
    {
    'TEST_SIGS_1':
    date           close    entry_long  exit_long  entry_short  exit_short
    2010-09-28       230         0          0            1           0
    2010-09-29       233         0          0            0           0
    2010-09-30       235         0          0            0           1
    2010-10-01       237         0          0            0           0

    'TEST_SIGS_2':
    date           close    entry_long  exit_long  entry_short  exit_short
    2010-09-28       300         0          0            0           0
    2010-09-29       280         1          0            0           0
    2010-09-30       390         0          0            0           0
    2010-10-01       340         0          1            0           0
    }
    """
    dates = ['2010-09-28', '2010-09-29', '2010-09-30', '2010-10-01']
    s1_data = {
        'close': [230, 233, 235, 237],
        'entry_long': [0,0,0,0],
        'exit_long': [0,0,0,0],
        'entry_short': [1,0,0,0],
        'exit_short': [0,0,1,0],
    }
    s2_data = {
        'close': [300, 280, 390, 340],
        'entry_long': [0,1,0,0],
        'exit_long': [0,0,0,1],
        'entry_short': [0,0,0,0],
        'exit_short': [0,0,0,0],
    }
    return {
        'TEST_SIGS_1': pd.DataFrame(s1_data, index=pd.DatetimeIndex(dates)),
        'TEST_SIGS_2': pd.DataFrame(s2_data, index=pd.DatetimeIndex(dates))
    }


def signals_test_sigs_3():
    """
    Creates signals:
    {
    'TEST_SIGS_1':
    date           close    entry_long  exit_long  entry_short  exit_short
    2019-01-01       120         1          0            0           0
    2019-01-02       125         0          0            0           0
    2019-01-03       130         0          0            0           0
    2019-01-04       140         0          0            0           0
    2019-01-05       145         0          0            0           0

    'TEST_SIGS_2':
    date           close    entry_long  exit_long  entry_short  exit_short
    2019-01-01       230         0          0            0           0
    2019-01-02       235         0          0            1           0
    2019-01-03       230         1          0            0           1
    2019-01-04       235         0          0            0           0
    2019-01-05       230         0          0            0           0 

    'TEST_SIGS_3':
    date           close    entry_long  exit_long  entry_short  exit_short
    2019-01-01       20         0          0            0           0
    2019-01-02       45         0          0            0           0
    2019-01-03       37         1          0            0           0
    2019-01-04       36         0          0            0           0
    2019-01-05       42         0          1            0           0
    }
    """
    dates = ['2019-01-01', '2019-01-02', '2019-01-03', '2019-01-04', '2019-01-05']
    s1_data = {
        'close': [120, 125, 130, 140, 145],
        'entry_long': [1,0,0,0,0],
        'exit_long': [0,0,0,0,0],
        'entry_short': [0,0,0,0,0],
        'exit_short': [0,0,0,0,0],
    }
    s2_data = {
        'close': [230, 235, 230, 235, 230],
        'entry_long': [0,0,1,0,0],
        'exit_long': [0,0,0,0,0],
        'entry_short': [0,1,0,0,0],
        'exit_short': [0,0,1,0,0],
    }
    s3_data = {
        'close': [20, 45, 37, 36, 42],
        'entry_long': [0,0,1,0,0],
        'exit_long': [0,0,0,0,1],
        'entry_short': [0,0,0,0,0],
        'exit_short': [0,0,0,0,0],
    }
    return {
        'TEST_SIGS_1': pd.DataFrame(s1_data, index=pd.DatetimeIndex(dates)),
        'TEST_SIGS_2': pd.DataFrame(s2_data, index=pd.DatetimeIndex(dates)),
        'TEST_SIGS_3': pd.DataFrame(s3_data, index=pd.DatetimeIndex(dates))
    }


def signals_test_stop_loss_1():
    """
    Creates signals with one symbol 'TEST_SIGS_1' and following data:
    date           close    entry_long  exit_long  entry_short  exit_short  stop_loss
    2010-09-28       100         1          0            0           0          85    
    2010-09-29        90         0          0            0           0          85
    2010-09-30        80         0          0            0           0          85
    2010-10-01        70         0          1            0           0           0
    """
    dates = ['2010-09-28', '2010-09-29', '2010-09-30', '2010-10-01']
    signals_data = {
        'close': [100, 90, 80, 70],
        'low': [100, 90, 80, 70],
        'high': [100, 90, 80, 70],
        'entry_long': [1,0,0,0],
        'exit_long': [0,0,0,1],
        'entry_short': [0,0,0,0],
        'exit_short': [0,0,0,0],
        'stop_loss': [85, 85, 85, 0]
    }
    return {'TEST_SL_1': pd.DataFrame(signals_data, index=pd.DatetimeIndex(dates))}


def signals_test_auto_stop_loss_1():
    """
    Creates signals with one symbol 'TEST_SIGS_1' and following data:
    date           close    entry_long  exit_long  entry_short  exit_short
    2010-09-28       100         1          0            0           0
    2010-09-29        81         0          0            0           0
    2010-09-30       120         0          0            0           0
    2010-10-01        81         0          0            0           0
    2010-10-02        50         0          1            0           0
    """
    dates = ['2010-09-28', '2010-09-29', '2010-09-30', '2010-10-01', '2010-10-02']
    signals_data = {
        'close': [100, 81, 120, 81, 50],
        'low': [100, 81, 120, 81, 50],
        'high': [100, 81, 120, 81, 50],
        'entry_long': [1,0,0,0,0],
        'exit_long': [0,0,0,0,1],
        'entry_short': [0,0,0,0,0],
        'exit_short': [0,0,0,0,0],
    }
    return {'TEST_ASL_1': pd.DataFrame(signals_data, index=pd.DatetimeIndex(dates))}


def signals_test_long_short_same():
    dates = ['2019-01-01', '2019-01-02']
    signals_data = {
        'close': [21, 21],
        'entry_long': [0, 1],
        'exit_long': [0, 0],
        'entry_short': [1, 0],
        'exit_short': [0, 0],
    }
    return {'TEST_SHORT_LONG_SAME': pd.DataFrame(signals_data, index=pd.DatetimeIndex(dates))}


def max_first_encountered_alpha_sizer():
    """
    Returns MaxFirstEncountered position sizer with alphabetical sorting. That position sizer will decide
    to buy maximum amount of shares of first encountered cadidate (processing cadidates in alphabetical order)
    """
    return MaxFirstEncountered(sort_type='alphabetically')


def fixed_capital_perc_sizer(capital_perc):
    return FixedCapitalPerc(capital_perc=capital_perc)


@pytest.fixture(params=['signals', 'position_sizer', 'init_capital'])
def backtester(request):
    """
    Creates backtester object with given signals, position_sizer and initial capital
    """
    return Backtester(
        request.param[0], 
        position_sizer=request.param[1],
        init_capital=request.param[2],
    )


@pytest.fixture(params=['signals', 'position_sizer', 'init_capital'])
def backtester_sl(request):
    """
    Creates backtester object with given signals, position_sizer and initial capital. Stop loss enabled.
    """
    return Backtester(
        request.param[0], 
        position_sizer=request.param[1],
        init_capital=request.param[2],
        stop_loss=True,
    )


@pytest.fixture(params=['signals', 'position_sizer', 'init_capital', 'auto_stop_loss'])
def backtester_auto_sl(request):
    """
    Creates backtester object with given signals, position_sizer and initial capital. Auto stop loss enabled.
    """
    return Backtester(
        request.param[0], 
        position_sizer=request.param[1],
        init_capital=request.param[2],
        auto_stop_loss=request.param[3],
    )


@pytest.mark.parametrize('backtester', [(signals_test_sigs_1(), max_first_encountered_alpha_sizer(), 500)], indirect=True)
def test_functional_backtester_1_day_TEST_SIGS_1(backtester):
    expected_available_money = 96
    expected_account_value = 400
    expected_nav = 496
    expected_owned_shares = {
        'TEST_SIGS_1': {
            'cnt': 4.0,
            'trx_id': '2010-09-28_TEST_SIGS_1_long'
        }
    }
    backtester.run(test_days=1)
    ds_key = pd.Timestamp('2010-09-28')
    assert(backtester._available_money == expected_available_money)
    assert(backtester._account_value[ds_key] == expected_account_value)
    assert(backtester._net_account_value[ds_key] == expected_nav)
    assert(backtester._owned_shares == expected_owned_shares)
    assert(backtester._money_from_short == {})


@pytest.mark.parametrize('backtester', [(signals_test_sigs_1(), max_first_encountered_alpha_sizer(), 500)], indirect=True)
def test_functional_backtester_2_days_TEST_SIGS_1(backtester):
    expected_available_money = 932  # 96 + ((4*210)-4)
    expected_account_value = 0
    expected_nav = 932
    backtester.run(test_days=2)
    ds_key = pd.Timestamp('2010-09-29')
    assert(backtester._available_money == expected_available_money)
    assert(backtester._account_value[ds_key] == expected_account_value)
    assert(backtester._net_account_value[ds_key] == expected_nav)
    assert(backtester._owned_shares == {})
    assert(backtester._money_from_short == {})


@pytest.mark.parametrize('backtester', [(signals_test_sigs_1(), max_first_encountered_alpha_sizer(), 500)], indirect=True)
def test_functional_backtester_3_days_TEST_SIGS_1(backtester):
    expected_available_money = 928
    expected_account_value = -900
    expected_nav = 928
    expected_owned_shares = {
        'TEST_SIGS_1': {
            'cnt': -10.0,
            'trx_id': '2010-09-30_TEST_SIGS_1_short'
        }
    }
    expected_money_from_short = {
        '2010-09-30_TEST_SIGS_1_short': 900
    }
    backtester.run(test_days=3)
    ds_key = pd.Timestamp('2010-09-30')
    assert(backtester._available_money == expected_available_money)
    assert(backtester._account_value[ds_key] == expected_account_value)
    assert(backtester._net_account_value[ds_key] == expected_nav)
    assert(backtester._owned_shares == expected_owned_shares)
    assert(backtester._money_from_short == expected_money_from_short)


@pytest.mark.parametrize('backtester', [(signals_test_sigs_1(), max_first_encountered_alpha_sizer(), 500)], indirect=True)
def test_functional_backtester_4_days_TEST_SIGS_1(backtester):
    expected_available_money =  1024 # 928 (previous state) + 96 (after sell short)
    expected_account_value = 0
    expected_nav = 1024
    backtester.run(test_days=4)
    ds_key = pd.Timestamp('2010-10-01')
    assert(backtester._available_money == expected_available_money)
    assert(backtester._account_value[ds_key] == expected_account_value)
    assert(backtester._net_account_value[ds_key] == expected_nav)
    assert(backtester._owned_shares == {})
    assert(backtester._money_from_short == {})


@pytest.mark.parametrize('backtester', [(signals_test_sigs_1(), max_first_encountered_alpha_sizer(), 500)], indirect=True)
def test_functional_backtesetr_4_days_trades_TEST_SIGS_1(backtester):
    results, trades = backtester.run(test_days=4)
    expected_trades = {
        '2010-09-28_TEST_SIGS_1_long': {
            'buy_ds': pd.Timestamp('2010-09-28'),
            'type': 'long',
            'trx_value_no_fee': 400.0,
            'trx_value_with_fee': 404.0,
            'sell_ds': pd.Timestamp('2010-09-29'),
            'sell_value_no_fee': 840.0,
            'sell_value_with_fee': 836.0,
            'profit': 432,
        },
        '2010-09-30_TEST_SIGS_1_short': {
            'buy_ds': pd.Timestamp('2010-09-30'),
            'type': 'short',
            'trx_value_no_fee': 900.0,
            'trx_value_with_fee': 896.0,
            'sell_ds': pd.Timestamp('2010-10-01'),
            'sell_value_no_fee': 800.0,
            'sell_value_with_fee': 804.0,
            'profit': 92
        }
    }
    assert(expected_trades == trades)


@pytest.mark.parametrize('backtester', [(signals_test_sigs_2(), fixed_capital_perc_sizer(0.2), 1000000)], indirect=True)
def test_functional_backtester_1_day_TEST_SIGS_2(backtester):
    expected_available_money = 999243.12
    expected_account_value = -199180
    expected_nav = 999243.12
    expected_money_from_short = {
        '2010-09-28_TEST_SIGS_1_short': 199180
    }
    expected_owned_shares = {
        'TEST_SIGS_1': {
            'cnt': -866.0,
            'trx_id': '2010-09-28_TEST_SIGS_1_short'
        }
    }
    backtester.run(test_days=1)
    ds_key = pd.Timestamp('2010-09-28')
    assert(backtester._available_money == expected_available_money)
    assert(backtester._account_value[ds_key] == expected_account_value)
    assert(backtester._net_account_value[ds_key] == expected_nav)
    assert(backtester._owned_shares == expected_owned_shares)
    assert(backtester._money_from_short == expected_money_from_short)


@pytest.mark.parametrize('backtester', [(signals_test_sigs_2(), fixed_capital_perc_sizer(0.2), 1000000)], indirect=True)
def test_functional_backtester_2_days_TEST_SIGS_2(backtester):
    expected_available_money = 799968.74
    expected_account_value = -3258
    expected_nav = 995890.74
    expected_money_from_short = {
        '2010-09-28_TEST_SIGS_1_short': 199180
    }
    expected_owned_shares = {
        'TEST_SIGS_1': {
            'cnt': -866.0,
            'trx_id': '2010-09-28_TEST_SIGS_1_short'
        },
        'TEST_SIGS_2': {
            'cnt': 709,
            'trx_id': '2010-09-29_TEST_SIGS_2_long'
        }
    }
    backtester.run(test_days=2)
    ds_key = pd.Timestamp('2010-09-29')
    assert(backtester._available_money == expected_available_money)
    assert(backtester._account_value[ds_key] == expected_account_value)
    assert(backtester._net_account_value[ds_key] == expected_nav)
    assert(backtester._owned_shares == expected_owned_shares)
    assert(backtester._money_from_short == expected_money_from_short)


@pytest.mark.parametrize('backtester', [(signals_test_sigs_2(), fixed_capital_perc_sizer(0.2), 1000000)], indirect=True)
def test_functional_backtester_3_days_TEST_SIGS_2(backtester):
    expected_available_money = 794865.4
    expected_account_value = 276510
    expected_nav = 1071375.4
    expected_money_from_short = {}
    expected_owned_shares = {
        'TEST_SIGS_2': {
            'cnt': 709  ,
            'trx_id': '2010-09-29_TEST_SIGS_2_long'
        }
    }
    backtester.run(test_days=3)
    ds_key = pd.Timestamp('2010-09-30')
    assert(backtester._available_money == expected_available_money)
    assert(backtester._account_value[ds_key] == expected_account_value)
    assert(backtester._net_account_value[ds_key] == expected_nav)
    assert(backtester._owned_shares == expected_owned_shares)
    assert(backtester._money_from_short == expected_money_from_short)


@pytest.mark.parametrize('backtester', [(signals_test_sigs_2(), fixed_capital_perc_sizer(0.2), 1000000)], indirect=True)
def test_functional_backtester_4_days_TEST_SIGS_2(backtester):
    expected_available_money = 1035009.37
    expected_account_value = 0
    expected_nav = 1035009.37
    backtester.run(test_days=4)
    ds_key = pd.Timestamp('2010-10-01')
    assert(backtester._available_money == expected_available_money)
    assert(backtester._account_value[ds_key] == expected_account_value)
    assert(backtester._net_account_value[ds_key] == expected_nav)
    assert(backtester._owned_shares == {})
    assert(backtester._money_from_short == {})


@pytest.mark.parametrize('backtester', [(signals_test_sigs_2(), fixed_capital_perc_sizer(0.2), 1000000)], indirect=True)
def test_functional_backtester_4_days_trades_TEST_SIGS_2(backtester):
    results, trades = backtester.run(test_days=4)
    expected_trades = {
        '2010-09-28_TEST_SIGS_1_short': {
            'buy_ds': pd.Timestamp('2010-09-28'),
            'type': 'short',
            'trx_value_no_fee': 199180.0,
            'trx_value_with_fee': 198423.12,
            'sell_ds': pd.Timestamp('2010-09-30'),
            'sell_value_no_fee': 203510,
            'sell_value_with_fee': 204283.34,
            'profit': -5860.22

        },
        '2010-09-29_TEST_SIGS_2_long': {
            'buy_ds': pd.Timestamp('2010-09-29'),
            'type': 'long',
            'trx_value_no_fee': 198520.0,
            'trx_value_with_fee': 199274.38,
            'sell_ds': pd.Timestamp('2010-10-01'),
            'sell_value_no_fee': 241060,
            'sell_value_with_fee': 240143.97,
            'profit': 40869.59
        }
    }    
    assert(expected_trades == trades)


@pytest.mark.parametrize('backtester', [(signals_test_sigs_2(), max_first_encountered_alpha_sizer(), 5000000)], indirect=True)
def test_bankruptcy(backtester):
    with pytest.raises(AccountBankruptError):
        results, trades = backtester.run(test_days=3)


@pytest.mark.parametrize('backtester', [(signals_test_sigs_3(), fixed_capital_perc_sizer(0.35), 400000)], indirect=True)
def test_functional_backtester_1_day_TEST_SIGS_3(backtester):
    expected_available_money = 260030.13
    expected_account_value = 139440
    expected_nav = 399470.13
    expected_owned_shares = {
        'TEST_SIGS_1': {
            'cnt': 1162,
            'trx_id': '2019-01-01_TEST_SIGS_1_long'
        }
    }
    backtester.run(test_days=1)
    ds_key = pd.Timestamp('2019-01-01')
    assert(backtester._available_money == expected_available_money)
    assert(backtester._account_value[ds_key] == expected_account_value)
    assert(backtester._net_account_value[ds_key] == expected_nav)
    assert(backtester._owned_shares == expected_owned_shares)
    assert(backtester._money_from_short == {})


@pytest.mark.parametrize('backtester', [(signals_test_sigs_3(), fixed_capital_perc_sizer(0.35), 400000)], indirect=True)
def test_functional_backtester_2_days_TEST_SIGS_3(backtester):
    expected_available_money = 259493.44
    expected_account_value = 4015
    expected_nav = 404743.44
    expected_owned_shares = {
        'TEST_SIGS_1': {
            'cnt': 1162,
            'trx_id': '2019-01-01_TEST_SIGS_1_long'
        },
        'TEST_SIGS_2': {
            'cnt': -601,
            'trx_id': '2019-01-02_TEST_SIGS_2_short'
        }
    }
    expected_money_from_short = {
        '2019-01-02_TEST_SIGS_2_short': 141235
    }
    backtester.run(test_days=2)
    ds_key = pd.Timestamp('2019-01-02')
    assert(backtester._available_money == expected_available_money)
    assert(backtester._account_value[ds_key] == expected_account_value)
    assert(backtester._net_account_value[ds_key] == expected_nav)
    assert(backtester._owned_shares == expected_owned_shares)
    assert(backtester._money_from_short == expected_money_from_short)


@pytest.mark.parametrize('backtester', [(signals_test_sigs_3(), fixed_capital_perc_sizer(0.35), 400000)], indirect=True)
def test_functional_backtester_3_days_TEST_SIGS_3(backtester):
    expected_available_money = 7.47
    expected_account_value = 412034
    expected_nav = 412041.47
    expected_owned_shares = {
        'TEST_SIGS_1': {
            'cnt': 1162,
            'trx_id': '2019-01-01_TEST_SIGS_1_long'
        },
        'TEST_SIGS_2': {
            'cnt': 626,
            'trx_id': '2019-01-03_TEST_SIGS_2_long'
        },
        'TEST_SIGS_3': {
            'cnt': 3162,
            'trx_id': '2019-01-03_TEST_SIGS_3_long'
        }
    }
    backtester.run(test_days=3)
    ds_key = pd.Timestamp('2019-01-03')
    assert(backtester._available_money == expected_available_money)
    assert(backtester._account_value[ds_key] == expected_account_value)
    assert(backtester._net_account_value[ds_key] == expected_nav)
    assert(backtester._owned_shares == expected_owned_shares)
    assert(backtester._money_from_short == {})


@pytest.mark.parametrize('backtester', [(signals_test_sigs_3(), fixed_capital_perc_sizer(0.35), 400000)], indirect=True)
def test_functional_backtester_4_days_TEST_SIGS_3(backtester):
    expected_available_money = 7.47
    expected_account_value = 423622
    expected_nav = 423629.47
    expected_owned_shares = {
        'TEST_SIGS_1': {
            'cnt': 1162,
            'trx_id': '2019-01-01_TEST_SIGS_1_long'
        },
        'TEST_SIGS_2': {
            'cnt': 626,
            'trx_id': '2019-01-03_TEST_SIGS_2_long'
        },
        'TEST_SIGS_3': {
            'cnt': 3162,
            'trx_id': '2019-01-03_TEST_SIGS_3_long'
        }
    }
    backtester.run(test_days=4)
    ds_key = pd.Timestamp('2019-01-04')
    assert(backtester._available_money == expected_available_money)
    assert(backtester._account_value[ds_key] == expected_account_value)
    assert(backtester._net_account_value[ds_key] == expected_nav)
    assert(backtester._owned_shares == expected_owned_shares)
    assert(backtester._money_from_short == {})


@pytest.mark.parametrize('backtester', [(signals_test_sigs_3(), fixed_capital_perc_sizer(0.35), 400000)], indirect=True)
def test_functional_backtester_5_days_TEST_SIGS_3(backtester):
    expected_available_money = 132306.81
    expected_account_value = 312470
    expected_nav = 444776.81
    expected_owned_shares = {
        'TEST_SIGS_1': {
            'cnt': 1162,
            'trx_id': '2019-01-01_TEST_SIGS_1_long'
        },
        'TEST_SIGS_2': {
            'cnt': 626,
            'trx_id': '2019-01-03_TEST_SIGS_2_long'
        }
    }
    backtester.run(test_days=5)
    ds_key = pd.Timestamp('2019-01-05')
    assert(backtester._available_money == expected_available_money)
    assert(backtester._account_value[ds_key] == expected_account_value)
    assert(backtester._net_account_value[ds_key] == expected_nav)
    assert(backtester._owned_shares == expected_owned_shares)
    assert(backtester._money_from_short == {})


@pytest.mark.parametrize('backtester', [(signals_test_sigs_3(), fixed_capital_perc_sizer(0.35), 400000)], indirect=True)
def test_functional_backtester_5_days_trades_TEST_SIGS_3(backtester):
    results, trades = backtester.run(test_days=5)
    expected_trades = {
        '2019-01-01_TEST_SIGS_1_long': {
            'buy_ds': pd.Timestamp('2019-01-01'),
            'type': 'long',
            'trx_value_no_fee': 139440,
            'trx_value_with_fee': 139969.87,
        },
        '2019-01-02_TEST_SIGS_2_short': {
            'buy_ds': pd.Timestamp('2019-01-02'),
            'type': 'short',
            'trx_value_no_fee': 141235,
            'trx_value_with_fee': 140698.31,
            'sell_ds': pd.Timestamp('2019-01-03'),
            'sell_value_no_fee': 138230,
            'sell_value_with_fee': 138755.27,
            'profit': 1943.04,
        },
        '2019-01-03_TEST_SIGS_2_long':{
            'buy_ds': pd.Timestamp('2019-01-03'),
            'type': 'long',
            'trx_value_no_fee': 143980,
            'trx_value_with_fee': 144527.12,
        },
        '2019-01-03_TEST_SIGS_3_long':{
            'buy_ds': pd.Timestamp('2019-01-03'),
            'type': 'long',
            'trx_value_no_fee': 116994,
            'trx_value_with_fee': 117438.58,
            'sell_ds': pd.Timestamp('2019-01-05'),
            'sell_value_no_fee': 132804,
            'sell_value_with_fee': 132299.34,
            'profit': 14860.76,
        },
    }    
    assert(expected_trades['2019-01-03_TEST_SIGS_3_long'] == trades['2019-01-03_TEST_SIGS_3_long'])


@pytest.mark.parametrize('backtester_sl', [(signals_test_stop_loss_1(), max_first_encountered_alpha_sizer(), 500)], indirect=True)
def test_functional_backtester_1_day_TEST_SL_1(backtester_sl):
    expected_available_money = 96
    expected_account_value = 400
    expected_nav = 496
    expected_owned_shares = {
        'TEST_SL_1': {
            'cnt': 4.0,
            'trx_id': '2010-09-28_TEST_SL_1_long'
        }
    }
    backtester_sl.run(test_days=1)
    ds_key = pd.Timestamp('2010-09-28')
    assert(backtester_sl._available_money == expected_available_money)
    assert(backtester_sl._account_value[ds_key] == expected_account_value)
    assert(backtester_sl._net_account_value[ds_key] == expected_nav)
    assert(backtester_sl._owned_shares == expected_owned_shares)
    assert(backtester_sl._money_from_short == {})


@pytest.mark.parametrize('backtester_sl', [(signals_test_stop_loss_1(), max_first_encountered_alpha_sizer(), 500)], indirect=True)
def test_functional_backtester_2_days_TEST_SL_1(backtester_sl):
    expected_available_money = 96
    expected_account_value = 360
    expected_nav = 456
    expected_owned_shares = {
        'TEST_SL_1': {
            'cnt': 4.0,
            'trx_id': '2010-09-28_TEST_SL_1_long'
        }
    }
    backtester_sl.run(test_days=2)
    ds_key = pd.Timestamp('2010-09-29')
    assert(backtester_sl._available_money == expected_available_money)
    assert(backtester_sl._account_value[ds_key] == expected_account_value)
    assert(backtester_sl._net_account_value[ds_key] == expected_nav)
    assert(backtester_sl._owned_shares == expected_owned_shares)
    assert(backtester_sl._money_from_short == {})


@pytest.mark.parametrize('backtester_sl', [(signals_test_stop_loss_1(), max_first_encountered_alpha_sizer(), 500)], indirect=True)
def test_functional_backtester_3_days_TEST_SL_1(backtester_sl):
    expected_available_money = 432
    expected_nav = 432
    results, trades = backtester_sl.run(test_days=3)
    ds_key = pd.Timestamp('2010-09-30')
    expected_trades = {
        '2010-09-28_TEST_SL_1_long': {
            'buy_ds': pd.Timestamp('2010-09-28'),
            'type': 'long',
            'trx_value_no_fee': 400,
            'trx_value_with_fee': 404,
            'sell_ds': ds_key,
            'sell_value_no_fee': 340,
            'sell_value_with_fee': 336,
            'profit': -68,
        },
    } 
    assert(backtester_sl._available_money == expected_available_money)
    assert(backtester_sl._net_account_value[ds_key] == expected_nav)
    assert(expected_trades == trades)


@pytest.mark.parametrize('backtester', [(signals_test_long_short_same(), max_first_encountered_alpha_sizer(), 100)], indirect=True)
def test_long_and_short_same_symbol(backtester):
    with pytest.raises(ValueError):
        results, trades = backtester.run(test_days=2)


@pytest.mark.parametrize(
    'backtester_auto_sl',
    [(signals_test_auto_stop_loss_1(), max_first_encountered_alpha_sizer(), 1000, 0.2)],
    indirect=True
)
def test_functional_backtester_auto_stop_loss_1(backtester_auto_sl):
    results, trades = backtester_auto_sl.run(test_days=3)
    ds_1 = pd.Timestamp('2010-09-29')
    ds_2 = pd.Timestamp('2010-10-01')
    assert(backtester_auto_sl._auto_stop_loss_tracker['TEST_ASL_1'] == 120.0)
    assert(backtester_auto_sl.signals['TEST_ASL_1']['stop_loss'][ds_1] == 80)
    assert(backtester_auto_sl.signals['TEST_ASL_1']['stop_loss'][ds_2] == 96) 


@pytest.mark.parametrize(
    'backtester_auto_sl',
    [(signals_test_auto_stop_loss_1(), max_first_encountered_alpha_sizer(), 500, 0.2)],
    indirect=True
)
def test_functional_backtester_auto_stop_loss_2(backtester_auto_sl):
    expected_available_money = 476
    expected_nav = 476
    results, trades = backtester_auto_sl.run()
    ds_key = pd.Timestamp('2010-10-01')
    expected_trades = {
        '2010-09-28_TEST_ASL_1_long': {
            'buy_ds': pd.Timestamp('2010-09-28'),
            'type': 'long',
            'trx_value_no_fee': 400,
            'trx_value_with_fee': 404,
            'sell_ds': ds_key,
            'sell_value_no_fee': 384,
            'sell_value_with_fee': 380,
            'profit': -24,
        },
    }
    assert(backtester_auto_sl._available_money == expected_available_money)
    assert(backtester_auto_sl._net_account_value[ds_key] == expected_nav)
    assert(expected_trades == trades)
