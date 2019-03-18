# 3rd party
import pandas as pd
import pytest

# custom
from position_size import (
    MaxFirstEncountered,
    FixedCapitalPerc,
)
from backtester import Backtester


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


def max_first_encountered_alpha_sizer():
    """
    Returns MaxFirstEncountered position sizer with alphabetical sorting. That position sizer will decide
    to buy maximum amount of shares of first encountered cadidate (processing cadidates in alphabetical order)
    """
    return MaxFirstEncountered(sort_type='alphabetically')


def fixed_capital_perc_sizer(capital_perc):
    return FixedCapitalPerc(capital_perc=capital_perc,
        # debug=True, # TODO: remove it from here
        )


@pytest.fixture(params=['signals', 'position_sizer', 'init_capital'])
def backtester(request):
    """
    Creates backtester object with given signals, position_sizer and initial capital
    """
    return Backtester(
        request.param[0], 
        position_sizer=request.param[1],
        init_capital=request.param[2],
        # debug=True, # TODO: remove it from here
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
        },
        '2010-09-30_TEST_SIGS_1_short': {
            'buy_ds': pd.Timestamp('2010-09-30'),
            'type': 'short',
            'trx_value_no_fee': 900.0,
            'trx_value_with_fee': 896.0,
            'sell_ds': pd.Timestamp('2010-10-01'),
            'sell_value_no_fee': 800.0,
            'sell_value_with_fee': 804.0,
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
    expected_available_money = 1035124.67
    expected_account_value = 0
    expected_nav = 1035124.67
    expected_money_from_short = {}
    expected_owned_shares = {}
    backtester.run(test_days=4)
    ds_key = pd.Timestamp('2010-10-01')
    assert(backtester._available_money == expected_available_money)
    assert(backtester._account_value[ds_key] == expected_account_value)
    assert(backtester._net_account_value[ds_key] == expected_nav)
    assert(backtester._owned_shares == expected_owned_shares)
    assert(backtester._money_from_short == expected_money_from_short)


@pytest.mark.parametrize('backtester', [(signals_test_sigs_2(), fixed_capital_perc_sizer(0.2), 1000000)], indirect=True)
def test_functional_backtester_4_days_trades_TEST_SIGS_2(backtester):
    results, trades = backtester.run(test_days=4)
    expected_trades = {
        '2010-09-28_TEST_SIGS_1_short': {
            'buy_ds': pd.Timestamp('2010-09-28'),
            'type': 'short',
            'trx_value_no_fee': 199180,
            'trx_value_with_fee': 198423.12,
            'sell_ds': pd.Timestamp('2010-09-30'),
            'sell_value_no_fee': 203510,
            'sell_value_with_fee': 204283.34

        },
        '2010-09-29_TEST_SIGS_2_long': {
            'buy_ds': pd.Timestamp('2010-09-29'),
            'type': 'long',
            'trx_value_no_fee': 199080,
            'trx_value_with_fee': 199836.5,
            'sell_ds': pd.Timestamp('2010-10-01'),
            'sell_value_no_fee': 241740,
            'sell_value_with_fee': 240821.39
        }
    }    
    assert(expected_trades == trades)


@pytest.mark.parametrize('backtester', [(signals_test_sigs_2(), max_first_encountered_alpha_sizer(), 5000000)], indirect=True)
def test_bankruptcy(backtester):
    with pytest.raises(ValueError):
        results, trades = backtester.run(test_days=3)


@pytest.mark.parametrize('backtester', [(signals_test_sigs_3(), fixed_capital_perc_sizer(0.35), 400000)], indirect=True)
def test_functional_backtester_1_day_trades_TEST_SIGS_3(backtester):
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
def test_functional_backtester_2_days_trades_TEST_SIGS_3(backtester):
    expected_available_money = 259501.47
    expected_account_value = 6130
    expected_nav = 404751.47
    expected_owned_shares = {
        'TEST_SIGS_1': {
            'cnt': 1162,
            'trx_id': '2019-01-01_TEST_SIGS_1_long'
        },
        'TEST_SIGS_2': {
            'cnt': -592,
            'trx_id': '2019-01-02_TEST_SIGS_2_short'
        }
    }
    expected_money_from_short = {
        '2019-01-02_TEST_SIGS_2_short': 139120
    }
    backtester.run(test_days=2)
    ds_key = pd.Timestamp('2019-01-02')
    assert(backtester._available_money == expected_available_money)
    assert(backtester._account_value[ds_key] == expected_account_value)
    assert(backtester._net_account_value[ds_key] == expected_nav)
    assert(backtester._owned_shares == expected_owned_shares)
    assert(backtester._money_from_short == expected_money_from_short)


@pytest.mark.parametrize('backtester', [(signals_test_sigs_3(), fixed_capital_perc_sizer(0.35), 400000)], indirect=True)
def test_functional_backtester_3_days_trades_TEST_SIGS_3(backtester):
    expected_available_money = 15.5
    expected_account_value = 411997
    expected_nav = 412012.5
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
            'cnt': 3161,
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







"""
dzien trzeci (2019-01-03) powinienem:
- exit short TEST_SIGS_2 za 230
- entry long TEST_SIGS_2 za 230
- entry long TEST_SIGS_3 za 37
(najpierw kupuje 2 a potem 3 poniewaz position sizer jest alfabetycnzy)

1) exiting short
staring available money -> 259501.47
sell fee: (592*230) * 0.0038 = 517.408 = 517.41
---> after sell
    -> available money: 259501.47 + 139120 - (592*230) - 517.41 = 261944.06
    -> account val: (1162*130) = 151060
    -> nav: 261944.06 + 151060 = 413004.06

2) entry long TEST_SIGS_2 for 230
trx limit = 413004.06 * 0.35 = 144551.421 = 144551.42
261944.06 (aval money) > 144551.42 (trx limit)

shares count = 144551.42 // (230 + (230*0.0038)) = 626
trx val = 626*230 = 143980
fee = 143980*0.0038 = 547.124 = 547.12
trx val with fee: 143980 + 547.12 = 144527.12
available money: 261944.06 - 144527.12 = 117416.94
account val: (1162*130) + (626*230) = 295040
NAV: 117416.94 + 295040 + 0 = 412456.94


3) entry long TEST_SIGS_3 za 37
przy kupowaniu "capital" bedzie taki sam dla kazdego kandydata. i bedzie to kapital po sprzedazy.
czy to dobrze? czy zle? zakladam ze dobrze.

trx limit -> 144551.42
117416.94 (aval money) < 144551.42 (trx limit)
wiec biore aval money

shares count = 117416.94 // (37 + (37*0.0038)) = 3161
trx val = 3161*37 = 116957
fee = 116957*0.0038 = 444.4366 = 444.44
trx val with fee = 116957+444.44 = 117401.44

available money: 117416.94 - 117401.44 = 15.5
account val: (1162*130) + (626*230) + (3161*37) = 411997
NAV: 15.5 + 411997 + 0 = 412012.5

_owned_shares = {
    'TEST_SIGS_1': {
        'cnt': 1162,
        'trx_id': '2019-01-01_TEST_SIGS_1_long'
    },
    'TEST_SIGS_2': {
        'cnt': 626,
        'trx_id': '2019-01-03_TEST_SIGS_2_long'
    },
    'TEST_SIGS_3': {
        'cnt': 3161,
        'trx_id': '2019-01-03_TEST_SIGS_3_long'
    }
}




"""


"""
dzien drugi (2019-01-02) powinienem:
- entry short TEST_SIGS_2 for 235

starting avail money -> 260030.13 
single trx limit -> 399470.13 * 0.35 = 139814.5455 = 139814.55


no shares = 139814.55 // (235 + (235*0.0038)) = 592

trx val no fee = 592*235 = 139120
fee = 139120 * 0.0038 = 528.656 = 528.66

trx val with fee = 139120 + 528.66  = 139648.66

e_available_money = 260030.13 - 528.66 = 259501.47

_account_value = (1162*125) + (-592*235) = 6130

expected_money_from_short = {
    '2019-01-02_TEST_SIGS_2_short': 139120
}

_net_account_value = 259501.47 + 6130 + 139120 = 404751.47

_owned_shares = {
    'TEST_SIGS_1': {
        'cnt': 1162,
        'trx_id': '2019-01-01_TEST_SIGS_1_long'
    },
    'TEST_SIGS_2': {
        'cnt': -592,
        'trx_id': '2019-01-02_TEST_SIGS_2_short'
    }
}

"""


"""
dzien 1 (2019-01-01) powinienem:
- buy long: TEST_SIGS_1 for 120

init avail money -> 400000
single trx limit -> 400000*0.35 = 140000

no shares: 140000 // (120 + (120*0.0038)) = 1162
trx val = 1162 * 120 = 139440
fee = 139440 * 0.0038 = 529.872 = 529.87
trx_val_with_fee = 139440 + 529.87 = 139969.87

e_available_money = 400000 - 139969.87 = 260030.13

_account_value = 139440

_net_account_value = 260030.12+139440+0 = 399470.13

_owned_shares = {
    'TEST_SIGS_1': {
        'cnt': 1162,
        'trx_id': '2019-01-01_TEST_SIGS_1_long'
    }
}
_money_from_short = {}
"""


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