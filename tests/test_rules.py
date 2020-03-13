# 3rd party
import numpy as np
import pytest

# custom
import rules


@pytest.fixture()
def prices_global_sr():
    return np.array([100., 210., 90., 80., 12., 60., 45., 78.])


@pytest.fixture()
def prices_local_sr():
    return np.array(
        [161.7, 159.3, 152.5, 148.1, 148.5, 157.7, 156.7, 153.9, 
         149.,  153.8, 150.5, 152.5, 153.3, 151.1, 149.2, 154., 
         151.4, 151.9, 154.3,]    
    )


@pytest.fixture()
def prices_multp_1():
    # high = low + (low*0.2)
    # last datapoint not included for means
    # high_mean, low_mean  =  154.0, 128.(3)
    return {
        'high': np.array([147.6, 160.8, 153.6, 200]),
        'low': np.array([123, 134, 128, 100]),
        'close': np.array([131, 160, 132, 120]),
    }

def test_uptrend():
    arr = np.array([1,2,3,4,5,6])
    rule_output = rules.trend(arr)
    assert(rule_output == 1)


def test_downtrend():
    arr = np.array([9,8,7,6,5,4])
    rule_output = rules.trend(arr)
    assert(rule_output == -1)


def test_horizontal():
    arr = np.array([-1,10,-2, 12, 0, 2.2])
    rule_output = rules.trend(arr)
    assert(rule_output == 0)


def test_find_support_resistance_global(prices_global_sr):
    support, resistance = rules._find_support_resistance(prices_global_sr[:-1])
    expected_support = 12
    expected_resistance = 210
    assert(expected_support == support)
    assert(expected_resistance == resistance)


def test_find_support_resistance_local(prices_local_sr):
    support, resistance = rules._find_support_resistance(prices_local_sr, e=7)
    expected_support = 151.4
    expected_resistance = 154.3
    assert(expected_support == support)
    assert(expected_resistance == resistance)


def test_between_global_levels(prices_global_sr):
    rule_output = rules.support_resistance(prices_global_sr)
    assert(rule_output == 0)


def test_above_global_resistance(prices_global_sr):
    arr = np.append(prices_global_sr, 500)
    rule_output = rules.support_resistance(arr)
    assert(rule_output == 1)


def test_below_global_support(prices_global_sr):
    arr = np.append(prices_global_sr, 5)
    rule_output = rules.support_resistance(arr)
    assert(rule_output == -1)


def test_above_local_resistance_with_b(prices_local_sr):
    arr = np.append(prices_local_sr, 155.9)
    rule_output = rules.support_resistance(arr, e=7, b=0.01)
    assert(rule_output == 1)


def test_between_local_levels_with_b(prices_local_sr):
    arr = np.append(prices_local_sr, 155.8)
    rule_output = rules.support_resistance(arr, e=7, b=0.01)
    assert(rule_output == 0)


def test_below_local_suport_with_b(prices_local_sr):
    arr = np.append(prices_local_sr, 143.82)
    rule_output = rules.support_resistance(arr, e=7, b=0.05)
    assert(rule_output == -1)


def test_simple_ma_base_ver(prices_global_sr):
    rule_output = rules.moving_average(prices_global_sr)
    assert(rule_output == -1)


def test_simple_ma_base_ver_with_b_neg(prices_global_sr):
    arr = np.append(prices_global_sr, 86.05)
    rule_output = rules.moving_average(arr, b=0.02)
    assert(rule_output == -1)


def test_simple_ma_base_ver_with_b_pos(prices_global_sr):
    arr = np.append(prices_global_sr, 86.07)
    rule_output = rules.moving_average(arr, b=0.02)
    assert(rule_output == 1)


def test_weighted_ma_base_ver(prices_global_sr):
    rule_output = rules.moving_average(prices_global_sr, weigth_ma=True)
    assert(rule_output == -1)


def test_weighted_ma_base_ver_pos(prices_global_sr):
    prices_global_sr[-1] = 119.3
    rule_output = rules.moving_average(prices_global_sr, weigth_ma=True)
    assert(rule_output == 1)


def test_weighted_two_mas(prices_global_sr):
    rule_output = rules.moving_average(prices_global_sr, weigth_ma=True, quick_ma_lookback=3)
    assert(rule_output == -1)


def test_weighted_two_mas_pos(prices_global_sr):
    prices_global_sr[-1] = 180
    rule_output = rules.moving_average(prices_global_sr, weigth_ma=True, quick_ma_lookback=2)
    assert(rule_output == 1)


def test_channel_break_out_1(prices_multp_1):
    rule_output = rules.channel_break_out(prices_multp_1, channel_width=0.2)
    assert(rule_output == -1)


def test_channel_break_out_2(prices_multp_1):
    prices_multp_1['close'][-1] = 160
    rule_output = rules.channel_break_out(prices_multp_1, channel_width=0.2)
    assert(rule_output == 1)


def test_channel_break_out_3(prices_multp_1):
    prices_multp_1['close'][-1] = 132
    rule_output = rules.channel_break_out(prices_multp_1, channel_width=0.2)
    assert(rule_output == 0)


def test_channel_break_out_4(prices_multp_1):
    rule_output = rules.channel_break_out(prices_multp_1, channel_width=0.2, b=0.1)
    assert(rule_output == 0)


def test_momentum_in_oscillator_cross_from_below():
    arr = np.array([.1, .15, .25, .1, .25])
    rule_output = rules.momentum_in_oscillator(arr, threshold=0.2)
    assert(rule_output == 1)


def test_momentum_in_oscillator_cross_from_above():
    arr = np.array([.1, .15, .25, .1, .05])
    rule_output = rules.momentum_in_oscillator(arr, threshold=0.1)
    assert(rule_output == -1)


def test_momentum_in_oscillator_close_cross_but_no():
    arr = np.array([.2, .1, .2, .1, .25])
    rule_output = rules.momentum_in_oscillator(arr, threshold=0.15)
    assert(rule_output == 0)


def test_candle_engulfing_short():
    dict_arrs = {
        'open': [3.6, 3.5, 3.47, 3.73, 3.85],
        'high': [3.6, 3.59, 3.71, 3.88, 3.92],
        'low': [3.42, 3.4, 3.42, 3.72, 3.53], 
        'close': [3.48, 3.44, 3.68, 3.83, 3.55]
    }
    rule_output = rules.candle_engulfing(dict_arrs)
    assert(rule_output == -1)


def test_candle_engulfing_long():
    dict_arrs = {
        'open': [4.7, 4.7, 4.7, 4.55, 4.55, 4.45, 4.4, 4.4, 4.4, 4.3, 4.05],
        'high': [4.7, 4.7, 4.7, 4.7, 4.55, 4.45, 4.5, 4.4, 4.5, 4.4, 4.45],
        'low': [4.65, 4.65, 4.6, 4.55, 4.55, 4.4, 4.4, 4.4, 4.35, 4.25, 4.05],
        'close': [4.7, 4.65, 4.6, 4.65, 4.55, 4.45, 4.5, 4.4, 4.4, 4.25, 4.45]
    }
    rule_output = rules.candle_engulfing(dict_arrs)
    assert(rule_output == 1)

