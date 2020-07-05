# built in
import os
import pickle

# 3rd party
import numpy as np
from numpy.testing import assert_array_equal
import pandas as pd
import pytest

# custom
from signal_generator import (
    SignalGenerator,
    triggers_to_states,
)
import rules

def simple_rule1(arr):
    """ If average value in array < 0 then -1, else 1. if 0 then 0 """
    mean = arr.mean()
    if mean < 0:
        return -1
    elif mean > 0:
        return 1
    return 0


def mock_multiple_ts_rule(dict_arr):
    fv = 0
    for v in dict_arr.values():
        fv += v
    if fv < 0:
        return -1
    elif fv == 0:
        return 0
    elif fv > 0:
        return 1


def simple_rule2(arr):
    d = {
        0: 0, 1:0, 2:1, 3:0, 4:0, 5:0, 6:-1, 7:0, 8:0, 9:0, 10:1, 11:-1, 12:0, 13:0
    }
    return d[arr[-1]]


def simple_rule3(arr):
    d = {
        0: 0, 1:0, 2:0, 3:0, 4:1, 5:0, 6:-1, 7:0, 8:0, 9:1, 10:0, 11:1, 12:1, 13:0
    }
    return d[arr[-1]]


@pytest.fixture()
def config_1():
    return {
        'rules': [
            {
                'id': 'trend',
                'type': 'simple',
                'ts': 'close',
                'lookback': 10,
                'params': {},
                'func': rules.trend,
            }
        ],
        'strategy': {
            'type': 'fixed',
            'strategy_rules': ['trend']
        }
    }


@pytest.fixture()
def config_2():
    return {
        'rules': [
            {
                'id': 'mock_rule',
                'type': 'simple',
                'ts': 'close',
                'lookback': 3,
                'params': {},
                'func': simple_rule1,
            }
        ],
        'strategy': {
            'type': 'fixed',
            'strategy_rules': ['mock_rule']
        }
    }


@pytest.fixture()
def config_3():
    return {
        'rules': [
            {
                'id': 'trend',
                'type': 'simple',
                'ts': 'close',
                'lookback': 3,
                'params': {},
                'func': rules.trend,
            },
            {
                'id': 'supprot/resistance',
                'type': 'simple',
                'ts': 'close',
                'lookback': 6,
                'params': {},
                'func': rules.support_resistance,
            },
            {
                'id': 'trend+supprot/resistance',
                'type': 'convoluted',
                'simple_rules': ['trend', 'supprot/resistance'],
                'aggregation_type': 'combine',
                'aggregation_params':{'mode':'strong'}
            },
        ],
        'strategy': {
            'type': 'fixed',
            'strategy_rules': ['trend']
        }
    }


@pytest.fixture()
def config_4():
    return {
        'rules': [
            {
                'id': 'mock_rule',
                'type': 'simple',
                'ts': 'close',
                'lookback': 1,
                'params': {},
                'func': simple_rule1,
            }
        ],
        'strategy': {
            'type': 'fixed',
            'strategy_rules': ['mock_rule'],
            'constraints': {
                'wait_entry_confirmation': 2
            }
        }
    }


@pytest.fixture()
def config_5():
    return {
        'rules': [
            {
                'id': 'mock_rule',
                'type': 'simple',
                'ts': 'close',
                'lookback': 1,
                'params': {},
                'func': simple_rule1,
            }
        ],
        'strategy': {
            'type': 'fixed',
            'strategy_rules': ['mock_rule'],
            'constraints': {
                'hold_x_days': 2
            }
        }
    }


@pytest.fixture()
def config_6():
    return {
        'rules': [
            {
                'id': 'mock_rule',
                'type': 'simple',
                'ts': 'close',
                'lookback': 1,
                'params': {},
                'func': simple_rule1,
            }
        ],
        'strategy': {
            'type': 'fixed',
            'strategy_rules': ['mock_rule'],
            'constraints': {
                'hold_x_days': 2,
                'wait_entry_confirmation': 2,
            }
        }
    }


@pytest.fixture()
def config_7():
    return {
        'rules': [
            {
                'id': 'simple_rule_2',
                'type': 'simple',
                'ts': 'close',
                'lookback': 1,
                'params': {},
                'func': simple_rule2,
            },
            {
                'id': 'conv',
                'type': 'convoluted',
                'simple_rules': ['simple_rule_2'],
                'aggregation_type': 'state-based',
                'aggregation_params': {
                    'long': [
                        {'simple_rule_2': 1}
                    ],
                    'short': [
                        {'simple_rule_2': -1}
                    ]
                }
            }
        ],
        'strategy': {
            'type': 'fixed',
            'strategy_rules': ['conv', 'simple_rule_2']
        }
    }


@pytest.fixture()
def config_8():
    return {
        'rules': [
            {
                'id': 'simple_rule_2',
                'type': 'simple',
                'ts': 'close',
                'lookback': 1,
                'params': {},
                'func': simple_rule2,
            },
            {
                'id': 'simple_rule_3',
                'type': 'simple',
                'ts': 'close',
                'lookback': 1,
                'params': {},
                'func': simple_rule3,
            },
            {
                'id': 'conv',
                'type': 'convoluted',
                'simple_rules': ['simple_rule_2', 'simple_rule_3'],
                'aggregation_type': 'state-based',
                'aggregation_params': {
                    'long': [
                        {'simple_rule_2': 1, 'simple_rule_3': 0}
                    ],
                    'short': [
                        {'simple_rule_2': -1, 'simple_rule_3': 0},
                        {'simple_rule_2': -1, 'simple_rule_3': -1},
                    ],
                    'neutral': [
                        {'simple_rule_2': 0, 'simple_rule_3': 1}
                    ]
                }
            }
        ],
        'strategy': {
            'type': 'fixed',
            'strategy_rules': ['conv']
        }
    }


@pytest.fixture()
def config_9():
    return {
        'rules': [
            {
                'id': 'trend',
                'type': 'simple',
                'ts': 'close',
                'lookback': 2,
                'params': {},
                'func': rules.trend,
            },
            {
                'id': 'weigthed_ma',
                'type': 'simple',
                'ts': 'close',
                'lookback': 3,
                'params': {'weigth_ma': True},
                'func': rules.moving_average,
            },
        ],
        'strategy': {
            'type': 'learning',
            'strategy_rules': ['trend', 'weigthed_ma'],
            'params':{
                'memory_span': 5,
                'review_span': 4,
                'performance_metric': 'avg_log_returns',
                'price_label': 'close',
            }
        }
    }


@pytest.fixture()
def config_10():
    return {
        'rules': [
            {
                'id': 'trend',
                'type': 'simple',
                'ts': ['high', 'low', 'close'],
                'lookback': 0,
                'params': {},
                'func': mock_multiple_ts_rule,
            }
        ],
        'strategy': {
            'type': 'fixed',
            'strategy_rules': ['trend']
        }
    }


@pytest.fixture()
def pricing_df1():
    return pd.DataFrame({
        'close': [20,21,45,32,15,45,23,21,21,12,14,48,15],
    })


@pytest.fixture()
def pricing_df2():
    return pd.DataFrame({
        'close': [2,1,5,0,0,0,0,0,-2,-5,-7,10,20],
    })

@pytest.fixture()
def pricing_df3():
    return pd.DataFrame({
        'close': list(range(14)),
    })


def test_proper_index_access(config_1, pricing_df1):
    signal_generator = SignalGenerator(
        df=pricing_df1,
        config=config_1,
    )
    arr = signal_generator._get_ts('close', 8, 5)
    expected_arr = np.array([32,15,45,23,21,21])
    assert_array_equal(arr, expected_arr)


def test_simpl_rule_results_appending(config_2, pricing_df2):
    signal_generator = SignalGenerator(
        df=pricing_df2,
        config=config_2,
    )
    expected_simple_rule_output = [
        1, 1, 1, 0, 0, -1, -1, -1, -1, 1
    ]
    signal_generator._generate_initial_signal()
    simple_rule_output = signal_generator.rules_results['mock_rule']
    assert(simple_rule_output == expected_simple_rule_output)


def test_accessing_index_for_convoluted_rule(config_3, pricing_df1):
    """
    [0,  0, -1, -1, -1, 1, 1]   'trend'
    [0,  0,  0, -1,  0, 1, 0]   'supprot/resistance'
    """
    sg = SignalGenerator(df=pricing_df1, config=config_3)
    sg._generate_initial_signal()
    rules_ids = config_3['rules'][2]['simple_rules']
    idxs = [0,2,3,5]
    test_results = []
    for idx in idxs:
        test_results.append(sg._get_simple_rules_results(rules_ids, idx))
    expexted_results = [[0,0], [-1,0], [-1,-1], [1,1]]
    assert(test_results == expexted_results)


def test_combine_strong(config_3, pricing_df1):
    sg = SignalGenerator(df=pricing_df1, config=config_3)
    rules_results = [[-1,-1,-1], [-1,-1, 0], [1,1,1], [0,1,-1]]
    test_output = []
    for results in rules_results:
        test_output.append(sg.combine_simple_results(
            rules_results=results,
            aggregation_type='combine',
            aggregation_params={'mode':'strong'}
        ))
    expected_output = [-1,0,1,0]
    assert(test_output == expected_output)


def test_combine_majority_voting(config_3, pricing_df1):
    sg = SignalGenerator(df=pricing_df1, config=config_3)
    rules_results = [
        [-1,-1,-1, 0, 1], [-1, 1, 0], [1,1,1,-1,0], [0,0,1,1,-1]
    ]
    test_output = []
    for results in rules_results:
        test_output.append(sg.combine_simple_results(
            rules_results=results,
            aggregation_type='combine',
            aggregation_params={'mode':'majority_voting'}
        ))
    expected_output = [-1,0,1,0]
    assert(test_output == expected_output)


def test_convoluted_rule_results_appending(config_3, pricing_df1):
    sg = SignalGenerator(
        df=pricing_df1,
        config=config_3,
    )
    expected_convoluted_rule_output = [
        0, 0, 0, -1, 0, 1, 0
    ]
    sg._generate_initial_signal()
    convoluted_rule_output = sg.rules_results['trend+supprot/resistance']
    assert(convoluted_rule_output == expected_convoluted_rule_output)


def test_combining_state_based_convoluted_rule_binary(config_7, pricing_df3):
    sg = SignalGenerator(
        df=pricing_df3,
        config=config_7,
    )
    sg._generate_initial_signal()
    expected_initial_signal = [0, 1, 1, 1, 1, -1, -1, -1, -1, 1, -1, -1, -1]
    initial_signal = sg.rules_results['conv']
    assert(expected_initial_signal == initial_signal)


def test_combining_state_based_convoluted_rule_3_states(config_8, pricing_df3):
    sg = SignalGenerator(
        df=pricing_df3,
        config=config_8,
    )
    sg._generate_initial_signal()
    expected_initial_signal = [0, 1, 1, 0, 0, -1, -1, -1, 0, 1, 1, 0, 0]
    initial_signal = sg.rules_results['conv']
    assert(expected_initial_signal == initial_signal)


def test_generate_final_signal_no_constraints(pricing_df1, config_2):
    sg = SignalGenerator(df=pricing_df1, config=config_2)
    test_initial_results = [1,1,1,1,0,0,0,-1,-1,1]
    test_final_results = sg._generate_final_signal(test_initial_results, return_signal=True)
    test_final_results.drop(['close'], axis=1, inplace=True)
    expected_results = pd.DataFrame({
        'entry_long': [0,0,0] + [1,0,0,0,0,0,0,0,0,1],
        'exit_long': [0,0,0] + [0,0,0,0,1,0,0,0,0,0],
        'entry_short': [0,0,0] + [0,0,0,0,0,0,0,1,0,0],
        'exit_short': [0,0,0] + [0,0,0,0,0,0,0,0,0,1],
    })
    assert(expected_results.to_dict() == test_final_results.to_dict())


def test_generate_final_signal_with_wait_for_confirmation(pricing_df1, config_4):
    sg = SignalGenerator(df=pricing_df1, config=config_4)
    # powinno miec 12. bo 13 df + 1d lookback
    test_initial_results = [0, 1, 1, 1, 1, -1, 0, 1, -1, -1, -1, 1]
    test_final_results = sg._generate_final_signal_with_constraints(
        test_initial_results, return_signal=True,
    )
    test_final_results.drop(['close'], axis=1, inplace=True)
    expected_results = pd.DataFrame({
        'entry_long': [0] + [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0],
        'exit_long': [0] + [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0],
        'entry_short': [0] + [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0],
        'exit_short': [0] + [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    })
    assert(expected_results.to_dict() == test_final_results.to_dict())


def test_generate_final_signal_with_hold_for_x_days(pricing_df1, config_5):
    sg = SignalGenerator(df=pricing_df1, config=config_5)
    test_initial_results = [1, 1, 1, 1, 1, -1, -1, -1, -1, 0, 0, 0]
    test_final_results = sg._generate_final_signal_with_constraints(
        test_initial_results, return_signal=True,
    )
    test_final_results.drop(['close'], axis=1, inplace=True)
    expected_results = pd.DataFrame({
        'entry_long': [0] + [1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0],
        'exit_long': [0] + [0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0],
        'entry_short': [0] + [0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0],
        'exit_short': [0] + [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
    })
    assert(expected_results.to_dict() == test_final_results.to_dict())


def test_generate_with_hold_for_x_days_exeed_index(pricing_df1, config_6):
    config_6['strategy']['constraints']['hold_x_days'] = 100
    init_results = [-1, 0, -1, 1, 1, -1, 1, 1, 1, 1, 0, 1]
    sg = SignalGenerator(df=pricing_df1, config=config_6)
    test_final_results = sg._generate_final_signal_with_constraints(
        init_results, return_signal=True
    )
    test_final_results.drop(['close'], axis=1, inplace=True)
    expected_results = pd.DataFrame({
        'entry_long': 13*[0],
        'exit_long': 13*[0],
        'entry_short': [0, 0, 0] + [1] + 9*[0],
        'exit_short': 13*[0],
    })
    assert(expected_results.to_dict() == test_final_results.to_dict())


def test_generate_final_signal_with_both_constraints(pricing_df1, config_6):
    sg = SignalGenerator(df=pricing_df1, config=config_6)
    test_initial_results = [-1, 0, -1, 1, 1, -1, 1, 1, 1, 1, 0, 1]
    test_final_results = sg._generate_final_signal_with_constraints(
        test_initial_results, return_signal=True
    )
    test_final_results.drop(['close'], axis=1, inplace=True)
    expected_results = pd.DataFrame({
        'entry_long': [0] + [0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0],
        'exit_long': [0] + [0, 0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 1],
        'entry_short': [0] + [0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        'exit_short': [0] + [0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0],
    })
    assert(expected_results.to_dict() == test_final_results.to_dict())


def test_review_performance_daily_returns(pricing_df3, config_7):
    sg = SignalGenerator(df=pricing_df3, config=config_7)
    sg._generate_initial_signal()
    # need to set up some values which would be there if "learning" strategy in config
    sg.past_reviews = {rule_id: [] for rule_id in sg.strategy_rules}
    sg.strategy_metric = 'daily_returns'
    sg.df.loc[:, 'daily_returns__learning'] = sg.df['close'].pct_change()
    sg._review_performance(strat_idx=3, end_idx=8)
    expected_conv = 0.073809
    expected_simple_rule_2 = -0.2
    assert(sg.past_reviews['conv'][0] == pytest.approx(expected_conv, abs=1e-4))
    assert(sg.past_reviews['simple_rule_2'][0] == pytest.approx(expected_simple_rule_2, abs=1e-4))


def test_review_performance_avg_log_returns(pricing_df1, config_9):
    sg = SignalGenerator(df=pricing_df1, config=config_9)
    sg._generate_initial_signal()
    """
    idx   close   daily log ret   trend  weigthed_ma   result_idx
    0      20             NaN       NaN      NaN          NaN
    1      21      -16.955478       NaN      NaN          NaN
    2      45      -17.193338       NaN      NaN          NaN
    3      32      -41.534264       1         -1          0
    4      15      -29.291950       -1        -1          1
    5      45      -11.193338       1         -1          2
    6      23      -41.864506       1         -1          3
    7      21      -19.955478       -1        -1          4
    8      21      -17.955478       -1        -1          5
    9      12      -18.515093       -1        -1          6
    10     14       -9.360943       -1        -1          7
    11     48      -10.128799       1          1          8
    12     15      -45.291950.      1.        -1          9
    """
    sg._review_performance(strat_idx=3, end_idx=6)
    expected_trend_metric = (-41.864506+19.955478+17.955478)/3
    expected_ma_metric = (41.864506+19.955478+17.955478)/3
    assert(sg.past_reviews['trend'][-1] == pytest.approx(expected_trend_metric, abs=1e-4))
    assert(sg.past_reviews['weigthed_ma'][-1] == pytest.approx(expected_ma_metric, abs=1e-4))


def test_review_performance_avg_log_returns_with_neutral(pricing_df1, config_9):
    sg = SignalGenerator(df=pricing_df1, config=config_9)
    sg._generate_initial_signal()
    sg.rules_results['trend'][3] = 0
    sg._review_performance(strat_idx=3, end_idx=6)
    expected_trend_metric = (19.955478+17.955478)/3
    assert(sg.past_reviews['trend'][-1] == pytest.approx(expected_trend_metric, abs=1e-4))


def test_review_performance_avg_log_returns_held_only(pricing_df1, config_9):
    config_9['strategy']['params']['performance_metric'] = 'avg_log_returns_held_only'
    sg = SignalGenerator(df=pricing_df1, config=config_9)
    sg._generate_initial_signal()
    sg.rules_results['trend'][3] = 0
    sg._review_performance(strat_idx=3, end_idx=6)
    expected_trend_metric = (19.955478+17.955478)/2
    assert(sg.past_reviews['trend'][-1] == pytest.approx(expected_trend_metric, abs=1e-4))


def test_review_performance_voting(pricing_df1, config_9):
    config_9['strategy']['params']['performance_metric'] = 'voting'
    sg = SignalGenerator(df=pricing_df1, config=config_9)
    sg._generate_initial_signal()
    sg.rules_results['trend'][3] = 0
    sg._review_performance(strat_idx=3, end_idx=6)
    expected_trend_metric = (2, 1, 0)
    for idx in range(3):
        assert(sg.past_reviews['trend'][-1][idx] == expected_trend_metric[idx])


def test_review_performance_rule_output(pricing_df1, config_9):
    config_9['strategy']['params']['performance_metric'] = 'daily_returns'
    sg = SignalGenerator(df=pricing_df1, config=config_9)
    sg._generate_initial_signal()
    expected_best_rule = 'weigthed_ma'
    test_best_rule = sg._review_performance(strat_idx=6, end_idx=10)
    assert(test_best_rule == expected_best_rule)


def test_review_performance_position_tie_output(pricing_df1, config_9):
    config_9['strategy']['params']['performance_metric'] = 'voting'
    sg = SignalGenerator(df=pricing_df1, config=config_9)
    sg._generate_initial_signal()
    test_position = sg._review_performance(strat_idx=7, end_idx=9)
    expected_position = 0
    assert(test_position == expected_position)


def test_review_performance_position_output(pricing_df1, config_9):
    config_9['strategy']['params']['performance_metric'] = 'voting'
    sg = SignalGenerator(df=pricing_df1, config=config_9)
    sg._generate_initial_signal()
    test_position = sg._review_performance(strat_idx=5, end_idx=9)
    expected_position = -1
    assert(test_position == expected_position)


def test_init_signal_learning_avg_log_returns(pricing_df1, config_9):
    sg = SignalGenerator(df=pricing_df1, config=config_9)
    sg._generate_initial_signal()
    expected_past_reviews = {
        'trend': [pytest.approx(-16.3250395, abs=1e-4), pytest.approx(4.7844972, abs=1e-4)],
        'weigthed_ma': [pytest.approx(30.9710145, abs=1e-4), pytest.approx(21.5302996, abs=1e-4)],
    }
    test_past_reviews = sg.past_reviews
    assert(sg.past_reviews == expected_past_reviews)


def test_init_signal_learning_avg_log_returns_rule_following(pricing_df1, config_9):
    sg = SignalGenerator(df=pricing_df1, config=config_9)
    test_initial_signal = sg._generate_initial_signal()
    expected_initial_signal = [0, 0, 0, -1, -1, -1, -1, -1, 1, -1]
    assert(test_initial_signal == expected_initial_signal)


def test_init_signal_learning_avg_log_returns_long_review_period(pricing_df1, config_9):
    config_9['strategy']['params']['memory_span'] = 40
    config_9['strategy']['params']['review_span'] = 20
    sg = SignalGenerator(df=pricing_df1, config=config_9)
    sg._generate_initial_signal()
    expected_past_reviews = {
        'trend': [pytest.approx(-9.068936, abs=1e-4)],
        'weigthed_ma': [pytest.approx(28.7679072, abs=1e-4)],
    }
    assert(sg.past_reviews == expected_past_reviews)


def test_init_signal_learning_voting_position_following(pricing_df1, config_9):
    config_9['strategy']['params']['performance_metric'] = 'voting'
    config_9['strategy']['params']['memory_span'] = 40
    config_9['strategy']['params']['review_span'] = 20
    sg = SignalGenerator(df=pricing_df1, config=config_9)
    test_initial_signal = sg._generate_initial_signal()
    expected_initial_signal = [0, 0, 0, 0, -1, -1, -1, -1, -1, -1]
    assert(test_initial_signal == expected_initial_signal)


def test_triggers_to_states_1(pricing_df1, config_1):
    mock_rule_triggers = {
        'entry_long':  [0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0],
        'exit_long':   [0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0],
        'entry_short': [0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0],
        'exit_short':  [0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0],
    }
    df = pd.DataFrame(mock_rule_triggers)
    test_states = triggers_to_states(df)
    expected_states = [0, 0, 1, -1, 1, 1, 1, 0, 0, -1, -1]
    assert(test_states == expected_states)


def test_triggers_to_states_legacy_vs_new(pricing_df1, config_3):
    sg = SignalGenerator(df=pricing_df1, config=config_3)
    test_results = sg.generate()
    legacy_impl = triggers_to_states(test_results)
    attr_impl = sg.final_positions
    assert(legacy_impl == attr_impl)


def test_multiple_ts_in_simple_rule_1(config_10):
    df = pd.DataFrame({
        'close': [1, 1, 1, 1, 1],
        'high': [2, 2, 2, 2, 2],
        'low': [-3, -2, 0, -4, -2]
    })
    sg = SignalGenerator(df=df, config=config_10)
    test_results = sg.generate()
    test_signals = triggers_to_states(test_results)
    expected_signals = [0, 1, 1, -1, 1]
    assert(test_signals == expected_signals)


def test_hold_x_days_on_rule_lvl2(pricing_df2, config_2):
    config_2['rules'][0]['hold_fixed_days'] = 3
    sg = SignalGenerator(df=pricing_df2, config=config_2)
    test_results = sg.generate()
    test_signals = triggers_to_states(test_results)
    expected_signals = [0, 0, 0, 1, 1, 1, 0, 0, -1, -1, -1, -1, -1]
    assert(test_signals == expected_signals)


def test_save_rules_results(tmpdir, config_7, pricing_df3):
    sg = SignalGenerator(
        df=pricing_df3,
        config=config_7,
    )
    prefix = 'XYZ_'
    sg.generate()
    sg.save_rules_results(path=tmpdir, prefix=prefix)
    test_results = []
    for rule_id in sg.rules_results.keys():
        expected_res = sg.rules_results[rule_id]
        with open(os.path.join(tmpdir, prefix+rule_id), 'rb') as fh:
            test_res = pickle.load(fh)
        if test_res == expected_res:
            test_results.append(True)
        else:
            test_results.append(False)
    assert(all(test_results) == True)


def test_load_rules_results(tmpdir, config_7, pricing_df3):
    sg = SignalGenerator(
        df=pricing_df3,
        config=config_7,
    )
    prefix = 'ABC_'
    sg.generate()
    sg.save_rules_results(path=tmpdir, prefix=prefix)
    sg_raw = SignalGenerator(
        df=pricing_df3,
        config=config_7,
        load_rules_results_path=tmpdir,
        load_rules_results_prefix=prefix
    )
    assert(sg.rules_results == sg_raw.rules_results)
    

def test_results_with_load_rules(tmpdir, config_7, pricing_df3):
    sg = SignalGenerator(
        df=pricing_df3,
        config=config_7,
    )
    sg.generate()
    sg.save_rules_results(path=tmpdir)
    sg_loaded = SignalGenerator(
        df=pricing_df3,
        config=config_7,
        load_rules_results_path=tmpdir
    )
    sg_loaded.generate()
    same_results = []
    for rule_id in sg.rules_results.keys():
        if sg.rules_results[rule_id] == sg_loaded.rules_results[rule_id]:
            same_results.append(True)
        else:
            same_results.append(False)
    assert(all(same_results) == True)


def test_positions_in_final_df(pricing_df2, config_2):
    sg = SignalGenerator(df=pricing_df2, config=config_2)
    test_results = sg.generate()
    test_positions = test_results['position'].tolist()
    expected_positions = [0, 0, 0, 1, 1, 1, 0, 0, -1, -1, -1, -1, 1]
    assert(test_positions == expected_positions)


def test_reversed_strategy(pricing_df2, config_2):
    sg_org = SignalGenerator(df=pricing_df2, config=config_2)
    org_results = sg_org.generate()
    config_2['strategy']['reversed'] = True
    sg_rev = SignalGenerator(df=pricing_df2, config=config_2)
    test_results = sg_rev.generate()
    expected_results = []
    for test, org in [
        ('entry_long', 'entry_short'), ('entry_short', 'entry_long'),
        ('exit_long', 'exit_short'), ('exit_short', 'exit_long')
    ]:
        if test_results[test].tolist() == org_results[org].tolist():
            expected_results.append(True)
        else:
            expected_results.append(False)
    if test_results['position'].tolist() == (-1*org_results['position']).tolist():
        expected_results.append(True)
    else:
        expected_results.append(False)
    assert(all(expected_results) == True)
