# 3rd party
import numpy as np
from numpy.testing import assert_array_equal
import pandas as pd
import pytest


# custom
from signal_generator import (
    SignalGenerator,
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
def pricing_df1():
    return pd.DataFrame({
        'close': [20,21,45,32,15,45,23,21,21,12,14,48,15],
    })


@pytest.fixture()
def pricing_df2():
    return pd.DataFrame({
        'close': [2,1,5,0,0,0,0,0,-2,-5,-7,10,20],
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
    [1, -1, -1, -1, -1, 1, 1]   'trend'
    [0,  0,  0, -1,  0, 1, 0]   'supprot/resistance'
    """
    sg = SignalGenerator(df=pricing_df1, config=config_3)
    sg._generate_initial_signal()
    rules_ids = config_3['rules'][2]['simple_rules']
    idxs = [0,2,3,5]
    test_results = []
    for idx in idxs:
        test_results.append(sg._get_simple_rules_results(rules_ids, idx))
    expexted_results = [[1,0], [-1,0], [-1,-1], [1,1]]
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


def test_generate_final_signal_no_constraints(pricing_df1, config_2):
    sg = SignalGenerator(df=pricing_df1, config=config_2)
    test_initial_results = [1,1,1,1,0,0,0,-1,-1,1]
    test_final_results = sg._generate_final_signal(test_initial_results)
    test_final_results.drop(['close'], axis=1, inplace=True)
    expected_results = pd.DataFrame({
        'entry_long': [0,0,0] + [1,0,0,0,0,0,0,0,0,1],
        'exit_long': [0,0,0] + [0,0,0,0,1,0,0,0,0,0],
        'entry_short': [0,0,0] + [0,0,0,0,0,0,0,1,0,0],
        'exit_short': [0,0,0] + [0,0,0,0,0,0,0,0,0,1],
    })
    assert(expected_results.to_dict() == test_final_results.to_dict())

