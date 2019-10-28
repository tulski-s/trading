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
            'rules': ['trend']
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
            'rules': ['trend']
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
        df = pricing_df1,
        config = config_1,
    )
    arr = signal_generator._get_ts('close', 8, 5)
    expected_arr = np.array([32,15,45,23,21,21])
    assert_array_equal(arr, expected_arr)


def test_rule_results_appending(config_2, pricing_df2):
    signal_generator = SignalGenerator(
        df = pricing_df2,
        config = config_2,
    )
    expected_simple_rule_output = [
        1, 1, 1, 0, 0, -1, -1, -1, -1, 1
    ]
    signal_generator.generate()
    simple_rule_output = signal_generator.rules_results['mock_rule']
    print('simple_rule_output is: ', simple_rule_output)
    assert(simple_rule_output == expected_simple_rule_output)


"""
TODO(slaw) - add more tests which could be useful
+ explicit test for results lengths 
""" 
