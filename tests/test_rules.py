# 3rd party
import numpy as np
import pytest

# custom
import rules


@pytest.fixture()
def prices_global_sr():
    return np.array([100, 210, 90, 80, 12, 60, 45, 78])


@pytest.fixture()
def prices_local_sr():
    return np.array(
        [161.7, 159.3, 152.5, 148.1, 148.5, 157.7, 156.7, 153.9, 
         149.,  153.8, 150.5, 152.5, 153.3, 151.1, 149.2, 154., 
         151.4, 151.9, 154.3,]    
    )


def test_uptrend():
    arr = np.array([1,2,3,4,5,6])
    rule_output = rules.trend(arr)
    assert(rule_output == 1)


def test_downtrend():
    arr = np.array([9,8,7,6,5,4])
    rule_output = rules.trend(arr)
    assert(rule_output == -1)


def test_horizontal():
    arr = np.array([-1,10,-2, 12, 0, 3])
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

