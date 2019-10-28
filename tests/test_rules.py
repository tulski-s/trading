# 3rd party
import numpy as np

# custom
import rules

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