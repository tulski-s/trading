# 3rd party
import pytest

# custom
from position_size import (
    FixedCapitalPerc,
    MaxFirstEncountered,
    PercentageRisk,
)


@pytest.fixture
def candidates_1():
    return [
        {'symbol': 'c1', 'entry_type': 'long', 'price': 123},
    ]


@pytest.fixture
def candidates_2():
    return [
        {'symbol': 'c1', 'entry_type': 'long', 'price': 462},
        {'symbol': 'c2', 'entry_type': 'long', 'price': 387},
    ]


@pytest.fixture
def candidates_3():
    return [
        {'symbol': 'c1', 'entry_type': 'long', 'price': 111},
        {'symbol': 'c2', 'entry_type': 'short', 'price': 103},
        {'symbol': 'c3', 'entry_type': 'long', 'price': 194},
    ]


@pytest.fixture
def candidates_10():
    return [
        {'symbol': 'c10', 'entry_type': 'short', 'price': 11},
        {'symbol': 'c2', 'entry_type': 'long', 'price': 201},
        {'symbol': 'c7', 'entry_type': 'long', 'price': 222},
        {'symbol': 'c3', 'entry_type': 'short', 'price': 301},
        {'symbol': 'c9', 'entry_type': 'long', 'price': 50},
        {'symbol': 'c6', 'entry_type': 'long', 'price': 333},
        {'symbol': 'c1', 'entry_type': 'short', 'price': 101},
        {'symbol': 'c8', 'entry_type': 'long', 'price': 101},
        {'symbol': 'c4', 'entry_type': 'short', 'price': 401},   
        {'symbol': 'c5', 'entry_type': 'long', 'price': 501},   
    ]


@pytest.fixture
def candidates_3_stop_loss():
    return [
        {'symbol': 'c1', 'entry_type': 'long', 'price': 111, 'stop_loss':100},
        {'symbol': 'c2', 'entry_type': 'short', 'price': 103, 'stop_loss':80},
        {'symbol': 'c3', 'entry_type': 'long', 'price': 194, 'stop_loss':192},
    ]


def test_sorting_alphabetical(candidates_10):
    sorted_candidates = MaxFirstEncountered().sort(candidates_10)
    expected_symbols_order = ['c1', 'c10', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'c8', 'c9']
    assert([x['symbol'] for x in sorted_candidates] == expected_symbols_order)


def test_sorting_cheapest(candidates_10):
    sorted_candidates = [x['symbol'] for x in MaxFirstEncountered(sort_type='cheapest').sort(candidates_10)]
    expected_symbols_order_v1 = ['c10', 'c9', 'c1', 'c8', 'c2', 'c7', 'c3', 'c6', 'c4', 'c5']
    expected_symbols_order_v2 = ['c10', 'c9', 'c8', 'c9', 'c2', 'c7', 'c3', 'c6', 'c4', 'c5']
    assert(sorted_candidates in [expected_symbols_order_v1, expected_symbols_order_v2])


def test_decide_what_to_buy_1_can(candidates_1):
    symbols_to_buy = FixedCapitalPerc(capital_perc=0.1).decide_what_to_buy(5000, candidates_1, capital=10000)
    expected_symbols_to_buy = [{
        'symbol': 'c1',
        'entry_type': 'long',
        'shares_count': 8,
        'price': 123,
        'trx_value': 984,
        'fee': 4
    }]
    assert(symbols_to_buy == expected_symbols_to_buy)


def test_decide_what_to_buy_2_can(candidates_2):
    symbols_to_buy = FixedCapitalPerc(sort_type='cheapest', capital_perc=0.1).decide_what_to_buy(9000, candidates_2, capital=100000)
    expected_symbols_to_buy = [{
        'symbol': 'c2',
        'entry_type': 'long',
        'shares_count': 23,
        'price': 387,
        'trx_value': 8901,
        'fee': 33.82
    }]
    assert(symbols_to_buy == expected_symbols_to_buy)


def test_decide_what_to_buy_3_can(candidates_3):
    symbols_to_buy = FixedCapitalPerc(sort_type='cheapest', capital_perc=0.1).decide_what_to_buy(50000, candidates_3, capital=100000)
    """
    ile? (10000 / (103+(103*0.0038))) -> 96
    trx val -> 96 * 103 = 9888
    fee -> 9888*0.0038 = 37.5744 = 37.57
    po trx: 50000 - (9888+37.57) = 40074.43
    """
    expected_symbols_to_buy = [
        {
        'symbol': 'c2',
        'entry_type': 'short',
        'shares_count': 96,
        'price': 103,
        'trx_value': 9888,
        'fee': 37.57
        },
        {
        'symbol': 'c1',
        'entry_type': 'long',
        'shares_count': 89,
        'price': 111,
        'trx_value': 9879,
        'fee': 37.54
        },
        {
        'symbol': 'c3',
        'entry_type': 'long',
        'shares_count': 51,
        'price': 194,
        'trx_value': 9894,
        'fee': 37.60
        }
    ]
    assert(symbols_to_buy == expected_symbols_to_buy)


def test_decide_what_to_buy_3_cans_with_sl(candidates_3_stop_loss):
    symbols_to_buy = PercentageRisk(sort_type='expensive', perc_risk=0.5).decide_what_to_buy(30000, candidates_3_stop_loss, capital=300000)
    expected_symbols_to_buy = [{
        'symbol': 'c3',
        'entry_type': 'long',
        'shares_count': 154,
        'price': 194,
        'trx_value': 29876,
        'fee': 113.53
    }]
    assert(symbols_to_buy == expected_symbols_to_buy)


def test_decide_what_to_buy_missing_sl_when_its_mandatory(candidates_1):
    with pytest.raises(ValueError):
        symbols_to_buy = PercentageRisk().decide_what_to_buy(1, candidates_1, capital=2)
