# built-in
import datetime
import itertools
import os
import pickle
from pprint import pprint
import random
import re
import sys
import time
PATH = '/Users/slaw/osobiste/trading'
sys.path.insert(0, PATH)

# 3rd party
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# custom
import backtester
import gpw_data
import strategies.helpers as helpers
import position_size
import results
import rules
import rules_mining
import signal_generator


ALL_SIGNALS_PATH = '/Users/slaw/osobiste/trading/data_mining_rules'
REVERSED_RULE_PREFIX = 'reversed_'
COMPLEX_RULE_PREFIX = 'CPX'
PRICE_LABEL = 'close'


def filter_rules():
    ### define and append filter rules
    filter_rules_configs = []
    fr_lookbacks = [3, 7, 14, 28, 56] # 5
    fr_xs = [
        0.005, 0.01, 0.015, 0.02, 0.025, 0.03, 0.035, 0.04, 0.045, 
        0.05, 0.06, 0.07, 0.08,  0.09, 0.1, 0.12, 0.14, 0.16, 0.18, 
        0.2, 0.25, 0.3, 0.4, 0.5
    ] # 24
    fr_cs = [5, 10, 25] # 3
    fr_es = [2, 3, 5, 10, 15, 20] # 6
    fr_ys = [0.005, 0.01, 0.015, 0.02, 0.025, 0.03, 0.04, 0.05, 0.07, 0.1, 0.14, 0.2] # 12


    # filter rule "Filter X DL" (1)
    filter_prefix_1 = 'filter_{x}_DL_lb{lb}'
    filter_prefix_1_desc = """
    TL;DR - If price above x% from last low - long. If x% below last high - go short. One always keeps position. 
    Last high/lows are within lookback period
    """
    for lb in fr_lookbacks:
        for x in fr_xs:
            rule_id = filter_prefix_1.format(x=str(x).replace('0.',''), lb=lb)
            filter_rules_configs.append({
                'rules': [
                    {
                        'id': rule_id+'_simple',
                        'type': 'simple',
                        'ts': 'close',
                        'lookback': lb,
                        'params': {
                            'b': x
                        },
                        'func': rules.support_resistance
                    },
                    {
                        'id': rule_id,
                        'type': 'convoluted',
                        'simple_rules': [rule_id+'_simple'],
                        'aggregation_type': 'state-based',
                        'aggregation_params': {
                            'long': [
                                {rule_id+'_simple': 1}
                            ],
                            'short': [
                                {rule_id+'_simple': -1}
                            ]
                        }
                    }
                ],
                'strategy': {
                    'type': 'fixed',
                    'strategy_rules': [rule_id],
                    'strategy_id': rule_id,
                }
            })
            
    # print('current number of appended filter rules is: ', len(filter_rules_configs))

    # filter rule "Filter X DL C" (2)
    filter_prefix_2 = 'filter_{x}_DL_C{c}_lb{lb}'
    filter_prefix_2_desc = """
    TL;DR - Same as "Filter X DL", but one keeps position only for C days.
    """
    for lb in fr_lookbacks:
        for c in fr_cs:
            for x in fr_xs:
                rule_id = filter_prefix_2.format(x=str(x).replace('0.',''), lb=lb, c=c)
                filter_rules_configs.append({
                    'rules': [
                        {
                            'id': rule_id+'_simple',
                            'type': 'simple',
                            'ts': 'close',
                            'lookback': lb,
                            'params': {
                                'b': x
                            },
                            'func': rules.support_resistance
                        },
                        {
                            'id': rule_id,
                            'type': 'convoluted',
                            'simple_rules': [rule_id+'_simple'],
                            'aggregation_type': 'state-based',
                            'aggregation_params': {
                                'long': [
                                    {rule_id+'_simple': 1}
                                ],
                                'short': [
                                    {rule_id+'_simple': -1}
                                ]
                            },
                            'hold_fixed_days': c,
                        }
                    ],
                    'strategy': {
                        'type': 'fixed',
                        'strategy_rules': [rule_id],
                        'strategy_id': rule_id,
                    }
                })
                
    # print('current number of appended filter rules is: ', len(filter_rules_configs))
                
                
    # filter rule "Filter X EL" (3)
    filter_prefix_3 = 'filter_{x}_EL{e}_lb{lb}'
    filter_prefix_3_desc = """
    TL;DR - Same as "Filter X DL", but last high/low is defined as the most recent 
    closing price that is less (greater) than the "e" previous closing prices
    """
    for lb in fr_lookbacks:
        for e in fr_es:
            if e>=lb:
                continue
            for x in fr_xs:
                rule_id = filter_prefix_3.format(x=str(x).replace('0.',''), lb=lb, e=e)
                filter_rules_configs.append({
                    'rules': [
                        {
                            'id': rule_id+'_simple',
                            'type': 'simple',
                            'ts': 'close',
                            'lookback': lb,
                            'params': {
                                'e': e,
                                'b': x,
                            },
                            'func': rules.support_resistance
                        },
                        {
                            'id': rule_id,
                            'type': 'convoluted',
                            'simple_rules': [rule_id+'_simple'],
                            'aggregation_type': 'state-based',
                            'aggregation_params': {
                                'long': [
                                    {rule_id+'_simple': 1}
                                ],
                                'short': [
                                    {rule_id+'_simple': -1}
                                ]
                            }
                        }
                    ],
                    'strategy': {
                        'type': 'fixed',
                        'strategy_rules': [rule_id],
                        'strategy_id': rule_id,
                    }
                })
                
    # print('current number of appended filter rules is: ', len(filter_rules_configs))


    # filter rule "Filter X EL C" (4)
    filter_prefix_4 = 'filter_{x}_EL{e}_C{c}_lb{lb}'
    filter_prefix_4_desc = """
    TL;DR - Same as "Filter X EL", but hold C days only
    """
    for lb in fr_lookbacks:
        for e in fr_es:
            if e>=lb:
                continue
            for c in fr_cs:
                for x in fr_xs:
                    rule_id = filter_prefix_4.format(x=str(x).replace('0.',''), lb=lb, e=e, c=c)
                    filter_rules_configs.append({
                        'rules': [
                            {
                                'id': rule_id+'_simple',
                                'type': 'simple',
                                'ts': 'close',
                                'lookback': lb,
                                'params': {
                                    'e': e,
                                    'b': x,
                                },
                                'func': rules.support_resistance
                            },
                            {
                                'id': rule_id,
                                'type': 'convoluted',
                                'simple_rules': [rule_id+'_simple'],
                                'aggregation_type': 'state-based',
                                'aggregation_params': {
                                    'long': [
                                        {rule_id+'_simple': 1}
                                    ],
                                    'short': [
                                        {rule_id+'_simple': -1}
                                    ]
                                },
                                'hold_fixed_days': c,
                            }
                        ],
                        'strategy': {
                            'type': 'fixed',
                            'strategy_rules': [rule_id],
                            'strategy_id': rule_id,
                        }
                    })
                    
    # filter rule "Filter XY" (5)
    filter_prefix_5 = 'filter_X{x}Y{y}_lb{lb}'
    filter_prefix_5_desc = """
    TL;DR - Same "Filter X DL" but allows for dynamic neutral position. If price goes down by Y from last 
    high - go neutral (from long). If price go up from recent low by Y go neutral (from short).
    """
    for lb in fr_lookbacks:
        for x in fr_xs:
            for y in fr_ys:
                if y >= x:
                    continue
                rule_id = filter_prefix_5.format(
                    x=str(x).replace('0.',''), y=str(y).replace('0.',''), lb=lb
                )
                filter_rules_configs.append({
                    'rules':[
                        {
                            'id': rule_id+'_simple_x',
                            'type': 'simple',
                            'ts': 'close',
                            'lookback': lb,
                            'params': {
                                'b': x,
                            },
                            'func': rules.support_resistance
                        },
                        {
                            'id': rule_id+'_simple_y',
                            'type': 'simple',
                            'ts': 'close',
                            'lookback': lb,
                            'params': {
                                'b': y,
                            },
                            'func': rules.support_resistance
                        },
                        {
                            'id': rule_id,
                            'type': 'convoluted',
                            'simple_rules': [rule_id+'_simple_x', rule_id+'_simple_y'],
                            'aggregation_type': 'state-based',
                            'aggregation_params': {
                                'long': [
                                    {rule_id+'_simple_x': 1, rule_id+'_simple_y': 1}
                                ],
                                'short': [
                                    {rule_id+'_simple_x': -1, rule_id+'_simple_y': -1}
                                ],
                                'neutral': [
                                    {rule_id+'_simple_x': 0, rule_id+'_simple_y': 1},
                                    {rule_id+'_simple_x': 0, rule_id+'_simple_y': -1},
                                ]
                            }
                        }
                    ],
                    'strategy': {
                        'type': 'fixed',
                        'strategy_rules': [rule_id],
                        'strategy_id': rule_id,
                    }
                })

                
    filter_1_exp_no = 120
    filter_2_exp_no = 360
    filter_3_exp_no = 480 # 24 + (3*24) + (4*24) + (6*24) + (6*24)
    filter_4_exp_no = 1440 # 3*480
    filter_5_exp_no = 925

    all_filter_rules_no = filter_1_exp_no+filter_2_exp_no+filter_3_exp_no+filter_4_exp_no+filter_5_exp_no

    assert(len(filter_rules_configs) == all_filter_rules_no)

    return filter_rules_configs


def support_resistance_rules():
    ### define and append support&resistance
    support_resistance_rules_configs = []
    sr_lookbacks = [3, 7, 14, 28, 56] # 5
    sr_cs = [5, 10, 15, 25] # 4
    sr_es = [2, 3, 5, 10, 15, 20] # 6

    # S&R rule "SR DL C" (1)
    sr_prefix_1 = 'sr_DL_C{c}_lb{lb}'
    sr_prefix_1_desc = """
    TL;DR - If price above last low - go long. If below last high - go short. Keep position for C days. 
    Last high/lows are within lookback period.
    """
    for lb in sr_lookbacks:
        for c in sr_cs:
            rule_id = sr_prefix_1.format(lb=lb, c=c)
            support_resistance_rules_configs.append({
                'rules': [
                    {
                        'id': rule_id+'simple',
                        'type': 'simple',
                        'ts': 'close',
                        'lookback': lb,
                        'func': rules.support_resistance,
                        'params': {}
                    },
                    {
                        'id': rule_id,
                        'type': 'convoluted',
                        'simple_rules': [rule_id+'simple'],
                        'aggregation_type': 'state-based',
                        'aggregation_params': {
                            'long': [
                                {rule_id+'simple': 1}
                            ],
                            'short': [
                                {rule_id+'simple': -1}
                            ]
                        },
                        'hold_fixed_days': c,
                    }
                ],
                'strategy': {
                    'type': 'fixed',
                    'strategy_rules': [rule_id],
                    'strategy_id': rule_id,
                }
            })
            
    # S&R rule "SR EL C" (2)
    sr_prefix_2 = 'sr_EL{e}_C{c}_lb{lb}'
    sr_prefix_2_desc = 'TL;DR - Same as "SR DL C" but alternative higl/low definition'
    for lb in sr_lookbacks:
        for e in sr_es:
            if e>=lb:
                continue
            for c in sr_cs:
                rule_id = sr_prefix_2.format(lb=lb, c=c, e=e)
                support_resistance_rules_configs.append({
                'rules': [
                    {
                        'id': rule_id+'simple',
                        'type': 'simple',
                        'ts': 'close',
                        'lookback': lb,
                        'params': {
                            'e': e,
                        },
                        'func': rules.support_resistance
                    },
                    {
                        'id': rule_id,
                        'type': 'convoluted',
                        'simple_rules': [rule_id+'simple'],
                        'aggregation_type': 'state-based',
                        'aggregation_params': {
                            'long': [
                                {rule_id+'simple': 1}
                            ],
                            'short': [
                                {rule_id+'simple': -1}
                            ]
                        },
                        'hold_fixed_days': c,
                    }
                ],
                'strategy': {
                    'type': 'fixed',
                    'strategy_rules': [rule_id],
                    'strategy_id': rule_id,
                }
            })
            
            
    sr_1_exp_no = 20
    sr_2_exp_no = 80 # 20+(4*4)+(4*4)+(3*4)+(2*4)+(2*4) = 80
    all_sr_rules_no = sr_1_exp_no+sr_2_exp_no

    assert(len(support_resistance_rules_configs) == all_sr_rules_no)
    return support_resistance_rules_configs


def ma_rules():
    ### define price moving averages rules
    ma_rules_configs = []
    ma_ns = [2, 5, 10, 15, 20, 25, 30, 40, 50, 75, 100, 125, 150, 200, 250] # 15
    ma_bs = [0.001, 0.005, 0.01, 0.015, 0.02, 0.03, 0.04, 0.05] # 8
    ma_ds = [2, 3, 4, 5]
    ma_cs = [5, 10, 25, 50]

    # MA rule "MA S n{n}" (1)
    ma_prefix_1 = 'ma_S_n{n}'
    ma_prefix_1_desc = """
    TL;DR - If price above moving average - long. If below - short. last low - go long. If below last high - go short.
    """
    for n in ma_ns:
        rule_id = ma_prefix_1.format(n=n)
        ma_rules_configs.append({
            'rules': [
                {
                    'id': rule_id,
                    'type': 'simple',
                    'ts': 'close',
                    'lookback': n,
                    'params': {},
                    'func': rules.moving_average
                }
            ],
            'strategy': {
                'type': 'fixed',
                'strategy_rules': [rule_id],
                'strategy_id': rule_id,
            }
        })

    # MA rule "MA S n m" (2)
    ma_prefix_2 = 'ma_S_n{n}m{m}'
    ma_prefix_2_desc = """
    Two moving averages - long and short one. Long is when short above long one. Short when below. 
    """
    for n in ma_ns:
        for m in ma_ns:
            if m >= n:
                # m should be shorter MA
                continue
            rule_id = ma_prefix_2.format(n=n, m=m)
            ma_rules_configs.append({
                'rules': [
                    {
                        'id': rule_id,
                        'type': 'simple',
                        'ts': 'close',
                        'lookback': n,
                        'params': {
                            'quick_ma_lookback': m,
                        },
                        'func': rules.moving_average
                    }
                ],
                'strategy': {
                    'type': 'fixed',
                    'strategy_rules': [rule_id],
                    'strategy_id': rule_id,
                }
            })
            
    # MA rule "MA S n m b" (3)
    ma_prefix_3 = 'ma_S_n{n}m{m}_b{b}'
    ma_prefix_3_desc = """Same as "MA S n m" but with additional parameter b. To enter position, quick ma price need 
    to be lower/higher from long one by this multiplicative value b
    """
    for n in ma_ns:
        for m in ma_ns:
            if m >= n:
                # m should be shorter MA
                continue
            for b in ma_bs:
                rule_id = ma_prefix_3.format(n=n, m=m, b=str(b).replace('0.',''))
                ma_rules_configs.append({
                    'rules': [
                        {
                            'id': rule_id,
                            'type': 'simple',
                            'ts': 'close',
                            'lookback': n,
                            'params': {
                                'quick_ma_lookback': m,
                                'b': b,
                            },
                            'func': rules.moving_average
                        }
                    ],
                    'strategy': {
                        'type': 'fixed',
                        'strategy_rules': [rule_id],
                        'strategy_id': rule_id,
                    }
                })
                
    # MA rule "MA S n m d" (4)
    ma_prefix_4 = 'ma_S_n{n}m{m}_d{d}'
    ma_prefix_4_desc = """Same as "MA S n m" but with additional parameter d. "d" is the time delay filter 
    required the buy or sell signal to remain valid before action is taken.
    """
    for n in ma_ns:
        for m in ma_ns:
            if m >= n:
                # m should be shorter MA
                continue
            for d in ma_ds:
                rule_id = ma_prefix_4.format(n=n, m=m, d=d)
                ma_rules_configs.append({
                    'rules': [
                        {
                            'id': rule_id,
                            'type': 'simple',
                            'ts': 'close',
                            'lookback': n,
                            'params': {
                                'quick_ma_lookback': m,
                            },
                            'func': rules.moving_average
                        }
                    ],
                    'strategy': {
                        'type': 'fixed',
                        'strategy_rules': [rule_id],
                        'constraints': {
                            'wait_entry_confirmation': d,
                        },
                        'strategy_id': rule_id,
                    }
                })
                
                
    # MA rule "MA S n m c" (5)
    ma_prefix_5 = 'ma_S_n{n}m{m}_c{c}'
    ma_prefix_5_desc = """Same as "MA S n m" but with additional parameter c. "c" is amount of days position
    will be held
    """
    for n in ma_ns:
        for m in ma_ns:
            if m >= n:
                # m should be shorter MA
                continue
            for c in ma_cs:
                rule_id = ma_prefix_5.format(n=n, m=m, c=c)
                ma_rules_configs.append({
                    'rules': [
                        {
                            'id': rule_id,
                            'type': 'simple',
                            'ts': 'close',
                            'lookback': n,
                            'params': {
                                'quick_ma_lookback': m,
                            },
                            'func': rules.moving_average,
                            'hold_fixed_days': c,
                        }
                    ],
                    'strategy': {
                        'type': 'fixed',
                        'strategy_rules': [rule_id],
                        'strategy_id': rule_id,
                    }
                })


    # MA rule "MA S n m b c" (6)
    ma_prefix_6 = 'ma_S_n{n}m{m}_b{b}_c{c}'
    ma_prefix_6_desc = """Same as "MA S n m" but with additional parameters: b and c. Meaning of additional parameters
    as in precious MA rules
    """
    for n in (50, 100, 200):
        for m in (2, 5, 10):
            b = 0.01
            c = 10
            rule_id = ma_prefix_6.format(n=n, m=m, c=c, b=str(b).replace('0.',''))
            ma_rules_configs.append({
                'rules': [
                    {
                        'id': rule_id,
                        'type': 'simple',
                        'ts': 'close',
                        'lookback': n,
                        'params': {
                            'quick_ma_lookback': m,
                            'b': b,
                        },
                        'func': rules.moving_average,
                        'hold_fixed_days': c,
                    }
                ],
                'strategy': {
                    'type': 'fixed',
                    'strategy_rules': [rule_id],
                    'strategy_id': rule_id,
                }
            })
            
    ma_1_exp_no = 15
    ma_2_exp_no = 105
    ma_3_exp_no = 840
    ma_4_exp_no = 420
    ma_5_exp_no = 420
    ma_6_exp_no = 9

    all_ma_rules_no = ma_1_exp_no+ma_2_exp_no+ma_3_exp_no+ma_4_exp_no+ma_5_exp_no+ma_6_exp_no

    assert(len(ma_rules_configs) == all_ma_rules_no)
    return ma_rules_configs


def cb_rules():
    # define channel break-outs rules
    cb_rules_configs = []
    cb_ns = [5, 10, 15, 20, 25, 50, 100, 150, 200, 250] # 10
    cb_xs = [0.005, 0.01, 0.02, 0.03, 0.05, 0.075, 0.10, 0.15] # 8
    cb_cs = [2, 5, 10, 25, 50]  # 5
    cb_bs = [0.001, 0.005, 0.01, 0.015, 0.02, 0.03, 0.04, 0.05] # 8

    # CB rule "CB n x c" (1)
    cb_prefix_1 = 'cb_n{n}_x{x}_c{c}'
    cb_prefix_1_desc = """
    Buy signal is triggered when the price exceeds the channel, and to sell signal when the price moves below 
    the channel. A channel can be said to occur when the high over the previous n days is within x percent of 
    the low over the previous n days, not including the current price. Hold for x days.
    """
    for n in cb_ns:
        for x in cb_xs:
            for c in cb_cs:
                rule_id = cb_prefix_1.format(n=n, c=c, x=str(x).replace('0.', ''))
                cb_rules_configs.append({
                    'rules': [
                        {
                            'id': rule_id,
                            'type': 'simple',
                            'ts': ['close', 'high', 'low'],
                            'lookback': n,
                            'params': {
                                'channel_width': x
                            },
                            'func': rules.channel_break_out,
                            'hold_fixed_days': c,
                        }
                    ],
                    'strategy': {
                        'type': 'fixed',
                        'strategy_rules': [rule_id],
                        'strategy_id': rule_id,
                    }
                })


    # CB rule "CB n x c" (2)
    cb_prefix_2 = 'cb_n{n}_x{x}_b{b}_c{c}'
    cb_prefix_2_desc = """Same as "CB n x c" but price has to exeed channel by at least b%"""
    for n in cb_ns:
        for x in cb_xs:
            for b in cb_bs:
                # b has to be less than x
                if b >= x:
                    continue
                for c in cb_cs:
                    rule_id = cb_prefix_2.format(
                        n=n, c=c, x=str(x).replace('0.', ''),
                        b=str(b).replace('0.', ''),
                    )
                    cb_rules_configs.append({
                        'rules': [
                            {
                                'id': rule_id,
                                'type': 'simple',
                                'ts': ['close', 'high', 'low'],
                                'lookback': n,
                                'params': {
                                    'channel_width': x,
                                    'b': b,
                                },
                                'func': rules.channel_break_out,
                                'hold_fixed_days': c,
                            }
                        ],
                        'strategy': {
                            'type': 'fixed',
                            'strategy_rules': [rule_id],
                            'strategy_id': rule_id,
                        }
                    })
                
    cb_1_exp_no = 400
    cb_2_exp_no = 2150
    all_cb_rules_no = cb_1_exp_no+cb_2_exp_no

    assert(len(cb_rules_configs) == all_cb_rules_no)
    return cb_rules_configs


def oba_rules():
    # define On-Balance Volume Averages rules
    oba_rules_configs = []
    oba_ns = [2, 5, 10, 15, 20, 25, 30, 40, 50, 75, 100, 125, 150, 200, 250] # 15
    oba_bs = [0.001, 0.005, 0.01, 0.015, 0.02, 0.03, 0.04, 0.05] # 8
    oba_ds = [2, 3, 4, 5]
    oba_cs = [5, 10, 25, 50]

    # OBA rule "OBA S n{n}" (1)
    oba_prefix_1 = 'oba_S_n{n}'
    oba_prefix_1_desc = 'Same as MA S n (1) but instead of prices uses On-balance volume indicator'

    for n in oba_ns:
        rule_id = oba_prefix_1.format(n=n)
        oba_rules_configs.append({
            'rules': [
                {
                    'id': rule_id,
                    'type': 'simple',
                    'ts': 'obv',
                    'lookback': n,
                    'params': {},
                    'func': rules.moving_average
                }
            ],
            'strategy': {
                'type': 'fixed',
                'strategy_rules': [rule_id],
                'strategy_id': rule_id,
            }
        })
        
        
    # OBA rule "OBA S n m" (2)
    oba_prefix_2 = 'oba_S_n{n}m{m}'
    oba_prefix_2_desc = 'Same as "MA S n m" (2) but instead of prices uses On-balance volume indicator'
    for n in oba_ns:
        for m in oba_ns:
            if m >= n:
                # m should be shorter MA
                continue
            rule_id = oba_prefix_2.format(n=n, m=m)
            oba_rules_configs.append({
                'rules': [
                    {
                        'id': rule_id,
                        'type': 'simple',
                        'ts': 'obv',
                        'lookback': n,
                        'params': {
                            'quick_ma_lookback': m,
                        },
                        'func': rules.moving_average
                    }
                ],
                'strategy': {
                    'type': 'fixed',
                    'strategy_rules': [rule_id],
                    'strategy_id': rule_id,
                }
            })
            
            
    # OBA rule "OBA S n m b" (3)
    oba_prefix_3 = 'oba_S_n{n}m{m}_b{b}'
    oba_prefix_3_desc = 'Same as "MA S n m b" (3) but instead of prices uses On-balance volume indicator'
    for n in oba_ns:
        for m in oba_ns:
            if m >= n:
                # m should be shorter MA
                continue
            for b in oba_bs:
                rule_id = oba_prefix_3.format(n=n, m=m, b=str(b).replace('0.',''))
                oba_rules_configs.append({
                    'rules': [
                        {
                            'id': rule_id,
                            'type': 'simple',
                            'ts': 'obv',
                            'lookback': n,
                            'params': {
                                'quick_ma_lookback': m,
                                'b': b,
                            },
                            'func': rules.moving_average
                        }
                    ],
                    'strategy': {
                        'type': 'fixed',
                        'strategy_rules': [rule_id],
                        'strategy_id': rule_id,
                    }
                })
                
    # OBA rule "OBA S n m d" (4)
    oba_prefix_4 = 'oba_S_n{n}m{m}_d{d}'
    oba_prefix_4_desc = 'Same as "MA S n m d" (4) but instead of prices uses On-balance volume indicator'
    for n in oba_ns:
        for m in oba_ns:
            if m >= n:
                # m should be shorter MA
                continue
            for d in oba_ds:
                rule_id = oba_prefix_4.format(n=n, m=m, d=d)
                oba_rules_configs.append({
                    'rules': [
                        {
                            'id': rule_id,
                            'type': 'simple',
                            'ts': 'obv',
                            'lookback': n,
                            'params': {
                                'quick_ma_lookback': m,
                            },
                            'func': rules.moving_average
                        }
                    ],
                    'strategy': {
                        'type': 'fixed',
                        'strategy_rules': [rule_id],
                        'constraints': {
                            'wait_entry_confirmation': d,
                        },
                        'strategy_id': rule_id,
                    }
                })

                
    # OBA rule "OBA S n m c" (5)
    oba_prefix_5 = 'oba_S_n{n}m{m}_c{c}'
    oba_prefix_5_desc = 'Same as "MA S n m c" (5) but instead of prices uses On-balance volume indicator'
    for n in oba_ns:
        for m in oba_ns:
            if m >= n:
                # m should be shorter MA
                continue
            for c in oba_cs:
                rule_id = oba_prefix_5.format(n=n, m=m, c=c)
                oba_rules_configs.append({
                    'rules': [
                        {
                            'id': rule_id,
                            'type': 'simple',
                            'ts': 'obv',
                            'lookback': n,
                            'params': {
                                'quick_ma_lookback': m,
                            },
                            'func': rules.moving_average,
                            'hold_fixed_days': c,
                        }
                    ],
                    'strategy': {
                        'type': 'fixed',
                        'strategy_rules': [rule_id],
                        'strategy_id': rule_id,
                    }
                })

    oba_1_exp_no = 15
    oba_2_exp_no = 105
    oba_3_exp_no = 840
    oba_4_exp_no = 420
    oba_5_exp_no = 420

    all_oba_rules_no = oba_1_exp_no+oba_2_exp_no+oba_3_exp_no+oba_4_exp_no+oba_5_exp_no
    assert(len(oba_rules_configs) == all_oba_rules_no)
    return oba_rules_configs


def msp_rules():
    # define Momentum Strategies in Price
    msp_rules_configs = []
    msp_ms = [2, 5, 10, 20, 30, 40, 50, 60, 125, 250] # 10
    msp_ks = [0.05, 0.10, 0.15, 0.2] # 4
    msp_cs = [2, 5, 10, 25, 50] # 5

    # MS rule "MS ROC m k c" (1)
    msp_prefix_1 = 'MSP_ROC_m{m}_k{k}c{c}'
    msp_prefix_1_desc = """
    ROC osscilator is used. When the oscillator crosses the overbought level from below, it is a signal for 
    initiating a long position. On the other hand, a signal for short position will be issued when 
    the oscillator crosses the oversold level from above. We set that, once a position is initiated, 
    the investor will hold the position for fixed holding days f and then liquidate it.
    """
    for m in msp_ms:
        for k in msp_ks:
            for c in msp_cs:
                rule_id = msp_prefix_1.format(m=m, k=str(k).replace('0.', ''), c=c)
                msp_rules_configs.append({
                    'rules': [
                        {
                            'id': rule_id,
                            'type': 'simple',
                            'ts': 'close_roc',
                            'lookback': m,
                            'params': {
                                'threshold': k,
                            },
                            'func': rules.momentum_in_oscillator,
                            'hold_fixed_days': c,
                        }
                    ],
                    'strategy': {
                        'type': 'fixed',
                        'strategy_rules': [rule_id],
                        'strategy_id': rule_id,
                    }
                })
                

    # MS rule "MS Avg m k c" (2)
    msp_prefix_2 = 'MSP_AVG_m{m}_k{k}c{c}'
    msp_prefix_2_desc = """Same as "MS ROC m k c" but ROC of simple moving avg. instead of raw ROC"""
    for m in msp_ms:
        for k in msp_ks:
            for c in msp_cs:
                rule_id = msp_prefix_2.format(m=m, k=str(k).replace('0.', ''), c=c)
                msp_rules_configs.append({
                    'rules': [
                        {
                            'id': rule_id,
                            'type': 'simple',
                            'ts': f'close_sma_roc_{m}',
                            'lookback': m,
                            'params': {
                                'threshold': k,
                            },
                            'func': rules.momentum_in_oscillator,
                            'hold_fixed_days': c,
                        }
                    ],
                    'strategy': {
                        'type': 'fixed',
                        'strategy_rules': [rule_id],
                        'strategy_id': rule_id,
                    }
                })
                
    # MS rule "MS XAvgs m k c" (3)
    msp_prefix_3 = 'MSP_XAVGS_m{m}n{n}_k{k}c{c}'
    msp_prefix_3_desc = """
    Same as "MS ROC m k c" but ROC of ratio of cross-over of 2 moving avg. (w1 < w2) instead of raw ROC
    """
    for m in msp_ms:
        for n in msp_ms:
            if n>=m:
                continue
            for k in msp_ks:
                for c in msp_cs:
                    rule_id = msp_prefix_3.format(m=m, n=n, k=str(k).replace('0.', ''), c=c)
                    msp_rules_configs.append({
                        'rules': [
                            {
                                'id': rule_id,
                                'type': 'simple',
                                'ts': f'close_xavgs_roc_{m}_{n}',
                                'lookback': m,
                                'params': {
                                    'threshold': k,
                                },
                                'func': rules.momentum_in_oscillator,
                                'hold_fixed_days': c,
                            }
                        ],
                        'strategy': {
                            'type': 'fixed',
                            'strategy_rules': [rule_id],
                            'strategy_id': rule_id,
                        }
                    })

    msp_1_exp_no = 200
    msp_2_exp_no = 200
    msp_3_exp_no = 900

    all_msp_rules_no = msp_1_exp_no+msp_2_exp_no+msp_3_exp_no
    assert(len(msp_rules_configs) == all_msp_rules_no)
    return msp_rules_configs


def msv_rules():
    # define Momentum Strategies in Volumes
    msv_rules_configs = []
    msv_ms = [2, 5, 10, 20, 30, 40, 50, 60, 125, 250] # 10
    msv_ks = [0.05, 0.10, 0.15, 0.2] # 4
    msv_cs = [2, 5, 10, 25, 50] # 5

    # MSV rule "MSV ROC m k c" (1)
    msv_prefix_1 = 'MSV_ROC_m{m}_k{k}c{c}'
    msv_prefix_1_desc = "Same as MSP_ROC_m{m}_k{k}c{c} but volume instead of price"
    for m in msv_ms:
        for k in msv_ks:
            for c in msv_cs:
                rule_id = msv_prefix_1.format(m=m, k=str(k).replace('0.', ''), c=c)
                msv_rules_configs.append({
                    'rules': [
                        {
                            'id': rule_id,
                            'type': 'simple',
                            'ts': 'volume_roc',
                            'lookback': m,
                            'params': {
                                'threshold': k,
                            },
                            'func': rules.momentum_in_oscillator,
                            'hold_fixed_days': c,
                        }
                    ],
                    'strategy': {
                        'type': 'fixed',
                        'strategy_rules': [rule_id],
                        'strategy_id': rule_id,
                    }
                })
                
    # MS rule "MSV Avg m k c" (2)
    msv_prefix_2 = 'MSV_AVG_m{m}_k{k}c{c}'
    msv_prefix_2_desc = 'Same as "MSP_AVG_m{m}_k{k}c{c}" but volume instead of price'
    for m in msv_ms:
        for k in msv_ks:
            for c in msv_cs:
                rule_id = msv_prefix_2.format(m=m, k=str(k).replace('0.', ''), c=c)
                msv_rules_configs.append({
                    'rules': [
                        {
                            'id': rule_id,
                            'type': 'simple',
                            'ts': f'volume_sma_roc_{m}',
                            'lookback': m,
                            'params': {
                                'threshold': k,
                            },
                            'func': rules.momentum_in_oscillator,
                            'hold_fixed_days': c,
                        }
                    ],
                    'strategy': {
                        'type': 'fixed',
                        'strategy_rules': [rule_id],
                        'strategy_id': rule_id,
                    }
                })
                
    # MSV rule "MSV XAvgs m k c" (3)
    msv_prefix_3 = 'MSV_XAVGS_m{m}n{n}_k{k}c{c}'
    msv_prefix_3_desc = 'Same as MSP_XAVGS_m{m}n{n}_k{k}c{c} but volume instead of price'
    for m in msv_ms:
        for n in msv_ms:
            if n>=m:
                continue
            for k in msv_ks:
                for c in msv_cs:
                    rule_id = msv_prefix_3.format(m=m, n=n, k=str(k).replace('0.', ''), c=c)
                    msv_rules_configs.append({
                        'rules': [
                            {
                                'id': rule_id,
                                'type': 'simple',
                                'ts': f'volume_xavgs_roc_{m}_{n}',
                                'lookback': m,
                                'params': {
                                    'threshold': k,
                                },
                                'func': rules.momentum_in_oscillator,
                                'hold_fixed_days': c,
                            }
                        ],
                        'strategy': {
                            'type': 'fixed',
                            'strategy_rules': [rule_id],
                            'strategy_id': rule_id,
                        }
                    })
                
    msv_1_exp_no = 200
    msv_2_exp_no = 200
    msv_3_exp_no = 900

    all_msv_rules_no = msv_1_exp_no+msv_2_exp_no+msv_3_exp_no
    assert(len(msv_rules_configs) == all_msv_rules_no)
    return msv_rules_configs


def cdl_rules():
    # define strategies based on japanese candles
    cdl_rules_configs = []
    cdl_lbs = [3, 7, 14, 21, 28, 56] # 6
    cdl_cs = [1, 2, 5, 7, 14, 21, 28] # 7

    # CDL rule "CDL lb c" (1)
    cdl_prefix_1 = 'CDL_lb{lb}_c{c}'
    cdl_prefix_1_desc = """
    Rule based on candles formations: morning/evening star, bearish/bullish engulfing and hammer/hanging man.
    If any of the formations (in mentioned order) triggers long/short signal - enters a trades. Trade will be
    hold for c days. Lookback is used within the formations calculations
    """
    for lb in cdl_lbs:
        for c in cdl_cs:
            rule_id = cdl_prefix_1.format(lb=lb, c=c)
            cdl_rules_configs.append({
                'rules': [
                    {
                        'id': rule_id+'_hammer_hanging_man',
                        'type': 'simple',
                        'ts': ['open', 'high', 'low', 'close'],
                        'lookback': lb,
                        'params': {},
                        'func': rules.candle_hammer_hanging_man,
                        'hold_fixed_days': c
                    },
                    {
                        'id': rule_id+'_engulfings',
                        'type': 'simple',
                        'ts': ['open', 'high', 'low', 'close'],
                        'lookback': lb,
                        'params': {},
                        'func': rules.candle_engulfing,
                        'hold_fixed_days': c
                    },
                    {
                        'id': rule_id+'_stars',
                        'type': 'simple',
                        'ts': ['open', 'high', 'low', 'close'],
                        'lookback': lb,
                        'params': {},
                        'func': rules.candle_stars,
                        'hold_fixed_days': c
                    }
                ],
                'strategy': {
                    'type': 'fixed',
                    'strategy_rules': [
                        rule_id+'_stars',
                        rule_id+'_engulfings',
                        rule_id+'_hammer_hanging_man',
                    ],
                    'strategy_id': rule_id
                }
            })

    cdl_1_exp_no = 42
    all_cdl_rules_no = cdl_1_exp_no
    assert(len(cdl_rules_configs) == all_cdl_rules_no)
    return cdl_rules_configs


def merge_final_configs(
        filter_rules_configs, support_resistance_rules_configs, ma_rules_configs, cb_rules_configs, oba_rules_configs, 
        msp_rules_configs, msv_rules_configs, cdl_rules_configs
    ):
    class_configs = [
        ('filter', filter_rules_configs),
        ('support_resistance', support_resistance_rules_configs),
        ('ma', ma_rules_configs),
        ('cb', cb_rules_configs),
        ('oba', oba_rules_configs),
        ('msp', msp_rules_configs),
        ('msv', msv_rules_configs),
        ('cdl', cdl_rules_configs),
    ]

    class_rules = {
        k: [rule for conf in configs for rule in conf['rules'] if rule['type'] == 'simple']
        for k, configs in class_configs
    }

    random.seed(30753277)
    RANDOM_SEEDS = random.sample(range(1, 1000000000), 100000)

    ### merge all simple configurations
    configs = []
    for rules_class in class_configs:
        configs.extend(rules_class[1])
    print(f'There is {len(configs)} basic configs')

    ### append combined convoluted rules
    combined_convoluted_configs = []
    class_rules_lst = list(class_rules.keys())
    for k in range(1,len(class_rules_lst)+1):
        for comb in itertools.combinations(class_rules_lst, k):
            for no_rls in (20, 40,):
                for v in range(10):
                    sample_rules = []
                    for cls in comb:
                        random.seed(RANDOM_SEEDS.pop())
                        sample_rules.extend(random.sample(class_rules[cls], no_rls))
                    # create and append config
                    cls = '_'.join(comb)
                    rule_id_template = f'{COMPLEX_RULE_PREFIX}_COM_{cls}_nr{no_rls}_v{v}'                
                    combined_convoluted_configs.append({
                        'rules': sample_rules + [
                            {
                                'id': rule_id_template,
                                'type': 'convoluted',
                                'simple_rules': [rule['id'] for rule in sample_rules],
                                'aggregation_type': 'combine',
                                'aggregation_params': {'mode': 'majority_voting'}
                            }
                        ],
                        'strategy': {
                            'type': 'fixed',
                            'strategy_rules': [rule_id_template],
                            'strategy_id': rule_id_template
                        }
                    }) 
    print(f'Plus {len(combined_convoluted_configs)} complex combined configs')
    configs.extend(combined_convoluted_configs)

                    
    ### append reversed rules
    reversed_configs = []
    for conf in configs:
        # pickle will create deep copy so that all objects are new (shallow copy would add references)
        conf_copy = pickle.loads(pickle.dumps(conf, -1))
        conf_copy['strategy']['reversed'] = True
        conf_copy['strategy']['strategy_id'] = REVERSED_RULE_PREFIX+conf['strategy']['strategy_id']
        reversed_configs.append(conf_copy)
    configs.extend(reversed_configs)
    print(f'Plus {len(reversed_configs)} reversed configs')


    ### append learning rules (+ reversed learning. do it here instead of in section above to avoid copying obj.)
    memory_spans = (5, 10, 20, 60, 120)
    review_spans = (5, 10, 20)
    memory_and_reviews = [(m, r) for m in memory_spans for r in review_spans if r <= m]
    performance_mnetrics = ['voting', 'daily_returns', 'avg_log_returns', 'avg_log_returns_held_only']
    learning_configs = []
    for k in range(1,len(class_rules_lst)+1):
        for comb in itertools.combinations(class_rules_lst, k):
            for m, r in memory_and_reviews:
                for pm in performance_mnetrics:
                    cls = '_'.join(comb)
                    rule_id_template = f'{COMPLEX_RULE_PREFIX}_LRN_{cls}_m{m}_r{r}_{pm}'
                    sample_rules = []
                    for cls in comb:
                        random.seed(RANDOM_SEEDS.pop())
                        # if len(class_rules[cls]) < 100:
                        #     no_rules = len(class_rules[cls])
                        # else:
                        #     no_rules = 100
                        no_rules = 20
                        sample_rules.extend(random.sample(class_rules[cls], no_rules))
                    # normal version
                    learning_configs.append({
                        'rules': sample_rules,
                        'strategy': {
                            'type': 'learning',
                            'strategy_rules': [rule['id'] for rule in sample_rules],
                            'strategy_id': rule_id_template,
                            'params':{
                                'memory_span': m,
                                'review_span': r,
                                'performance_metric': pm,
                                'price_label': 'close',
                            }
                        }
                    })
                    # reversed version
                    learning_configs.append({
                        'rules': sample_rules,
                        'strategy': {
                            'type': 'learning',
                            'strategy_rules': [rule['id'] for rule in sample_rules],
                            'strategy_id': REVERSED_RULE_PREFIX+rule_id_template,
                            'params':{
                                'memory_span': m,
                                'review_span': r,
                                'performance_metric': pm,
                                'price_label': 'close',
                            },
                            'reversed': True
                        }
                    })
                    
    print(f'Plus {len(learning_configs)} learning rules configs')
    configs.extend(learning_configs)

    print(f'Total: {len(configs)}')

    return configs


def loop_with_progressbar(it, prefix="", size=60, out=sys.stdout):
    count = len(it)
    def show(j):
        x = int(size*j/count)
        out.write("%s[%s%s] %i/%i\r" % (prefix, "#"*x, "."*(size-x), j, count))
        out.flush()        
    show(0)
    for i, item in enumerate(it):
        yield item
        show(i+1)
    out.write("\n")
    out.flush()


def _run_sg_and_store_results(input_df, conf, strategy_id, rules_dir, symbol, final_file_full_path):
    sg = signal_generator.SignalGenerator(
        df = input_df,
        config = conf,
    )
    rule_signals = sg.generate()
    if not REVERSED_RULE_PREFIX in strategy_id:
        sg.save_rules_results(
            path=rules_dir,
            prefix=f'{symbol}_'
        )
    with open(final_file_full_path, "wb" ) as fh:
        pickle.dump(rule_signals, fh)
        
    return rule_signals


def _prepare_strategy_dataframe(strategy_id, input_df, rules_ids_lst):
    # selective pre-processing for non-complex rules
    if not COMPLEX_RULE_PREFIX in strategy_id:
        # get label for potentail MSP or MSV strategy
        if 'MSP' in strategy_id:
            _label = 'close'
        elif 'MSV' in strategy_id:
            _label = 'volume'
        # create additional columns
        if 'oba_' in strategy_id:
            # OBV indicator
            input_df = helpers.on_balance_volume_indicator(input_df)
        elif '_ROC_' in strategy_id:
            # daily ROC osscilator
            input_df.loc[:, f'{_label}_roc'] = helpers.roc_oscillator(input_df, days=1, col=_label)
        elif '_AVG_' in strategy_id:
            # daily SMA ROC osscilator
            _days = int(re.search(r'.*_AVG_m(\d+)', strategy_id)[1])    
            input_df.loc[:, 'sma'] = helpers.simple_ma(input_df, days=_days, col=_label)
            input_df.loc[:, f'{_label}_sma_roc_{_days}'] = helpers.roc_oscillator(input_df, days=1, col='sma')
            input_df.drop('sma', axis=1, inplace=True)
        elif '_XAVGS_' in strategy_id:
            # daily osscilator based on ROC of ratio of 2 SMAs
            re_res = re.search(r'.*_XAVGS.*_m(\d+)n(\d+)', strategy_id)
            _days_long, _days_short = int(re_res[1]), int(re_res[2])
            input_df.loc[:, 'sma_l'] = helpers.simple_ma(input_df, days=_days_long, col=_label)
            input_df.loc[:, 'sma_s'] = helpers.simple_ma(input_df, days=_days_short, col=_label)
            input_df.loc[:, 'ratio'] = input_df['sma_l'] / input_df['sma_s']
            input_df.loc[:, f'{_label}_xavgs_roc_{_days_long}_{_days_short}'] = helpers.roc_oscillator(
                input_df, days=1, col='ratio'
            )
            for c in ('sma_l', 'sma_s', 'ratio'):
                input_df.drop(c, axis=1, inplace=True)
    else:
        # complex rule pre-processing
        oba_encountered = 0
        msp_roc_encountered = 0
        msv_roc_encountered = 0
        for rule_id in rules_ids_lst:
            if ('oba_' in rule_id) and (oba_encountered==0):
                input_df = helpers.on_balance_volume_indicator(input_df)
                oba_encountered = 1
            elif ('MSP_ROC_' in rule_id) and (msp_roc_encountered==0):
                input_df.loc[:, 'close_roc'] = helpers.roc_oscillator(input_df, days=1, col='close')
                msp_roc_encountered = 1
            elif ('MSP_AVG_' in rule_id):
                _days = int(re.search(r'.*_AVG_m(\d+)', rule_id)[1])
                input_df.loc[:, 'sma'] = helpers.simple_ma(input_df, days=_days, col='close')
                input_df.loc[:, f'close_sma_roc_{_days}'] = helpers.roc_oscillator(input_df, days=1, col='sma')
                input_df.drop('sma', axis=1, inplace=True)
            elif ('MSP_XAVGS_' in rule_id):
                re_res = re.search(r'.*_XAVGS.*_m(\d+)n(\d+)', rule_id)
                _days_long, _days_short = int(re_res[1]), int(re_res[2])
                input_df.loc[:, 'sma_l'] = helpers.simple_ma(input_df, days=_days_long, col='close')
                input_df.loc[:, 'sma_s'] = helpers.simple_ma(input_df, days=_days_short, col='close')
                input_df.loc[:, 'ratio'] = input_df['sma_l'] / input_df['sma_s']
                input_df.loc[:, f'close_xavgs_roc_{_days_long}_{_days_short}'] = helpers.roc_oscillator(
                    input_df, days=1, col='ratio'
                )
                for c in ('sma_l', 'sma_s', 'ratio'):
                    input_df.drop(c, axis=1, inplace=True)
            elif ('MSV_ROC_' in rule_id) and (msv_roc_encountered==0):
                input_df.loc[:, 'volume_roc'] = helpers.roc_oscillator(input_df, days=1, col='volume')
                msv_roc_encountered = 1
            elif ('MSV_AVG_' in rule_id):
                _days = int(re.search(r'.*_AVG_m(\d+)', rule_id)[1])
                input_df.loc[:, 'sma'] = helpers.simple_ma(input_df, days=_days, col='volume')
                input_df.loc[:, f'volume_sma_roc_{_days}'] = helpers.roc_oscillator(input_df, days=1, col='sma')
                input_df.drop('sma', axis=1, inplace=True)
            elif ('MSV_XAVGS_' in rule_id):
                re_res = re.search(r'.*_XAVGS.*_m(\d+)n(\d+)', rule_id)
                _days_long, _days_short = int(re_res[1]), int(re_res[2])
                input_df.loc[:, 'sma_l'] = helpers.simple_ma(input_df, days=_days_long, col='volume')
                input_df.loc[:, 'sma_s'] = helpers.simple_ma(input_df, days=_days_short, col='volume')
                input_df.loc[:, 'ratio'] = input_df['sma_l'] / input_df['sma_s']
                input_df.loc[:, f'volume_xavgs_roc_{_days_long}_{_days_short}'] = helpers.roc_oscillator(
                    input_df, days=1, col='ratio'
                )
                for c in ('sma_l', 'sma_s', 'ratio'):
                    input_df.drop(c, axis=1, inplace=True)
    return input_df


def get_symbol_signals(
    symbol=None,
    pricing_data=None,
    configs=None,
    run_and_overwrite = False,
    data_collector=None,
):
    """
    Generates signal for all rules for given symbol. Save/retrive data to speed things up
    
    data_mining_rules
        /SYMBOL
            /rules
                /simple_rule...
                /convoluted_rule...
            /final
                /strategy_id...    
    """
    input_df = pricing_data[symbol]
    # detrend data
    # input_df = data_collector.detrend(input_df)
    
    # create folder structure if does not exists
    rules_dir = os.path.join(ALL_SIGNALS_PATH, symbol, 'rules')
    final_dir = os.path.join(ALL_SIGNALS_PATH, symbol, 'final')
    for _path in (rules_dir, final_dir):
        if not os.path.exists(_path):
            os.makedirs(_path)
        
    signals = {}
    states = {}
    for conf in loop_with_progressbar(configs):
        # assumes single final rule in strategy. may need to change in later
        strategy_id = conf['strategy']['strategy_id']
        
        # define proper full paths and check files existence
        signal_file_name = f'{symbol}_{strategy_id}'
        final_file_full_path = os.path.join(final_dir, signal_file_name)
        signal_file_exists = os.path.isfile(os.path.join(final_dir, signal_file_name))
        
        # if complex combined rule check only for simple rules files existence
        if f'{COMPLEX_RULE_PREFIX}_' in strategy_id:
            rules_results_file_names = [
                '{symbol}_{rule_id}'.format(symbol=symbol, rule_id=rule_dict['id']) 
                for rule_dict in conf['rules'] if rule_dict['type'] == 'simple'
            ]
            load_only_simple = True
        else:
            rules_results_file_names = [
                '{symbol}_{rule_id}'.format(symbol=symbol, rule_id=rule_dict['id']) 
                for rule_dict in conf['rules']
            ]
            load_only_simple = False
        
        all_rules_files_exists = all([
            os.path.isfile(os.path.join(rules_dir, rule_file_name))
            for rule_file_name in rules_results_file_names
        ])
        
        # generate signal (it may be partially or fully retrived from file. or just newly generated)
        if (signal_file_exists == True) and (run_and_overwrite == False):
            # if final signal file exists and its not overwrite -> just load from pickle
            try:
                with open(final_file_full_path, 'rb') as fh:
                    rule_signals = pickle.load(fh)
            except:
                print(final_file_full_path)
                raise Exception
                
        elif run_and_overwrite == True:
            # if run and overwrite -> run full generate. ignore saving rules results for reversed strategies.
            rules_ids_lst = [rule['id'] for rule in conf['rules']]
            input_df = _prepare_strategy_dataframe(strategy_id, input_df, rules_ids_lst)
            rule_signals = _run_sg_and_store_results(
                input_df, conf, strategy_id, rules_dir, symbol, final_file_full_path
            )
            
        elif signal_file_exists == False:
            # final signal file does not exists
            if all_rules_files_exists == True:
                # generate signal with "load_rules_results_path". store only final result
                sg = signal_generator.SignalGenerator(
                    df = input_df,
                    config = conf,
                    load_rules_results_path = rules_dir,
                    load_rules_results_prefix = f'{symbol}_',
                    load_only_simple=load_only_simple,
                )
                #print('Will generate: ', strategy_id)
                rule_signals = sg.generate()
                with open(final_file_full_path, "wb" ) as fh:
                    pickle.dump(rule_signals, fh)
            else:
                # run full generate. ignore saving rules results for reversed strategies.
                rules_ids_lst = [rule['id'] for rule in conf['rules']]
                input_df = _prepare_strategy_dataframe(strategy_id, input_df, rules_ids_lst)
                rule_signals = _run_sg_and_store_results(
                    input_df, conf, strategy_id, rules_dir, symbol, final_file_full_path
                )
        # append generated result to output dictionary. leave only necessery columns
        signals[strategy_id] = rule_signals[[PRICE_LABEL, 'entry_long', 'exit_long', 'entry_short', 'exit_short', 'position']]
        states[strategy_id] = rule_signals['position'].to_numpy()
    return signals, states


def data_mine_symbol(
    symbol=None,
    pricing_data=None,
    data_collector=None,
    configs=None,
    limit_rules=None,
    no_samples=500,
    run_and_overwrite=False
):
    time_format = '%H:%M:%S'
    t1 = datetime.datetime.fromtimestamp(time.time())
    print(f'[{t1.strftime(time_format)}] Generating signals for symbol: {symbol}')
    symbol_signals, symbol_states = get_symbol_signals(
        symbol=symbol,
        pricing_data=pricing_data,
        configs=configs,
        data_collector=data_collector,
        run_and_overwrite=run_and_overwrite
    )

    name_signal_list = list(symbol_signals.items())
    if limit_rules:
        name_signal_list = name_signal_list[:limit_rules]

    # manualy delete it. pointers are in name_signal_list anyway
    del symbol_signals

    rules_results = {}
    t2 = datetime.datetime.fromtimestamp(time.time())
    print(f'[{t2.strftime(time_format)}] Running backtests')
    _idx = 0  # used as index to replace name_signal_list values with 0
    for r_name, r_signal in loop_with_progressbar(name_signal_list):
        tester = backtester.SimpleBacktest(
            df = r_signal,
            price_label=PRICE_LABEL,
        )
        try:
            tester_results = tester.run()
            # additional values/metrics used later by White's Reality Check and Monte Carlo simulation
            tester_results.loc[:, 'daily_returns'] = results.get_daily_returns(tester_results)
            tester_results.loc[:, 'position_states'] = r_signal['position']
            tester_results.loc[:, PRICE_LABEL] = r_signal[PRICE_LABEL]
            # leave only necessery columns
            rules_results[r_name] = tester_results[['daily_returns', PRICE_LABEL, 'position_states', 'nav']]

        except backtester.AccountBankruptError:
            tester_results = pd.DataFrame({'avg_daily_returns': -100, 'nav': [-1]}, index=(1,))
            rules_results[r_name] = tester_results
        
        # clear from memory used strategy signal. there should be no poiters left when I replace it with None here.
        name_signal_list[_idx] = None
        _idx += 1

    # for sampling distribution use only rules that did not bankrupted
    non_empty_rules_results = {
        n: r for n, r in rules_results.items() if r.shape[0] > 1
    }
    if len(non_empty_rules_results) == 0:
        print('All rules are bankrupt')
        return None

    avg_daily_returns = {r: df['daily_returns'].mean() for r, df in non_empty_rules_results.items()}
    rule_ret_lst = sorted([(r, hr) for r, hr in avg_daily_returns.items()], key=lambda x: x[1], reverse=True)
    best_rule = rule_ret_lst[0][0]
    highest_daily_ret = rule_ret_lst[0][1]
    print(f'Best rule is "{best_rule}" with avg. daily ret equal to {highest_daily_ret}')

    # White's Reality Check
    t3 = datetime.datetime.fromtimestamp(time.time())
    print(f"[{t3.strftime(time_format)}] Running: White's Reality Check")
    wrc_sampling_dist = rules_mining.create_wrc_sampling_dist(non_empty_rules_results, no_samples=no_samples)
    # find p-val and asses statistical significancee
    wrc_exceeds_observed = [x for x in wrc_sampling_dist if x >= highest_daily_ret]
    wrc_pval = len(wrc_exceeds_observed)/len(wrc_sampling_dist)
    print(f'p-val for WRC is {wrc_pval}')
    rules_mining.pval_msg(wrc_pval)
    
    # Monte Carlo simulation
    t4 = datetime.datetime.fromtimestamp(time.time())
    print(f"[{t4.strftime(time_format)}] Running: Monte Carlo simulation")
    # get actualy price change. it's the same for any rule (as single signal processed)
    
    price_changes = non_empty_rules_results[
        list(non_empty_rules_results.keys())[0]
    ][PRICE_LABEL].pct_change().to_numpy()
    
    price_changes[~np.isfinite(price_changes)] = 0

    # get dict with positions for non empty Strategies
    non_empty_rules_ids = set(non_empty_rules_results.keys())
    rules_states = {rule_id: positions for rule_id, positions in symbol_states.items() if rule_id in non_empty_rules_ids}
    

    # are those price changes adjusted?
    mc_sampling_dist = rules_mining.create_mc_sampling_distr(rules_states, price_changes, no_samples=no_samples)

    mc_exceeds_observed = [x for x in mc_sampling_dist if x >= highest_daily_ret]
    mc_pval = len(mc_exceeds_observed)/len(mc_sampling_dist)
    print(f'p-val for MC is {mc_pval}')
    rules_mining.pval_msg(mc_pval)
    
    t5 = datetime.datetime.fromtimestamp(time.time())
    print(f'Signal generation: {t2-t1}')
    print(f'Backtests: {t3-t2}')
    print(f'WRC and MC simulations: {t5-t3}')
    print(f'\nTotal time: {t5-t1}')
    
    return {
        'avg_daily_returns': avg_daily_returns,
        'highest_mean_daily_ret': highest_daily_ret,
        'best_rule': best_rule,
        'wrc_dist': wrc_sampling_dist,
        'mc_dist': mc_sampling_dist,
        'rules_summary': {
            r: {
                'final_nav': df['nav'][-1],
                'avg_daily_ret': avg_daily_returns[r],
                'no_days_in_position': sum(df['position_states'] != 0),
            } 
            for r, df in non_empty_rules_results.items()
        }
    }


def main():
    print('### Get data and prepare connfigs')
    dc = gpw_data.GPWData(pricing_data_path='/Users/slaw/osobiste/trading/pricing_data')
    universe = [
        symbol for i in dc.indicies_stocks.keys() for symbol in dc.indicies_stocks[i] 
    ]

    # exclude some symbols that I was not able to get data for. screw them for now
    universe = sorted(list(set(universe) - set(['LCCORP', 'POLIMEXMS', 'VISTULA'])))

    pricing_data = {
        symbol: dc.load(symbols=symbol, from_csv=True)
        for symbol in universe
    }

    filter_rules_configs = filter_rules()
    support_resistance_rules_configs = support_resistance_rules()
    ma_rules_configs = ma_rules()
    cb_rules_configs = cb_rules()
    oba_rules_configs = oba_rules()
    msp_rules_configs = msp_rules()
    msv_rules_configs = msv_rules()
    cdl_rules_configs = cdl_rules()

    configs = merge_final_configs(
        filter_rules_configs,
        support_resistance_rules_configs,
        ma_rules_configs,
        cb_rules_configs,
        oba_rules_configs, 
        msp_rules_configs,
        msv_rules_configs,
        cdl_rules_configs
    )

    print('\n###########\n')

    symbols = universe[0:]

    for symbol in symbols:
        mining_res = data_mine_symbol(
            symbol=symbol,
            pricing_data=pricing_data,
            data_collector=dc,
            configs=configs,
            no_samples=1000,
            run_and_overwrite=False,
            # limit_rules=100,
        )

        strategy_results_path = os.path.join(ALL_SIGNALS_PATH, symbol)
        results_file = os.path.join(strategy_results_path, f'{symbol}.pickle')
        with open(results_file, "wb" ) as fh:
            pickle.dump(mining_res, fh)


if __name__ == '__main__':
    main()
