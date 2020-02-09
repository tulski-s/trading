# 3rd party
import numpy as np

def trend(arr):
    """
    Finds trend in 'arr'. Returns trading signals (1,0,-1) based on slope 
    of the fitted straight line.
    """
    x = np.array(range(1,len(arr)+1))
    A = np.vstack([x, np.ones(len(x))]).T
    # fits: y = ax + b
    a, b = np.linalg.lstsq(A, arr, rcond=None)[0]
    # arbitrary threshold of 0.25. no science here.
    if a >= .25:
        return 1
    elif a < -.25:
        return -1
    return 0


def _find_support_resistance(y, e=False):
    """
    Finds support/resistance level in 'y' (returns tuple (support, resistance)). It looks at either 'global' or 'local' definition of 
    support/resistance. If global - whole period is looked at. If local, then price has to be 
    less/greater than the e previous prices.
    e - Should be int. Used for alternative supprot/resistance definition.
    """
    # "global" definition of supp/res 
    if not e or e>y.shape[0]:
        support = min(y)
        resistance = max(y)
    # "local" definition
    elif e:
        # support
        past_times_smaller_than_itself = list(
            map(lambda x: np.where(y[:x[0]]<x[1])[0].size, list(enumerate(y)))
        )
        smaller_than_e_xtimes_idxs = np.where(np.array(past_times_smaller_than_itself) < e)[0]
        support_idx = max(smaller_than_e_xtimes_idxs)
        support = y[support_idx]
        # resistance
        past_times_bigger_than_itself = list(
            map(lambda x: np.where(y[:x[0]]>x[1])[0].size, list(enumerate(y)))
        )
        bigger_than_e_xtimes_idxs = np.where(np.array(past_times_smaller_than_itself) > e)[0]
        resistance_idx = max(bigger_than_e_xtimes_idxs)
        resistance = y[resistance_idx]
    return (support, resistance)


def _simple_average(arr):
    return sum(arr)/len(arr)


def _weigted_average(arr):
    # latest price will be 2x more important than the earliest price
    weights = 1 + (2 / (np.array(list(range(1,len(arr)+1))[::-1])+1))
    weighted_sum = sum(arr*weights)
    return weighted_sum / len(arr)


def support_resistance(arr, e=False, b=False):
    """
    Trading signal is based on support/resistance level in 'arr'. If last (current) price in 'arr'
    is above resistance - buy (1), if below - sell (-1). Do nothing in between (0).

    b - The fixed percentage band filter requires the buy or sell signal to exceed the support/resistance by 
        a fixed multiplicative amount, b.
    """
    price = arr[-1] # last day is treated as current
    y = arr[:-1] # look t-1 days back
    support, resistance = _find_support_resistance(y, e=e)
    
    if not b:
        if price > resistance:
            return 1
        elif price < support:
            return -1
        return 0
    # b filter has to be applied
    if price > resistance+(resistance*b):
        return 1
    elif price < support-(support*b):
        return -1
    return 0


def moving_average(arr, weigth_ma=None, quick_ma_lookback=None, b=None):
    """
    Binary moving average rule. Returns 1 (long) if value is bigger than threshold and -1 (short) in other
    cases. Moving average can be simple (default) or weigthed (`weigth_ma` if set as true). If weigthed, 
    most recent prices will be more important than the earliest one (up to 2x more important).

    Base version uses price as a value and MA (Moving Average) as a threshold. If `b` parameter is passed,
    value has to bigger than threshold plus a fixed multiplicative amount (b).

    Extended version (with `quick_ma_lookback` parameter set to integer value) looks at 2 different MAs. 
    Here all `arr` values are used to calculate 2 MAs (slow and quick). As a value - quick MA is used.
    As threshold - slow MA is used. `b` parameters applies in the same way as in base version.

    Note: This rule is binary, but if used as convoluted rule along with trend simple rule - one can 
    generate also neutral (0) positions.
    """
    ma_type = 'simple' if weigth_ma == None else 'weighted'
    mean = lambda a: _simple_average(a) if ma_type=='simple' else _weigted_average(a)
    # single moving average (base version)
    if not quick_ma_lookback:
        val = arr[-1] # last day is treated as current price
        ma = mean(arr[:-1])
    # slower and quicker moving average (look at whole arr here for both averages)
    else:
        val = mean(arr[len(arr)-1-quick_ma_lookback:])
        ma = mean(arr)
    threshold = ma if b == None else ma+(ma*b)
    if val > threshold:
        return 1
    else:
        return -1


def channel_break_out(dict_arrs, channel_width=None, b=False, high='high', low='low', base='close'):
    """
    Buy when the price exceeds the channel, and to sell when the price moves below the channel.
    A channel can be said to occur when the high over the previous n days is within x percent of the low over
    the previous n days, not including the current price.

    *channel_width* is allowed % diff between high and low. should be expressed e.g. as 0.2 for 20%
    *b* is the fixed percentage filter. if set, price has to be above/below channel by that fixed 
    multiplicative amount

    Notes, this rule requires 3 arrays, two (e.g. high and low) to create a channel and one to define if
    channel was exeeced. Rule is state-based, so when used type "convoluted" and aggregation_type "state-based" 
    should be used. Also, if used alone its rather binary, that is after it enters it will be either 1, -1.
    It can be used along with other rules to extend it and create also neutral state.
    """
    channel_tops = dict_arrs[high][:-1]
    channel_bottoms = dict_arrs[low][:-1]
    perc_diffs = abs(channel_tops-channel_bottoms) / ((channel_tops+channel_bottoms)/2)
    is_channel = all(perc_diffs <= channel_width)
    if is_channel:
        price = dict_arrs[base][-1]
        if not b:
            if price > channel_tops.mean():
                return 1
            elif price < channel_bottoms.mean():
                return -1
        top_mean = channel_tops.mean()
        bottom_mean = channel_bottoms.mean()
        th_top = top_mean + (top_mean * b)
        th_bottom = bottom_mean - (bottom_mean*bottom_mean)
        if price > th_top:
            return 1
        elif price < th_bottom:
            return -1
    return 0

