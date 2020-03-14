# 3rd party
import numpy as np


class Candle():
    def __init__(self, open=None, high=None, low=None, close=None):
        self._open = open
        self._high = high
        self._low = low
        self._close = close
        self.body = abs(open-close)
        if close-open > 0:
            self.color = 'white'
            self.upper_shadow = high - close
            self.lower_shadow = open - low
        elif close-open < 0:
            self.color = 'black'
            self.upper_shadow = high - open
            self.lower_shadow = close - low
        else:
            self.color = 'doji'
            self.upper_shadow = high - close
            self.lower_shadow = open - low


def _get_candles(dict_arrs, open='open', high='high', low='low', close='close'):
    # all arrays should have the same length so can take any
    idxs = range(len(dict_arrs[close]))
    candels = []
    for idx in idxs:
        o, h, l, c = [dict_arrs[d][idx] for d in (open, high, low, close)]
        candels.append(
            Candle(open=o, high=h, low=l, close=c)
        )
    return candels


def _rescale(arr, new_max=100, new_min=0):
    arr_min = min(arr)
    arr_max = max(arr)
    if arr_max == arr_min:
        # all values are the same. do not scale
        return arr
    k = (new_max-new_min)/(arr_max - arr_min)
    return k * (arr-arr_max)+new_max


def trend(arr):
    """
    Finds trend in 'arr'. Returns trading signals (1,0,-1) based on slope 
    of the fitted straight line.
    """
    if not isinstance(arr, np.ndarray):
        arr = np.array(arr)
    # scale values to allow similar comparison of "a"
    arr = _rescale(arr)
    len_arr = len(arr)
    x = np.array(range(1,len_arr+1))
    A = np.vstack([x, np.ones(len(x))]).T
    # fits: y = ax + b
    a, b = np.linalg.lstsq(A, arr, rcond=None)[0]
    # arbitrary threshold of 2. no science here - I've visually tested and adjusted this number
    if len_arr <= 7:
        if a > 9:
            return 1
        elif a < -9:
            return -1
    elif len_arr <= 14:
        if a > 5:
            return 1
        elif a < -5:
            return -1
    else:
        if a > .7:
            return 1
        elif a < -.7:
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
    Buy signal is triggered when the price exceeds the channel, and to sell signal when the price moves below 
    the channel. A channel can be said to occur when the high over the previous n days is within x percent of 
    the low over the previous n days, not including the current price.

    *channel_width* is allowed % diff between high and low. should be expressed e.g. as 0.2 for 20%
    *b* is the fixed percentage filter. if set, price has to be above/below channel by that fixed 
    multiplicative amount

    Notes, this rule requires 3 arrays, two (e.g. high and low) to create a channel and one to define if the 
    channel was crossed. The rule is state-based, so when it is used - type "convoluted" and aggregation_type 
    "state-based" should be set. Also, if used alone it is rather binary, meaning after it enters it will be 
    either 1, -1. It can be used along with other rules to extend it and create also a neutral state.
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


def momentum_in_oscillator(arr, threshold=None):
    """
    Usually, arr will contain “oscillator” data, not raw prices. The oscillator can be constructed from various 
    momentum measures. For example the rate of change (ROC) in price or volume in a given period. In general, 
    the oscillator used should be in percentages and then `threshold` will be the level which triggers signal 
    if crossed from below (buy) or above (sell). The reasoning behind it is that if the oscillator speeds up 
    or slows down it will continue to raise/lower the price in the near future.

    The rule is state-based, so when it is used - type "convoluted" and aggregation_type "state-based" should 
    be set. It is probably useful to use it with "hold_for_x_days" constraint as the rule itself just 
    triggers 1/-1 after crossing the threshold.

    To define if a recent price crossed the threshold from below/above - three values are used. Previous value, 
    current value, and threshold. The current value is just the last oscillator value in the arr. The previous 
    value is the average of the rest of the earlier prices. 
    """
    cur_val = arr[-1]
    prev_val = arr[:-1].mean()
    if (prev_val < threshold) and (cur_val > threshold):
        return 1
    elif (prev_val > threshold) and (cur_val < threshold):
        return -1
    return 0


def candle_hammer_hanging_man(dict_arrs, open='open', high='high', low='low', close='close', no_std=1, conf=True):
    """
    Rule looks for Hammer/Hanging Man candle pattern. Lookback period is used to determine relative sizes of
    candles in given dataset as well as for determining the overall trend. For the main pattern one/two last candles
    are used. One shuld be Hammer/Hanging Man candle and the other should confirm the reversal. If confirmation is
    set up (default), it would be gap between subsequent open/closes or black/wite candle with higher/lower open/close

    Hammer/Hanging Man should have:
        - small body (diff between the body and avg. body size of preceding candles should be bigger 
          than `no_std` standard deviations of the body sizes)
        - long lower shadow (>1.5 x body)
        - short/none upper shadow (that is: upper shadow is shorter than a body)

    Notes:
    - It is useful to use it with "hold_fixed_days" for the rule
    """
    candels = _get_candles(dict_arrs, open=open, high=high, low=low, close=close)
    if conf == True:
        form_candle = candels[-2]
        preceding_candles = candels[:-2]
    else:
        form_candle = candels[-1]
        preceding_candles = candels[:-1]
    # if no trend - formation is not valid already at this point
    preceding_trend = trend([c._close for c in preceding_candles])
    if preceding_trend == 0:
        return 0
    # if there is a trend - check if Hammer/Hanging Man exists
    preceding_bodies = np.array([c.body for c in preceding_candles[:-2]])
    avg_body_size = preceding_bodies.mean()
    std_body_size = preceding_bodies.std()
    is_body_small = (avg_body_size - form_candle.body) > (no_std * std_body_size)
    is_lower_shadow_long = form_candle.lower_shadow >= 1.5*form_candle.body
    is_upper_shadow_short = form_candle.upper_shadow < form_candle.body
    if not all((is_body_small, is_lower_shadow_long, is_upper_shadow_short)):
        return 0
    if conf != True:
        if preceding_trend == -1:
            return 1
        elif preceding_trend == 1:
            return -1
    # if there is Hammer/Hanging Man candel - check if next canlde confirms it
    conf_candle = candels[-1]
    if preceding_trend == -1:
        # there was downtrend so it is a Hammer
        if conf_candle.color == 'white':
            return 1
        elif (form_candle == 'white') and (form_candle._close < conf_candle._open):
            return 1
        elif (form_candle == 'black') and (form_candle._open < conf_candle._open):
            return 1
        else:
            return 0
    elif preceding_trend == 1:
        # there was uptrend so it is a Hanging Man
        if conf_candle.color == 'black':
            return -1
        elif (form_candle == 'white') and (form_candle._open > conf_candle._open):
            return -1
        elif (form_candle == 'black') and (form_candle._close > conf_candle._open):
            return -1
        else:
            return 0


def candle_engulfing(dict_arrs, open='open', high='high', low='low', close='close'):
    """
    The engulfing pattern is a major reversal signal with two opposite color candles composing this pattern.
    There is bullish and bearish engulfing pattern. There are three criteria for an engulfing pattern:
    1) The market has to be in a clearly definable uptrend or downtrend (even if the trend is short term)
    2) Two candlesticks comprise the pattern. The second real body must "engulf" the prior real body. That is
       both open/close prices are higher/lower in latter candle. Shadows do not need to be engulfed.
    3) The second candle should be the opposite color of the first candle.

    Bullish:
    The market is in a downtrend, then a white bullish real body wraps around the prior period's black real body.

    Bearish:
    The market is up trending. The white real body engulfed by a black body is the signal for a top reversal. 

    Notes:
    - It is useful to use it with "hold_fixed_days" for the rule
    - Increased volume on the second candle may be an additional confirmation
    """
    candels = _get_candles(dict_arrs, open=open, high=high, low=low, close=close)
    preceding_trend = trend([c._close for c in candels[:-2]])
    # if no trend - formation is not valid already at this point
    if preceding_trend == 0:
        return 0
    first_candle = candels[-2]
    second_candle = candels[-1]
    # bullish
    if preceding_trend == -1:
        # first candle needs to be black
        if first_candle.color != 'black':
            return 0
        # second candle needs to be white
        if second_candle.color != 'white':
            return 0
        # a second needs to engulf the first one
        if (first_candle._open < second_candle._close) and (first_candle._close > second_candle._open):
            return 1
        else:
            return 0
    # bearish
    elif preceding_trend == 1:
        # first candle needs to be white
        if first_candle.color != 'white':
            return 0
        # second candle needs to be black
        if second_candle.color != 'black':
            return 0
        # a second needs to engulf the first one
        if (first_candle._close < second_candle._open) and (first_candle._open > second_candle._close):
            return -1
        else:
            return 0


def main():
    dict_arrs = {
        'open': np.array([420.0, 424.8, 430.0, 425.4, 429.8, 434.6, 429.0, 422.2, 421.6, 432.2]),
        'high': np.array([429.0, 428.8, 430.0, 429.4, 440.0, 440.0, 429.6, 425.4, 428.0, 439.0]),
        'low': np.array([417.4, 418.2, 415.0, 415.8, 429.0, 429.8, 416.0, 418.4, 420.0, 421.0]),
        'close': np.array([426.6, 423.4, 424.2, 428.0, 436.4, 432.2, 420.0, 425.0, 425.8, 425.4])
    }
    res = candle_hammer_hanging_man(dict_arrs)


if __name__ == '__main__':
    main()
