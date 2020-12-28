# built in
import datetime

# 3rd part
import numpy as np


def split_into_subsets(pricing_data, ratio, df=True):
        """
        Returns 2 dictionaries - test and validation. Both are in form of {symbol<string>: data<df|dict>}
        *pricing_data* is dict in the form of:
            {'symbol_key': output princing data from load method (df or dictionary)}
        *ratio* defines what portion of data will be in first sample
        """
        # find max date for test set. that will be the base for the split.
        dates = set()
        if df == True:
            for _df in pricing_data.values():
                dates |= set(_df.index.tolist())
        else:
            for vals in pricing_data.values():
                dates |= set([x[0] for x in vals])
        ordered_dates = sorted(list(dates))
        # use ratio to define the split
        max_test_date = ordered_dates[int(len(ordered_dates)*ratio)-1]
        # iterate over signals and cut/move them into test validation collections
        test_set, validation_set = {}, {}
        if df == True:
            for sym, _df in pricing_data.items():
                mask = (_df.index <= max_test_date)
                test_set[sym] = _df[mask]
                validation_set[sym] = _df[~mask]
        else:
            for sym, vals in pricing_data.items():
                test_vals, validation_vals = [], []
                for r in vals:
                    if r[0] <= max_test_date:
                        test_vals.append(r)
                    else:
                        validation_vals.append(r)
                test_set[sym] = test_vals
                validation_set[sym] = validation_vals
        return test_set, validation_set


def get_recent_x_sessions(pricing_data=None, days=None, ignore_current_ds=False):
    """
    input: *pricing_data* is dict in the form of: {'symbol_key': df}

    It returns dict in the same form as input but symbols will have data only for most
    recent `days` days.

    It does NOT make sure if the dates for all symbols are aligned. E.g. if one symbol
    has data from year ago and other will be up-to-date, then sessions may not even overlap
    """
    if ignore_current_ds == False:
        return {
            sym: df.iloc[-days:]
            for sym, df in pricing_data.items()
        }
    else:
        output = {}
        cur_ds = datetime.datetime.now().strftime('%Y-%m-%d') 
        for sym, df in pricing_data.items():
            lst_ds = df.index[-1].strftime('%Y-%m-%d')
            if lst_ds == cur_ds:
                output[sym] = df.iloc[-days:-1]
            else:
                output[sym] = df.iloc[-days:]
        return output


def gather_entry_exist_signals_dates(df):
    """
    Useful when one need to visualise periods of long/short positions on the graph.

    Inputs:
    `df` - Pandas dataframe with entry_long, entry_short, exit_long, exit_short columns.

    Returns lists (for long and short trades) with tuples like: (entry_date, exit_date).
    """
    periods = {}
    for _type in ('long', 'short'):
        idxs_entries = df.index[df['entry_'+_type] == 1].tolist()
        idxs_exits = df.index[df['exit_'+_type] == 1].tolist()
        if len(idxs_entries) > len(idxs_exits):
            idxs_entries = idxs_entries[:-1]            
        elif len(idxs_exits) > len(idxs_entries):
            idxs_exits = idxs_exits[:-1]            
        elif len(idxs_entries) != len(idxs_exits):
            # logically they can differ only by 1, so if its not the case sth is wrong
            raise ValueError
        periods[_type]  = list(zip(idxs_entries, idxs_exits))
    return periods['long'], periods['short']


def create_bollinger_bands(df_org, price_label='close', ma_type='simple', time_window=20, no_std=2, with_nans=False):
    """
    Inputs:
    `df` - Pandas dataframe with prices.
    `price_label` - Name of the column with price which shuold be used.
    `ma_type` - Type of the moving average. Can be "simple" or "exp" (exponential).
    `time_window` - Period of time over which averages and standard deviations will be calculated. In days.

    Returns df with additional columns:
    - central_ma: central moving average (middle Bolliner Band)
    - ma_std: standard deviation of `central_ma`
    - lower_ma: central band shifted down by `no_std` standard deviations (lower Bolliner Band)
    - upper_ma: central band shifted up by `no_std` standard deviations (upper Bolliner Band)

    If with_nans is False (default) all rows where there is NaN in Bollinger Bands columns will be dropped.
    """
    df = df_org.copy()
    if ma_type == 'simple':
        df.loc[:, 'central_ma'] = df[price_label].rolling(window=time_window).mean()
    elif ma_type == 'exp':
        df.loc[:, 'central_ma'] = df[price_label].ewm(span=time_window, adjust=False).mean()
    df.loc[:, 'ma_std'] = df[price_label].rolling(window=time_window).std()
    df.loc[:, 'lower_ma'] = df['central_ma'] - (no_std*df['ma_std'])
    df.loc[:, 'upper_ma'] = df['central_ma'] + (no_std*df['ma_std'])
    if with_nans:
        return df
    df_no_nans = df[~df['ma_std'].isnull()]
    return df_no_nans


def on_balance_volume_indicator(df_org, price_label='close', volume_label='volume'):
    """
    The on-balance volume (OBV) indicator is calculated by keeping a running total of the indicator each day and adding 
    the entire amount of daily volume when the closing price increases, and subtracting the daily volume when the closing 
    price decreases.

    Returns original df with new 'obv' columns
    """
    df = df_org.copy()
    df.loc[:, 'multiplier'] = np.where(
        df[price_label] - df[price_label].shift(1) > 0, 1, -1
    )
    df.loc[:, 'volume_change'] = df[volume_label] * df['multiplier']
    df.loc[:, 'obv'] = df['volume_change'].cumsum()
    for col in ('multiplier', 'volume_change'):
        df.drop(col, axis=1, inplace=True)
    return df


def roc_oscillator(df_org, days=None, col=None):
    """
    Simple m-day ROC (Rate Of Change) oscillator. Where the m-day ROC at time t is:
    (qt − qt_minus_m)/qt_minus_m, where qt is the value of `col` (e.g. price pr volume) at time t.
    """
    df = df_org.copy()
    df.loc[:, '_mday_ago'] = df[col].shift(days)
    df.loc[:, '_roc'] = (df[col] - df['_mday_ago']) / df['_mday_ago']
    return df['_roc']


def simple_ma(df_org, days=None, col=None):
    # Simple wrapper around pandas rollign to gfet simple moving average 
    df = df_org.copy()
    df.loc[:, f'sma{days}'] = df[col].rolling(window=days).mean()
    return df[f'sma{days}']

