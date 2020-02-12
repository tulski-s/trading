# 3rd part
import numpy as np

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
    df.loc[:, 'mday_ago'] = df[col].shift(days)
    df.loc[:, 'roc'] = (df['close'] - df['mday_ago']) / df['mday_ago']
    return df['roc']

