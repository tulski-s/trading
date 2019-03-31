

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
    # print(df[price_label].rolling(window=time_window).mean().head(10))
    if ma_type == 'simple':
        df.loc[:, 'central_ma'] = df[price_label].rolling(window=time_window).mean()
    elif ma_type == 'exp':
        df.loc[:, 'central_ma'] = df[price_label].ewm(span=time_window, adjust=False).mean()
    # print('central powinna byc tylko: ', df.head(10))
    df.loc[:, 'ma_std'] = df[price_label].rolling(window=time_window).std()
    df.loc[:, 'lower_ma'] = df['central_ma'] - (no_std*df['ma_std'])
    df.loc[:, 'upper_ma'] = df['central_ma'] + (no_std*df['ma_std'])
    if with_nans:
        return df
    df_no_nans = df[~df['ma_std'].isnull()]
    return df_no_nans


    