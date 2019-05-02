import sys
import itertools
sys.path.insert(0, '/Users/slaw/osobiste/trading')

# 3rd party
import numpy as np

# custom
import gpw_data
import strategies.helpers as helpers

import position_size
import backtester

import commons


def generate_signals(df, ma_type='simple', time_window=20, no_std=2):
    df = df.copy()
    
    # 1) bollinger_bands
    df = helpers.create_bollinger_bands(df, ma_type=ma_type, time_window=time_window, no_std=no_std)
    
    # 2) find breakout candles ("Big Bold Candels" from the strategy description)
    # get lenght of clandles body, it's average and std dev
    df.loc[:, 'candle_range'] = abs(df['close'] - df['open'])
    df.loc[:, 'candle_range_avg'] = df['candle_range'].rolling(window=time_window).mean()
    df.loc[:, 'candle_range_std'] = df['candle_range'].rolling(window=time_window).std()
    # identify "long" candles
    df.loc[:, 'is_long'] = abs(df['candle_range_avg'] - df['candle_range']) > no_std*df['candle_range_std']
    
    # 3) Overbought and oversold regimes
    upper_diff_idx0 = abs(df['upper_ma'].iloc[0] - df['close'].iloc[0])
    lower_diff_idx0 = abs(df['lower_ma'].iloc[0] - df['close'].iloc[0])
    if upper_diff_idx0 >= lower_diff_idx0:
        first_period = 'L'
    elif upper_diff_idx0 < lower_diff_idx0:
        first_period = 'U'
    df.loc[:, 'previous_period'] = ''
    df.at[df.index[0], 'previous_period'] = first_period
    df.replace('', np.nan, inplace=True)
    df.loc[:, 'previous_period'] = np.where(df['close'] <= df['lower_ma'], 'L', df['previous_period'])
    df.loc[:, 'previous_period'] = np.where(df['close'] >= df['upper_ma'], 'U', df['previous_period'])
    df['previous_period'].fillna(method='ffill', inplace=True)
    
    # 4) *for debuging* get first/last dates for Overbought/oversold periods
    overbought = []
    oversold = []
    prev = first_period
    prev_idx_int = 0
    idx_int = 1
    for idx, row in itertools.islice(df.iterrows(), 1, None):
        cur = row['previous_period']
        if prev == 'U' and cur == 'L':
            overbought.append((df.index[prev_idx_int], df.index[idx_int-1]))
            prev = cur
            prev_idx_int = idx_int
        elif prev == 'L' and cur == 'U':
            oversold.append((df.index[prev_idx_int], df.index[idx_int-1]))
            prev = cur
            prev_idx_int = idx_int
        if prev == cur:
            idx_int += 1
            continue
        idx_int += 1
        
    # 5) GENERATE ENTRY AND EXIT SIGNALS
    df.loc[:, 'entry_long'] = 0
    df.loc[:, 'entry_short'] = 0
    df.loc[:, 'exit_long'] = 0
    df.loc[:, 'exit_short'] = 0
    df.loc[:, 'stop_loss'] = np.nan
    # helper tracking variable to correctly set up things
    _long_position = 0
    _short_position = 0
    _long_above_cma = 0
    _short_below_cma = 0
    _stop_loss = 0
    for i, row in df.iterrows():
        # entries
        entry_long_signal = (row['is_long'] == True) and (row['close'] > row['open']) and (row['previous_period'] == 'L')
        entry_short_signal = (row['is_long'] == True) and (row['close'] < row['open']) and (row['previous_period'] == 'U')
        if (entry_long_signal == True) and (_long_position == 0) and (_short_position == 0):
            df.at[i,'entry_long'] = 1
            _long_position = 1
            _stop_loss = row['low']
        elif (entry_short_signal == True) and (_long_position == 0) and (_short_position == 0):
            df.at[i,'entry_short'] = 1
            _short_position = 1
            _stop_loss = row['high']
        # exits
        exit_long_signal = (row['close'] <= row['central_ma']) and _long_above_cma == 1
        exit_short_signal = (row['close'] >= row['central_ma']) and _short_below_cma == 1
        if exit_long_signal and _long_position == 1:
            df.at[i,'exit_long'] = 1
            _long_position = 0
            _long_above_cma = 0
        elif exit_short_signal and _short_position == 1:
            df.at[i,'exit_short'] = 1
            _short_position = 0
            _short_below_cma = 0
        # set up crossing middle central moving average
        if _long_position == 1 and row['open'] > row['central_ma']:
            _long_above_cma = 1
        elif _short_position == 1 and row['open'] < row['central_ma']:
            _short_below_cma = 1
        # roll over stop loss
        if _long_position == 1 or _short_position == 1:
            df.at[i,'stop_loss'] = _stop_loss
            
    # 6) *for debuging* get long and short trades periods
    long_periods, short_periods = helpers.gather_entry_exist_signals_dates(df)
    
    return {
        'df': df,
        'long_periods': long_periods,
        'short_periods': short_periods,
        'overbought_periods': overbought,
        'oversold_periods': oversold,
    }


def run_strategy(days=-1, debug=False, stop_loss=False):
    gpwdata = gpw_data.GPWData(pricing_data_path='./pricing_data')
    wig_20_stocks = gpwdata.load(index='WIG20')
    
    symbol_key = 'ENEA'
    raw_prices = wig_20_stocks[symbol_key]

    signals = {
        symbol_key: generate_signals(
            raw_prices,
            ma_type='simple',
            time_window=20,
            no_std=2
        )['df']
    }

    # signals_test, signals_validation = gpwdata.split_into_subsets(signals, 0.5)

    position_sizer = position_size.MaxFirstEncountered()
    tester = backtester.Backtester(
        # signals_test,
        signals,
        position_sizer=position_sizer,
        debug=debug,
        stop_loss=stop_loss
    )
    
    if days == -1:
        tester_results, tester_trades = tester.run()
    else:
        tester_results, tester_trades = tester.run(test_days=days)

    return tester_results, tester_trades


if __name__ == '__main__':
    parser = commons.get_parser()
    parser.add_argument('--days', '-d', type=int, default=-1, help='number of days to run backtester for')
    parser.add_argument('--stop_loss', '-sl', action='store_true', help='should use stop loss')
    args = parser.parse_args()
    run_strategy(args.days, args.debug, args.stop_loss)
    