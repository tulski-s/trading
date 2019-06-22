# built-in
import sys
import itertools
sys.path.insert(0, '/Users/slaw/osobiste/trading')

# 3rd party
import numpy as np

# custom
import backtester
import commons
import gpw_data
import position_size
import results
import strategy
import strategies.helpers


def generate_signals(df, ma_type='simple', time_window=None, no_std=None, min_holding_period=None, perc_to_region=None):
    # 1) bollinger_bands
    df = strategies.helpers.create_bollinger_bands(df, ma_type=ma_type, time_window=time_window, no_std=no_std)

    # 2) find breakout candles ("Big Bold Candels" from the strategy description)
    # get lenght of clandles body, it's average and std dev
    df.loc[:, 'candle_range'] = abs(df['close'] - df['open'])
    df.loc[:, 'candle_range_avg'] = df['candle_range'].rolling(window=time_window).mean()
    df.loc[:, 'candle_range_std'] = df['candle_range'].rolling(window=time_window).std()
    # identify "long" candles
    df.loc[:, 'is_long'] = abs(df['candle_range_avg'] - df['candle_range']) > no_std*df['candle_range_std']
    
    # 3) # Overbought and oversold regimes
    upper_diff_idx0 = abs(df['upper_ma'].iloc[0] - df['close'].iloc[0])
    lower_diff_idx0 = abs(df['lower_ma'].iloc[0] - df['close'].iloc[0])
    if upper_diff_idx0 >= lower_diff_idx0:
        first_period = 'L'
    elif upper_diff_idx0 < lower_diff_idx0:
        first_period = 'U'
    df.loc[:, 'previous_period'] = ''
    df.at[df.index[0], 'previous_period'] = first_period
    df.replace('', np.nan, inplace=True)
    
    # close enough to top/bottom
    df.loc[:, 'previous_period'] = np.where(
        ((df['low'] <= (df['lower_ma'] + ((df['central_ma']-df['lower_ma'])*perc_to_region))) | (df['low'] <= (df['lower_ma'] + ((df['central_ma']-df['lower_ma'])*perc_to_region)))),
        'L', df['previous_period']
    )
    df.loc[:, 'previous_period'] = np.where(
        ((df['high'] >= (df['upper_ma'] - ((df['upper_ma']-df['central_ma'])*perc_to_region))) | (df['high'] >= (df['upper_ma'] - ((df['upper_ma']-df['central_ma'])*perc_to_region)))),
        'U', df['previous_period']
    )
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
    _stop_loss = 0
    _holding_period = 0
    
    for i, row in df.iterrows():
        # Long exit - close of green candle is X% to overbought (|central - high|). Also - above min holding period
        upper_signal = row['upper_ma'] - ((row['upper_ma']-row['central_ma'])*perc_to_region)
        exit_long_signal = (
            (row['close'] > row['open']) and \
            (row['close']>= upper_signal) and  \
            _holding_period >= min_holding_period
        ) 
        
        # Short exit - close of red candle is 20% to oversold and above min holding period        
        lower_signal = row['lower_ma'] + ((row['central_ma']-row['lower_ma'])*perc_to_region)
        exit_short_signal = (
            (row['close'] < row['open']) and \
            (row['close']<= lower_signal) and  \
            _holding_period >= min_holding_period
        )
        
        if (exit_long_signal and _long_position == 1) or (_long_position == 1 and row['close'] <= _stop_loss):
            df.at[i,'exit_long'] = 1
            _long_position = 0
            _long_above_cma = 0
        elif (exit_short_signal and _short_position == 1) or (_short_position == 1 and row['close'] >= _stop_loss):
            df.at[i,'exit_short'] = 1
            _short_position = 0
            _short_below_cma = 0
            
        # entries
        entry_long_signal = (row['is_long'] == True) and (row['close'] > row['open']) and (row['previous_period'] == 'L')
        entry_short_signal = (row['is_long'] == True) and (row['close'] < row['open']) and (row['previous_period'] == 'U')
        
        if (entry_long_signal == True) and (_long_position == 0) and (_short_position == 0):
            df.at[i,'entry_long'] = 1
            _long_position = 1
            _stop_loss = row['low']
            df.at[i,'stop_loss'] = _stop_loss
        elif (entry_short_signal == True) and (_long_position == 0) and (_short_position == 0):
            df.at[i,'entry_short'] = 1
            _short_position = 1
            _stop_loss = row['high']
            df.at[i,'stop_loss'] = _stop_loss
        
        # set up crossing middle central moving average
        if _long_position == 1 or _short_position == 1:
            _holding_period += 1
        # roll over stop loss
        if _long_position == 1 or _short_position == 1:
            df.at[i,'stop_loss'] = _stop_loss
            
    return df

def run_strategy(days=-1, debug=False):
    gpwdata = gpw_data.GPWData(pricing_data_path='./pricing_data')
    index = 'WIG20'
    wig_20_stocks = gpwdata.load(index=index)
    data_test, data_validation = gpwdata.split_into_subsets(wig_20_stocks, 0.5)

    print('Generating signals for data...', end='', flush=True)
    signals={
        symbol: generate_signals(
            df,
            ma_type='exp',
            time_window=25,
            no_std=3,
            min_holding_period=5,
            perc_to_region=0.2,
        )
        for symbol, df in data_validation.items() if not df.empty
    }
    print('... Done! [OK]')

    print('Running backtest...', end='', flush=True)
    tester = backtester.Backtester(
        signals,
        position_sizer=position_size.PercentageRisk(perc_risk=0.005, debug=debug, sort_type='cheapest'), # 0.5% risk
        debug=debug,
    )
    if days == -1:
        tester_results, tester_trades = tester.run()
    else:
        tester_results, tester_trades = tester.run(test_days=days)
    
    results.performance_report(tester_results, tester_trades)
    print('... Done! [OK]')


def optimize():
    strategy_kwargs = {
        'ma_type': ['simple', 'exp'],
        'time_window':[5, 7, 10, 14, 21, 25, 28],
        'no_std': [1,2,3],
        'min_holding_period': [1, 5, 10, 15],
        'perc_to_region': [0.2, 0.4, 0.6, 0.8]
    }

    gpwdata = gpw_data.GPWData(pricing_data_path='./pricing_data')
    index = 'WIG20'
    wig_20_stocks = gpwdata.load(index=index)
    data_test, data_validation = gpwdata.split_into_subsets(wig_20_stocks, 0.5)
    position_sizers = {
        '1': position_size.PercentageRisk(perc_risk=0.005, sort_type='cheapest'),
        '2': position_size.PercentageRisk(perc_risk=0.01, sort_type='cheapest'),
        '3': position_size.PercentageRisk(perc_risk=0.015, sort_type='cheapest'),
        '4': position_size.PercentageRisk(perc_risk=0.005),
        '5': position_size.PercentageRisk(perc_risk=0.01),
        '6': position_size.PercentageRisk(perc_risk=0.015),
        '7': position_size.FixedCapitalPerc(capital_perc=0.1, sort_type='cheapest'),
        '8': position_size.FixedCapitalPerc(capital_perc=0.2, sort_type='cheapest'),
        '9': position_size.FixedCapitalPerc(capital_perc=0.3, sort_type='cheapest'),
        '10': position_size.FixedCapitalPerc(capital_perc=0.1),
        '11': position_size.FixedCapitalPerc(capital_perc=0.2),
        '12': position_size.FixedCapitalPerc(capital_perc=0.3),
    }
    for idx, sizer in position_sizers.items():
        print('Optimizing siezer: ', idx)
        res = strategy.optimize_strategy(
            data=data_test,
            signal_gen_func=generate_signals,
            strategy_kwargs=strategy_kwargs,
            position_sizer=sizer,
            init_capital=10000,
            results_path='/Users/slaw/osobiste/trading/{}_optimization_results_all.csv'.format(idx),
        )

if __name__ == '__main__':
    parser = commons.get_parser()
    parser.add_argument('--days', '-d', type=int, default=-1, help='number of days to run backtester for')
    parser.add_argument('--optimize', '-o', action='store_true', help='run optimization')
    args = parser.parse_args()
    if args.optimize:
        optimize()
    else:
        run_strategy(args.days, args.debug)
    

