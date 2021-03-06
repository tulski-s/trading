"""
Strategy 1 - Swing trading on ETFs
source: https://seekingalpha.com/instablog/20641661-marc-cohn/2770913-a-swing-trading-strategy-for-nipping-profits-from-short-term-spikes-in-etf-prices

Momentum trading relies on the principle that money follows money, and investors like to chase winners. Trends, however, never
follow a straight line. This system relies on recent intraday highs and lows to generate trading signals that detect relatively strong, 
short-term upward or downward price movements. These price swings can indicate a short-term price spike away from the trend, with the 
expectation that the price will  revert to the mean in a short amount of time.

The trading system proposed in this article calculates:
- an Exponential Moving Average (EMA) of the intraday highs
- an EMA of the intraday lows
- an EMA of the daily closes of one ETF
The difference between the EMA of highs and the EMA of lows is calculated as an Average Daily Range (ADR).

A percentage is then calculated to determine where the EMA of closes sits within in the ADR relative to the highs. 

Example 1
Assume the EMA of highs is 50 and the EMA of lows is 40. If the EMA of closes is 45, then the system will generate a percentage
of 50% because the EMA of closes falls in the middle of the daily range. 

EMA_H = 50
EMA_L = 40
EMA_C = 45
ADR = EMA_H - EMA_L = 10
perc = ((EMA_H - EMA_C)*100)/ADR = ((50-45)*100)/10 = 50

Example 2
If the EMA of closes was 43 in the example above, then the system will generate a percentage of 70%. If the EMA of closes was 48.3, 
then the system will generate a percentage of 17%, and so on. A lower percentage means the EMA of closes is nearer to the EMA of highs.
EMA_H = 50
EMA_L = 40
EMA_C = 43
ADR = 10
perc = ((50-43)*100)/10 = 70

As the EMA of highs, lows, and closes fluctates over time, the percentage generated by the system will also fluctuate. When the percentage is high,
this means an upward spike has occurred, because the closes are at the high end of the daily range. When the percentage is low, this means a downward
spike has occurred, because the closes are at the low end of the daily range.

Implementation:
Once the percentage is calculated as discussed above, implementation of the system is simple. When the percentage is greater than a certain value, 
i.e., price has fallen, then go long the ETF. When the percentage is below a certain value, i.e., price has risen, go short the ETF.

Parameters (final ones) from the article:
(universe)
MDY, which tracks a market-cap-weighted index of midcap US companies. Author tested also on different ETFs. In all cases system outperforms
buy-and-hold in terms of returns, std.dev., max.dd etc. 

(time window)
3-day EMAs

(entry) Author chose 35% as the threshold for long trades and 25% as the threshold for short trades, ie. if the calculated percentage 
was greater than 35% - go long, if it was lower than 25% go short.

(exit) Exit the long trades whenever the percentage falls below 35% (because price has risen back to the mean) and exit the short trades 
whenever the percentage falls above 25% (because price has fallen back to the mean).

My notes and thoughts:
- from what I understand in long only strategy...?, that is because by "shorting" author means selling ETF units previously bought
(ie. closing position). Is there scenarios where for example MDY is going down and author is taking short position (to gain from downwards movement)?
- Above note is wrong. It clearly says in the article that: "Exit the long trades (...) and exit the short trades(...)".
- Accroding to: http://www.etf.com.pl/Informacje/Zalety-ETF/Krotka-sprzedaz ETF's units in Poland can be a subject of short sale so theoretically
I should be able to execute this strategy in real in Poland
- first date for ETFW20L (ETF which represents stocks in WIG20 index) is 2010-09-28 so I will have ~7 years of data
"""

# Step 1 - For single ETF create chart with etries and exits signals for both long and short trades. That should give you a little bit more
# intuition about the systems

# built-in
import sys
sys.path.insert(0, '/Users/slaw/osobiste/trading')

# 3rd party
import numpy as np

# custom
from gpw_data import GPWData
from backtester import Backtester

from position_size import (
    MaxFirstEncountered,
    FixedCapitalPerc,
)

from commons import (
    get_parser,
)


def get_strategy_signals(symbols):
    data = GPWData()
    etfs = data.load(symbols=symbols)
    if not isinstance(etfs, dict):
        etfs = {symbols: etfs}

    # strategy params (should come as params but hardcoded here for simplicity)
    time_window=3
    long_threshold=35
    short_threshold=30
    
    # calculate signals
    for sym, etf in etfs.items():
        for price in ('high', 'low', 'close'):
            etf.loc[:, 'ema_{}_{}'.format(time_window, price)] = etf[price].ewm(span=time_window, adjust=False).mean()
        etf.loc[:, 'adr'] = etf['ema_{}_high'.format(time_window)] - etf['ema_{}_low'.format(time_window)]
        etf.loc[:, 'perc_range'] = \
            ((etf['ema_{}_high'.format(time_window)]-etf['ema_{}_close'.format(time_window)])*100)/etf['adr']
        
        for signal_type in ('long', 'short'):
            if signal_type == 'long':
                etf.loc[:, 'potential_signal'] = np.where(etf['perc_range'] > long_threshold, 1, 0)
            elif signal_type == 'short':
                etf.loc[:, 'potential_signal'] = np.where(etf['perc_range'] < short_threshold, 1, 0)
            etf.loc[:, 'previous_potential_signal'] = etf['potential_signal'].shift(1)
            etf['previous_potential_signal'].fillna(value=0, inplace=True)
            etf.loc[:, 'entry_{}'.format(signal_type)] = np.where(
                (etf['potential_signal']==1) & (etf['previous_potential_signal']==0), 1, 0
            )
            etf.loc[:, 'exit_{}'.format(signal_type)] = np.where(
                (etf['potential_signal']==0) & (etf['previous_potential_signal']==1), 1, 0
            )
            etf.drop(['potential_signal', 'previous_potential_signal'], axis=1, inplace=True)

    etfs_t, etfs_v = data.split_into_subsets(etfs, 0.5)

    return etfs_t, etfs_v


def run_test_strategy_ETFW20L(days=-1, debug=False):
    symbols = 'ETFW20L'
    test_signals, validation_signals = get_strategy_signals(symbols)
    position_sizer = MaxFirstEncountered(debug=debug, sort_type='cheapest')
    backtester = Backtester(test_signals, position_sizer=position_sizer, debug=debug)
    if days == -1:
        results, trades = backtester.run()
    else:
        results, trades = backtester.run(test_days=days)
    return results, trades


def run_test_strategy_etfs(days=-1, debug=False):
    symbols = ['ETFW20L', 'ETFSP500', 'ETFDAX']
    test_signals, validation_signals = get_strategy_signals(symbols)
    position_sizer = FixedCapitalPerc(debug=debug, sort_type='cheapest', capital_perc=0.2)
    backtester = Backtester(test_signals, position_sizer=position_sizer, debug=debug)
    if days == -1:
        results, trades = backtester.run()
    else:
        results, trades = backtester.run(test_days=days)

    return results, trades


if __name__ == '__main__':
    parser = get_parser()
    parser.add_argument('--days', '-d', type=int, default=-1, help='number of days to run backtester for')
    args = parser.parse_args()
    run_test_strategy_etfs(args.days, args.debug)

