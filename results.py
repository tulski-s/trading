# 3rd party
import matplotlib
from matplotlib.font_manager import FontProperties
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import pandas as pd
import scipy.stats as stats


def get_daily_returns(results_df):
    """
    Calculates daily returns given nav (net account value). Note:
    To find the return 𝑅(𝑡1,𝑡2) between dates 𝑡1 and 𝑡2 one takes 𝑅(𝑡1,𝑡2)=𝑁𝐴𝑉(𝑡2)/𝑁𝐴𝑉(𝑡1)−1
    """
    df = results_df.copy()
    df.loc[:, 'prev_day_nav'] = df['nav'].shift(1)
    df.loc[:, 'daily_returns'] = df['nav']/df['prev_day_nav']-1
    return df['daily_returns']


def get_price_change(results_df, price_label='close'):
    df = results_df.copy()
    df.loc[:, 'prev_price'] = df[price_label].shift(1)
    df.loc[:, 'price_change'] = df[price_label] - df['prev_price']
    df.fillna(value={'price_change':0}, inplace=True)
    return df['price_change']


def evaluate(results_df, trades):
    """
    Input: results dataframe from backtester run

    Outputs:
        - Daily Returns
        - Annualized Sharpe Ratio
        - Maximum Drowndown
        - Maximum Drowndown Duration
        - Annualized Returns
        - Expectation
        - No. of trades
        - Win rate
    """
    df = results_df.copy()

    # Daily Returns
    df.loc[:, 'daily_returns'] = get_daily_returns(df)

    # Annualized Sharpe Ratio
    risk_free_rate = 0.02
    sessions_per_year = 252
    excess_returns = df['daily_returns'] - risk_free_rate/sessions_per_year
    mean_er = excess_returns.mean()
    std_dev_er = excess_returns.std()
    annualized_sharpe_ratio=(sessions_per_year**.5)*mean_er/std_dev_er

    # Maximum Drowndown
    df.loc[:, 'highwatermark'] = df['rate_of_return'].cummax()
    df.loc[:, 'drawdown'] = df['highwatermark'] - df['rate_of_return']
    maximum_drawdown = df['drawdown'].max()

    # Maximum Drowndown Duration - cumulative sum of drowdown occurence which "restarts" everytime it encounters 0
    df.loc[:, 'is_dd'] = df['drawdown'] != 0
    df.loc[:, 'drowdown_duration'] = \
        df['is_dd'].cumsum()-df['is_dd'].cumsum().where(~df['is_dd']).ffill().fillna(0).astype(int)
    maximum_drawdown_duration = df.loc[:, 'drowdown_duration'].max()

    # Annualized Return - ((1 + cumulative_return%) ** (365/days held))-1
    annualized_return = (((1+(df['rate_of_return'].iloc[-1]/100))**(365/df.shape[0]))-1)*100

    # Expectation - Win% * Avg_Win_Size - Loss% * Avg_Loss_Size
    profits = []
    losses = []
    for trade in trades.values():
        # trade was not closed after backtest
        if ('sell_value_with_fee' not in trade.keys()):
            continue
        if trade['profit'] > 0:
            profits.append(trade['profit'])
        else:
            losses.append(trade['profit'])

    no_trades = len(trades)
    avg_win = 0 if len(profits) == 0 else sum(profits)/len(profits)
    avg_loss = 0 if len(losses) == 0 else abs(sum(losses)/len(losses))
    expectation = ((len(profits)/no_trades)*avg_win) - ((len(losses)/no_trades)*avg_loss)
    win_rate = int((len(profits)/no_trades)*100)
    return {
        'sharpe': annualized_sharpe_ratio,
        'max_dd': maximum_drawdown,
        'max_dd_duration': maximum_drawdown_duration,
        'annualized_return': annualized_return,
        'no_trades': no_trades,
        'expectation': expectation,
        'win_rate': win_rate
    }


def performance_report(results, trades):
    fig = plt.figure(figsize=(9,7))
    gs = gridspec.GridSpec(2, 6)
    ax1 = plt.subplot(gs[0, :2])
    ax2 = plt.subplot(gs[0, 2:])
    ax3 = plt.subplot(gs[1, :3])
    ax4 = plt.subplot(gs[1, 3:])
    
    df = results.copy()
    metrics = evaluate(results, trades)

    def _msg(arr):
        return 'Mean: {}\nStd Dev: {}\nExcess kurtosis: {}\nSkewness: {}'.format(
            round(arr.mean(), 2),
            round(arr.std(), 2),
            round(stats.kurtosis(arr), 2),
            round(stats.skew(arr), 2),
        ).strip()

    # Draw table with metrics
    metrics_labels = ('sharpe', 'expectation', 'max_dd', 'max_dd_duration', 'annualized_return', 'max_dd_duration', 'no_trades', 'win_rate')
    metrics_tbl = ax1.table(
        cellText=[[k, round(metrics[k], 2)] for k in metrics_labels],
        colLabels=['Metric', 'Value'], 
        colWidths=[0.6, 0.3],
        loc='center', 
        cellLoc='center',
    )
    for (row, col), cell in metrics_tbl.get_celld().items():
        if (row == 0) or (col == -1):
            cell.set_text_props(fontproperties=FontProperties(weight='bold'))
    ax1.axis("off")
    metrics_tbl.set_fontsize(12)
    metrics_tbl.scale(1, 2)

    # Draw rate of returns curve
    df['rate_of_return'].plot(ax=ax2, title='Rate of returns')

    # Draw distribution of daily returns
    df.loc[:, 'daily_returns'] = (df['nav']/df['nav'].shift(1)-1)
    daily_returns = (df[df['daily_returns'] != 0]['daily_returns']).copy()
    daily_returns.dropna(axis=0, inplace=True)
    daily_returns.plot(ax=ax3, kind='hist', title='Distribution of daily returns')
    ax3.text(0.01, -0.4, _msg(daily_returns.values), ha="left", transform=ax3.transAxes)

    # Draw distribution of trades
    profits = {'profit': [t['profit'] for t in trades.values() if t.get('profit', None)]} 
    profits_df = pd.DataFrame(profits)['profit']
    profits_df.plot(ax=ax4, kind='hist', title='Distribution of profits from trades')
    ax4.text(0.01, -0.4, _msg(profits_df.values), ha="left", transform=ax4.transAxes)
    
    plt.tight_layout()
    plt.show()


def year_trades(year, trades):
    """
    Detailed summary of trades for given year(s). `year` can be int or iterable with ints.
    IT plots summary table and returns dict with metrics and their values.
    """
    years = []
    if isinstance(year, int):
        years = [year]
    else:
        years = year
    
    metrics_labels = (
        'total_profit', 'no_trades', 'no_winning_trades', 'no_loosing_trades', 'win_rate',
        'no_open_at_the_end', 'no_long_trades', 'no_short_trades', 'avg_profit',
        'avg_winning_profit', 'avg_loosing_profit', 'expectation', 'max_loosing_trade',
        'max_winning_trade','avg_trade_length','max_trade_length','min_trade_length',
    )
    metrics = {m: 0 for m in metrics_labels}
    _avg_profit = 0
    _winning_trades = []
    _loosing_trades = []
    _trade_lengths = []
    for t_name, t_details in trades.items():
        # trade names should start with YYYY-MM-DD
        if int(t_name[:4]) in years:
            _profit = t_details['profit']
            metrics['total_profit'] += _profit
            if _profit > 0:
                metrics['no_winning_trades'] += 1
                metrics['avg_winning_profit'] += _profit
                _winning_trades.append(_profit)
            else:
                metrics['no_loosing_trades'] += 1
                metrics['avg_loosing_profit'] += _profit
                _loosing_trades.append(_profit)
            metrics['no_trades'] += 1
            sell_ds = str(t_details.get('sell_ds', None))
            if (sell_ds == None) or (int(sell_ds[:4]) not in years):
                metrics['no_open_at_the_end'] += 1
            if sell_ds:
                _trade_lengths.append(
                    (t_details['sell_ds'] - t_details['buy_ds']).days
                )
            if t_details['type'] == 'long':
                metrics['no_long_trades'] += 1
            elif t_details['type'] == 'short':
                metrics['no_short_trades'] += 1
    metrics['total_profit'] = round(metrics['total_profit'], 2)
    metrics['avg_profit'] = round(metrics['total_profit'] / metrics['no_trades'], 2)
    metrics['win_rate'] = round(metrics['no_winning_trades']/metrics['no_trades'], 2)
    metrics['avg_winning_profit'] = round(metrics['avg_winning_profit'] / metrics['no_trades'], 2)
    metrics['avg_loosing_profit'] = round(metrics['avg_loosing_profit'] / metrics['no_trades'], 2)
    metrics['expectation'] = round(
        (metrics['win_rate'] * metrics['avg_winning_profit'])
        - ((1-metrics['win_rate']) * metrics['avg_loosing_profit'])
        , 2
    )
    if len(_winning_trades) > 0:
        metrics['max_winning_trade'] = max(_winning_trades)
    if len(_loosing_trades) > 0:
        metrics['max_loosing_trade'] = min(_loosing_trades)
    if len(_trade_lengths) > 0:
        metrics['avg_trade_length'] = int(sum(_trade_lengths) / len(_trade_lengths))
        metrics['max_trade_length'] = max(_trade_lengths)
        metrics['min_trade_length'] = min(_trade_lengths)
        
    # Create matplotlib TABLE
    fig, ax = plt.subplots()
    fig.patch.set_visible(False)
    ax.axis('off')
    ax.axis('tight')
    if len(years) == 1:
        ax.set_title(f'Trades sumary for year: {year}', loc='center')
    else:
        _sorted_ys = sorted(years)
        ax.set_title(f'Trades sumary for years: {_sorted_ys[0]} - {_sorted_ys[-1]}', loc='center')
    table = ax.table(
        cellText=[[k, metrics[k]] for k in metrics_labels],
        colLabels=['Metric', 'Value'], 
        colWidths=[0.4, 0.3],
        loc='center', 
        cellLoc='center',
    )
    for (row, col), cell in table.get_celld().items():
        if (row == 0):
            cell.set_text_props(fontproperties=FontProperties(weight='bold'))
        if cell.get_text()._text == 'expectation':
            if metrics['expectation'] > 0:
                table[(row, 1)].set_facecolor("#90EE90")
            else:
                table[(row, 1)].set_facecolor("#DC143C")
    table.set_fontsize(9)
    table.scale(1, 1.2)
    
    return metrics
    


def main():
    pass



if __name__ == '__main__':
    main()