# 3rd party
import matplotlib.pyplot as plt

# custom
from initial_strategy import Portfolio as Portfolio
from stocks_universe import StocksUniverse
from initial_strategy import Trade as Trade

class Strategy():
    def __init__(self, available_capital=20000, init_stop_const=3.4):
        self.init_stop_const=init_stop_const
        self.results = []# temporary solution for storing results. will change that with reporting object

        #scope = ['LPP']
        #scope = ['PKOBP','CCC','KGHM']
        scope = 'WIG20'
        self.universe = StocksUniverse(scope=scope)
        self.portfolio = Portfolio(available_capital=available_capital)

        # populates self.data
        # self._prepare_data()

        self.setup_trades = {}

    def run(self, development_days=None):
        dev_counter = 0
        available_sessions_dates = self.universe.available_sessions()

        for day in available_sessions_dates:
            if development_days and (dev_counter >= development_days):
                break
            current_date = str(day).split('T')[0]

            stocks_to_buy = self._open_session(current_date=current_date)
            self._stocks_purchase(stocks_to_buy=stocks_to_buy, current_date=current_date)
            value_in_stocks = self._look_after_trades(current_date=current_date)

            print('[{}] Money in stocks is: {}'.format(current_date, value_in_stocks))
            total_profit = value_in_stocks + self.portfolio.available_capital
            print('[{}] Total profit is: {}'.format(current_date, total_profit))

            self.results.append(total_profit)
            dev_counter += 1

    def show_graph(self):
        plt.plot(list(range(len(self.results))), self.results)
        plt.show()

    def _open_session(self, current_date=None):
        """
        First check setup for stocks in universe. Consider buying if you have more than 1000.
        If setup is positive, checking for entry signal.
        """
        stocks_to_buy = []
        for symbol in self.universe.symbols:
            stock_data = self.universe.day_stock_data(symbol=symbol, date=current_date)
            if isinstance(stock_data, type(None)):
                continue

            # setup
            if stock_data['name'] not in self.setup_trades:
                if (self.portfolio.available_capital >= 1000) and (stock_data['prediction'] == 1):
                    print('[{}] {} added to setup trades'.format(current_date,
                                                                       stock_data['name']))
                    
                    self.setup_trades[stock_data['name']] = Trade(symbol=stock_data['name'], 
                                                                  date=current_date, 
                                                                  price=stock_data['open'],
                                                                  atr=stock_data['avg_atr'])

                elif (self.portfolio.available_capital < 1000) and (stock_data['prediction'] == 0):
                    print('Skipped {} for {}'.format(str(current_date), stock_data['name']))

            # entry
            else:
                setup_trade = self.setup_trades[stock_data['name']]
                entry_signal = setup_trade.check_if_entry(price=stock_data['open'])

                if entry_signal == 1:
                    stocks_to_buy.append(stock_data['name'])
                    self.setup_trades.pop(stock_data['name'])

                elif entry_signal == 2:
                    pass # still wait
                elif entry_signal == 3:
                    print('[{}] False positive setup for {}.'.format(current_date, stock_data['name']),
                                'Removing from setup trades.')
                    self.setup_trades.pop(stock_data['name'])
        return stocks_to_buy

    def _stocks_purchase(self, stocks_to_buy=None, current_date=None):
        if len(stocks_to_buy) > 1:
            growths = [self.universe.relative_growth(symbol=symbol, date='2016-07-25', session_cnt=20)
                       for symbol in stocks_to_buy]

            # currently still buying just one stock...
            stocks_to_buy = [stocks_to_buy[growths.index(max(growths))]]    

        for symbol in stocks_to_buy:
            stock_data = self.universe.day_stock_data(symbol=symbol, date=current_date)
            trade = Trade(symbol=stock_data['name'], date=current_date, 
                                  price=stock_data['open'], atr=stock_data['avg_atr'])
            trade.buy(capital=self.portfolio.available_capital)

            # as I'm not doing anything with trade later at acquiring day,
            # purchase (open) price can be used for stop, even if for stop calculation
            # atr is used (which uses given session high and low price, 'see-future error')
            trade.set_init_stop(init_stop_const=self.init_stop_const, avg_atr=stock_data['avg_atr'])

            if trade.shares_count == 0:
                # no capital to buy stock
                continue

            self.portfolio.available_capital = round(self.portfolio.available_capital - \
                                          (trade.shares_count*trade.initial_price) - trade.buy_fee, 2)
            
            self.portfolio.trades[trade.trade_id] = trade

            print('[{}] Bought {} units of {}.'.format(current_date, trade.shares_count, trade.stock_symbol))
            print('Price:{}, fee:{}, init stop: {}'.format(trade.initial_price,trade.buy_fee,trade.trailing_stop))
            print('Capital left: {}'.format(self.portfolio.available_capital))

    def _look_after_trades(self, current_date=None):
        value_in_stocks = 0
        self.trades_to_remove = []
        for trade in self.portfolio.trades.values():
            stock_data = self.universe.day_stock_data(symbol=trade.stock_symbol, date=current_date)
            
            if isinstance(stock_data, type(None)):
                # sth wrong with data for given day
                closest_price = self.universe.closest_past_price(symbol=trade.stock_symbol,
                                                                 date=current_date,
                                                                 label='close')
                value_in_stocks += trade.get_value(closest_price)
                print('Day closed at: ', closest_price)
                continue

            # skip for trades acquired at current date
            if trade.start_date == current_date:
                value_in_stocks += trade.get_value(stock_data['close'])
                print('Day closed at: ', stock_data['close'])
                continue

            if stock_data['close'] <= trade.trailing_stop:
                # exit was triggered becaouse of trailing stop
                print('[{}] Trailing stop triggered with price: '.format(current_date), 
                            stock_data['close'])
                print('[{}] Will sell all shares of {}'.format(current_date, trade.stock_symbol))
                print('Bought for {}, sell for {}'.format(trade.initial_price, stock_data['close']))
                self._sell(trade=trade, price=stock_data['close'])
                continue

            trade.update_trailing_stop(price=stock_data['close'], avg_atr=stock_data['avg_atr'], stop_const=6.4)
            trade.session_days += 1

            # increase equity for close price
            print('[{}] {} closed at: {}'.format(current_date, trade.stock_symbol, stock_data['close']))
            value_in_stocks += trade.get_value(stock_data['close'])

        for trade_id in self.trades_to_remove:
            self.portfolio.trades.pop(trade_id)

        return value_in_stocks

    def _sell(self, trade=None, price=None):
        profit = trade.sell(price=price)
        print('Profit is: ', profit)
        self.portfolio.available_capital = self.portfolio.available_capital + profit
        print('Capital after sell : {}'.format(self.portfolio.available_capital))
        self.trades_to_remove.append(trade.trade_id)

if __name__ == '__main__':
    s = Strategy()
    #s.run(development_days=360)
    s.run()
    s.show_graph()

# stop const 6.4
# 96413.77
# 38651.46000000001
# 47950.00000000001
# 74891.17000000001
# 74891.17000000001
# 74891.17
# 38743.52
# 38743.520000000004
# 49446.26 # after choosing between stock phase1
# 49446.26

# stop 10.0
# 67285.8
# 67285.8
# 67285.8
# 67285.8
# 81434.56999999999 # after choosing between stock phase1

"""
TODOS:

OK - ustabilizowanie wyboru kupna spolki jezeli kilka jednego dnia
- position sizing albo cos w tym stylu?

"""