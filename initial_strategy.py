# built-in
import datetime

# 3rd party
from sklearn.tree import DecisionTreeClassifier
import matplotlib.pyplot as plt
import pandas as pd

# custom
from stock_predictor import StockPredictor
from features_calculator import FeaturesCalculator

class Portfolio():
    def __init__(self, available_capital=None):
        self.available_capital = available_capital
        self.value_in_stocks = 0
        self.trades = {}

    def trades_count(self):
        return len(self.trades)


class Trade():
    def __init__(self, symbol=None, date=None, price=None, atr=None, fee_perc=0.0038, min_fee_perc=5):
        self.stock_symbol = symbol
        self.trade_id = '{} {}'.format(symbol, date)
        self.initial_price = price
        
        self.fee_perc = fee_perc
        self.min_fee_perc = min_fee_perc

        self.start_date = date
        self.session_days = 0

        # signals
        self.setup = 0
        self.setup_days = 0
        self.atr_at_setup = atr

    def check_if_entry(self, price=None, atr=None, signal_const=3, signal_lifetime=7):
        """
        returns: 1 - entry signal
                 2 - still waiting for confirmation of setup
                 3 - false setup
        """
        self.setup_days += 1
        resistence_level = self.initial_price + (self.atr_at_setup * signal_const)

        if price >= resistence_level:
            return 1

        elif (price < resistence_level) and (self.setup_days < signal_lifetime):
            return 2

        elif (price < resistence_level) and (self.setup_days >= signal_lifetime):
            return 3

    def buy(self, capital=None):
        self.shares_count = self._get_shares_count(capital=capital)
        self.buy_fee = round(self._calculate_fee(price=self.initial_price), 2)

    def set_init_stop(self, avg_atr=None, init_stop_const=3.4):
        self.trailing_stop = round(self.initial_price - (avg_atr*init_stop_const), 2)

    def update_trailing_stop(self, price=None, avg_atr=None, stop_const=3.4):
        stop_value = round(price - (avg_atr*stop_const), 2)
        if stop_value > self.trailing_stop:
            self.trailing_stop = stop_value

    def sell(self, price=None):
        self.sell_fee = self._calculate_fee(price)
        self.profit = (self.shares_count * price) - self.sell_fee
        return self.profit

    def get_value(self, price):
        return self.shares_count * price

    def _get_shares_count(self, capital=None):
        return int(capital // (self.initial_price +  self.initial_price * self.fee_perc))

    def _calculate_fee(self, price=None):
        fee = self.shares_count * price * self.fee_perc
        if fee < self.min_fee_perc:
            fee = self.min_fee_perc
        return fee


class Strategy():
    def __init__(self, available_capital=20000, init_stop_const=3.4):
        self.prediction_day = 14 # prediction is made for that given day
        self.init_stop_const=init_stop_const
        self.results = []# temporary solution for storing results. will change that with reporting object
        self.ref_results = []

        self.data = self._prepare_data('LPP')
        self.portfolio = Portfolio(available_capital=available_capital)

        self.setup_trades = {}

    def run(self, development_days=None):
        dev_counter = 0

        for index, day in self.data.iterrows():
            if development_days and (dev_counter >= development_days):
                break

            if dev_counter == 0:
                ref_shares_count = (self.portfolio.available_capital // day['close']) + 1

            current_date = index.date()

            self._open_session(day_data=day, current_date=current_date)
            value_in_stocks = self._look_after_trades(day_data=day, current_date=current_date)

            print('Money in stocks is: {}'.format(value_in_stocks))

            total_profit = value_in_stocks + self.portfolio.available_capital
            print('Total profit is: {}'.format(total_profit))
            self.results.append(total_profit)

            self.ref_results.append(ref_shares_count * day['close'])

            dev_counter += 1

    def show_graph(self):
        self.data['strategy'] = self.results
        self.data['ref_strategy'] = self.ref_results

        self.data['pred'] = self.data['prediction'] * 10000
        
        self.data[['strategy', 'ref_strategy', 'pred']].plot()
        plt.show()

    def _prepare_data(self, symbol):
        dtree = DecisionTreeClassifier(max_leaf_nodes=10, max_depth=9, random_state=0)

        features= [{'name':'sma', 'label':'close', 'days':24},
                   {'name':'sma', 'label':'close', 'days':50},
                   {'name':'dm', 'days':7, 'plus_dm':True, 'minus_dm': True},
                   {'name':'adx', 'days':7},
                   {'name':'pe'},
                   {'name':'obv'},
                   {'name':'x_days_ago', 'label':'close', 'days':20}]

        predictor = StockPredictor(symbol=symbol, features=features)
        predictor.apply_model(model=dtree, prediction_day=self.prediction_day)

        data = predictor.data.dropna(axis=0, how='any')[predictor.split_index:]

        # prepare initial stop data - calculate average atr mulitplied by constant
        calculator = FeaturesCalculator()
        data['atr'] = calculator.atr(predictor.data, 'close', 'high', 'low', 10)
        df_sma_atr = calculator.sma(data, 'atr', 10)
        data['avg_atr'] = df_sma_atr['sma10']

        data['prediction'] = predictor.prediction
        for feature in predictor.features_list:
            data.drop(feature, axis=1, inplace=True)
        data.drop('label', axis=1, inplace=True)

        # drop NA which appeard after atr stop related calculations
        data.dropna(axis=0, how='any', inplace=True)

        return data

    def _open_session(self, day_data=None, current_date=None):
        # setup
        if day_data['name'] not in self.setup_trades:
            # currently buying if no stocks there. that will change in the future
            if (self.portfolio.trades_count() == 0) and (day_data['prediction'] == 1):

                # ugly fix for open pirice == 0 (TODO: do it proper way...)
                if day_data['open'] <= 0.0:
                    day_data['open'] = day_data['close']

                self.setup_trades[day_data['name']] = Trade(symbol=day_data['name'], 
                                                            date=current_date, 
                                                            price=day_data['open'],
                                                            atr=day_data['avg_atr'])

            elif (self.portfolio.trades_count() == 0) and (day_data['prediction'] == 0):
                print('Skipped {}'.format(str(current_date)))
        
        # entry
        else:
            trade = self.setup_trades[day_data['name']]
            entry_signal = trade.check_if_entry(price=day_data['open'])

            if entry_signal == 1:
                # ERROR HERE!!!! YOU'RE BUYING USING OUTDATED SETUP PRICE!
                trade.buy(capital=self.portfolio.available_capital)

                # as I'm not doing anything with trade later at acquiring day,
                # purchase (open) price can be used for stop, even if for stop calculation
                # atr is used (which uses given session high and low price, 'see-future error')
                trade.set_init_stop(init_stop_const=self.init_stop_const, avg_atr=day_data['avg_atr'])

                self.portfolio.available_capital = self.portfolio.available_capital - \
                                              (trade.shares_count*trade.initial_price) - trade.buy_fee
                
                self.portfolio.trades[trade.trade_id] = trade

                print('Bought {} units for {}. Fee was: {}. Init stop: {}'.format(trade.shares_count, 
                                                                                  trade.initial_price,
                                                                                  trade.buy_fee,
                                                                                  trade.trailing_stop))
                print('Capital left: {}'.format(self.portfolio.available_capital))
                self.setup_trades.pop(day_data['name']) # no longer setup. you have it

            elif entry_signal == 2:
                pass # still wait
            elif entry_signal == 3:
                self.setup_trades.pop(day_data['name']) # it was false setup, remove

    def _look_after_trades(self, day_data=None, current_date=None):
        value_in_stocks = 0
        self.trades_to_remove = []
        for trade in self.portfolio.trades.values():

            # skip trades acquired at current date
            if trade.start_date == current_date:
                value_in_stocks += trade.get_value(day_data['close'])
                print('Day closed at: ', day_data['close'])
                continue

            if day_data['close'] <= trade.trailing_stop:
                # exit was triggered becaouse of trailing stop
                print('Trailing stop triggered with price: ', day_data['close'])
                self._sell(trade=trade, price=day_data['close'])
                continue

            trade.update_trailing_stop(price=day_data['close'], avg_atr=day_data['avg_atr'], stop_const=3.4)
            trade.session_days += 1

            # increase equity for close price
            print('Day closed at: ', day_data['close'])
            value_in_stocks += trade.get_value(day_data['close'])

        for trade_id in self.trades_to_remove:
            self.portfolio.trades.pop(trade_id)

        return value_in_stocks

    def _sell(self, trade=None, price=None):
        profit = trade.sell(price=price)
        print('Will sell all shares of {}. Profit is: {}'.format(trade.trade_id, profit))
        self.portfolio.available_capital = self.portfolio.available_capital + profit
        print('Capital after sell : {}'.format(self.portfolio.available_capital))
        self.trades_to_remove.append(trade.trade_id)
        

def main():
    s = Strategy()
    s.run()
    s.show_graph()

if __name__ == '__main__':
    main()
