# built-in
import datetime

# 3rd party
from sklearn.tree import DecisionTreeClassifier

# custom
from stock_predictor import StockPredictor
from features_calculator import FeaturesCalculator

class StocksUniverse():
    def __init__(self, scope='WIG20'):
        self.symbols = self._get_symbols(scope)
        self.prediction_day = 14 # prediction is made for that given day
        
        # sets self.data
        self._prepare_data()

    def available_sessions(self):
        all_dates = []
        [all_dates.extend(self.data[stock].index.values) for stock in self.data]
        return sorted(list(set(all_dates)))

    def day_stock_data(self, symbol=None, date=None):
        try:
            stock_data = self.data[symbol].ix[date].T.squeeze()
            if not stock_data.empty == True:
                return stock_data
        except KeyError:
            return None

    def closest_past_price(self, symbol=None, date=None, label='close'):
        first_date = self.data[symbol].index[0].date()
        lookup_date = datetime.datetime.strptime(date, '%Y-%m-%d').date()

        if lookup_date < first_date:
            return None

        idx = 1
        while not first_date >= lookup_date:
            first_date = self.data[symbol].index[idx].date()
            idx +=1

        return self.data[symbol][label][self.data[symbol].index[idx-2]]

    def relative_growth(self, symbol=None, date=None, session_cnt=None):
        to_date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
        to_date_iloc = self.data[symbol].index.get_loc(date).start+1
        data_subpart = self.data[symbol].iloc[(to_date_iloc-session_cnt):to_date_iloc]
        return round((data_subpart['close'].iloc[-1]*100 / data_subpart['close'].iloc[0]) - 100, 2)

    def _get_symbols(self, scope):
        if scope == 'WIG20':
            return ['ALIOR','ASSECOPOL','BZWBK','CCC','CYFRPLSAT','ENEA',
                    'ENERGA','EUROCASH','KGHM','LOTOS','LPP','MBANK',
                    'ORANGEPL','PEKAO','PGE','PGNIG','PKNORLEN','PKOBP','PZU','TAURONPE']
        else:
            return scope

    def _prepare_data(self):
        print('Getting data and building/applying prediction models')
        self.data = {}
        for symbol in self.symbols:
            print('Preparing: ', symbol)
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

            self.data[symbol] = data


if __name__ == '__main__':
    symbol = 'LPP'
    universe = StocksUniverse(scope=[symbol])
    print(universe.relative_growth(symbol=symbol, date='2016-07-25', session_cnt=20))
    # price = universe.closest_past_price(symbol=symmbol, date='2009-07-25')
    # print(price)