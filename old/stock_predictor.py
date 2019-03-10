# 3rd party
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score
from sklearn import preprocessing

# custom
#import collector
import collect_prices
import features_calculator
import fundamentals_processor

class StockPredictor():
    """
    *start_date* and *end_data* should be in YYYY-MM-DD format.
    *features* is list of dictionaries with features and relevant parameters, for
    example: [{'name':'x_days_ago', 'label':'close', 'days':2},
              {'name':'rsi', 'days':7},
              {'name':'sma', 'label':'close', 'days':2}]
    *fundamentals* includes fundamental data in self.data
    """

    def __init__(self, symbol=None, start_date=None, end_date=None, features=None, fundamentals=True):
        # sets data
        self._get_data(symbol, start_date=None, end_date=None, fundamentals=fundamentals)

        # add features to data and sets features_list
        if features != None:
            self._get_features(features)

            # scale features
            # for feature in self.features_list:
            #     self.data[feature] = preprocessing.scale(np.array(self.data[feature]))
                #print(preprocessing.scale(np.array(self.data[feature])))
        
    def apply_model(self, split=50, model=None, prediction_day=1, prediction_price='close'):
        """
        *prediction_day* is integer indicating how many days in future prediction will be
        *prediction_price* which column will be predicted (default: close)
        *split* is integer <0;100> indicating what percentage of data train set will be
        *model* is scikit learn model obect, e.g. LogisticRegression(C=1e3, max_iter=20)
        """
        # add prediction labels to data
        self._prepare_labels(prediction_day, prediction_price)

        # sets X_train, Y_train, X_test, Y_test
        self._split_data(split)

        # fits, train and score model (sets model, train_score, test_score)
        self._fit_model(model)

    def _get_data(self, symbol, start_date=None, end_date=None, fundamentals=True):
        # get pricing data
        pm = collect_prices.PricingDataManager()
        self.data = pm.load_stock(symbol)

        # get fundamentals
        if not fundamentals == True:
            return            
        
        fp = fundamentals_processor.FundamentalsProcessor(symbol)
        for label in ('EPS', 'BVPS', 'net_profit'):
            self.data[label] = fp.merge_data(self.data, label)

    def _get_features(self, features):
        calculator = features_calculator.FeaturesCalculator()
        self.data, self.features_list = calculator.map_features(self.data, features)
        self.data.dropna(axis=0, how='any', inplace=True)

    def _prepare_labels(self, prediction_day, prediction_price):
        future_prices = self.data[prediction_price].shift(-prediction_day)
        diff = future_prices - self.data[prediction_price]
        self.data['label'] = np.where(diff>=0, 1, -1)
        # changes last days labels into nan's (as you cant see the future...)
        self.data.loc[-prediction_day:, 'label'] = np.nan

    def _split_data(self, split):
        data = self.data.dropna(axis=0, how='any')
        length = data.shape[0]
        self.split_index = int(length * (split/100))

        self.X_train = np.array(data[:self.split_index][self.features_list])
        self.Y_train = np.array(data[:self.split_index]['label'])

        self.X_test = np.array(data[self.split_index:][self.features_list])
        self.Y_test = np.array(data[self.split_index:]['label'])

    def _fit_model(self, model):
        self.model = model
        self.model.fit(self.X_train, self.Y_train)

        # training set
        y_train = self.model.predict(self.X_train)
        self.train_score = round(accuracy_score(self.Y_train, y_train), 3)

        # test set
        self.prediction = self.model.predict(self.X_test)
        self.test_score = round(accuracy_score(self.Y_test, self.prediction), 3)

def main():
    from sklearn.tree import DecisionTreeClassifier

    features= [{'name':'sma', 'label':'close', 'days':24},
               {'name':'sma', 'label':'close', 'days':50},
               {'name':'dm', 'days':7, 'plus_dm':True, 'minus_dm': True},
               {'name':'adx', 'days':7},
               {'name':'pe'},
               {'name':'obv'},
               {'name':'x_days_ago', 'label':'close', 'days':20}]

    dtree = DecisionTreeClassifier(max_leaf_nodes=10, max_depth=9, random_state=0)

    stocks = ['ALIOR','ASSECOPOL','BZWBK','CCC','CYFRPLSAT','ENEA',
              'ENERGA','EUROCASH','KGHM','LOTOS','LPP','MBANK',
              'ORANGEPL','PEKAO','PGE','PGNIG','PKNORLEN','PKOBP','PZU','TAURONPE']
    
    pred_days = 14

    scores = []
    for stock in stocks:
        print('\n'+stock)
        p = StockPredictor(symbol=stock, features=features)
        p.apply_model(model=dtree, prediction_day=pred_days)

        print('Test score is: ', p.test_score)
        scores.append(p.test_score)

    print('\n###')
    arr = np.array(scores)
    print('mean score is: ', np.mean(arr))
    print('std dev is: ', np.std(arr))

    critical_value = 1.7291 # t-student for .95 and 19 degrees of freedom
    H_0 = 0.5
    test_value = ((arr.mean() - H_0) / (arr.std())*(len(arr)-1)**0.5)
    print('Test statistic value is: ', test_value)
    print('critical region is: ({}; inf)'.format(critical_value))
    

if __name__ == '__main__':
    main()
