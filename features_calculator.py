# built in
import re

# 3rd party
import numpy as np
import pandas as pd

class FeaturesCalculator():

    def map_features(self, org_df, features):
        df = org_df.copy()
        features_column_names = []
        if features == None:
            return 

        for feature in features:
            if feature['name'] == 'x_days_ago':
                features_df = self.x_days_ago(df, feature['label'], feature['days'])
            
            elif feature['name'] == 'rsi':
                features_df = self.rsi(df, feature['days'])
            
            elif feature['name'] == 'sma':
                features_df = self.sma(df, feature['label'], feature['days'])
            
            elif feature['name'] == 'dm':
                features_df = self.dm(df, 'high', 'low', plus_dm=feature['plus_dm'], 
                                      minus_dm=feature['minus_dm'], days=feature['days'])
            
            elif feature['name'] == 'atr':
                features_df = self.atr(df, 'close', 'high', 'low', feature['days'])

            elif feature['name'] == 'adx':
                features_df = self.adx(df, 'close', 'high', 'low', feature['days'])

            elif feature['name'] == 'pe':
                features_df = self.pe_ratio(df, 'EPS', 'close')

            elif feature['name'] == 'pb':
                features_df = self.pb_ratio(df, 'BVPS', 'close')

            elif feature['name'] == 'obv':
                features_df = self.obv(df, 'volume', 'close')

            elif feature['name'] == 'diff':
                features_df = self.feature_diff(df, feature['label_name'], feature['label1'], feature['label2'])
            
            else:
                features_column_names.append(feature['name'])
                continue

            features_column_names.extend(list(features_df.columns.values))
            df = pd.concat([df, features_df], axis=1)

        return df, features_column_names

    def x_days_ago(self, df, label, days):
        previous_days_df = pd.DataFrame()
        for day in reversed(range(1,days+1)):
            previous_days_df['{}_{}day_ago'.format(label, day)] = df[label].shift(day)
        return previous_days_df

    def sma(self, df, label, days):
        # Simple Moving Average
        previous_days = self.x_days_ago(df, label, days)
        return pd.DataFrame({self._sma_label(days): previous_days.mean(axis=1, skipna=False)})

    def rsi(self, df, days=14):
        # Relative Strength Index
        df_copy = df.copy()
        df_copy['change'] = df_copy['close'] - df_copy['close'].shift(1)
        
        df_copy['gain'] = df_copy['change'][df_copy['change']>0]
        df_copy = pd.concat([df_copy, self.x_days_ago(df_copy, 'gain', days-1)], axis=1)
        
        df_copy['loss'] = df_copy['change'][df_copy['change']<0].abs()
        df_copy = pd.concat([df_copy, self.x_days_ago(df_copy, 'loss', days-1)], axis=1)
        
        df_copy['avg_gain'] = df_copy[['gain']+self._x_ago_labels(df_copy, 'gain')].sum(axis=1)/days
        df_copy['avg_loss'] = df_copy[['loss']+self._x_ago_labels(df_copy, 'loss')].sum(axis=1)/days

        df_copy['RS'] = df_copy['avg_gain']/df_copy['avg_loss']
        df_copy.loc[:days, 'RS'] = np.nan
        df_copy['RSI'] = 100 - (100//(1 + df_copy['RS']))

        return df_copy['RSI'].to_frame()

    def dm(self, df, high_label, low_label, days=None, plus_dm=True, minus_dm=True):
        # Directional Movements (+DM and -DM)
        df_copy = df.copy()
        
        df_copy['previous_high'] = df_copy[high_label].shift(1)
        df_copy['previous_low'] = df_copy[low_label].shift(1)
        df_copy['high_diff'] = (df_copy[high_label] - df_copy['previous_high'])
        df_copy['low_diff'] = (df_copy['previous_low'] - df_copy[low_label])

        # calculate +DM
        df_copy['plus_dm'] = df_copy['high_diff']
        df_copy['plus_dm'].where(df_copy['high_diff'] > df_copy['low_diff'], other=0, inplace=True)
        df_copy['plus_dm'].where(df_copy['plus_dm'] > 0, other=0, inplace=True)

        # calculate -DM
        df_copy['minus_dm'] = df_copy['low_diff']
        df_copy['minus_dm'].where(df_copy['low_diff'] > df_copy['high_diff'], other=0, inplace=True)
        df_copy['minus_dm'].where(df_copy['minus_dm'] > 0, other=0, inplace=True)

        # if more days (averaging)
        if days != None:
            pdm_label ='plus_dm{}'.format(days)
            df_copy[pdm_label] = self.sma(df_copy, 'plus_dm', days)[self._sma_label(days)]

            mdm_label = 'minus_dm{}'.format(days)
            df_copy[mdm_label] = self.sma(df_copy, 'minus_dm', days)[self._sma_label(days)]
        else:
            pdm_label, mdm_label = 'plus_dm', 'minus_dm'

        if plus_dm==True and minus_dm==True:
            return df_copy[[pdm_label, mdm_label]]
        elif plus_dm==True and minus_dm==False:
            return df_copy[pdm_label].to_frame()
        elif plus_dm==False and minus_dm==True:
            return df_copy[mdm_label].to_frame()

    def atr(self, df, close_label, high_label, low_label, days):
        # Average True Range
        df_copy = df.copy()
        df_copy['previous_close'] = df_copy[close_label].shift(1)
        df_copy['currentHigh_currentLow'] = df_copy[high_label] - df_copy[low_label]
        df_copy['currentHigh_previousClose'] = df_copy[high_label] - df_copy['previous_close']
        df_copy['currentLow_previousClose'] = df_copy[low_label] - df_copy['previous_close']

        df_copy['TR'] = df_copy[['currentHigh_currentLow',
                                 'currentHigh_previousClose',
                                 'currentLow_previousClose']].max(axis=1)

        df_copy['ATR'] = self.sma(df_copy, 'TR', days)[self._sma_label(days)]
        return df_copy['ATR'].to_frame()

    def adx(self, df, close_label, high_label, low_label, days):
        # Average Directional Index
        df_copy = df.copy()
        
        df_copy['ATR'] = self.atr(df_copy, close_label, high_label, low_label, days)

        # check if dms are present in df and act based on that
        if 'plus_dm{}'.format(days) in df_copy and not 'minus_dm{}'.format(days) in df_copy:
            dms = self.dm(df_copy, high_label, low_label, days=days, plus_dm=False, minus_dm=True)
            df_copy = pd.concat([df_copy, dms], axis=1)
        elif 'plus_dm{}'.format(days) not in df_copy and 'minus_dm{}'.format(days) in df_copy:
            dms = self.dm(df_copy, high_label, low_label, days=days, plus_dm=True, minus_dm=False)
            df_copy = pd.concat([df_copy, dms], axis=1)
        elif 'plus_dm{}'.format(days) not in df_copy and not 'minus_dm{}'.format(days) in df_copy:
            dms = self.dm(df_copy, high_label, low_label, days=days)
            df_copy = pd.concat([df_copy, dms], axis=1)

        df_copy['plus_DI'] = 100*(df_copy['plus_dm{}'.format(days)]/df_copy['ATR'])
        df_copy['minus_DI'] = 100*(df_copy['minus_dm{}'.format(days)]/df_copy['ATR'])

        df_copy['diff_DI'] = (df_copy['plus_DI'] - df_copy['minus_DI']).abs()
        df_copy['sum_DI'] = df_copy['plus_DI'] + df_copy['minus_DI']

        df_copy['DX'] = 100*(df_copy['diff_DI']/df_copy['sum_DI'])
        df_copy['ADX'] = self.sma(df_copy, 'DX', days)[self._sma_label(days)]

        return df_copy['ADX'].to_frame()

    def pe_ratio(self, df, eps_label, close_label):
        # Price to earnings ratio (P/E Ratio)
        df_copy = df.copy()
        df_copy['P/E'] = df_copy[eps_label]/df_copy[close_label]
        return df_copy['P/E'].to_frame()

    def pb_ratio(self, df, bvps_label, close_label):
        # Price-To-Book Ratio (P/B Ratio)
        df_copy = df.copy()
        df_copy['P/B'] = df_copy[bvps_label]/df_copy[close_label]
        return df_copy['P/B'].to_frame()

    def obv(self, df, volume_label, close_label):
        # On Balance Volume
        df_copy = df.copy()
        df_copy['previous_close'] = df_copy[close_label].shift(1)
        df_copy['sign'] = np.sign(np.array(df_copy[close_label] - df_copy['previous_close']))
        df_copy['previous_volume'] = df_copy[volume_label].shift(1)
        df_copy['multiplied_vol'] = df_copy['sign'] * df_copy['previous_volume']
        df_copy['OBV'] = df_copy['multiplied_vol'].cumsum()

        return df_copy['OBV'].to_frame()

    def feature_diff(self, df, name, label1, label2):
        df_copy = df.copy()
        df_copy[name] = df_copy[label2] - df_copy[label1]
        return df_copy[name].to_frame()

    def _x_ago_labels(self, df, label):
        return [col for col in df.columns if re.match(r'{}_\d+day_ago'.format(label), col)]

    def _sma_label(self, days):
        return 'sma{}'.format(days)

if __name__ == '__main__':
    import collector
    symbol = 'CCC'
    c = collector.Collector()
    data = np.array(c.get_historical_data(symbol))
    df = pd.DataFrame(data, columns=c.col_names)
    df.set_index(pd.DatetimeIndex(df['timestamp']), inplace=True)
    df.drop('timestamp', axis=1, inplace=True)
    for col in df.columns:
        if col not in ('name',):
            df[col] = df[col].astype('float64')


    fc = FeaturesCalculator()

    dm1 = fc.obv(df, 'volume', 'close')
    print(dm1.head(500))

