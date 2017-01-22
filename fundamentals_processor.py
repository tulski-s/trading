# built in
import os

# 3rd party
import pandas as pd
import numpy as np

class FundamentalsProcessor():

    def __init__(self, symbol, data_path='fundamental_data'):
        self.data_path = data_path
        self._get_stored_data(symbol)
        self._clean_data()
        self._normalize_col_names()

    def merge_data(self, collector_df, label):
        df_copy = collector_df.copy()
        df_copy[label] = df_copy.index.map(lambda date_index: self._assign_value(date_index, label))
        df_copy[label].fillna(method='ffill', inplace=True)
        return df_copy[label].to_frame()

    def _get_stored_data(self, symbol):
        symbol_file = None
        for file_ in os.listdir(self.data_path):
            if file_.startswith(symbol.upper()) and file_.endswith('fundamentals.csv'):
                symbol_file = file_
                break
        if symbol_file == None:
            return

        df = pd.read_csv(os.path.join(self.data_path, symbol_file))
        df.set_index(df['index'], inplace=True)
        self.data = df

    def _order_cols(self, list_item):
        vals = [item.strip() for item in list_item.split('Q')]
        return vals[1], vals[0]

    def _clean_data(self):
        columns = list(self.data.columns.values)
        for name in [' Q 0000', 'index']:
            if name in columns:
                columns.remove(name)

        # find years to remove: less than 4 quaters (exept last year)
        years = sorted([item.split('Q')[1].strip() for item in columns])
        years_dedup = sorted(list(set([item.split('Q')[1].strip() for item in columns])))
        years_to_remove = []
        for year in years_dedup[:-1]:
            if years.count(year) < 4:
                years_to_remove.append(year)

        # append only valid years into new list and than swap columns variable 
        new_cols = []
        for column in columns:
            flag = 0
            for year in years_to_remove:
                if year in column:
                    flag = 1
            if flag == 0:
                new_cols.append(column)
        columns = new_cols

        # futher cleaning and filling missing values
        ordered_columns = sorted(columns, key=lambda x: self._order_cols(x))
        self.data = self.data[ordered_columns].transpose()
        self.data = self.data.drop('Raport zbadany przez audytora', 1)
        for column in self.data.columns.values:
            self.data[column] = self.data[column].str.replace(',', '.').apply(pd.to_numeric)
            self.data[column] = self.data[column].replace(to_replace=0, method='backfill')

    def _normalize_col_names(self):
        columns = list(self.data.columns)
        transaltor = {'Wartosc ksiegowa na akcje (zl)': 'BVPS',
                      'Zysk na akcje (zl)': 'EPS',
                      'Zysk na akcje (tys. zl)': 'EPS',
                      'Liczba akcji (tys. szt.)': 'shares_count',
                      'Liczba akcji (tys. zl)': 'shares_count',
                      'Kapital wlasny (tys. zl)': 'equity_capital',
                      'Aktywa (tys. zl)': 'assets',
                      'Amortyzacja (tys. zl)':'amortization',
                      'Zysk (strata) netto (tys. zl)': 'net_profit',
                      'Zysk (strata) brutto (tys. zl)': 'gross_profit',
                      'Zysk (strata) z dzialal. oper. (tys. zl)': 'profit_from_operations',
                      'Wynik na dzialalnoÅ\x9bci bankowej (tys. zl)': 'results_on_banking_activity',
                      'Przychody z tytulu prowizji (tys. zl)':'fee_and_commission_income',
                      'Przychody z tytulu odsetek (tys. zl)': 'interests_income',
                      'Skladka na udziale wlasnym (tys. zl)': 'premium_on_the_share_of_own',
                      'Przychody z lokat (tys. zl)': 'income_from_investments',
                      'Przychody netto ze sprzedaÅ¼y (tys. zl)': 'income_from_sale',
                      'Wynik techniczny ubezpieczen majatkowych i osobowych (tys. zl)': 'technical_result_of_property_and_casualty_insurance',
                      'EBITDA (tys. zl)': 'EBITDA'}
        for idx, col in enumerate(columns):
            columns[idx] = transaltor[col]
        self.data.columns = columns

    def _assign_value(self, date_index, label):
        index_date = str(date_index).split(' ')[0].split('-')
        index_month = int(index_date[1])
        index_year = int(index_date[0])

        try:
            if index_month >= 1 and index_month <= 3:
                return self.data.ix['I Q {}'.format(index_year)][label]
            elif index_month >= 4 and index_month <= 6:
                return self.data.ix['II Q {}'.format(index_year)][label]
            elif index_month >= 7 and index_month <= 9:
                return self.data.ix['III Q {}'.format(index_year)][label]
            elif index_month >= 10 and index_month <= 12:
                return self.data.ix['IV Q {}'.format(index_year)][label]
        except:
            return np.nan
