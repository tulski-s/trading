# built-in
import datetime
from collections import OrderedDict
import json
import time
import xml.etree.ElementTree as element_tree

# 3rd party
import requests
import urllib
from selenium import webdriver

# custom
import useragents

class PriceCollector():

    def __init__(self):
        self.headers = {
            'User-Agent': useragents.random_useragent(),
            'Accept':'application/json, text/plain, */*',
            'Connection':'keep-alive',
            'Accept-Encoding':'gzip, deflate, sdch',
        }

    def get_historical_data(self, symbol, from_date=None, to_date=None):
        """
            Returns historical data for symbol between *from_date* and *to_date*. 
            Both dates should be string with  format: %Y-%m-%d. If dates not specified, 
            from 1990-01-01 untillnow will be given.

            Returns OrderedDict date as key and [open, high, close, low, volume] as value.
        """
        params = {
            'symbol': symbol,
            'intraday': 'false',
            'type': 'candlestick'
        }
        if not from_date and not to_date:
            params.update({'max_period': 'true'})
        else:
            if not from_date:
                from_date = self._date_to_ts('1990-01-01') 
            if not to_date:
                to_date = self._date_to_ts(datetime.date.today().strftime('%Y-%m-%d'))
            params.update({
                'date_from': self._date_to_ts(from_date),
                'date_to': self._date_to_ts(to_date)
            })
        url = 'https://www.bankier.pl/new-charts/get-data'
        r = requests.get(url, params=params)
        r_json = json.loads(r.text)
        # ts: [open, max, min, close]
        prices = OrderedDict(
            (self._ts_to_date(row[0]), [float(row[1]), float(row[2]), float(row[3]), float(row[4])]) for row in r_json['main']
        )
        # append volume
        for row in r_json['volume']:
            prices[self._ts_to_date(row[0])].append(int(row[1]))
        return prices

    def get_stocks_symbols(self):
        """
        Returns dictionary with stocks symbols as a key and dict wtih some additional information as value
        """
        r = requests.get('https://www.parkiet.com/data/stock.json')
        return {
            company['short_name']: {
                'full_name': company['short_name'],
                'isin': company['isin'],
                'description': urllib.parse.unquote(company['activity']), #.decode('utf8') ,
                'sector': company['sector_name']
            }
            for company in json.loads(r.text)
        }

    def get_indicies_symbols(self):
        """
        Returns dictionary with indicies symbols as a key and ISIN number as value
        """
        return {
            'WIG-NRCHOM': 'PL9999999706',
            'WIG': 'PL9999999995',
            'WIG-INFO': 'PL9999999771',
            'WIG-CEE': 'PL9999999433',
            'WIG30TR': 'PL9999999367',
            'WIG-BANKI': 'PL9999999904',
            'mWIG40': 'PL9999999912',
            'WIG-SPOZYW': 'PL9999999888',
            'WIG-CHEMIA': 'PL9999999847',
            'WIG-PALIWA': 'PL9999999722',
            'WIG-Poland': 'PL9999999599',
            'WIG20lev': 'PL9999999532',
            'WIG20short': 'PL9999999524',
            'WIG-ENERG': 'PL9999999516',
            'WIG-Ukrain': 'PL9999999458',
            'WIG20TR': 'PL9999999425',
            'WIG-LEKI': 'PL9999999250',
            'WIG-MOTO': 'PL9999999243',
            'sWIG80TR': 'PL9999999060',
            'WIG-MEDIA': 'PL9999999755',
            'InvestorMS': 'PL9999999672',
            'WIG-BUDOW': 'PL9999999896',
            'WIG-TELKOM': 'PL9999999870',
            'WIG20': 'PL9999999987',
            'RESPECT': 'PL9999999540',
            'mWIG40TR': 'PL9999999078',
            'WIG30': 'PL9999999375',
            'WIG-ODZIEZ': 'PL9999999268',
            'NCIndex': 'PL9999999565',
            'WIGdiv': 'PL9999999482',
            'WIG-GORNIC': 'PL9999999466',
            'sWIG80': 'PL9999999979'
        }

    def get_etfs_symbols(self):
        """
        Returns dictionary with ETFs symbols as a key and ISIN number as value
        """
        return {
            'ETFDAX': 'LU0252633754',
            'ETFSP500': 'LU0496786574',
            'ETFW20L': 'LU0459113907'
        }

    def _date_to_ts(self, d):
        "Converts YYYY-MM-DD to unix timestamp"
        return int(time.mktime(datetime.datetime.strptime(d, '%Y-%m-%d').timetuple())*1000)

    def _ts_to_date(self, d):
        "Converts unix timestamp (multiplied by 1000 and as string) to datetime date obj with YYYY-MM-DD format"
        return datetime.datetime.utcfromtimestamp(int(d/1000)).strftime('%Y-%m-%d')


if __name__ == '__main__':
    collector = PriceCollector()
    symbols = list(collector.get_stocks_symbols().keys())

    collector.get_historical_data('PKNORLEN')
    collector.get_historical_data(symbols[10], from_date='2018-10-06', to_date='2018-10-12')
