# built-in
import datetime
import json
import time
import xml.etree.ElementTree as element_tree

# 3rd party
import requests

# custom
import useragents

class Collector():

    def __init__(self):
        headers = { 'User-Agent': useragents.random_useragent(),
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Connection':'keep-alive',
            'Accept-Encoding':'gzip, deflate, sdch'
            }
        self.headers = headers
        self.companies = self._get_companies()
        self.col_names = ['name', 'timestamp', 'open', 'high', 'close', 'low', 'volume']

    def get_historical_data(self, full_name,  from_date=None, to_date=None):
        """
            Returns historical data for stock with *full_name* and for 
            *from_date* and *to_date*. Both dates should be string with 
            format: %Y-%m-%d. If dates not specified, from 1990-01-01 'till
            now will be given.

            Returns list of lists with records, where:
            [name, timestamp, open, high, close, low, volume]               
        """

        if not from_date:
            from_date = '1990-01-01'
        if not to_date:
            to_date = datetime.date.today().strftime('%Y-%m-%d')
        
        #todo(stulski): validate input date
        from_time_obj = datetime.datetime.strptime(from_date, '%Y-%m-%d')
        to_time_obj = datetime.datetime.strptime(to_date, '%Y-%m-%d')
        
        url = 'http://www.parkiet.com/WarsetServer/XMLServer'
        params = {'ext':1,
                  'entityId': self.companies[full_name]['id'],
                  'type':'zamk_dates',
                  'start': '{},{},{}'.format(from_time_obj.year, from_time_obj.month, from_time_obj.day),
                  'stop': '{},{},{}'.format(to_time_obj.year, to_time_obj.month, to_time_obj.day)}
        #print(params)

        r = requests.get(url, params=params, headers=self.headers)
        data = self._parse_historical_data(r.text)
        return data

    def _get_companies(self):
        url = 'http://www.parkiet.com/QuotationProvider/ajax/getQuotations'

        params = {'dataSource':'warset',
                  'tableType':'FULL',
                  'group':'AKCJE_CIAGLE',
                  '_':str(int(time.time())*1000)}

        r = requests.get(url, params=params, headers=self.headers)
        companies = self._parse_companies_names(r.text)
        return companies

    def _parse_companies_names(self, raw_companies):
        json_data = json.loads(raw_companies)
        companies = {}
        for entry in json_data['items']:
            companies[entry['fullName']] = {'shortName':entry['shortName'], 'id':entry['entityId']}
        return companies

    def _parse_historical_data(data, raw_xml):
        xml_ = element_tree.fromstring(raw_xml)
        
        data = []
        plot = xml_.findall('plot')[0]
        name = plot.get('assetName').strip()
        for node in plot.findall('data'):
            row = [name, 
                   node.get('timestamp').strip()[:-3],
                   float(node.get('open')),
                   float(node.get('high')),
                   float(node.text),
                   float(node.get('low')),
                   int(node.get('wolumen'))]
            data.append(row)
        return data
