# bankier.pl fundamentals scraper

# built in
import random
import re
import time

#3rd party
from bs4 import BeautifulSoup as bs
import pandas as pd
import requests

# custom
import useragents

class BankierFundamentals():

    def __init__(self):
        self.headers = {'User-Agent': useragents.random_useragent(),
                        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Connection':'keep-alive',
                        'Accept-Encoding':'gzip, deflate, sdch'}
        self.base_url = 'http://www.bankier.pl/gielda/notowania/akcje'
        self.symbols = self._stocks_symbols()

    def get_fundamentals(self, symbol):
        htmls = self._get_fundametnal_pagination(symbol)

        df = pd.DataFrame()
        for html in htmls:
            print('Getting fundamentals from: ', html)
            bs_obj = self._get_bs_obj(html)
            table = bs_obj.findAll('div', {'class': 'boxContent boxTable'})[0].findAll('table')[0]
            
            cols = [col.text for col in table.findAll('thead')[0].findAll('strong')]
            cols.insert(0, 'index') # placeholder, so cols nad rows have same lenght
            
            rows = []
            for row in table.findAll('tr'):
                entires = row.findAll('td')
                if entires == []:
                    continue

                vals = [entry.text.replace('Å\x82', 'l').replace('\n', '')\
                                  .replace('Å\x84', 'n').replace('\t', '')\
                                  .replace('Ä\x85', 'a').replace('Ä\x99', 'e')\
                                  .replace('Å\x9bÄ\x87', 'sc').replace('\xa0', '')\
                                  .replace('*', '').strip() for entry in entires]
                rows.append(vals)

            temp_df = pd.DataFrame(rows, columns=cols)
            temp_df.set_index('index', drop=True, inplace=True)

            df = pd.concat([df, temp_df], axis=1)

            time.sleep(random.randint(1,4))

        return df

    def _get_fundametnal_pagination(self, symbol):
        url = self.base_url + '/{}/wyniki-finansowe/skonsolidowany/kwartalny/standardowy/'.format(symbol)
        bs_obj = self._get_bs_obj(url + str(1))

        pages = bs_obj.findAll('a', {'class': 'numeral btn '})
        return [url + str(page_num) for page_num in range(1, max([int(page.text) for page in pages])+1)]

    def _stocks_symbols(self):
        bs_obj = self._get_bs_obj(self.base_url)

        pattern = re.compile(r'/inwestowanie/profile/quote\.html\?symbol=(.*)')
        links = bs_obj.findAll('a', {'href': pattern})

        sybols_list = [re.match(pattern, link).group(1) for link in [link['href'] for link in links]]
        return list(set(symbols_list))

    def _get_bs_obj(self, html_str):
        html = requests.get(html_str, headers=self.headers).text
        return bs(html, 'html.parser')

def save_data(bankier_obj, symbol):
    df = bankier_obj.get_fundamentals(symbol)
    df.to_csv('{}_fundamentals.csv'.format(symbol))

def main():
    bankier = BankierFundamentals()

    for symbol in bankier.symbols:
        try:
            save_data(bankier, symbol)
        except ValueError:
            time.sleep(2)
            try:
                save_data(bankier, symbol)
            except ValueError:
                continue


if __name__ == '__main__':
    main()
